package com.architecture.ordercommand.concurrency;

import com.architecture.ordercommand.domain.OrderEntity;
import com.architecture.ordercommand.dto.Dtos.BatchResult;
import com.architecture.ordercommand.dto.Dtos.CreateOrderCommand;
import com.architecture.ordercommand.service.OrderCommandService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * High-concurrency batch order creation:
 *  - one virtual thread per order (blocking JDBC is cheap on virtual threads),
 *  - CompletableFuture.supplyAsync on a bounded platform pool orchestrates each batch,
 *  - AtomicLong counters / AtomicBoolean gate are lock-free,
 *  - ReentrantReadWriteLock guards the in-memory "recent status" map (many readers, few writers),
 *  - ReentrantLock serialises the (rare) snapshot/reset operation.
 */
@Component
public class VirtualThreadOrderProcessor implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(VirtualThreadOrderProcessor.class);
    private static final int MAX_TRACKED = 10_000;

    private final ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ThreadPoolTaskExecutor batchExecutor;
    private final OrderCommandService service;

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final Map<UUID, String> recentStatus = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, String> eldest) {
            return size() > MAX_TRACKED;
        }
    };
    private final ReentrantLock snapshotLock = new ReentrantLock();

    public VirtualThreadOrderProcessor(@Qualifier("batchExecutor") ThreadPoolTaskExecutor batchExecutor,
                                       OrderCommandService service) {
        this.batchExecutor = batchExecutor;
        this.service = service;
    }

    public CompletableFuture<BatchResult> processBatch(List<CreateOrderCommand> commands) {
        if (!accepting.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("processor is draining"));
        }
        return CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            List<CompletableFuture<Optional<UUID>>> futures = commands.stream()
                    .map(c -> CompletableFuture.supplyAsync(() -> createOne(c), virtualThreads))
                    .toList();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            List<UUID> ids = futures.stream().map(CompletableFuture::join).flatMap(Optional::stream).toList();
            return new BatchResult(commands.size(), ids.size(), commands.size() - ids.size(), ids,
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }, batchExecutor);
    }

    private Optional<UUID> createOne(CreateOrderCommand cmd) {
        submitted.incrementAndGet();
        try {
            OrderEntity o = service.create(cmd);
            remember(o.getId(), o.getStatus().name());
            succeeded.incrementAndGet();
            return Optional.of(o.getId());
        } catch (Exception e) {
            failed.incrementAndGet();
            log.warn("order creation failed: {}", e.toString());
            return Optional.empty();
        }
    }

    private void remember(UUID id, String status) {
        stateLock.writeLock().lock();
        try {
            recentStatus.put(id, status);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public Optional<String> recentStatus(UUID id) {
        stateLock.readLock().lock();
        try {
            return Optional.ofNullable(recentStatus.get(id));
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /** Consistent stats snapshot; reset=true zeroes the counters. */
    public Map<String, Long> snapshot(boolean reset) {
        snapshotLock.lock();
        try {
            Map<String, Long> m = Map.of("submitted", submitted.get(), "succeeded", succeeded.get(),
                    "failed", failed.get());
            if (reset) {
                submitted.set(0);
                succeeded.set(0);
                failed.set(0);
            }
            return m;
        } finally {
            snapshotLock.unlock();
        }
    }

    @Override
    public void destroy() throws InterruptedException {
        accepting.set(false);
        virtualThreads.shutdown();
        if (!virtualThreads.awaitTermination(15, TimeUnit.SECONDS)) {
            virtualThreads.shutdownNow();
        }
    }
}

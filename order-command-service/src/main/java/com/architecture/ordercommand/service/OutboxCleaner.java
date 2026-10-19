package com.architecture.ordercommand.service;

import com.architecture.ordercommand.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

/** Debezium reads the WAL, not the table, so old rows can be purged safely. */
@Component
public class OutboxCleaner {
    private static final Logger log = LoggerFactory.getLogger(OutboxCleaner.class);
    private final OutboxRepository repo;

    public OutboxCleaner(OutboxRepository repo) {
        this.repo = repo;
    }

    @Scheduled(cron = "0 15 * * * *")
    @Transactional
    public void purge() {
        int n = repo.deleteOlderThan(Instant.now().minus(Duration.ofDays(1)));
        if (n > 0) log.info("purged {} outbox rows", n);
    }
}

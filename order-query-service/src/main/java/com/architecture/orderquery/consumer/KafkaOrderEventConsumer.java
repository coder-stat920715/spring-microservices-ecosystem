package com.architecture.orderquery.consumer;

import com.architecture.orderquery.model.OrderEventPayload;
import com.architecture.orderquery.model.OrderView;
import com.architecture.orderquery.repository.OrderViewRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Projects Debezium-routed outbox events into the Mongo read model, idempotently by version. */
@Component
public class KafkaOrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderEventConsumer.class);
    private final ObjectMapper mapper;
    private final OrderViewRepository repo;
    private final CacheManager caches;

    public KafkaOrderEventConsumer(ObjectMapper mapper, OrderViewRepository repo, CacheManager caches) {
        this.mapper = mapper;
        this.repo = repo;
        this.caches = caches;
    }

    @KafkaListener(topics = "order-events", groupId = "order-query")
    public void onEvent(String json) throws JsonProcessingException {
        OrderEventPayload e = mapper.readValue(json, OrderEventPayload.class);
        String id = e.orderId().toString();
        boolean stale = repo.findById(id).map(v -> v.version() >= e.version()).orElse(false);
        if (stale) {
            log.debug("ignoring stale/duplicate event {} v{}", e.eventType(), e.version());
            return;
        }
        repo.save(new OrderView(id, e.customerId(), e.tenantId(), e.totalAmount(), e.currency(),
                e.status(), e.version(), e.occurredAt()));
        Cache cache = caches.getCache("orders");
        if (cache != null) cache.evict(id);      // L2 + L1 + broadcast to other pods
    }
}

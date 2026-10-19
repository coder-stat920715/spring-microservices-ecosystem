package com.architecture.orderquery.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/** Denormalised read model (MongoDB). Serializable because the L2 Redis cache stores it. */
@Document("order_views")
public record OrderView(@Id String id, String customerId, @Indexed String tenantId, BigDecimal totalAmount,
                        String currency, String status, long version, Instant updatedAt) implements Serializable {
}

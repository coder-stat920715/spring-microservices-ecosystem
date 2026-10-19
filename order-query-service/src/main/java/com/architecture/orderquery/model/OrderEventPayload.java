package com.architecture.orderquery.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderEventPayload(UUID orderId, String eventType, String customerId, String tenantId,
                                BigDecimal totalAmount, String currency, String status,
                                long version, Instant occurredAt) {
}

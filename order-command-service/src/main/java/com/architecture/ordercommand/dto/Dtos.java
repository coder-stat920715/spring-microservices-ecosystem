package com.architecture.ordercommand.dto;

import com.architecture.ordercommand.domain.OrderStatus;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class Dtos {
    private Dtos() {
    }

    public record CreateOrderCommand(
            @NotBlank String customerId,
            @NotBlank String tenantId,
            @NotNull @DecimalMin("0.01") BigDecimal totalAmount,
            @NotBlank @Size(min = 3, max = 3) String currency) {
    }

    public record UpdateOrderCommand(BigDecimal totalAmount, OrderStatus status) {
    }

    public record OrderResponse(UUID id, String status, long version) {
    }

    public record BatchResult(int total, int succeeded, int failed, java.util.List<UUID> orderIds, long durationMs) {
    }

    /** Event contract written to the outbox (and consumed by query/payment services). */
    public record OrderEventPayload(UUID orderId, String eventType, String customerId, String tenantId,
                                    BigDecimal totalAmount, String currency, String status,
                                    long version, Instant occurredAt) {
    }

    public record PaymentResultEvent(UUID orderId, String status, String reason) {
    }
}

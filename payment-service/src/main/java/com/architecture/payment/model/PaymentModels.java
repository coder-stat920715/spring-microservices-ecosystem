package com.architecture.payment.model;

import java.math.BigDecimal;
import java.util.UUID;

public final class PaymentModels {
    private PaymentModels() {
    }

    public record PaymentRequest(UUID orderId, String eventType, String customerId, BigDecimal totalAmount, String currency) {
    }

    public record ChargeRequest(String reference, BigDecimal amount, String currency) {
    }

    public record ChargeResult(String chargeId, String status, String reason) {
    }

    public record PaymentResultEvent(UUID orderId, String status, String reason) {
    }
}

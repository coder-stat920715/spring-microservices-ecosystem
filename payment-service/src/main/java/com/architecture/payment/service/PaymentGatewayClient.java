package com.architecture.payment.service;

import com.architecture.payment.model.PaymentModels.ChargeRequest;
import com.architecture.payment.model.PaymentModels.ChargeResult;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Aspect order is Retry( CircuitBreaker( call ) ), so the fallback sits on the outermost
 * annotation (Retry) and runs only after retries are exhausted or the breaker is open.
 */
@Component
public class PaymentGatewayClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentGatewayClient.class);
    private final RestClient client;

    public PaymentGatewayClient(RestClient paymentProviderRestClient) {
        this.client = paymentProviderRestClient;
    }

    @Retry(name = "paymentProvider", fallbackMethod = "fallback")
    @CircuitBreaker(name = "paymentProvider")
    public ChargeResult charge(UUID orderId, BigDecimal amount, String currency) {
        return client.post().uri("/v1/charges")
                .header("Idempotency-Key", orderId.toString())     // provider-side de-duplication
                .body(new ChargeRequest(orderId.toString(), amount, currency))
                .retrieve()
                .body(ChargeResult.class);
    }

    @SuppressWarnings("unused")
    public ChargeResult fallback(UUID orderId, BigDecimal amount, String currency, Throwable t) {
        log.warn("payment provider unavailable for order {}: {}", orderId, t.toString());
        return new ChargeResult(null, "FAILED", "provider unavailable: " + t.getClass().getSimpleName());
    }
}

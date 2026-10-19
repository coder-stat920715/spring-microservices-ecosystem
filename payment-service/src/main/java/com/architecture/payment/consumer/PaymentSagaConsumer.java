package com.architecture.payment.consumer;

import com.architecture.payment.model.PaymentModels.ChargeResult;
import com.architecture.payment.model.PaymentModels.PaymentRequest;
import com.architecture.payment.model.PaymentModels.PaymentResultEvent;
import com.architecture.payment.service.PaymentGatewayClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Saga participant. Consumes PAYMENT_REQUESTED from payment-events (outbox -> Debezium), charges the
 * provider (circuit breaker + retry), then publishes the outcome to payment-results.
 * Infrastructure failures use non-blocking retry topics: 1 original + 3 retries with exponential
 * backoff (1s, 2s, 4s), then the record lands in payment-events.DLT.
 */
@Component
public class PaymentSagaConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentSagaConsumer.class);
    private final ObjectMapper mapper;
    private final PaymentGatewayClient gateway;
    private final KafkaTemplate<String, String> kafka;

    public PaymentSagaConsumer(ObjectMapper mapper, PaymentGatewayClient gateway, KafkaTemplate<String, String> kafka) {
        this.mapper = mapper;
        this.gateway = gateway;
        this.kafka = kafka;
    }

    @RetryableTopic(attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 8000),
            dltTopicSuffix = ".DLT",
            retryTopicSuffix = "-retry")
    @KafkaListener(topics = "payment-events", groupId = "payment-service")
    public void onPaymentRequested(String json) throws JsonProcessingException {
        PaymentRequest req = mapper.readValue(json, PaymentRequest.class);
        if (!"PAYMENT_REQUESTED".equals(req.eventType())) return;

        ChargeResult result = gateway.charge(req.orderId(), req.totalAmount(), req.currency());
        String status = "SUCCEEDED".equalsIgnoreCase(result.status()) ? "SUCCEEDED" : "FAILED";
        PaymentResultEvent out = new PaymentResultEvent(req.orderId(), status, result.reason());
        // Blocking send so a broker failure raises an exception and triggers the retry topics.
        kafka.send("payment-results", req.orderId().toString(), mapper.writeValueAsString(out)).join();
    }

    @DltHandler
    public void onDeadLetter(String json, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("payment event exhausted retries, parked on {} : {}", topic, json);
        // Hook: raise an alert / write to a manual-review table. Compensation happens when ops replays the DLT.
    }
}

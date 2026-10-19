package com.architecture.ordercommand.messaging;

import com.architecture.ordercommand.dto.Dtos.PaymentResultEvent;
import com.architecture.ordercommand.service.OrderCommandService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Closes the saga: PAID on success, CANCELLED (compensation) on failure. */
@Component
public class PaymentResultListener {

    private final OrderCommandService service;
    private final ObjectMapper mapper;

    public PaymentResultListener(OrderCommandService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "payment-results", groupId = "order-command")
    public void onResult(String json) throws JsonProcessingException {
        PaymentResultEvent e = mapper.readValue(json, PaymentResultEvent.class);
        service.applyPaymentResult(e.orderId(), "SUCCEEDED".equals(e.status()));
    }
}

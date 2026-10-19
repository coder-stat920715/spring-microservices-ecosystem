package com.architecture.ordercommand.service;

import com.architecture.ordercommand.domain.OrderEntity;
import com.architecture.ordercommand.domain.OrderStatus;
import com.architecture.ordercommand.domain.OutboxEvent;
import com.architecture.ordercommand.dto.Dtos.CreateOrderCommand;
import com.architecture.ordercommand.dto.Dtos.OrderEventPayload;
import com.architecture.ordercommand.dto.Dtos.UpdateOrderCommand;
import com.architecture.ordercommand.repository.OrderRepository;
import com.architecture.ordercommand.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Write side. The order row and its outbox rows are committed in ONE database transaction, so
 * there is no dual write to Kafka: Debezium streams the outbox INSERTs from the Postgres WAL.
 */
@Service
public class OrderCommandService {

    private final OrderRepository orders;
    private final OutboxRepository outbox;
    private final ObjectMapper mapper;

    public OrderCommandService(OrderRepository orders, OutboxRepository outbox, ObjectMapper mapper) {
        this.orders = orders;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    @Transactional
    public OrderEntity create(CreateOrderCommand cmd) {
        OrderEntity order = orders.saveAndFlush(new OrderEntity(UUID.randomUUID(), cmd.customerId(),
                cmd.tenantId(), cmd.totalAmount(), cmd.currency(), OrderStatus.PENDING));
        emit("order", order, "ORDER_CREATED");
        emit("payment", order, "PAYMENT_REQUESTED");   // saga step 1 -> payment-events topic
        return order;
    }

    @Transactional
    public OrderEntity update(UUID id, UpdateOrderCommand cmd) {
        OrderEntity order = orders.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("order " + id));
        if (cmd.totalAmount() != null) order.setTotalAmount(cmd.totalAmount());
        if (cmd.status() != null) order.setStatus(cmd.status());
        order = orders.saveAndFlush(order);
        emit("order", order, "ORDER_UPDATED");
        return order;
    }

    /** Saga completion / compensation driven by payment-results. */
    @Transactional
    public void applyPaymentResult(UUID orderId, boolean success) {
        OrderEntity order = orders.findById(orderId)
                .orElseThrow(() -> new EntityNotFoundException("order " + orderId));
        if (order.getStatus() != OrderStatus.PENDING) return;   // idempotent
        order.setStatus(success ? OrderStatus.PAID : OrderStatus.CANCELLED);
        order = orders.saveAndFlush(order);
        emit("order", order, success ? "ORDER_PAID" : "ORDER_CANCELLED");
    }

    private void emit(String aggregateType, OrderEntity o, String eventType) {
        OrderEventPayload payload = new OrderEventPayload(o.getId(), eventType, o.getCustomerId(), o.getTenantId(),
                o.getTotalAmount(), o.getCurrency(), o.getStatus().name(), o.getVersion(), Instant.now());
        try {
            outbox.save(new OutboxEvent(aggregateType, o.getId().toString(), eventType,
                    mapper.writeValueAsString(payload)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialise outbox payload", e);
        }
    }
}

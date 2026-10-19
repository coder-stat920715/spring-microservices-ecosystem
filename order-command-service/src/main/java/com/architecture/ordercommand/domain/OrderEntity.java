package com.architecture.ordercommand.domain;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private UUID id;
    @Column(name = "customer_id", nullable = false)
    private String customerId;
    @Column(name = "tenant_id", nullable = false)
    private String tenantId;
    @Column(name = "total_amount", nullable = false)
    private BigDecimal totalAmount;
    @Column(nullable = false, length = 3)
    private String currency;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;
    @Version
    private long version;
    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    protected OrderEntity() {
    }

    public OrderEntity(UUID id, String customerId, String tenantId, BigDecimal totalAmount,
                       String currency, OrderStatus status) {
        this.id = id;
        this.customerId = customerId;
        this.tenantId = tenantId;
        this.totalAmount = totalAmount;
        this.currency = currency;
        this.status = status;
    }

    @PreUpdate
    void touch() {
        this.updatedAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getCustomerId() { return customerId; }
    public String getTenantId() { return tenantId; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    public String getCurrency() { return currency; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public long getVersion() { return version; }
}

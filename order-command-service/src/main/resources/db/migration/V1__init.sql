-- Write model + transactional outbox (PostgreSQL 16, logical replication enabled)
CREATE TABLE IF NOT EXISTS orders (
    id            UUID PRIMARY KEY,
    customer_id   VARCHAR(100)   NOT NULL,
    tenant_id     VARCHAR(100)   NOT NULL,
    total_amount  NUMERIC(19, 2) NOT NULL CHECK (total_amount > 0),
    currency      CHAR(3)        NOT NULL,
    status        VARCHAR(20)    NOT NULL,
    version       BIGINT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_tenant ON orders (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,   -- 'order' -> topic order-events, 'payment' -> payment-events
    aggregate_id   VARCHAR(100) NOT NULL,   -- Kafka message key
    event_type     VARCHAR(100) NOT NULL,
    payload        JSONB        NOT NULL,
    status         VARCHAR(20)  NOT NULL DEFAULT 'NEW',
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_created ON outbox_events (created_at);

-- 주문 서비스의 orders/accounts/positions와 독립적인 영속 전달 대기열.
CREATE TABLE IF NOT EXISTS adapter_orders (
    client_order_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'filled', 'rejected', 'unknown')),
    broker_result JSONB,
    last_error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token UUID,
    leased_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS adapter_orders_pending
    ON adapter_orders (next_attempt_at, created_at) WHERE status = 'pending';

-- 거래 테이블을 비우지 않고 외부 주문 전달/예약 기록만 추가함.
CREATE TABLE IF NOT EXISTS order_external (
    client_order_id TEXT PRIMARY KEY REFERENCES orders(client_order_id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    reserved_krw NUMERIC(24,4) NOT NULL DEFAULT 0 CHECK (reserved_krw >= 0),
    reserved_volume NUMERIC(30,8) NOT NULL DEFAULT 0 CHECK (reserved_volume >= 0),
    reason TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token UUID,
    leased_until TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS order_external_schedule ON order_external(next_attempt_at);
-- 기존 자동 명명 제약도 보존된 데이터와 함께 확장함.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'orders'::regclass AND conname = 'orders_status_check'
          AND position('unknown' in pg_get_constraintdef(oid)) > 0
    ) THEN
        ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_check;
        ALTER TABLE orders ADD CONSTRAINT orders_status_check
            CHECK (status IN ('accepted', 'pending', 'filled', 'rejected', 'unknown'));
    END IF;
END $$;

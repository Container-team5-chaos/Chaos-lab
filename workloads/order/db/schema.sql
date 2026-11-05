-- 모의 거래소 주문 서비스 스키마

-- 사용자별 원화 잔고
CREATE TABLE accounts (
    user_id     TEXT PRIMARY KEY,
    krw         NUMERIC(24,4) NOT NULL CHECK (krw >= 0),
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- 사용자별 코인 보유 현황
CREATE TABLE positions (
    user_id     TEXT          NOT NULL REFERENCES accounts(user_id),
    market      TEXT          NOT NULL,                    -- e.g. KRW-BTC
    volume      NUMERIC(30,8) NOT NULL CHECK (volume >= 0),
    avg_price   NUMERIC(24,8) NOT NULL CHECK (avg_price >= 0),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, market)
);

-- 주문 (이번 주는 체결된 주문만 저장, 거절은 저장하지 않음)
CREATE TABLE orders (
    id               BIGSERIAL     PRIMARY KEY,
    client_order_id  TEXT          NOT NULL UNIQUE,
    user_id          TEXT          NOT NULL REFERENCES accounts(user_id),
    market           TEXT          NOT NULL,
    side             TEXT          NOT NULL CHECK (side IN ('bid', 'ask')), -- bid 매수, ask 매도
    volume           NUMERIC(30,8) NOT NULL CHECK (volume > 0),
    status           TEXT          NOT NULL
                     CHECK (status IN ('accepted', 'pending', 'filled', 'rejected')),
    fill_price       NUMERIC(24,8),  -- 체결가 (체결 전에는 NULL임)
    amount           NUMERIC(24,4),  -- 체결 금액 = fill_price * volume
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    filled_at        TIMESTAMPTZ,
    CHECK (status <> 'filled' OR (fill_price IS NOT NULL AND amount IS NOT NULL AND filled_at IS NOT NULL))
);
CREATE INDEX orders_user_created ON orders (user_id, created_at);

-- 수집기가 받은 시세 기록
CREATE TABLE ticks (
    id           BIGSERIAL     PRIMARY KEY,
    market       TEXT          NOT NULL,
    price        NUMERIC(24,8) NOT NULL,
    received_at  TIMESTAMPTZ   NOT NULL
);
CREATE INDEX ticks_market_time ON ticks (market, received_at);

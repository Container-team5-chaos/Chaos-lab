-- ts는 수집기 수신 시각, source_ts는 업비트 메시지 생성 시각임. 모두 Unix 밀리초임.
-- event_id는 동일 배치 재시도 시 중복 INSERT를 방지함.
CREATE TABLE IF NOT EXISTS price_history (
    event_id UUID PRIMARY KEY,
    market TEXT NOT NULL,
    price NUMERIC NOT NULL CHECK (price > 0),
    ts BIGINT NOT NULL,
    source_ts BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS price_history_market_ts
    ON price_history (market, ts DESC);

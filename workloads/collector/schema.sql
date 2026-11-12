-- 주문 서비스와 판정기가 사용하는 ticks를 공유함.
-- received_at은 수집기 수신 시각, source_ts는 업비트 원본 Unix 밀리초임.
-- 가격은 정밀도/소수 자릿수를 제한하지 않으며, 종목은 길이 제한 없는 TEXT임.
CREATE TABLE IF NOT EXISTS ticks (
    id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL, --유지
    price NUMERIC NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    source_ts BIGINT,
    event_id UUID --유지
);

-- 기존 ticks에도 적용함. 기존 행의 알 수 없는 원본 시각/UUID는 NULL로 유지함.
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS source_ts BIGINT;
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS event_id UUID;
ALTER TABLE ticks ALTER COLUMN price TYPE NUMERIC;
-- event_id는 동일 배치 재시도 시 중복 INSERT를 방지함. 기존 NULL 행은 허용함.
CREATE UNIQUE INDEX IF NOT EXISTS ticks_event_id_key ON ticks (event_id);
CREATE INDEX IF NOT EXISTS ticks_market_time ON ticks (market, received_at);

-- 명세 요약에서 옮긴 테스트용 DDL. 앱 팀 postgres-init 사본으로 교체 예정
-- 근거: workbook-week1/reference/design-spec.md 5절 "1단계 스키마 요약"(L436–457)과 권한 주석.
-- 명세에 없는 NOT NULL·기본값은 넣지 않는다(판정 SQL이 NULL을 다루는지 확인하려고).
-- 쓰임: tests/judge/test_sql_pg.py(-m pg). 슈퍼유저로 papertrade DB에 넣는다.

-- 역할은 클러스터 전역이라 이미 있으면 건너뛴다. 로컬 Unix 소켓 trust로 접속한다(비밀번호 없음).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pt_app') THEN
        CREATE ROLE pt_app LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pt_ro') THEN
        CREATE ROLE pt_ro LOGIN;
    END IF;
END
$$;

CREATE TABLE accounts (
    id uuid PRIMARY KEY,
    nickname text,
    mode text CHECK (mode IN ('live', 'replay')),
    token_hash text,
    initial_cash bigint,
    cash bigint CHECK (cash >= 0),
    version bigint,
    created_at timestamptz
);

CREATE TABLE orders (
    id uuid PRIMARY KEY,
    account_id uuid REFERENCES accounts (id),
    idem_key uuid,
    body_hash text,
    symbol text,
    side text,
    qty numeric(24, 8),
    limit_price numeric(24, 8),
    status text CHECK (status IN ('FILLED', 'REJECTED')),
    reject_reason text,
    ref_source text,
    ref_seq text,
    ref_event_ts bigint,
    ref_recv_ts bigint,
    ref_price numeric(24, 8),
    ref_age_ms int,
    received_at timestamptz,
    UNIQUE (account_id, idem_key)
);

CREATE TABLE fills (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id uuid UNIQUE REFERENCES orders (id),
    account_id uuid,
    symbol text,
    side text,
    qty numeric(24, 8),
    price numeric(24, 8),
    amount bigint,
    fee bigint,
    fee_rate numeric(8, 6),
    realized_pnl bigint,  -- 명세 4절 "금액 계산". 매수는 -수수료
    created_at timestamptz
);

CREATE TABLE positions (
    account_id uuid,
    symbol text,
    qty numeric(24, 8) CHECK (qty >= 0),
    cost_basis bigint CHECK (cost_basis >= 0),  -- 원, 수수료 제외
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE reconcile_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ran_at timestamptz,
    ok bool,
    checked_accounts int,
    mismatches int,
    detail jsonb
);

-- 권한(명세 주석 그대로)
-- pt_app = accounts·positions SELECT/INSERT/UPDATE, orders·fills SELECT/INSERT(UPDATE·DELETE 없음),
--          reconcile_runs SELECT
-- pt_ro  = 전체 SELECT, reconcile_runs INSERT
GRANT SELECT, INSERT, UPDATE ON accounts, positions TO pt_app;
GRANT SELECT, INSERT ON orders, fills TO pt_app;
GRANT SELECT ON reconcile_runs TO pt_app;
GRANT SELECT ON accounts, orders, fills, positions, reconcile_runs TO pt_ro;
GRANT INSERT ON reconcile_runs TO pt_ro;

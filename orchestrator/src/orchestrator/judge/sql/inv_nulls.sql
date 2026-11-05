-- inv_nulls (설계 5.4). 핵심 열의 NULL, FILLED 주문의 근거(ref) 필드 NULL, side 허용 밖 값.
-- 명세 스키마에 NOT NULL이 없고 CHECK는 NULL을 만족으로 본다. 그래서 따로 본다.
-- 위반 행만 낸다. 빈 결과 = PASS.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
),
ro AS (
    SELECT * FROM orders WHERE account_id IN (SELECT id FROM ra)
),
rf AS (
    SELECT * FROM fills
    WHERE account_id IN (SELECT id FROM ra) OR order_id IN (SELECT id FROM ro)
)
SELECT 'accounts' AS table_name, a.id::text AS row_key, c.column_name, c.problem
FROM accounts a
JOIN ra ON ra.id = a.id
CROSS JOIN LATERAL (VALUES
    ('mode', CASE WHEN a.mode IS NULL THEN 'null' END),
    ('initial_cash', CASE WHEN a.initial_cash IS NULL THEN 'null' END),
    ('cash', CASE WHEN a.cash IS NULL THEN 'null' END)
) AS c (column_name, problem)
WHERE c.problem IS NOT NULL
UNION ALL
SELECT 'orders', o.id::text, c.column_name, c.problem
FROM ro o
CROSS JOIN LATERAL (VALUES
    ('idem_key', CASE WHEN o.idem_key IS NULL THEN 'null' END),
    ('symbol', CASE WHEN o.symbol IS NULL THEN 'null' END),
    ('side', CASE WHEN o.side IS NULL THEN 'null' WHEN o.side NOT IN ('buy', 'sell') THEN 'invalid' END),
    ('qty', CASE WHEN o.qty IS NULL THEN 'null' END),
    ('limit_price', CASE WHEN o.limit_price IS NULL THEN 'null' END),
    ('status', CASE WHEN o.status IS NULL THEN 'null' END),
    ('ref_source', CASE WHEN o.status = 'FILLED' AND o.ref_source IS NULL THEN 'null' END),
    ('ref_seq', CASE WHEN o.status = 'FILLED' AND o.ref_seq IS NULL THEN 'null' END),
    ('ref_event_ts', CASE WHEN o.status = 'FILLED' AND o.ref_event_ts IS NULL THEN 'null' END),
    ('ref_recv_ts', CASE WHEN o.status = 'FILLED' AND o.ref_recv_ts IS NULL THEN 'null' END),
    ('ref_price', CASE WHEN o.status = 'FILLED' AND o.ref_price IS NULL THEN 'null' END),
    ('ref_age_ms', CASE WHEN o.status = 'FILLED' AND o.ref_age_ms IS NULL THEN 'null' END)
) AS c (column_name, problem)
WHERE c.problem IS NOT NULL
UNION ALL
SELECT 'fills', f.id::text, c.column_name, c.problem
FROM rf f
CROSS JOIN LATERAL (VALUES
    ('order_id', CASE WHEN f.order_id IS NULL THEN 'null' END),
    ('account_id', CASE WHEN f.account_id IS NULL THEN 'null' END),
    ('symbol', CASE WHEN f.symbol IS NULL THEN 'null' END),
    ('side', CASE WHEN f.side IS NULL THEN 'null' WHEN f.side NOT IN ('buy', 'sell') THEN 'invalid' END),
    ('qty', CASE WHEN f.qty IS NULL THEN 'null' END),
    ('price', CASE WHEN f.price IS NULL THEN 'null' END),
    ('amount', CASE WHEN f.amount IS NULL THEN 'null' END),
    ('fee', CASE WHEN f.fee IS NULL THEN 'null' END),
    ('fee_rate', CASE WHEN f.fee_rate IS NULL THEN 'null' END),
    ('realized_pnl', CASE WHEN f.realized_pnl IS NULL THEN 'null' END)
) AS c (column_name, problem)
WHERE c.problem IS NOT NULL
UNION ALL
SELECT 'positions', p.account_id::text || '/' || p.symbol, c.column_name, c.problem
FROM positions p
JOIN ra ON ra.id = p.account_id
CROSS JOIN LATERAL (VALUES
    ('qty', CASE WHEN p.qty IS NULL THEN 'null' END),
    ('cost_basis', CASE WHEN p.cost_basis IS NULL THEN 'null' END)
) AS c (column_name, problem)
WHERE c.problem IS NOT NULL
ORDER BY 1, 2, 3

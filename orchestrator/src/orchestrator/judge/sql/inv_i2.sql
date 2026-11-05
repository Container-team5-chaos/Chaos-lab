-- inv_i2 (설계 5.4, 명세 6절 I2). 음수 현금, 음수 보유 수량, 음수 취득원가.
-- 위반 행만 낸다. 빈 결과 = PASS. NULL은 inv_nulls가 본다.
-- psql 변수 ids, extra, nick_re는 db.build_bundle이 정한다.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
)
SELECT a.id AS account_id, 'cash' AS field, NULL::text AS symbol, a.cash::text AS value
FROM accounts a
JOIN ra ON ra.id = a.id
WHERE a.cash < 0
UNION ALL
SELECT p.account_id, 'qty', p.symbol, p.qty::text
FROM positions p
JOIN ra ON ra.id = p.account_id
WHERE p.qty < 0
UNION ALL
SELECT p.account_id, 'cost_basis', p.symbol, p.cost_basis::text
FROM positions p
JOIN ra ON ra.id = p.account_id
WHERE p.cost_basis < 0
ORDER BY 1, 2, 3

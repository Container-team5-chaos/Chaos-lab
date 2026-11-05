-- inv_arith (설계 5.4 선택 검사, 명세 4절 금액 계산). 반올림 방식이 확정되면 required로 올린다.
-- problem
--   amount   abs(amount - qty * price) > 0.5
--   fee      fee가 ceil(amount * fee_rate)와 다르다
--   buy_pnl  매수인데 realized_pnl이 -fee가 아니다
-- 위반 행만 낸다. 빈 결과 = PASS.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
),
rf AS (
    SELECT * FROM fills
    WHERE account_id IN (SELECT id FROM ra)
       OR order_id IN (SELECT id FROM orders WHERE account_id IN (SELECT id FROM ra))
)
SELECT 'amount' AS problem, f.id AS fill_id, f.order_id,
       f.amount::numeric AS actual, f.qty * f.price AS expect
FROM rf f
WHERE (abs(f.amount - f.qty * f.price) <= 0.5) IS NOT TRUE
UNION ALL
SELECT 'fee', f.id, f.order_id, f.fee, ceil(f.amount * f.fee_rate)
FROM rf f
WHERE f.fee IS DISTINCT FROM ceil(f.amount * f.fee_rate)
UNION ALL
SELECT 'buy_pnl', f.id, f.order_id, f.realized_pnl, -f.fee
FROM rf f
WHERE f.side = 'buy' AND f.realized_pnl IS DISTINCT FROM -f.fee
ORDER BY 1, 2

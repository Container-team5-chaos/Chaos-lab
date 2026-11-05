-- inv_i5_cash (설계 5.4, 명세 6절 I5). 계좌마다 두 식을 본다.
--   현금식   cash = initial_cash + 매도(amount - fee) 합 - 매수(amount + fee) 합
--   보존식   cash + cost_basis 합 = initial_cash + realized_pnl 합
-- 비교는 IS DISTINCT FROM이다. 위반 행만 낸다. 빈 결과 = PASS.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
),
f AS (
    SELECT account_id,
           sum(CASE side WHEN 'buy' THEN -(amount + fee) WHEN 'sell' THEN amount - fee END) AS flow,
           sum(realized_pnl) AS pnl
    FROM fills
    WHERE account_id IN (SELECT id FROM ra)
    GROUP BY account_id
),
p AS (
    SELECT account_id, sum(cost_basis) AS cost_basis
    FROM positions
    WHERE account_id IN (SELECT id FROM ra)
    GROUP BY account_id
)
SELECT a.id AS account_id,
       a.initial_cash,
       a.cash,
       a.initial_cash + coalesce(f.flow, 0) AS expect_cash,
       coalesce(p.cost_basis, 0) AS cost_basis_sum,
       coalesce(f.pnl, 0) AS realized_pnl_sum
FROM accounts a
JOIN ra ON ra.id = a.id
LEFT JOIN f ON f.account_id = a.id
LEFT JOIN p ON p.account_id = a.id
WHERE a.cash IS DISTINCT FROM a.initial_cash + coalesce(f.flow, 0)
   OR a.cash + coalesce(p.cost_basis, 0) IS DISTINCT FROM a.initial_cash + coalesce(f.pnl, 0)
ORDER BY 1

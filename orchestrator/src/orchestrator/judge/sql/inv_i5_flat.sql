-- inv_i5_flat (설계 5.4, 명세 4절 금액 계산). 전량 매도면 원가 전부가 빠진다.
-- 그래서 qty = 0인데 cost_basis가 0이 아니면 위반이다.
-- 위반 행만 낸다. 빈 결과 = PASS.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
)
SELECT p.account_id, p.symbol, p.qty, p.cost_basis
FROM positions p
JOIN ra ON ra.id = p.account_id
WHERE p.qty = 0 AND p.cost_basis IS DISTINCT FROM 0
ORDER BY 1, 2

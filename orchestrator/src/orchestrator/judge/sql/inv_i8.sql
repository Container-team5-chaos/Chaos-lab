-- inv_i8 (설계 5.4, 명세 6절 I8). replay 계좌는 REPLAY 종목만, live 계좌는 REPLAY 아닌 종목만 체결한다.
-- 종목이 REPLAY인지와 계좌가 replay인지가 다르면 위반이다. NULL도 다르다고 본다.
-- 위반 행만 낸다. 빈 결과 = PASS.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
)
SELECT f.id AS fill_id, f.account_id, a.mode, f.symbol, f.order_id
FROM fills f
JOIN accounts a ON a.id = f.account_id
JOIN ra ON ra.id = a.id
WHERE (f.symbol LIKE 'REPLAY:%') IS DISTINCT FROM (a.mode = 'replay')
ORDER BY 1

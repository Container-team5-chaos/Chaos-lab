-- inv_i5_qty (설계 5.4, 명세 6절 I5). (계좌, 종목)별 체결 순수량(매수 - 매도)과 positions.qty가 다르다.
-- 한쪽에만 있으면 없는 쪽을 0으로 본다. 있는데 NULL이면 다르다고 본다.
-- 위반 행만 낸다. 빈 결과 = PASS.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
),
f AS (
    SELECT account_id, symbol,
           sum(CASE side WHEN 'buy' THEN qty WHEN 'sell' THEN -qty END) AS net
    FROM fills
    WHERE account_id IN (SELECT id FROM ra)
    GROUP BY account_id, symbol
),
p AS (
    SELECT account_id, symbol, qty
    FROM positions
    WHERE account_id IN (SELECT id FROM ra)
)
SELECT coalesce(p.account_id, f.account_id) AS account_id,
       coalesce(p.symbol, f.symbol) AS symbol,
       p.qty AS position_qty,
       f.net AS fill_net_qty
FROM p
FULL JOIN f ON f.account_id = p.account_id AND f.symbol = p.symbol
WHERE (CASE WHEN p.account_id IS NULL THEN 0 ELSE p.qty END)
      IS DISTINCT FROM (CASE WHEN f.account_id IS NULL THEN 0 ELSE f.net END)
ORDER BY 1, 2

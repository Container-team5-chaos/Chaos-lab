-- inv_i6_db (설계 5.4, 명세 6절 I6). DB만으로 보는 근거 체결 검사.
-- problem
--   price_ne_ref  체결가가 주문의 ref_price와 다르다(required)
--   ref_null      FILLED 주문의 ref_source, ref_seq, ref_event_ts, ref_price 중 NULL(required)
--   ref_source    REPLAY 종목인데 ref_source가 replay가 아니다(추론. 판정에 넣지 않고 보고만 한다)
-- 위반 행만 낸다. 빈 결과 = PASS.
WITH ra AS (
    SELECT id FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
),
ro AS (
    SELECT * FROM orders WHERE account_id IN (SELECT id FROM ra)
)
SELECT 'price_ne_ref' AS problem, o.id AS order_id, o.account_id, o.symbol,
       f.price AS fill_price, o.ref_price, o.ref_source
FROM fills f
JOIN ro o ON o.id = f.order_id
WHERE f.price IS DISTINCT FROM o.ref_price
UNION ALL
SELECT 'ref_null', o.id, o.account_id, o.symbol, NULL, o.ref_price, o.ref_source
FROM ro o
WHERE o.status = 'FILLED'
  AND (o.ref_source IS NULL OR o.ref_seq IS NULL OR o.ref_event_ts IS NULL OR o.ref_price IS NULL)
UNION ALL
SELECT 'ref_source', o.id, o.account_id, o.symbol, NULL, o.ref_price, o.ref_source
FROM ro o
WHERE o.status = 'FILLED'
  AND o.symbol LIKE 'REPLAY:%'
  AND o.ref_source IS DISTINCT FROM 'replay'
ORDER BY 1, 2

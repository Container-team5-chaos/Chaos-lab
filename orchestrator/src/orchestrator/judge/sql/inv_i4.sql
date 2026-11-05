-- inv_i4 (설계 5.4, 명세 6절 I4). 분류기와 교차 확인한다(다르면 JUDGE_SELF_CHECK).
-- problem
--   dup_key              (account_id, idem_key)당 주문 2건 이상
--   dup_fill             주문 1건에 체결 2건 이상
--   filled_without_fill  FILLED인데 체결 없음
--   rejected_with_fill   REJECTED인데 체결 있음
--   fill_without_order   체결이 가리키는 주문이 없음
--   fill_field           체결과 주문의 account_id, symbol, side, qty가 다름(detail에 열 이름)
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
),
fc AS (
    SELECT order_id, count(*) AS n FROM rf WHERE order_id IS NOT NULL GROUP BY order_id
)
SELECT 'dup_key' AS problem, o.account_id, o.idem_key, NULL::uuid AS order_id,
       count(*) AS n, NULL::text AS detail
FROM ro o
WHERE o.idem_key IS NOT NULL
GROUP BY o.account_id, o.idem_key
HAVING count(*) > 1
UNION ALL
SELECT 'dup_fill', o.account_id, o.idem_key, fc.order_id, fc.n, NULL
FROM fc
LEFT JOIN orders o ON o.id = fc.order_id
WHERE fc.n > 1
UNION ALL
SELECT 'filled_without_fill', o.account_id, o.idem_key, o.id, 0, NULL
FROM ro o
LEFT JOIN fc ON fc.order_id = o.id
WHERE o.status = 'FILLED' AND fc.order_id IS NULL
UNION ALL
SELECT 'rejected_with_fill', o.account_id, o.idem_key, o.id, fc.n, NULL
FROM ro o
JOIN fc ON fc.order_id = o.id
WHERE o.status = 'REJECTED'
UNION ALL
SELECT 'fill_without_order', f.account_id, NULL, f.order_id, 1, f.id::text
FROM rf f
LEFT JOIN orders o ON o.id = f.order_id
WHERE o.id IS NULL
UNION ALL
SELECT 'fill_field', o.account_id, o.idem_key, o.id, 1,
       concat_ws(',',
                 CASE WHEN f.account_id IS DISTINCT FROM o.account_id THEN 'account_id' END,
                 CASE WHEN f.symbol IS DISTINCT FROM o.symbol THEN 'symbol' END,
                 CASE WHEN f.side IS DISTINCT FROM o.side THEN 'side' END,
                 CASE WHEN f.qty IS DISTINCT FROM o.qty THEN 'qty' END)
FROM rf f
JOIN orders o ON o.id = f.order_id
WHERE f.account_id IS DISTINCT FROM o.account_id
   OR f.symbol IS DISTINCT FROM o.symbol
   OR f.side IS DISTINCT FROM o.side
   OR f.qty IS DISTINCT FROM o.qty
ORDER BY 1, 2, 3, 4

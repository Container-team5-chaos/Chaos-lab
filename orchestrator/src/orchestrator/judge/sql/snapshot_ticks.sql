-- snapshot_ticks (계약 정렬 명세 6, 7 fresh). run 주문(+extra 계좌 주문) 종목의 ticks 중 체결 시각 범위 안의 것.
-- 범위(market별): [min(filled_at) - stale_ms - tick_slack_ms, max(filled_at) + tick_slack_ms].
-- 주문마다의 fresh 창 [filled_at - stale_ms - 1s, filled_at + 1s]을 모두 덮는다.
-- run 주문은 client_order_id가 run_key_re(`^k6-<run_id>-[0-9]+-[0-9]+$`)에 맞는 것이다(접두어만 보지 않는다).
-- psql 변수 run_key_re, extra, stale_ms, tick_slack_ms는 db.build_bundle이 정한다.
WITH ro AS (
    SELECT market, filled_at
    FROM orders
    WHERE status = 'filled'
      AND filled_at IS NOT NULL
      AND (client_order_id ~ :'run_key_re' OR user_id = ANY (:'extra'::text[]))
),
w AS (
    SELECT market,
           min(filled_at) - (:stale_ms + :tick_slack_ms) * interval '1 millisecond' AS lo,
           max(filled_at) + :tick_slack_ms * interval '1 millisecond' AS hi
    FROM ro
    GROUP BY market
)
SELECT t.market, t.price, t.received_at
FROM ticks t
JOIN w ON w.market = t.market
WHERE t.received_at BETWEEN w.lo AND w.hi
ORDER BY t.market COLLATE "C", t.received_at, t.price

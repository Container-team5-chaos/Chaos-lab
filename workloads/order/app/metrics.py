"""Prometheus 지표"""

from prometheus_client import Counter, Gauge, Histogram

ORDERS = Counter("orders_total", "처리한 주문 수", ["status", "reason"])
REPLAYED = Counter(
    "orders_replayed_total", "같은 client_order_id 재요청으로 기존 결과를 돌려준 수"
)
DURATION = Histogram(
    "order_duration_seconds",
    "주문 처리 시간",
    ["status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
PRICE_AGE = Gauge("price_age_seconds", "마지막으로 읽은 현재가의 age", ["market"])

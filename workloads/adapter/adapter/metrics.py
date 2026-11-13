from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.accepted = Counter(
            "adapter_orders_accepted_total",
            "Durably accepted orders",
            registry=self.registry,
        )
        self.replayed = Counter(
            "adapter_orders_replayed_total",
            "Existing orders returned",
            registry=self.registry,
        )
        self.retries = Counter(
            "adapter_retries_total",
            "Broker retries",
            ["reason"],
            registry=self.registry,
        )
        self.completed = Counter(
            "adapter_orders_completed_total",
            "Final order states",
            ["status"],
            registry=self.registry,
        )
        self.pending = Gauge(
            "adapter_queue_depth", "Pending durable orders", registry=self.registry
        )
        self.duration = Histogram(
            "adapter_broker_attempt_seconds",
            "Lookup and submit attempt duration",
            registry=self.registry,
        )

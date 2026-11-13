import math
import os
from dataclasses import dataclass
from urllib.parse import quote, urlsplit


def database_url_from_env():
    if os.getenv("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    user = quote(os.environ["POSTGRES_USER"], safe="")
    password = quote(os.environ["POSTGRES_PASSWORD"], safe="")
    database = quote(os.environ["POSTGRES_DB"], safe="")
    return f"postgresql://{user}:{password}@{os.environ['PGHOST']}:{os.getenv('PGPORT', '5432')}/{database}"


@dataclass(frozen=True)
class Settings:
    database_url: str
    broker_url: str = "http://mock-broker.exchange.svc.cluster.local:8000"
    db_pool_size: int = 5
    broker_rps: float = 5.0
    request_timeout: float = 5.0
    poll_seconds: float = 0.2
    lease_seconds: float = 30.0
    max_attempts: int = 8
    retry_base: float = 1.0
    retry_max: float = 30.0

    def __post_init__(self):
        for name in (
            "db_pool_size",
            "broker_rps",
            "request_timeout",
            "poll_seconds",
            "lease_seconds",
            "max_attempts",
            "retry_base",
            "retry_max",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        url = urlsplit(self.broker_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.netloc
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
        ):
            raise ValueError("BROKER_URL must be an HTTP(S) base URL")
        if self.retry_max < self.retry_base:
            raise ValueError("RETRY_MAX_SECONDS must be >= RETRY_BASE_SECONDS")
        if self.lease_seconds <= 2 * self.request_timeout + 2 / self.broker_rps + 2:
            raise ValueError("LEASE_SECONDS must cover two broker calls and rate waits")

    @classmethod
    def from_env(cls):
        return cls(
            database_url=database_url_from_env(),
            broker_url=os.getenv("BROKER_URL", cls.broker_url),
            db_pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            broker_rps=float(os.getenv("BROKER_RPS", "5")),
            request_timeout=float(os.getenv("BROKER_TIMEOUT_SECONDS", "5")),
            poll_seconds=float(os.getenv("POLL_SECONDS", "0.2")),
            lease_seconds=float(os.getenv("LEASE_SECONDS", "30")),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", "8")),
            retry_base=float(os.getenv("RETRY_BASE_SECONDS", "1")),
            retry_max=float(os.getenv("RETRY_MAX_SECONDS", "30")),
        )

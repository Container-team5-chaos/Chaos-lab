"""환경변수로 받는 설정값"""

import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://exchange:exchange@localhost:5432/exchange"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
STALE_MS = int(
    os.environ.get("STALE_MS", "5000")
)  # 이보다 오래된 시세로는 체결하지 않음
DB_POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "10"))  # Pod당 DB 커넥션 수
SUPPORTED_MARKETS = {"KRW-BTC"}

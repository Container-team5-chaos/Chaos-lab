import copy
from datetime import datetime, timezone

import pytest

from adapter.config import Settings
from adapter.store import Conflict


@pytest.fixture
def settings():
    return Settings("postgresql://unused", broker_url="http://broker.test")


@pytest.fixture
def payload():
    return {
        "client_order_id": "test-1",
        "user_id": "user-0001",
        "market": "KRW-BTC",
        "side": "bid",
        "volume": "0.00100000",
    }


def filled(payload):
    return {
        **payload,
        "status": "filled",
        "broker_order_id": "broker-1",
        "fill_price": "98000000.00000000",
        "amount": "98000.0000",
        "filled_at": "2026-11-11T09:00:00Z",
    }


class MemoryStore:
    """API/외부 통신 테스트용 대역. 실제 영속/잠금 검증은 test_store.py에서 수행함."""

    def __init__(self):
        self.rows = {}
        self.delays = []

    async def ready(self):
        pass

    async def accept(self, payload):
        key = payload["client_order_id"]
        if key in self.rows:
            if self.rows[key]["payload"] != payload:
                raise Conflict()
            return copy.deepcopy(self.rows[key]), True
        now = datetime.now(timezone.utc)
        self.rows[key] = {
            "client_order_id": key,
            "payload": dict(payload),
            "status": "pending",
            "attempts": 0,
            "broker_result": None,
            "last_error": None,
            "created_at": now,
            "updated_at": now,
            "lease_token": None,
        }
        return copy.deepcopy(self.rows[key]), False

    async def get(self, key):
        return copy.deepcopy(self.rows.get(key))

    async def claim(self, seconds):
        for row in self.rows.values():
            if row["status"] == "pending":
                row["attempts"] += 1
                return copy.deepcopy(row)

    async def retry(self, job, delay, error):
        self.delays.append(delay)
        self.rows[job["client_order_id"]]["last_error"] = error

    async def finish(self, job, status, result=None, error=None):
        self.rows[job["client_order_id"]].update(
            status=status, broker_result=result, last_error=error
        )

    async def pending_count(self):
        return sum(row["status"] == "pending" for row in self.rows.values())

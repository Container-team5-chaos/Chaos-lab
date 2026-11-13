import asyncio
import json
import time
from unittest.mock import AsyncMock

import httpx
import pytest

from push_server.main import Client, Hub, Settings, create_app, parse_price


def tick(price="98000000", ts=1000):
    return json.dumps({"price": price, "ts": ts})


@pytest.mark.parametrize(
    "data",
    [
        {},
        [],
        {"price": 100, "ts": 1},
        {"price": "NaN", "ts": 1},
        {"price": "Infinity", "ts": 1},
        {"price": "-1", "ts": 1},
        {"price": "0", "ts": 1},
        {"price": "1", "ts": True},
        {"price": "1", "ts": "123"},
        {"price": "1", "ts": -1},
        {"price": "1", "ts": 9007199254740992},
    ],
)
def test_invalid_price(data):
    with pytest.raises(ValueError):
        parse_price(json.dumps(data), "KRW-BTC")


def test_collector_payload_preserved():
    raw = '{"price":"98000000.12345678","ts":1762330000123,"source":"stub"}'
    assert parse_price(raw, "KRW-BTC") == {
        "type": "price",
        "market": "KRW-BTC",
        "price": "98000000.12345678",
        "ts": 1762330000123,
        "source": "stub",
    }


def test_queue_coalesces_without_blocking_and_rejects_old_snapshot():
    hub = Hub(Settings(), AsyncMock())
    slow, fast = Client(AsyncMock()), Client(AsyncMock())
    hub.clients.update([slow, fast])
    hub.ingest(tick(ts=1000))
    assert fast.queue.get_nowait()["ts"] == 1000
    hub.ingest(tick(ts=2000))
    hub.ingest(tick(ts=1500))
    hub.ingest(tick(ts=2000))
    assert slow.queue.qsize() == 1
    assert slow.queue.get_nowait()["ts"] == 2000
    assert fast.queue.get_nowait()["ts"] == 2000
    # Same millisecond, different price: retain the last published tick.
    hub.ingest(tick("98000001", ts=2000))
    assert hub.latest["price"] == "98000001"
    hub.ingest("not-json")
    assert hub.latest["price"] == "98000001"


async def test_slow_socket_send_times_out():
    hub = Hub(Settings(send_timeout=0.01), AsyncMock())
    ws = AsyncMock()

    async def blocked(_):
        await asyncio.sleep(10)

    ws.send_json.side_effect = blocked
    client = Client(ws)
    hub.offer(client, parse_price(tick(), "KRW-BTC"))
    with pytest.raises(TimeoutError):
        await hub.sender(client)


async def test_health_independent_of_redis_and_readiness_tracks_worker():
    app = create_app(redis_client=AsyncMock())
    hub = app.state.hub
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/readyz")).status_code == 503
        hub.task = asyncio.create_task(asyncio.sleep(10))
        try:
            hub.redis_ok = True
            assert (await client.get("/readyz")).status_code == 200
            hub.stopping = True
            assert (await client.get("/readyz")).status_code == 503
            hub.ingest(tick(ts=int(time.time() * 1000) - 10000))
            metrics = await client.get("/metrics")
            assert "push_price_age_seconds" in metrics.text
            assert "push_connections" in metrics.text
        finally:
            hub.task.cancel()
            await asyncio.gather(hub.task, return_exceptions=True)

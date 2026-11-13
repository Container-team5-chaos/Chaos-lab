import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import asyncpg
import httpx
import pytest
from asgi_lifespan import LifespanManager

from adapter.main import create_app

from .conftest import MemoryStore, filled


@pytest.fixture
async def service(settings):
    store = MemoryStore()
    app = create_app(settings, store=store, run_worker=False)
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, store, client


async def test_accept_replay_and_conflict(service, payload):
    app, store, client = service
    first = await client.post("/orders", json={**payload, "volume": "0.001"})
    assert first.status_code == 202
    assert first.json()["status"] == "pending"
    second = await client.post("/orders", json=payload)
    assert second.status_code == 202 and second.json()["replayed"] is True
    assert len(store.rows) == 1
    conflict = await client.post("/orders", json={**payload, "side": "ask"})
    assert conflict.status_code == 409
    assert conflict.json()["reason"] == "idempotency_conflict"


async def test_concurrent_duplicate_accepts_only_once(service, payload):
    _, store, client = service
    responses = await asyncio.gather(
        *[client.post("/orders", json=payload) for _ in range(20)]
    )
    assert all(r.status_code == 202 for r in responses)
    assert sum(not r.json()["replayed"] for r in responses) == 1
    assert len(store.rows) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"volume": 0.001},
        {"volume": "NaN"},
        {"volume": "Infinity"},
        {"volume": "0"},
        {"volume": "-1"},
        {"volume": "0.000000001"},
        {"volume": "1e30"},
        {"market": "KRW-ETH"},
        {"side": "buy"},
        {"client_order_id": "../x"},
        {"user_id": ""},
        {"extra": "unexpected"},
    ],
)
async def test_invalid_requests(service, payload, change):
    _, store, client = service
    result = await client.post("/orders", json={**payload, **change})
    assert result.status_code == 400
    assert not store.rows


async def test_decimal_precision_not_limited_by_decimal_context(service, payload):
    _, _, client = service
    volume = "9999999999999999999999.12345678"
    result = await client.post("/orders", json={**payload, "volume": volume})
    assert result.status_code == 202 and result.json()["volume"] == volume


async def test_read_terminal_replay_health_and_metrics(service, payload):
    _, store, client = service
    assert (await client.get("/orders/missing")).status_code == 404
    await client.post("/orders", json=payload)
    job = await store.claim(30)
    await store.finish(job, "filled", filled(payload))
    result = await client.get("/orders/test-1")
    assert result.status_code == 200
    assert result.json()["result"]["amount"] == "98000.0000"
    assert (await client.post("/orders", json=payload)).status_code == 200
    assert (await client.get("/healthz")).status_code == 200
    assert (await client.get("/readyz")).status_code == 200
    assert "adapter_orders_replayed_total 1.0" in (await client.get("/metrics/")).text


async def test_database_failure_and_shutdown(service, payload):
    app, store, client = service
    store.accept = AsyncMock(side_effect=asyncpg.PostgresConnectionError())
    assert (await client.post("/orders", json=payload)).status_code == 503
    store.ready = AsyncMock(side_effect=TimeoutError())
    assert (await client.get("/readyz")).status_code == 503
    assert (await client.get("/healthz")).status_code == 200
    app.state.stopping.set()
    assert (await client.post("/orders", json=payload)).status_code == 503


async def test_worker_runs_end_to_end_without_order_service(settings, payload):
    store = MemoryStore()
    requests = []

    async def upstream(request):
        requests.append(request.method)
        return (
            httpx.Response(404)
            if request.method == "GET"
            else httpx.Response(200, json=filled(payload))
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://broker.test"
    ) as broker:
        app = create_app(
            replace(settings, poll_seconds=0.01, broker_rps=100),
            store=store,
            broker_client=broker,
        )
        async with LifespanManager(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                assert (await client.post("/orders", json=payload)).status_code == 202
                for _ in range(100):
                    result = (await client.get("/orders/test-1")).json()
                    if result["status"] == "filled":
                        break
                    await asyncio.sleep(0.01)
                assert result["status"] == "filled"
        assert requests == ["GET", "POST"]


async def test_dead_worker_makes_readiness_and_accept_fail(settings, payload):
    store = MemoryStore()
    store.claim = AsyncMock(side_effect=RuntimeError("worker failed"))
    app = create_app(settings, store=store)
    async with LifespanManager(app):
        await asyncio.sleep(0)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/readyz")).status_code == 503
            assert (await client.post("/orders", json=payload)).status_code == 503

"""실제 DB/Redis + 실제 order/adapter ASGI API + 외부 증권사 HTTP 대역."""

import asyncio
import json
import sys
import socket
from contextlib import asynccontextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import asyncpg
import uvicorn
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from fastapi.responses import JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "adapter"))
from adapter.broker import Broker, RateLimiter
from adapter.config import Settings
from adapter.main import create_app
from adapter.metrics import Metrics
from adapter.store import Store
from adapter.worker import Worker
from app import config, external
from app.adapter_client import AdapterClient, validate_receipt
from app.main import app
from app.orders import Rejected, place_order
from app.schemas import OrderRequest

from .conftest import set_price
from .test_orders import order


@dataclass
class Harness:
    client: httpx.AsyncClient
    pool: object
    order_worker: external.ExternalWorker
    adapter_worker: Worker
    broker_orders: dict
    broker_calls: list
    mode: dict

    async def order_step(self):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE order_external SET next_attempt_at = now() WHERE lease_token IS NULL"
            )
        job = await self.order_worker.claim()
        assert job is not None
        await self.order_worker.process(job)
        return job

    async def broker_step(self):
        job = await self.adapter_worker.store.claim(30)
        assert job is not None
        await self.adapter_worker.process(job)


@pytest_asyncio.fixture
async def integration(db, monkeypatch):
    monkeypatch.setattr(config, "ADAPTER_URL", "http://adapter.test")

    async def paused(worker):
        await asyncio.Event().wait()

    monkeypatch.setattr(external.ExternalWorker, "run", paused)
    await set_price()
    async with LifespanManager(app):
        pool = app.state.pool
        store = Store(pool)
        await store.initialize()
        await db.execute("TRUNCATE adapter_orders")
        broker_orders, broker_calls, mode = {}, [], {"value": "filled"}

        async def broker_upstream(request):
            broker_calls.append(request.method)
            key = request.url.path.rsplit("/", 1)[-1]
            if request.method == "GET":
                return (
                    httpx.Response(200, json=broker_orders[key])
                    if key in broker_orders
                    else httpx.Response(404)
                )
            payload = json.loads(request.content)
            if mode["value"] == "down":
                return httpx.Response(503)
            result = {**payload, "status": "rejected", "reason": "market_closed"}
            if mode["value"] != "rejected":
                price = Decimal("97000000")
                amount = (price * Decimal(payload["volume"])).quantize(
                    Decimal("0.0001"), rounding=ROUND_DOWN
                )
                result = {
                    **payload,
                    "status": "filled",
                    "broker_order_id": "broker-" + payload["client_order_id"],
                    "fill_price": str(price),
                    "amount": str(amount),
                    "filled_at": datetime.now(timezone.utc).isoformat(),
                }
            broker_orders[payload["client_order_id"]] = result
            if mode["value"] == "lost":
                raise httpx.ReadTimeout("filled but response lost")
            return httpx.Response(200, json=result)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(broker_upstream),
            base_url="http://broker.test",
        ) as upstream:
            settings = Settings("unused", max_attempts=2)
            adapter_app = create_app(
                settings, store=store, broker_client=upstream, run_worker=False
            )
            async with LifespanManager(adapter_app):
                async with (
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=adapter_app),
                        base_url="http://adapter.test",
                    ) as adapter_http,
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="http://order.test",
                    ) as order_http,
                ):
                    yield Harness(
                        order_http,
                        pool,
                        external.ExternalWorker(
                            pool, AdapterClient(adapter_http), poll=0.01
                        ),
                        Worker(
                            settings,
                            store,
                            Broker(upstream, RateLimiter(10000)),
                            Metrics(),
                            asyncio.Event(),
                        ),
                        broker_orders,
                        broker_calls,
                        mode,
                    )


async def test_api_to_adapter_fill_reserves_and_settles_once(integration, db):
    h = integration
    first = await h.client.post("/orders", json=order())
    assert first.status_code == 202 and first.json()["status"] == "pending"
    duplicate = await h.client.post("/orders", json=order())
    assert duplicate.status_code == 202 and duplicate.json()["replayed"] is True
    account = (await h.client.get("/accounts/user-0001")).json()
    assert Decimal(account["krw"]) == 10000000
    assert Decimal(account["available_krw"]) == 9902000
    await h.order_step()
    await h.broker_step()
    job = await h.order_step()
    result = (await h.client.get("/orders/t-1")).json()
    assert result["status"] == "filled" and result["fill_price"] == "97000000.00000000"
    assert await db.fetchval(
        "SELECT krw FROM accounts WHERE user_id='user-0001'"
    ) == Decimal("9903000")
    assert not await h.order_worker.settle(job, {"status": "filled"})
    assert (await h.client.post("/orders", json=order())).json()["replayed"] is True
    assert await db.fetchval("SELECT count(*) FROM orders") == 1
    assert h.broker_calls == ["GET", "POST"]
    assert (
        Decimal((await h.client.get("/accounts/user-0001")).json()["reserved_krw"]) == 0
    )


async def test_concurrent_duplicate_requests_and_conflict(integration, db):
    h = integration
    results = await asyncio.gather(
        *(h.client.post("/orders", json=order()) for _ in range(20))
    )
    assert all(r.status_code == 202 for r in results)
    assert sum(not r.json()["replayed"] for r in results) == 1
    assert await db.fetchval("SELECT count(*) FROM order_external") == 1
    assert (
        await h.client.post("/orders", json=order(volume="0.002"))
    ).status_code == 409
    assert (
        await h.client.post("/orders", json=order(user="user-0002"))
    ).status_code == 409


async def test_reserved_cash_blocks_external_and_local_overspending(integration, db):
    h = integration
    results = await asyncio.gather(
        *(
            h.client.post("/orders", json=order(cid=f"reserve-{i}", volume="0.02"))
            for i in range(10)
        )
    )
    assert sum(r.status_code == 202 for r in results) == 5
    assert sum(r.status_code == 422 for r in results) == 5
    with pytest.raises(Rejected) as error:
        await place_order(
            h.pool,
            app.state.redis,
            OrderRequest(**order(cid="local-too-much", volume="0.01")),
        )
    assert error.value.reason == "insufficient_balance"
    assert (
        await db.fetchval("SELECT krw FROM accounts WHERE user_id='user-0001'")
        == 10000000
    )


async def test_rejection_releases_hold_and_is_replayed(integration, db):
    h = integration
    h.mode["value"] = "rejected"
    await h.client.post("/orders", json=order())
    await h.order_step()
    await h.broker_step()
    await h.order_step()
    rejected = await h.client.post("/orders", json=order())
    assert rejected.status_code == 422 and rejected.json()["reason"] == "market_closed"
    assert rejected.json()["replayed"] is True
    account = (await h.client.get("/accounts/user-0001")).json()
    assert Decimal(account["available_krw"]) == 10000000
    assert await db.fetchval("SELECT count(*) FROM positions") == 0


async def test_sell_reservation_and_settlement(integration, db):
    h = integration
    await place_order(
        h.pool, app.state.redis, OrderRequest(**order(cid="local-buy", volume="0.002"))
    )
    assert (
        await h.client.post(
            "/orders", json=order(cid="sell-1", side="ask", volume="0.002")
        )
    ).status_code == 202
    assert (
        await h.client.post("/orders", json=order(cid="sell-2", side="ask"))
    ).status_code == 422
    account = (await h.client.get("/accounts/user-0001")).json()
    assert Decimal(account["positions"][0]["available_volume"]) == 0
    with pytest.raises(Rejected):
        await place_order(
            h.pool, app.state.redis, OrderRequest(**order(cid="local-sell", side="ask"))
        )
    await h.order_step()
    await h.broker_step()
    await h.order_step()
    assert (
        await db.fetchval("SELECT volume FROM positions WHERE user_id='user-0001'") == 0
    )
    assert (
        await db.fetchval("SELECT krw FROM accounts WHERE user_id='user-0001'")
        == 9998000
    )


async def test_broker_response_loss_recovers_without_duplicate_fill(integration, db):
    h = integration
    h.mode["value"] = "lost"
    await h.client.post("/orders", json=order())
    await h.order_step()
    await h.broker_step()
    async with h.pool.acquire() as conn:
        await conn.execute("UPDATE adapter_orders SET next_attempt_at = now()")
    await h.broker_step()
    await h.order_step()
    assert h.broker_calls == ["GET", "POST", "GET"]
    assert (
        await db.fetchval("SELECT krw FROM accounts WHERE user_id='user-0001'")
        == 9903000
    )


async def test_unknown_keeps_reservation(integration, db):
    h = integration
    h.mode["value"] = "down"
    await h.client.post("/orders", json=order())
    await h.order_step()
    await h.broker_step()
    await db.execute("UPDATE adapter_orders SET next_attempt_at = now()")
    await h.broker_step()
    await h.order_step()
    result = (await h.client.get("/orders/t-1")).json()
    assert result["status"] == "unknown" and result["reason"] == "broker_unknown"
    assert (
        await db.fetchval("SELECT krw FROM accounts WHERE user_id='user-0001'")
        == 10000000
    )
    assert (
        Decimal((await h.client.get("/accounts/user-0001")).json()["reserved_krw"])
        == 98000
    )


async def test_expired_lease_recovery_fences_old_worker(integration, db):
    h = integration
    await h.client.post("/orders", json=order())
    old = await h.order_worker.claim()
    await db.execute(
        "UPDATE order_external SET leased_until = now() - interval '1 second'"
    )
    new = await external.ExternalWorker(h.pool, None).claim()
    assert old["lease_token"] != new["lease_token"]
    payload = new["payload"]
    body = {
        **payload,
        "status": "filled",
        "result": {
            **payload,
            "status": "filled",
            "broker_order_id": "b-1",
            "fill_price": "97000000",
            "amount": "97000.0000",
            "filled_at": "2026-11-13T09:00:00Z",
        },
    }
    receipt = validate_receipt(body, payload)
    assert not await h.order_worker.settle(old, receipt)
    assert await h.order_worker.settle(new, receipt)
    assert not await h.order_worker.settle(new, receipt)
    assert (
        await db.fetchval("SELECT krw FROM accounts WHERE user_id='user-0001'")
        == 9903000
    )


async def test_external_schema_can_be_reapplied_without_losing_orders(integration, db):
    h = integration
    await h.client.post("/orders", json=order())
    await external.initialize(h.pool)
    assert await db.fetchval("SELECT count(*) FROM order_external") == 1


async def test_local_mode_replay_keeps_external_order_identity(
    integration, monkeypatch
):
    h = integration
    await h.client.post("/orders", json=order())
    monkeypatch.setattr(config, "ADAPTER_URL", "")
    replay = await h.client.post("/orders", json=order())
    assert replay.status_code == 202
    assert replay.json()["execution_mode"] == "adapter"
    assert replay.json()["replayed"] is True


@asynccontextmanager
async def live_api(api):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(api, log_level="error", lifespan="on"))
    server.capture_signals = lambda: nullcontext()
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(300):
            if server.started:
                break
            if task.done():
                await task
                raise AssertionError("HTTP server exited before startup")
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("HTTP server startup timed out")
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 5)
        finally:
            listener.close()


async def test_real_http_servers_and_background_workers(db, monkeypatch):
    await set_price()
    broker_api = FastAPI()
    broker_orders = {}
    submitted = []

    @broker_api.get("/orders/{key}")
    async def lookup(key: str):
        return broker_orders.get(key) or JSONResponse(status_code=404, content={})

    @broker_api.post("/orders")
    async def submit(payload: dict):
        submitted.append(payload["client_order_id"])
        amount = (Decimal("97000000") * Decimal(payload["volume"])).quantize(
            Decimal("0.0001")
        )
        assert amount <= Decimal(payload["max_amount"])
        broker_orders[payload["client_order_id"]] = {
            **payload,
            "status": "filled",
            "broker_order_id": "live-broker-1",
            "fill_price": "97000000",
            "amount": str(amount),
            "filled_at": datetime.now(timezone.utc).isoformat(),
        }
        return broker_orders[payload["client_order_id"]]

    pool = await asyncpg.create_pool(config.DATABASE_URL, min_size=1, max_size=4)
    try:
        store = Store(pool)
        await store.initialize()
        await db.execute("TRUNCATE adapter_orders")
        async with live_api(broker_api) as broker_url:
            adapter_api = create_app(
                Settings(config.DATABASE_URL, broker_url=broker_url, poll_seconds=0.01),
                store=store,
            )
            async with live_api(adapter_api) as adapter_url:
                monkeypatch.setattr(config, "ADAPTER_URL", adapter_url)
                monkeypatch.setattr(config, "ADAPTER_POLL", 0.01)
                async with live_api(app) as order_url:
                    async with httpx.AsyncClient(
                        base_url=order_url, trust_env=False
                    ) as client:
                        accepted = await client.post(
                            "/orders", json=order(cid="live-1")
                        )
                        assert accepted.status_code == 202
                        for _ in range(200):
                            result = (await client.get("/orders/live-1")).json()
                            if result["status"] == "filled":
                                break
                            await asyncio.sleep(0.02)
                        assert result["status"] == "filled"
                        assert (
                            await client.post("/orders", json=order(cid="live-1"))
                        ).json()["replayed"] is True
                        assert (await client.get("/readyz")).status_code == 200
        assert submitted == ["live-1"]
        assert (
            await db.fetchval("SELECT krw FROM accounts WHERE user_id='user-0001'")
            == 9903000
        )
    finally:
        await pool.close()

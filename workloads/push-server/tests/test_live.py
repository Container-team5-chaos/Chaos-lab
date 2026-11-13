"""Real Redis + real TCP WebSocket tests. TEST_REDIS_URL must be a dedicated test DB."""

import asyncio
import json
import os
import socket
import time
from contextlib import asynccontextmanager

import httpx
import pytest
import redis.asyncio as redis
import uvicorn
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from push_server.main import Settings, create_app

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_REDIS_URL"), reason="Set dedicated TEST_REDIS_URL"
)


async def wait_for(check, timeout=8):
    async with asyncio.timeout(timeout):
        while not await check():
            await asyncio.sleep(0.05)


@asynccontextmanager
async def live_server(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="warning", ws="websockets", lifespan="on")
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))

    async def started():
        if task.done():
            task.result()
        return server.started

    try:
        await wait_for(started)
        yield f"http://127.0.0.1:{port}", f"ws://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 15)
        sock.close()


@pytest.fixture
async def live():
    r = redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    await r.delete("price:KRW-BTC")
    app = create_app(
        Settings(redis_url=os.environ["TEST_REDIS_URL"], heartbeat_seconds=0.25)
    )
    async with live_server(app) as urls:

        async def ready():
            return app.state.hub.ready

        await wait_for(ready)
        yield app, r, *urls
    await r.delete("price:KRW-BTC")
    await r.aclose()


async def publish(r, price, ts=None, source=None):
    payload = {"price": price, "ts": ts or int(time.time() * 1000)}
    if source:
        payload["source"] = source
    async with r.pipeline(transaction=True) as pipe:
        pipe.set("price:KRW-BTC", json.dumps(payload))
        pipe.publish("price:KRW-BTC", json.dumps(payload))
        await pipe.execute()
    return payload


async def receive(ws, kind="price"):
    async with asyncio.timeout(5):
        while True:
            message = json.loads(await ws.recv())
            if message["type"] == kind:
                return message


async def test_snapshot_fanout_latest_and_disconnect_cleanup(live):
    app, r, http, ws_url = live
    original = await publish(r, "98000000", source="stub")

    async def latest():
        return app.state.hub.latest is not None

    await wait_for(latest)
    async with (
        connect(ws_url + "/ws") as one,
        connect(ws_url + "/ws/prices?market=KRW-BTC") as two,
    ):
        for ws in (one, two):
            first = await receive(ws)
            assert first["ts"] == original["ts"]
            assert first["source"] == "stub"
        updated = await publish(r, "99000000", original["ts"] + 1)
        for ws in (one, two):
            assert (await receive(ws))["price"] == "99000000"
        async with connect(ws_url + "/ws") as late:
            assert (await receive(late))["ts"] == updated["ts"]

    async def empty():
        return not app.state.hub.clients

    await wait_for(empty)
    async with httpx.AsyncClient() as client:
        assert (await client.get(http + "/readyz")).status_code == 200


async def test_missing_and_stale_prices_are_explicit(live):
    _, r, _, ws_url = live
    async with connect(ws_url + "/ws") as ws:
        assert (await receive(ws, "status"))["reason"] == "price_unavailable"
        old = await publish(r, "98000000", int(time.time() * 1000) - 10000)
        assert (await receive(ws))["ts"] == old["ts"]
        assert (await receive(ws, "status"))["reason"] == "stale_price"


async def test_bad_market_and_connection_limit(live):
    app, _, _, ws_url = live
    with pytest.raises(InvalidStatus):
        async with connect(ws_url + "/ws?market=KRW-ETH"):
            pass
    object.__setattr__(app.state.hub.settings, "max_clients", 1)
    async with connect(ws_url + "/ws"):
        with pytest.raises(InvalidStatus):
            async with connect(ws_url + "/ws"):
                pass


async def test_recovers_after_subscription_connection_killed(live):
    app, r, _, ws_url = live
    await publish(r, "98000000")
    # Simulate dropped Pub/Sub TCP connection; periodic GET also heals missed publications.
    await r.client_kill_filter(_type="pubsub")
    updated = await publish(r, "99000000")

    async def recovered():
        return (
            app.state.hub.ready
            and app.state.hub.latest
            and app.state.hub.latest["price"] == "99000000"
        )

    await wait_for(recovered)
    async with connect(ws_url + "/ws") as ws:
        assert (await receive(ws))["ts"] == updated["ts"]

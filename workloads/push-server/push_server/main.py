import asyncio
import json
import logging
import os
import random
import re
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

LOG = logging.getLogger("push-server")


@dataclass(frozen=True)
class Settings:
    redis_url: str = "redis://redis:6379/0"
    market: str = "KRW-BTC"
    stale_ms: int = 5000
    max_clients: int = 1000
    send_timeout: float = 5
    redis_timeout: float = 2
    heartbeat_seconds: float = 2

    @classmethod
    def from_env(cls):
        settings = cls(
            redis_url=os.getenv("REDIS_URL", cls.redis_url),
            market=os.getenv("MARKET", cls.market),
            stale_ms=int(os.getenv("STALE_MS", cls.stale_ms)),
            max_clients=int(os.getenv("MAX_CLIENTS", cls.max_clients)),
            send_timeout=float(os.getenv("SEND_TIMEOUT_SECONDS", cls.send_timeout)),
        )
        if settings.market != "KRW-BTC":
            raise ValueError("Only KRW-BTC is supported")
        if min(settings.stale_ms, settings.max_clients, settings.send_timeout) <= 0:
            raise ValueError("Timeouts and limits must be positive")
        return settings


def parse_price(raw, market):
    """Keep collector timestamps and decimal strings intact; never refresh old data."""
    if not isinstance(raw, (str, bytes)) or len(raw) > 4096:
        raise ValueError("Invalid price message size")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Price must be an object")
    price, ts = data.get("price"), data.get("ts")
    if not isinstance(price, str) or not re.fullmatch(
        r"[0-9]{1,40}(\.[0-9]{1,40})?", price
    ):
        raise ValueError("Price must be a decimal string")
    if not Decimal(price).is_finite() or Decimal(price) <= 0:
        raise ValueError("Price must be positive")
    if type(ts) is not int or not 0 < ts <= 9007199254740991:
        raise ValueError("Timestamp must be positive Unix milliseconds")
    message = {"type": "price", "market": market, "price": price, "ts": ts}
    if data.get("source") == "stub":
        message["source"] = "stub"
    return message


@dataclass(eq=False)
class Client:
    ws: WebSocket
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=1))


class Hub:
    def __init__(self, settings, redis_client):
        self.settings = settings
        self.redis = redis_client
        self.channel = f"price:{settings.market}"
        self.latest = None
        self.clients = set()
        self.redis_ok = False
        self.stopping = False
        self.task = None
        self.registry = CollectorRegistry()
        self.connections = Gauge(
            "push_connections", "Active WebSocket clients", registry=self.registry
        )
        self.redis_up = Gauge(
            "push_redis_up",
            "Redis subscription and reads available",
            registry=self.registry,
        )
        self.sent = Counter(
            "push_messages_total",
            "Messages sent to clients",
            ["type"],
            registry=self.registry,
        )
        self.coalesced = Counter(
            "push_coalesced_total", "Superseded queued prices", registry=self.registry
        )
        self.invalid = Counter(
            "push_invalid_messages_total",
            "Invalid Redis prices",
            registry=self.registry,
        )
        self.reconnects = Counter(
            "push_redis_reconnects_total",
            "Redis reconnect attempts",
            registry=self.registry,
        )
        self.price_age = Gauge(
            "push_price_age_seconds",
            "Age of last price; -1 when absent",
            ["market"],
            registry=self.registry,
        )
        self.price_age.labels(settings.market).set_function(
            lambda: max(0, time.time() - self.latest["ts"] / 1000)
            if self.latest
            else -1
        )

    @property
    def ready(self):
        return (
            self.redis_ok
            and not self.stopping
            and self.task is not None
            and not self.task.done()
        )

    def offer(self, client, message):
        if client.queue.full():
            client.queue.get_nowait()
            self.coalesced.inc()
        client.queue.put_nowait(message)

    def ingest(self, raw):
        if raw is None:
            return
        try:
            message = parse_price(raw, self.settings.market)
        except (ValueError, TypeError, InvalidOperation):
            self.invalid.inc()
            return
        # Subscribe before GET. Buffered older messages must not replace that snapshot.
        # Equal timestamps can represent different ticks received in the same millisecond.
        if self.latest and (
            message["ts"] < self.latest["ts"] or message == self.latest
        ):
            return
        self.latest = message
        for client in tuple(self.clients):
            self.offer(client, message)

    async def snapshot(self):
        self.ingest(await self.redis.get(self.channel))

    async def close_clients(self, code):
        async def close(client):
            with suppress(Exception):
                await asyncio.wait_for(
                    client.ws.close(code=code), self.settings.send_timeout
                )

        await asyncio.gather(*(close(c) for c in tuple(self.clients)))

    async def relay(self):
        backoff = 0.25
        while not self.stopping:
            try:
                async with self.redis.pubsub() as ps:
                    await ps.subscribe(self.channel)
                    # subscribe() sends the command; wait for the acknowledgement.
                    async with asyncio.timeout(self.settings.redis_timeout):
                        while True:
                            ack = await ps.get_message(timeout=1)
                            if ack and ack["type"] == "subscribe":
                                break
                    self.latest = None
                    await self.snapshot()
                    self.redis_ok = True
                    self.redis_up.set(1)
                    backoff = 0.25
                    LOG.info("Subscribed to %s", self.channel)
                    next_check = time.monotonic() + self.settings.heartbeat_seconds
                    pong_deadline = None
                    while not self.stopping:
                        item = await ps.get_message(timeout=0.5)
                        if item:
                            if item["type"] == "message":
                                self.ingest(item["data"])
                            elif item["type"] == "pong":
                                pong_deadline = None
                        now = time.monotonic()
                        if pong_deadline and now >= pong_deadline:
                            raise TimeoutError("Redis subscription heartbeat timed out")
                        if now >= next_check:
                            # Also recovers a snapshot after redis-py's automatic resubscription.
                            await self.snapshot()
                            if pong_deadline is None:
                                await ps.ping()
                                pong_deadline = now + self.settings.redis_timeout
                            next_check = now + self.settings.heartbeat_seconds
            except asyncio.CancelledError:
                raise
            except (RedisError, OSError, TimeoutError) as exc:
                LOG.warning("Redis unavailable: %s", type(exc).__name__)
                self.redis_ok = False
                self.redis_up.set(0)
                await self.close_clients(1013)
                self.reconnects.inc()
                await asyncio.sleep(random.uniform(backoff / 2, backoff))
                backoff = min(backoff * 2, 10)
            finally:
                self.redis_ok = False
                self.redis_up.set(0)

    async def sender(self, client):
        last_status = None
        while True:
            try:
                message = await asyncio.wait_for(client.queue.get(), 1)
                last_status = None
            except TimeoutError:
                reason = (
                    "price_unavailable"
                    if not self.latest
                    else (
                        "stale_price"
                        if time.time() * 1000 - self.latest["ts"]
                        >= self.settings.stale_ms
                        else None
                    )
                )
                if reason is None or reason == last_status:
                    continue
                message = {
                    "type": "status",
                    "market": self.settings.market,
                    "reason": reason,
                }
                last_status = reason
            await asyncio.wait_for(
                client.ws.send_json(message), self.settings.send_timeout
            )
            self.sent.labels(message["type"]).inc()


def create_app(settings=None, redis_client=None):
    settings = settings or Settings.from_env()
    client = redis_client or redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=settings.redis_timeout,
        socket_timeout=settings.redis_timeout,
        retry=Retry(NoBackoff(), 0),
        health_check_interval=5,
        max_connections=8,
    )
    hub = Hub(settings, client)

    @asynccontextmanager
    async def lifespan(app):
        hub.task = asyncio.create_task(hub.relay(), name="redis-relay")
        try:
            yield
        finally:
            hub.stopping = True
            hub.task.cancel()
            with suppress(asyncio.CancelledError):
                await hub.task
            await hub.close_clients(1012)
            if redis_client is None:
                await client.aclose()

    app = FastAPI(title="ChaoStock Push Server", lifespan=lifespan)
    app.state.hub = hub

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz():
        return JSONResponse(
            {"status": "ready" if hub.ready else "not_ready"},
            status_code=200 if hub.ready else 503,
        )

    @app.get("/metrics")
    async def metrics():
        return Response(
            generate_latest(hub.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    @app.websocket("/ws")
    @app.websocket("/ws/prices")
    async def prices(ws: WebSocket):
        if ws.query_params.get("market", settings.market) != settings.market:
            await ws.close(code=1008)
            return
        if not hub.ready or len(hub.clients) >= settings.max_clients:
            await ws.close(code=1013)
            return
        # Register before the first await so simultaneous connects respect MAX_CLIENTS.
        browser = Client(ws)
        hub.clients.add(browser)
        hub.connections.inc()
        tasks = []
        try:
            await ws.accept()
            # Atomic with respect to ingest (no await); latest already follows subscription.
            if hub.latest:
                hub.offer(browser, hub.latest)

            async def receive():
                while True:
                    event = await ws.receive()
                    if event["type"] == "websocket.disconnect":
                        return
                    # This endpoint is a read-only subscription selected by query string.
                    await ws.close(code=1008)
                    return

            tasks = [
                asyncio.create_task(hub.sender(browser)),
                asyncio.create_task(receive()),
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except (WebSocketDisconnect, OSError, RuntimeError, TimeoutError):
            with suppress(Exception):
                await ws.close(code=1013)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            hub.clients.discard(browser)
            hub.connections.dec()

    return app

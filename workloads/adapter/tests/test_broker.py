import asyncio
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest

from adapter.broker import Broker, RateLimiter, Retry, retry_after
from adapter.metrics import Metrics
from adapter.worker import Worker

from .conftest import MemoryStore, filled


async def test_timeout_after_fill_is_reconciled_without_second_submit(
    settings, payload
):
    orders = {}
    calls = []

    async def upstream(request):
        calls.append(request.method)
        if request.method == "GET":
            return (
                httpx.Response(200, json=orders["test-1"])
                if orders
                else httpx.Response(404)
            )
        assert request.headers["Idempotency-Key"] == "test-1"
        assert json.loads(request.content) == payload
        orders["test-1"] = filled(payload)
        raise httpx.ReadTimeout("response lost")

    store = MemoryStore()
    await store.accept(payload)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://broker.test"
    ) as client:
        broker = Broker(client, RateLimiter(10000))
        worker = Worker(settings, store, broker, Metrics(), asyncio.Event())
        await worker.process(await store.claim(30))
        assert (await store.get("test-1"))["status"] == "pending"
        # 새 worker도 같은 대기열을 읽고 외부 조회로 이전 체결을 복원함.
        restarted = Worker(settings, store, broker, Metrics(), asyncio.Event())
        await restarted.process(await store.claim(30))
    assert calls == ["GET", "POST", "GET"]
    assert (await store.get("test-1"))["status"] == "filled"


async def test_rate_limit_honors_retry_after_and_exhaustion_is_unknown(
    settings, payload
):
    store = MemoryStore()
    await store.accept(payload)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(429, headers={"Retry-After": "12"})
        ),
        base_url="http://broker.test",
    ) as client:
        worker = Worker(
            replace(settings, max_attempts=2),
            store,
            Broker(client, RateLimiter(10000)),
            Metrics(),
            asyncio.Event(),
        )
        await worker.process(await store.claim(30))
        assert store.delays[0] >= 12
        await worker.process(await store.claim(30))
    assert (await store.get("test-1"))["status"] == "unknown"


@pytest.mark.parametrize(
    "status,body",
    [
        (200, {"status": "filled"}),
        (200, {"override": "mismatch"}),
        (200, {"override": "negative"}),
        (200, {"override": "missing_time"}),
        (400, {"override": "valid"}),
        (503, {}),
        (302, {}),
    ],
)
async def test_bad_response_is_not_a_fill(payload, status, body):
    result = filled(payload)
    if body.get("override") == "mismatch":
        result["volume"] = "0.002"
    elif body.get("override") == "negative":
        result["fill_price"] = "-1"
    elif body.get("override") == "missing_time":
        result["filled_at"] = None
    elif not body.get("override"):
        result = body
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status, json=result)
        ),
        base_url="http://broker.test",
    ) as client:
        with pytest.raises(Retry):
            await Broker(client, RateLimiter(10000)).reconcile(payload)


async def test_business_rejection_and_pending_poll(payload):
    responses = iter(
        [
            httpx.Response(200, json={**payload, "status": "pending"}),
            httpx.Response(
                422, json={**payload, "status": "rejected", "reason": "market_closed"}
            ),
        ]
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: next(responses)),
        base_url="http://broker.test",
    ) as client:
        broker = Broker(client, RateLimiter(10000))
        with pytest.raises(Retry) as exc:
            await broker.reconcile(payload)
        assert exc.value.reason == "broker_pending"
        result = await broker.reconcile(payload)
        assert result.status == "rejected" and result.reason == "market_closed"


async def test_expired_last_lease_only_looks_up_never_submits(settings, payload):
    calls = []

    def upstream(request):
        calls.append(request.method)
        return httpx.Response(404)

    store = MemoryStore()
    await store.accept(payload)
    store.rows["test-1"]["attempts"] = settings.max_attempts
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://broker.test"
    ) as client:
        worker = Worker(
            settings,
            store,
            Broker(client, RateLimiter(10000)),
            Metrics(),
            asyncio.Event(),
        )
        await worker.process(await store.claim(30))
    assert calls == ["GET"]
    assert (await store.get("test-1"))["status"] == "unknown"


async def test_total_request_timeout(payload):
    async def upstream(request):
        await asyncio.sleep(10)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://broker.test"
    ) as client:
        with pytest.raises(Retry) as exc:
            await Broker(client, RateLimiter(10000), timeout=0.01).reconcile(payload)
        assert exc.value.reason == "broker_transport_error"


async def test_rate_limiter_counts_all_calls():
    now = 0.0
    waits = []

    async def sleep(delay):
        nonlocal now
        waits.append(delay)
        now += delay

    limiter = RateLimiter(2, clock=lambda: now, sleep=sleep)
    await asyncio.gather(*(limiter.wait() for _ in range(4)))
    assert waits == [0, 0.5, 0.5, 0.5]


def test_retry_after_values():
    assert retry_after("5") == 5
    for value in [None, "NaN", "inf", "bad", "-3"]:
        assert retry_after(value) == 0
    future = format_datetime(
        datetime.now(timezone.utc) + timedelta(seconds=10), usegmt=True
    )
    assert 8 < retry_after(future) <= 10

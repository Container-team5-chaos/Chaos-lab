"""ADAPTER_TEST_DATABASE_URL이 있는 경우 전용 임시 schema에서 실제 PostgreSQL 검증."""

import asyncio
import os
import uuid

import asyncpg
import httpx
import pytest
import pytest_asyncio

from adapter.store import Conflict, Store
from adapter.broker import Broker, RateLimiter
from adapter.metrics import Metrics
from adapter.worker import Worker

from .conftest import filled


@pytest_asyncio.fixture
async def db_store():
    dsn = os.getenv("ADAPTER_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("set ADAPTER_TEST_DATABASE_URL for PostgreSQL integration tests")
    schema = "adapter_test_" + uuid.uuid4().hex
    conn = await asyncpg.connect(dsn)
    pool = None
    try:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        pool = await asyncpg.create_pool(
            dsn, min_size=1, max_size=4, server_settings={"search_path": schema}
        )
        store = Store(pool)
        await store.initialize()
        yield store
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_concurrent_accept_is_durable_and_conflicts(db_store, payload):
    accepted = await asyncio.gather(*(db_store.accept(payload) for _ in range(20)))
    assert sum(not replayed for _, replayed in accepted) == 1
    assert await db_store.pending_count() == 1
    assert (await Store(db_store.pool).get("test-1"))["payload"] == payload
    with pytest.raises(Conflict):
        await db_store.accept({**payload, "volume": "0.00200000"})
    await db_store.initialize()  # 재기동 시 DDL 재실행으로 대기열을 지우지 않음.
    assert await db_store.pending_count() == 1


async def test_single_claim_lease_recovery_and_fencing(db_store, payload):
    await db_store.accept(payload)
    claims = await asyncio.gather(*(db_store.claim(30) for _ in range(4)))
    old = next(job for job in claims if job is not None)
    assert sum(job is not None for job in claims) == 1
    async with db_store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE adapter_orders SET leased_until = now() - interval '1 second'"
        )
    recovered = await Store(db_store.pool).claim(30)
    assert recovered["attempts"] == 2 and recovered["lease_token"] != old["lease_token"]
    await db_store.finish(old, "filled", {"stale": True})
    assert (await db_store.get("test-1"))["status"] == "pending"
    await db_store.finish(recovered, "filled", {"confirmed": True})
    assert (await db_store.get("test-1"))["broker_result"] == {"confirmed": True}
    assert await db_store.claim(30) is None


async def test_retry_is_scheduled_and_survives_new_store(db_store, payload):
    await db_store.accept(payload)
    job = await db_store.claim(30)
    await db_store.retry(job, 30, "broker_unavailable")
    restarted = Store(db_store.pool)
    assert await restarted.claim(30) is None
    assert (await restarted.get("test-1"))["last_error"] == "broker_unavailable"
    async with db_store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE adapter_orders SET next_attempt_at = now() - interval '1 second'"
        )
    job = await restarted.claim(30)
    await restarted.finish(job, "unknown", error="broker_transport_error")
    assert await restarted.pending_count() == 0


async def test_lost_response_recovery_with_real_database(db_store, settings, payload):
    upstream_orders = {}
    submits = 0

    async def upstream(request):
        nonlocal submits
        if request.method == "GET":
            return (
                httpx.Response(200, json=upstream_orders["test-1"])
                if upstream_orders
                else httpx.Response(404)
            )
        submits += 1
        upstream_orders["test-1"] = filled(payload)
        raise httpx.ReadTimeout("committed upstream, response lost")

    await db_store.accept(payload)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://broker.test"
    ) as client:
        broker = Broker(client, RateLimiter(10000))
        worker = Worker(settings, db_store, broker, Metrics(), asyncio.Event())
        await worker.process(await db_store.claim(30))
        assert (await db_store.get("test-1"))["status"] == "pending"
        async with db_store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE adapter_orders SET next_attempt_at = now() - interval '1 second'"
            )
        reopened = Store(db_store.pool)
        restarted_worker = Worker(
            settings, reopened, broker, Metrics(), asyncio.Event()
        )
        await restarted_worker.process(await reopened.claim(30))
    assert submits == 1
    assert (await db_store.get("test-1"))["status"] == "filled"

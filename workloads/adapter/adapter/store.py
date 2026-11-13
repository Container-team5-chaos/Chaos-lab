import json
import uuid
from pathlib import Path

import asyncpg


class Conflict(Exception):
    pass


def decode(row):
    if row is None:
        return None
    result = dict(row)
    for field in ("payload", "broker_result"):
        if isinstance(result[field], str):
            result[field] = json.loads(result[field])
    return result


class Store:
    def __init__(self, pool):
        self.pool = pool

    @classmethod
    async def connect(cls, settings):
        pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=1,
            max_size=settings.db_pool_size,
            timeout=5,
            command_timeout=5,
        )
        return cls(pool)

    async def close(self):
        await self.pool.close()

    async def initialize(self):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 동시에 시작한 init container 사이의 DDL 충돌을 방지함.
                await conn.execute("SELECT pg_advisory_xact_lock(728401932)")
                await conn.execute(
                    Path(__file__).with_name("schema.sql").read_text("utf-8")
                )

    async def ready(self):
        async with self.pool.acquire() as conn:
            await conn.fetchval("SELECT 1 FROM adapter_orders LIMIT 1")

    async def accept(self, payload):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """INSERT INTO adapter_orders (client_order_id, payload)
                       VALUES ($1, $2::jsonb) ON CONFLICT DO NOTHING RETURNING *""",
                    payload["client_order_id"],
                    json.dumps(payload),
                )
                if row is not None:
                    return decode(row), False
                existing = decode(
                    await conn.fetchrow(
                        "SELECT * FROM adapter_orders WHERE client_order_id = $1",
                        payload["client_order_id"],
                    )
                )
                if existing["payload"] != payload:
                    raise Conflict()
                return existing, True

    async def get(self, key):
        async with self.pool.acquire() as conn:
            return decode(
                await conn.fetchrow(
                    "SELECT * FROM adapter_orders WHERE client_order_id = $1",
                    key,
                )
            )

    async def claim(self, lease_seconds):
        async with self.pool.acquire() as conn:
            return decode(
                await conn.fetchrow(
                    """WITH candidate AS (
                       SELECT client_order_id FROM adapter_orders
                       WHERE status = 'pending' AND next_attempt_at <= now()
                         AND (leased_until IS NULL OR leased_until <= now())
                       ORDER BY next_attempt_at, created_at
                       FOR UPDATE SKIP LOCKED LIMIT 1
                   )
                   UPDATE adapter_orders AS o
                   SET attempts = o.attempts + 1, lease_token = $1,
                       leased_until = now() + $2 * interval '1 second', updated_at = now()
                   FROM candidate AS c WHERE o.client_order_id = c.client_order_id
                   RETURNING o.*""",
                    uuid.uuid4(),
                    lease_seconds,
                )
            )

    async def finish(self, job, status, result=None, error=None):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE adapter_orders SET status = $3, broker_result = $4::jsonb,
                       last_error = $5, lease_token = NULL, leased_until = NULL,
                       updated_at = now()
                   WHERE client_order_id = $1 AND lease_token = $2""",
                job["client_order_id"],
                job["lease_token"],
                status,
                None if result is None else json.dumps(result),
                error,
            )

    async def retry(self, job, delay, error):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE adapter_orders SET last_error = $3,
                       next_attempt_at = now() + $4 * interval '1 second',
                       lease_token = NULL, leased_until = NULL, updated_at = now()
                   WHERE client_order_id = $1 AND lease_token = $2""",
                job["client_order_id"],
                job["lease_token"],
                error,
                delay,
            )

    async def pending_count(self):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT count(*) FROM adapter_orders WHERE status = 'pending'"
            )

"""예약과 외부 전달을 원자적으로 기록하고, 확정 체결만 한 번 정산함."""

import asyncio
import json
import logging
import re
import uuid
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path

import asyncpg

from .adapter_client import Uncertain
from .metrics import EXTERNAL_UNKNOWN, ORDERS
from .orders import Rejected, find_order, get_price, replay_or_conflict
from .reservations import reserved

LOG = logging.getLogger("order.external")


async def initialize(pool):
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(728401933)")
            await conn.execute(
                Path(__file__).with_name("external_schema.sql").read_text("utf-8")
            )


async def details(conn, row):
    row = dict(row)
    if await conn.fetchval("SELECT to_regclass('order_external')") is not None:
        reason = await conn.fetchrow(
            "SELECT reason FROM order_external WHERE client_order_id = $1",
            row["client_order_id"],
        )
        if reason is not None:
            row["execution_mode"] = "adapter"
            row["reason"] = reason["reason"]
    return row


async def get_order(pool, key):
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            row = await find_order(conn, key)
            if row is None:
                raise Rejected(404, "order_not_found")
            return await details(conn, row)


async def accept(pool, redis, req):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}", req.client_order_id):
        raise Rejected(400, "invalid_request")
    async with pool.acquire() as conn:
        existing = await find_order(conn, req.client_order_id)
        if existing:
            return await details(conn, await replay_or_conflict(existing, req)), True
        price = await get_price(redis, req.market)
        if not price.is_finite() or price <= 0 or price >= Decimal("1e16"):
            raise Rejected(503, "price_unavailable")
        with localcontext() as ctx:
            ctx.prec = 64
            budget = (price * req.volume).quantize(
                Decimal("0.0001"), rounding=ROUND_DOWN
            )
        if budget >= Decimal("1e20"):
            raise Rejected(400, "invalid_request")
        async with conn.transaction():
            account = await conn.fetchrow(
                "SELECT krw FROM accounts WHERE user_id = $1 FOR UPDATE",
                req.user_id,
            )
            if account is None:
                raise Rejected(404, "user_not_found")
            # 계좌 잠금을 기다리는 동안 같은 주문이 먼저 접수됐을 수 있음.
            existing = await find_order(conn, req.client_order_id)
            if existing:
                return await details(
                    conn, await replay_or_conflict(existing, req)
                ), True
            position = await conn.fetchrow(
                "SELECT volume FROM positions WHERE user_id = $1 AND market = $2 FOR UPDATE",
                req.user_id,
                req.market,
            )
            reserved_krw, reserved_volume = await reserved(
                conn, req.user_id, req.market
            )
            held = position["volume"] if position else Decimal(0)
            if req.side == "bid" and account["krw"] - reserved_krw < budget:
                raise Rejected(422, "insufficient_balance")
            if req.side == "ask" and held - reserved_volume < req.volume:
                raise Rejected(422, "insufficient_volume")
            # 다른 사용자가 같은 전역 키를 동시에 쓴 경우에도 변경 없이 충돌로 반환함.
            row = await conn.fetchrow(
                """INSERT INTO orders (client_order_id, user_id, market, side, volume, status)
                   VALUES ($1, $2, $3, $4, $5, 'pending')
                   ON CONFLICT (client_order_id) DO NOTHING RETURNING *""",
                req.client_order_id,
                req.user_id,
                req.market,
                req.side,
                req.volume,
            )
            if row is None:
                existing = await find_order(conn, req.client_order_id)
                return await details(
                    conn, await replay_or_conflict(existing, req)
                ), True
            payload = req.model_dump(mode="json")
            payload["volume"] = f"{req.volume:.8f}"
            if req.side == "bid":
                payload["max_amount"] = f"{budget:.4f}"
            await conn.execute(
                """INSERT INTO order_external (client_order_id, payload, reserved_krw, reserved_volume)
                   VALUES ($1, $2::jsonb, $3, $4)""",
                req.client_order_id,
                json.dumps(payload),
                budget if req.side == "bid" else Decimal(0),
                req.volume if req.side == "ask" else Decimal(0),
            )
            return {**dict(row), "execution_mode": "adapter", "reason": None}, False


class ExternalWorker:
    def __init__(self, pool, adapter, *, poll=0.5, timeout=5):
        self.pool = pool
        self.adapter = adapter
        self.poll = poll
        self.lease = 2 * timeout + 30

    async def claim(self):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """WITH candidate AS (
                       SELECT e.client_order_id FROM order_external e
                       JOIN orders o USING(client_order_id)
                       WHERE o.status = 'pending' AND e.next_attempt_at <= now()
                         AND (e.leased_until IS NULL OR e.leased_until <= now())
                       ORDER BY e.next_attempt_at FOR UPDATE OF e SKIP LOCKED LIMIT 1
                   ) UPDATE order_external e
                   SET lease_token = $1, leased_until = now() + $2 * interval '1 second',
                       attempts = attempts + 1
                   FROM candidate c WHERE e.client_order_id = c.client_order_id
                   RETURNING e.*""",
                uuid.uuid4(),
                self.lease,
            )
            if row is None:
                return None
            job = dict(row)
            job["payload"] = json.loads(job["payload"])
            return job

    async def retry(self, job, delay):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE order_external SET next_attempt_at = now() + $3 * interval '1 second',
                       lease_token = NULL, leased_until = NULL
                   WHERE client_order_id = $1 AND lease_token = $2""",
                job["client_order_id"],
                job["lease_token"],
                delay,
            )

    async def settle(self, job, receipt):
        payload = job["payload"]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 예약 접수·로컬 체결과 같은 계좌→포지션 잠금 순서를 사용함.
                account = await conn.fetchrow(
                    "SELECT krw FROM accounts WHERE user_id = $1 FOR UPDATE",
                    payload["user_id"],
                )
                external = await conn.fetchrow(
                    "SELECT * FROM order_external WHERE client_order_id = $1 FOR UPDATE",
                    job["client_order_id"],
                )
                row = await conn.fetchrow(
                    "SELECT * FROM orders WHERE client_order_id = $1 FOR UPDATE",
                    job["client_order_id"],
                )
                if (
                    external is None
                    or external["lease_token"] != job["lease_token"]
                    or row["status"] != "pending"
                ):
                    return False
                status = receipt["status"]
                if status == "filled":
                    pos = await conn.fetchrow(
                        "SELECT volume, avg_price FROM positions WHERE user_id = $1 AND market = $2 FOR UPDATE",
                        row["user_id"],
                        row["market"],
                    )
                    held = pos["volume"] if pos else Decimal(0)
                    avg = pos["avg_price"] if pos else Decimal(0)
                    price, amount = receipt["price"], receipt["amount"]
                    cash_hold, volume_hold = await reserved(
                        conn, row["user_id"], row["market"]
                    )
                    if row["side"] == "bid":
                        safe = (
                            amount <= external["reserved_krw"]
                            and account["krw"] - (cash_hold - external["reserved_krw"])
                            >= amount
                        )
                        volume = held + row["volume"]
                        with localcontext() as ctx:
                            ctx.prec = 64
                            avg = (
                                (held * avg + row["volume"] * price) / volume
                            ).quantize(Decimal("0.00000001"))
                        delta = -amount
                    else:
                        safe = held - (
                            volume_hold - external["reserved_volume"]
                        ) >= row["volume"] and account["krw"] + amount < Decimal("1e20")
                        volume = held - row["volume"]
                        delta = amount
                    if not safe or volume >= Decimal("1e22"):
                        receipt = {
                            "status": "unknown",
                            "reason": "settlement_inconsistent",
                        }
                        status = "unknown"
                    else:
                        await conn.execute(
                            "UPDATE accounts SET krw = krw + $1 WHERE user_id = $2",
                            delta,
                            row["user_id"],
                        )
                        await conn.execute(
                            """INSERT INTO positions(user_id, market, volume, avg_price)
                               VALUES($1, $2, $3, $4) ON CONFLICT(user_id, market)
                               DO UPDATE SET volume = $3, avg_price = $4, updated_at = now()""",
                            row["user_id"],
                            row["market"],
                            volume,
                            avg,
                        )
                        await conn.execute(
                            """UPDATE orders SET status = 'filled', fill_price = $2, amount = $3, filled_at = $4
                               WHERE client_order_id = $1""",
                            row["client_order_id"],
                            price,
                            amount,
                            receipt["filled_at"],
                        )
                if status != "filled":
                    await conn.execute(
                        "UPDATE orders SET status = $2 WHERE client_order_id = $1",
                        row["client_order_id"],
                        status,
                    )
                await conn.execute(
                    """UPDATE order_external SET reason = $2, lease_token = NULL, leased_until = NULL,
                       reserved_krw = CASE WHEN $3 THEN 0 ELSE reserved_krw END,
                       reserved_volume = CASE WHEN $3 THEN 0 ELSE reserved_volume END
                       WHERE client_order_id = $1""",
                    row["client_order_id"],
                    receipt.get("reason"),
                    status in {"filled", "rejected"},
                )
        if status == "unknown":
            EXTERNAL_UNKNOWN.inc()
        else:
            ORDERS.labels(
                status, "broker_rejected" if status == "rejected" else ""
            ).inc()
        return True

    async def process(self, job):
        try:
            receipt = await self.adapter.reconcile(job["payload"])
        except Uncertain as exc:
            receipt = {"status": "unknown", "reason": str(exc)}
        if receipt is None or receipt["status"] == "pending":
            delay = (
                min(30, 2 ** min(job["attempts"] - 1, 5))
                if receipt is None
                else self.poll
            )
            await self.retry(job, delay)
        else:
            await self.settle(job, receipt)

    async def run(self):
        while True:
            try:
                job = await self.claim()
                if job is not None:
                    await self.process(job)
            except (asyncpg.PostgresError, OSError, TimeoutError) as exc:
                LOG.warning(
                    "external reconciliation delayed error_type=%s", type(exc).__name__
                )
            await asyncio.sleep(self.poll)

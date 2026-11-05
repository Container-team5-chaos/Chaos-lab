"""주문 처리 핵심 로직"""

import json
import time
from decimal import ROUND_DOWN, Decimal

import asyncpg

from .config import STALE_MS
from .metrics import PRICE_AGE
from .schemas import OrderRequest


class Rejected(Exception):
    """API 실패 표의 한 줄에 해당하는 거절."""

    def __init__(self, http_status: int, reason: str):
        self.http_status = http_status
        self.reason = reason


async def get_price(redis, market: str) -> Decimal:
    """Redis 현재가를 읽고, 없거나 오래됐으면 거절."""
    raw = await redis.get(f"price:{market}")
    if raw is None:
        raise Rejected(503, "price_unavailable")
    data = json.loads(raw)
    age_ms = time.time() * 1000 - int(data["ts"])
    PRICE_AGE.labels(market).set(age_ms / 1000)
    if age_ms > STALE_MS:
        raise Rejected(503, "stale_price")
    return Decimal(str(data["price"]))


async def find_order(conn, client_order_id: str):
    return await conn.fetchrow(
        "SELECT * FROM orders WHERE client_order_id = $1", client_order_id
    )


def same_request(row, req: OrderRequest) -> bool:
    return (row["user_id"], row["market"], row["side"], row["volume"]) == (
        req.user_id,
        req.market,
        req.side,
        req.volume,
    )


async def replay_or_conflict(row, req: OrderRequest):
    """이미 처리된 주문: 내용이 같으면 그대로 돌려주고, 다르면 409."""
    if not same_request(row, req):
        raise Rejected(409, "idempotency_conflict")
    return row


async def place_order(pool, redis, req: OrderRequest):
    """주문을 처리하고 (주문 행, replayed 여부)를 반환한다."""
    async with pool.acquire() as conn:
        # 1. 이미 처리한 주문인지 먼저 확인 (멱등)
        existing = await find_order(conn, req.client_order_id)
        if existing:
            return await replay_or_conflict(existing, req), True

        # 2. 현재가 확인
        price = await get_price(redis, req.market)
        amount = (price * req.volume).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

        # 3. 한 트랜잭션 안에서 잔고·보유량·주문을 함께 반영
        try:
            async with conn.transaction():
                acc = await conn.fetchrow(
                    "SELECT krw FROM accounts WHERE user_id = $1 FOR UPDATE",
                    req.user_id,
                )
                if acc is None:
                    raise Rejected(404, "user_not_found")

                pos = await conn.fetchrow(
                    "SELECT volume, avg_price FROM positions "
                    "WHERE user_id = $1 AND market = $2 FOR UPDATE",
                    req.user_id,
                    req.market,
                )
                held = pos["volume"] if pos else Decimal(0)
                avg = pos["avg_price"] if pos else Decimal(0)

                if req.side == "bid":
                    if acc["krw"] < amount:
                        raise Rejected(422, "insufficient_balance")
                    new_volume = held + req.volume
                    new_avg = ((held * avg + req.volume * price) / new_volume).quantize(
                        Decimal("0.00000001")
                    )
                    krw_delta = -amount
                else:
                    if held < req.volume:
                        raise Rejected(422, "insufficient_volume")
                    new_volume = held - req.volume
                    new_avg = avg  # 매도는 평균 매수가를 바꾸지 않음
                    krw_delta = amount

                await conn.execute(
                    "UPDATE accounts SET krw = krw + $1 WHERE user_id = $2",
                    krw_delta,
                    req.user_id,
                )
                await conn.execute(
                    """INSERT INTO positions (user_id, market, volume, avg_price)
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT (user_id, market)
                       DO UPDATE SET volume = $3, avg_price = $4, updated_at = now()""",
                    req.user_id,
                    req.market,
                    new_volume,
                    new_avg,
                )
                row = await conn.fetchrow(
                    """INSERT INTO orders (client_order_id, user_id, market, side, volume,
                                           status, fill_price, amount, filled_at)
                       VALUES ($1, $2, $3, $4, $5, 'filled', $6, $7, now())
                       RETURNING *""",
                    req.client_order_id,
                    req.user_id,
                    req.market,
                    req.side,
                    req.volume,
                    price,
                    amount,
                )
                return row, False

        except asyncpg.UniqueViolationError:
            # 같은 client_order_id가 동시에 들어와 다른 요청이 먼저 저장한 경우.
            # 트랜잭션은 이미 롤백됐으므로 잔고는 변하지 않았다. 먼저 저장된 주문을 돌려준다.
            existing = await find_order(conn, req.client_order_id)
            return await replay_or_conflict(existing, req), True


async def get_account(pool, user_id: str):
    async with pool.acquire() as conn:
        acc = await conn.fetchrow(
            "SELECT krw FROM accounts WHERE user_id = $1", user_id
        )
        if acc is None:
            raise Rejected(404, "user_not_found")
        positions = await conn.fetch(
            "SELECT market, volume, avg_price FROM positions "
            "WHERE user_id = $1 AND volume > 0 ORDER BY market",
            user_id,
        )
        return acc, positions

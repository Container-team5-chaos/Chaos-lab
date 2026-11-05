"""테스트 준비: 매 테스트마다 DB를 seed.sql로 초기화하고 Redis에 현재가를 넣는다.

먼저 docker에 PostgreSQL과 Redis를 띄워 둬야 함.
"""

import json
import pathlib
import time

import asyncpg
import pytest_asyncio
import redis.asyncio as aioredis
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from app import config
from app.main import app

SEED = (pathlib.Path(__file__).parent.parent / "db" / "seed.sql").read_text(
    encoding="utf-8"
)
PRICE = "98000000"


async def set_price(price: str = PRICE, age_ms: int = 0, market: str = "KRW-BTC"):
    r = aioredis.from_url(config.REDIS_URL)
    ts = int(time.time() * 1000) - age_ms
    await r.set(f"price:{market}", json.dumps({"price": price, "ts": ts}))
    await r.aclose()


async def delete_price(market: str = "KRW-BTC"):
    r = aioredis.from_url(config.REDIS_URL)
    await r.delete(f"price:{market}")
    await r.aclose()


@pytest_asyncio.fixture
async def db():
    conn = await asyncpg.connect(config.DATABASE_URL)
    await conn.execute(SEED)
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def client(db):
    await set_price()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c

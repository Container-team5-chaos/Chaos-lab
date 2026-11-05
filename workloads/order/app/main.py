"""주문 서비스 진입점. 경로와 상태 코드는 API를 따른다."""

import time
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from . import config
from .metrics import DURATION, ORDERS, REPLAYED
from .orders import Rejected, get_account, place_order
from .schemas import OrderRequest, dec, order_to_dict


class State:
    shutting_down = False


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        config.DATABASE_URL, min_size=1, max_size=config.DB_POOL_SIZE
    )
    app.state.redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    yield
    await app.state.redis.aclose()
    await app.state.pool.close()


app = FastAPI(title="order", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


def rejected_body(reason: str, client_order_id: str | None = None) -> dict:
    return {"status": "rejected", "reason": reason, "client_order_id": client_order_id}


@app.exception_handler(RequestValidationError)
async def invalid_request(request: Request, exc: RequestValidationError):
    ORDERS.labels("rejected", "invalid_request").inc()
    return JSONResponse(status_code=400, content=rejected_body("invalid_request"))


@app.post("/orders")
async def create_order(req: OrderRequest, request: Request):
    started = time.perf_counter()
    if state.shutting_down:
        return JSONResponse(
            status_code=503, content=rejected_body("not_ready", req.client_order_id)
        )
    try:
        row, replayed = await place_order(
            request.app.state.pool, request.app.state.redis, req
        )
    except Rejected as r:
        ORDERS.labels("rejected", r.reason).inc()
        DURATION.labels("rejected").observe(time.perf_counter() - started)
        return JSONResponse(
            status_code=r.http_status,
            content=rejected_body(r.reason, req.client_order_id),
        )
    except (OSError, asyncpg.PostgresConnectionError, aioredis.ConnectionError):
        ORDERS.labels("rejected", "not_ready").inc()
        return JSONResponse(
            status_code=503, content=rejected_body("not_ready", req.client_order_id)
        )

    if replayed:
        REPLAYED.inc()
    else:
        ORDERS.labels("filled", "").inc()
    DURATION.labels("filled").observe(time.perf_counter() - started)
    return order_to_dict(row, replayed)


@app.get("/accounts/{user_id}")
async def read_account(user_id: str, request: Request):
    try:
        acc, positions = await get_account(request.app.state.pool, user_id)
    except Rejected as r:
        return JSONResponse(status_code=r.http_status, content={"reason": r.reason})
    return {
        "user_id": user_id,
        "krw": dec(acc["krw"]),
        "positions": [
            {
                "market": p["market"],
                "volume": dec(p["volume"]),
                "avg_price": dec(p["avg_price"]),
            }
            for p in positions
        ],
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/readyz")
async def readyz(request: Request):
    if state.shutting_down:
        return JSONResponse(
            status_code=503, content={"ready": False, "reason": "shutting_down"}
        )
    try:
        async with request.app.state.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        await request.app.state.redis.ping()
    except Exception as e:
        return JSONResponse(
            status_code=503, content={"ready": False, "reason": type(e).__name__}
        )
    return {"ready": True}

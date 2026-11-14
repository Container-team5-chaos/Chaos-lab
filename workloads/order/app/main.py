"""주문 서비스 진입점. 경로와 상태 코드는 API를 따른다."""

import asyncio
import math
import time
from contextlib import asynccontextmanager

import asyncpg
import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from . import config, external
from .adapter_client import AdapterClient
from .metrics import DURATION, EXTERNAL_ACCEPTED, ORDERS, REPLAYED
from .orders import Rejected, get_account, place_order
from .positions import get_positions
from .schemas import OrderRequest, dec, order_to_dict


class State:
    shutting_down = False


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.shutting_down = False
    app.state.pool = await asyncpg.create_pool(
        config.DATABASE_URL, min_size=1, max_size=config.DB_POOL_SIZE
    )
    app.state.redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    app.state.adapter_worker = None
    client = None
    try:
        if config.ADAPTER_URL:
            if not all(
                math.isfinite(v) and v > 0
                for v in (config.ADAPTER_TIMEOUT, config.ADAPTER_POLL)
            ):
                raise ValueError("adapter timeout and poll must be positive and finite")
            # 데이터 초기화 없이 외부 주문 예약/전달 스키마만 준비함.
            await external.initialize(app.state.pool)
            client = httpx.AsyncClient(
                base_url=config.ADAPTER_URL,
                timeout=config.ADAPTER_TIMEOUT,
                trust_env=False,
            )
            worker = external.ExternalWorker(
                app.state.pool,
                AdapterClient(client, config.ADAPTER_TIMEOUT),
                poll=config.ADAPTER_POLL,
                timeout=config.ADAPTER_TIMEOUT,
            )
            app.state.adapter_worker = asyncio.create_task(worker.run())
        yield
    finally:
        if app.state.adapter_worker is not None:
            app.state.adapter_worker.cancel()
            await asyncio.gather(app.state.adapter_worker, return_exceptions=True)
        if client is not None:
            await client.aclose()
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
    if state.shutting_down or (
        request.app.state.adapter_worker is not None
        and request.app.state.adapter_worker.done()
    ):
        return JSONResponse(
            status_code=503, content=rejected_body("not_ready", req.client_order_id)
        )
    try:
        execute = external.accept if config.ADAPTER_URL else place_order
        row, replayed = await execute(
            request.app.state.pool, request.app.state.redis, req
        )
        if not config.ADAPTER_URL and row["status"] in {
            "pending",
            "unknown",
            "rejected",
        }:
            row = await external.get_order(request.app.state.pool, req.client_order_id)
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
    elif row["status"] == "pending":
        EXTERNAL_ACCEPTED.inc()
    elif row["status"] == "filled" and row.get("execution_mode") != "adapter":
        ORDERS.labels("filled", "").inc()
    DURATION.labels(row["status"]).observe(time.perf_counter() - started)
    return JSONResponse(
        status_code=202
        if row["status"] == "pending"
        else 422
        if row["status"] == "rejected"
        else 200,
        content=order_to_dict(row, replayed),
    )


@app.get("/orders/{client_order_id}")
async def read_order(client_order_id: str, request: Request):
    try:
        return order_to_dict(
            await external.get_order(request.app.state.pool, client_order_id), False
        )
    except Rejected as r:
        return JSONResponse(
            status_code=r.http_status, content=rejected_body(r.reason, client_order_id)
        )
    except (OSError, asyncpg.PostgresConnectionError, TimeoutError):
        return JSONResponse(
            status_code=503, content=rejected_body("not_ready", client_order_id)
        )


@app.get("/accounts/{user_id}")
async def read_account(user_id: str, request: Request):
    try:
        acc, positions = await get_account(request.app.state.pool, user_id)
    except Rejected as r:
        return JSONResponse(status_code=r.http_status, content={"reason": r.reason})
    body = {
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
    if (
        config.ADAPTER_URL
        or acc["reserved_krw"]
        or any(p["reserved_volume"] for p in positions)
    ):
        body.update(
            reserved_krw=dec(acc["reserved_krw"]),
            available_krw=dec(acc["krw"] - acc["reserved_krw"]),
        )
        for result, pos in zip(body["positions"], positions):
            result.update(
                reserved_volume=dec(pos["reserved_volume"]),
                available_volume=dec(pos["volume"] - pos["reserved_volume"]),
            )
    return body


@app.get("/accounts/{user_id}/positions")
async def read_positions(user_id: str, request: Request):
    try:
        return await get_positions(
            request.app.state.pool, request.app.state.redis, user_id
        )
    except Rejected as r:
        return JSONResponse(status_code=r.http_status, content={"reason": r.reason})
    except (OSError, asyncpg.PostgresConnectionError, TimeoutError):
        return JSONResponse(status_code=503, content={"reason": "not_ready"})


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/readyz")
async def readyz(request: Request):
    if state.shutting_down or (
        request.app.state.adapter_worker is not None
        and request.app.state.adapter_worker.done()
    ):
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

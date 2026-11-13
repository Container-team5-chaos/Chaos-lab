import asyncio
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from .broker import Broker, RateLimiter
from .config import Settings
from .metrics import Metrics
from .models import ErrorResponse, OrderRequest, OrderResponse, response
from .store import Conflict, Store
from .worker import Worker

DB_ERRORS = (asyncpg.PostgresError, OSError, TimeoutError)


def create_app(settings=None, *, store=None, broker_client=None, run_worker=True):
    stopping = asyncio.Event()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        cfg = settings or Settings.from_env()
        db = store if store is not None else await Store.connect(cfg)
        client = broker_client or httpx.AsyncClient(
            base_url=cfg.broker_url.rstrip("/"),
            timeout=cfg.request_timeout,
            follow_redirects=False,
            trust_env=False,
        )
        task = None
        try:
            await db.ready()
            stopping.clear()
            app.state.store = db
            if run_worker:
                worker = Worker(
                    cfg,
                    db,
                    Broker(client, RateLimiter(cfg.broker_rps), cfg.request_timeout),
                    metrics,
                    stopping,
                )
                task = asyncio.create_task(worker.run())
            app.state.worker = task
            yield
        finally:
            stopping.set()
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if broker_client is None:
                await client.aclose()
            if store is None:
                await db.close()

    app = FastAPI(title="broker-adapter", version="0.1.0", lifespan=lifespan)
    app.state.stopping = stopping
    app.state.metrics = metrics
    app.mount("/metrics", make_asgi_app(registry=metrics.registry))

    def rejected(reason, key=None, code=503):
        return JSONResponse(
            status_code=code,
            content={
                "status": "rejected",
                "reason": reason,
                "client_order_id": key,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError):
        return rejected("invalid_request", code=400)

    @app.post(
        "/orders",
        status_code=202,
        response_model=OrderResponse,
        responses={
            200: {"model": OrderResponse, "description": "Existing terminal order"},
            400: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def submit(order: OrderRequest, request: Request):
        if stopping.is_set() or (
            request.app.state.worker is not None and request.app.state.worker.done()
        ):
            return rejected("not_ready", order.client_order_id)
        try:
            row, replayed = await request.app.state.store.accept(
                order.model_dump(exclude_none=True)
            )
        except Conflict:
            return rejected("idempotency_conflict", order.client_order_id, 409)
        except DB_ERRORS:
            return rejected("not_ready", order.client_order_id)
        if replayed:
            metrics.replayed.inc()
        else:
            metrics.accepted.inc()
        return JSONResponse(
            status_code=202 if row["status"] == "pending" else 200,
            content=response(row, replayed=replayed),
        )

    @app.get(
        "/orders/{client_order_id}",
        response_model=OrderResponse,
        responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    )
    async def get_order(client_order_id: str, request: Request):
        try:
            row = await request.app.state.store.get(client_order_id)
        except DB_ERRORS:
            return rejected("not_ready", client_order_id)
        if row is None:
            return rejected("order_not_found", client_order_id, 404)
        return response(row)

    @app.get("/healthz")
    async def health():
        return {"ok": True}

    @app.get("/readyz")
    async def ready(request: Request):
        if stopping.is_set() or (
            request.app.state.worker is not None and request.app.state.worker.done()
        ):
            return JSONResponse(status_code=503, content={"ready": False})
        try:
            await request.app.state.store.ready()
        except DB_ERRORS:
            return JSONResponse(status_code=503, content={"ready": False})
        return {"ready": True}

    return app


app = create_app()

import asyncio
import logging
import random

import asyncpg

from .broker import Retry

LOG = logging.getLogger("adapter.worker")


class Worker:
    def __init__(self, settings, store, broker, metrics, stopping):
        self.settings = settings
        self.store = store
        self.broker = broker
        self.metrics = metrics
        self.stopping = stopping

    async def process(self, job):
        try:
            with self.metrics.duration.time():
                result = await self.broker.reconcile(
                    job["payload"],
                    lookup_only=job["attempts"] > self.settings.max_attempts,
                )
        except Retry as exc:
            if job["attempts"] >= self.settings.max_attempts:
                # 응답 유실은 미체결 증거가 아님. 실패/재전송으로 확정하지 않음.
                await self.store.finish(job, "unknown", error=exc.reason)
                self.metrics.completed.labels("unknown").inc()
            else:
                upper = min(
                    self.settings.retry_max,
                    self.settings.retry_base * 2 ** min(job["attempts"] - 1, 30),
                )
                delay = max(random.uniform(upper / 2, upper), exc.after)
                await self.store.retry(job, delay, exc.reason)
                self.metrics.retries.labels(exc.reason).inc()
            return
        await self.store.finish(job, result.status, result.model_dump(mode="json"))
        self.metrics.completed.labels(result.status).inc()

    async def run(self):
        while not self.stopping.is_set():
            try:
                job = await self.store.claim(self.settings.lease_seconds)
                if job is not None:
                    await self.process(job)
                self.metrics.pending.set(await self.store.pending_count())
            except (asyncpg.PostgresError, OSError, TimeoutError) as exc:
                # DB 장애 중엔 lease를 만료시켜 다음 시도에서 외부 주문을 재조회함.
                LOG.warning("database unavailable error_type=%s", type(exc).__name__)
            try:
                await asyncio.wait_for(self.stopping.wait(), self.settings.poll_seconds)
            except TimeoutError:
                pass

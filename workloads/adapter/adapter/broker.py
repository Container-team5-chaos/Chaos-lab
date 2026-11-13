import asyncio
import math
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import httpx
from pydantic import ValidationError

from .models import BrokerResult


class Retry(Exception):
    def __init__(self, reason, after=0):
        self.reason = reason
        self.after = after


def retry_after(value):
    if not value:
        return 0
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = (
                parsedate_to_datetime(value) - datetime.now(timezone.utc)
            ).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return 0
    return max(seconds, 0) if math.isfinite(seconds) else 0


class RateLimiter:
    """단일 worker의 GET/POST 전체에 적용하는 프로세스당 호출 간격."""

    def __init__(self, rps, *, clock=time.monotonic, sleep=asyncio.sleep):
        self.interval = 1 / rps
        self.clock = clock
        self.sleep = sleep
        self.next_at = 0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            await self.sleep(max(0, self.next_at - self.clock()))
            self.next_at = self.clock() + self.interval


class Broker:
    def __init__(self, client, limiter, timeout=5):
        self.client = client
        self.limiter = limiter
        self.timeout = timeout

    async def request(self, method, url, **kwargs):
        await self.limiter.wait()
        try:
            result = await asyncio.wait_for(
                self.client.request(method, url, **kwargs), self.timeout
            )
        except (httpx.HTTPError, TimeoutError) as exc:
            raise Retry("broker_transport_error") from exc
        if result.status_code == 429:
            raise Retry(
                "broker_rate_limited", retry_after(result.headers.get("Retry-After"))
            )
        if result.status_code >= 500:
            raise Retry("broker_unavailable")
        return result

    @staticmethod
    def parse(result, payload):
        try:
            order = BrokerResult.model_validate_json(result.content)
        except (ValidationError, ValueError) as exc:
            raise Retry("broker_invalid_response") from exc
        if not order.matches(payload):
            raise Retry("broker_order_mismatch")
        if result.status_code not in {200, 201, 202} and not (
            400 <= result.status_code < 500 and order.status == "rejected"
        ):
            raise Retry("broker_http_error")
        return order

    async def reconcile(self, payload, *, lookup_only=False):
        key = quote(payload["client_order_id"], safe="")
        result = await self.request("GET", f"/orders/{key}")
        if result.status_code == 404:
            if lookup_only:
                raise Retry("broker_order_not_found")
            result = await self.request(
                "POST",
                "/orders",
                json=payload,
                headers={"Idempotency-Key": payload["client_order_id"]},
            )
        order = self.parse(result, payload)
        if order.status == "pending":
            raise Retry(
                "broker_pending", retry_after(result.headers.get("Retry-After"))
            )
        return order

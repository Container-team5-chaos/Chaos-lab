"""Prometheus 즉시 쿼리. 값 하나를 숫자로 돌려주고, 실패하면 None(모름)을 돌려준다.

None은 0이 아니다: 지표가 비어 있으면 '대기열 0'이 아니라 '모름'으로 다룬다(fail-closed).
"""

import json
import urllib.parse
import urllib.request
from typing import Protocol

from .watch import PROM_URL


class Metrics(Protocol):
    def value(self, query: str) -> float | None: ...


class PromQuery:
    def __init__(self, url: str = PROM_URL, timeout: float = 5.0):
        self.url = url.rstrip("/") + "/api/v1/query"
        self.timeout = timeout

    def value(self, query: str) -> float | None:
        q = urllib.parse.urlencode({"query": query})
        try:
            with urllib.request.urlopen(f"{self.url}?{q}", timeout=self.timeout) as r:
                body = json.load(r)
        except Exception:  # noqa: BLE001 - 어떤 실패든 '모름'
            return None
        result = (body.get("data") or {}).get("result") or []
        # 빈 결과·여러 시계열은 값 하나로 볼 수 없다(sum()으로 묶어 쓴다)
        if body.get("status") != "success" or len(result) != 1:
            return None
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None


class NoMetrics:
    def value(self, query: str) -> float | None:
        return None

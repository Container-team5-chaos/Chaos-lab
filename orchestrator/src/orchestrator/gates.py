"""시작 게이트: 실험을 시작해도 되는 상태인지 확인한다. 하나라도 실패하면 시작하지 않는다.

- 이전 run의 정리가 끝났는가 (state.json cleanup != INCOMPLETE, 남은 Chaos 0)
- 주문 서비스가 받을 준비가 됐는가 (/readyz 200)
- 현재가가 신선한가 (Redis price:<market>의 ts가 STALE_MS 이내)
- 관측이 살아 있는가 (Prometheus)
"""

import json
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

ORDER_URL = "http://order.exchange.svc.cluster.local:8000"
REDIS_URL = "redis://redis.exchange.svc.cluster.local:6379/0"
STALE_MS = 5000


@dataclass
class GateResult:
    ok: bool
    reason: str = ""
    checks: dict = field(default_factory=dict)


class Gate(Protocol):
    def check(self, scenario: dict) -> GateResult: ...


class AlwaysOpen:
    def check(self, scenario: dict) -> GateResult:
        return GateResult(True)


Check = Callable[[dict], tuple[bool, str]]


class CompositeGate:
    """이름 붙은 검사들을 순서대로 돌려 모두 통과해야 연다. 실패 이유를 전부 남긴다."""

    def __init__(self, checks: dict[str, Check]):
        self.checks = checks

    def check(self, scenario: dict) -> GateResult:
        results, failed = {}, []
        for name, fn in self.checks.items():
            try:
                ok, msg = fn(scenario)
            except Exception as e:  # noqa: BLE001 - 확인 못 하면 닫는다
                ok, msg = False, f"error: {e!r}"
            results[name] = {"ok": ok, "detail": msg}
            if not ok:
                failed.append(f"{name}: {msg}")
        return GateResult(not failed, "; ".join(failed), results)


# ---- 개별 검사 ------------------------------------------------------------
def previous_cleanup_ok(store) -> Check:
    def _c(scenario):
        last = store.load()
        if last and last.get("cleanup") == "INCOMPLETE":
            return False, f"이전 run {last.get('run_id')} 정리 미완료"
        return True, "ok"

    return _c


def no_leftover_chaos(injector) -> Check:
    def _c(scenario):
        left = injector.leftovers()
        return (not left, f"남은 Chaos {left}" if left else "0")

    return _c


def http_ready(url: str = ORDER_URL + "/readyz", timeout: float = 3.0) -> Check:
    def _c(scenario):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.status == 200, f"HTTP {r.status}"
        except Exception as e:  # noqa: BLE001
            return False, repr(e)

    return _c


def price_fresh(
    get_price_raw: Callable[[str], str | None], stale_ms: int = STALE_MS, now=time.time
) -> Check:
    """get_price_raw(market) → Redis 값 문자열 {"price": "...", "ts": ms} 또는 None."""

    def _c(scenario):
        market = scenario.get("market", "KRW-BTC")
        raw = get_price_raw(market)
        if raw is None:
            return False, f"price:{market} 없음"
        age = int(now() * 1000) - int(json.loads(raw)["ts"])
        return age <= stale_ms, f"age {age}ms"

    return _c


def redis_price_getter(url: str = REDIS_URL):
    import redis  # pip install .[k8s]

    client = redis.Redis.from_url(url, decode_responses=True, socket_timeout=3)
    return lambda market: client.get(f"price:{market}")


def observability_up(watch) -> Check:
    def _c(scenario):
        return watch.healthy(), "prometheus"

    return _c


def check(gate: Gate, scenario: dict) -> GateResult:
    return gate.check(scenario)

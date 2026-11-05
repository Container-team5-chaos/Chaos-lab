"""PromQL 판정(설계 5.9, 7.3). 2차. 시그니처만 있다.

- 구간 끝 시각에 창 = 구간 길이로 instant 쿼리를 부른다. `$res`(scrape 간격)를 늘 명시한다.
- 창 하한: rate는 `$win ≥ 4 × scrape_interval`, gauge subquery는 점 3개 이상.
  모자라면 `judge plan`이 입력 오류(12)를 낸다.
- NaN·±Inf·빈 결과·status:error는 INCONCLUSIVE다. 비교 전에 math.isfinite를 검사한다.
- 서버 지표는 교차 확인(required: false)이다. 모든 쿼리에 namespace="papertrade"를 강제한다.
- Prometheus 주소·토큰은 노트북 .env에만 둔다(공개 저장소, 설계 7.4). 코드에 넣지 않는다.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .model import CheckResult, Criteria, RunContext
from .transport import Transports

__all__ = [
    "PromResult",
    "evaluate_prom_checks",
    "instant_query",
    "parse_wget_status",
    "relay_argv",
    "render_query",
    "window_ok",
]

NAMESPACE_MATCHER = 'namespace="papertrade"'  # 7.3
MIN_RATE_SCRAPES = 4  # rate 창 ≥ 4 × scrape_interval
MIN_SUBQUERY_POINTS = 3  # gauge subquery 점 수
QUERY_DELAY_SCRAPES = 2  # 조회는 구간 끝 + 2 × scrape 이후
RELAY_TIMEOUT_S = 15  # busybox wget -T
P95_BUCKET_LE = "0.3"  # 주문 p95 < 300ms를 버킷 비율로 본다


@dataclass(frozen=True)
class PromResult:
    """instant 쿼리 결과. value가 None이면 빈 결과·NaN·Inf·오류(INCONCLUSIVE).

    status: Prometheus 응답 status 또는 전송 상태. reasons: PROM_ERROR, CRITERIA_ERROR 등.
    """

    query: str
    at_ms: int
    value: Decimal | None
    status: str
    reasons: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, hash=False)


def render_query(template: str, *, selector: str, win_s: int, res_s: int) -> str:
    """`$sel`, `$win`, `$res`를 채우고 namespace 매처를 강제한다(설계 5.9, 7.3)."""
    raise NotImplementedError("설계 5.9, 2차")


def window_ok(win_s: int, scrape_s: int, *, kind: str) -> bool:
    """창 하한 검사. kind는 rate | gauge(설계 5.9). `judge plan`이 쓴다."""
    raise NotImplementedError("설계 5.9, 2차")


def relay_argv(tr: Transports, pod: str, url: str) -> tuple[str, ...]:
    """relay 경로 `exec <k6-pod> -c holder -- wget -q -O - -T 15 "<URL>"`(설계 5.9)."""
    raise NotImplementedError("설계 5.9, 2차")


def parse_wget_status(stderr: str) -> int | None:
    """busybox wget stderr `server returned error: HTTP/1.x NNN`에서 상태 코드를 읽는다.

    400·422 → CRITERIA_ERROR, 503 → 재시도 후 PROM_ERROR(설계 5.9).
    """
    raise NotImplementedError("설계 5.9, 2차")


def instant_query(
    tr: Transports,
    query: str,
    at_ms: int,
    *,
    base_url: str | None = None,
    relay_pod: str | None = None,
) -> PromResult:
    """instant 쿼리. direct(base_url) 우선, 없으면 relay(relay_pod)(설계 5.9, 7.3)."""
    raise NotImplementedError("설계 5.9, 2차")


def evaluate_prom_checks(
    ctx: RunContext,
    crit: Criteria,
    results: Sequence[PromResult],
) -> list[CheckResult]:
    """kind: prom 기준을 판정한다. 결과 파일만 읽는 순수 함수(설계 5.9, 5.7)."""
    raise NotImplementedError("설계 5.9, 2차")

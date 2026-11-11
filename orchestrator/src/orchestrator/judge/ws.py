"""WS 판정(설계 4.9, 5.11). 2차. 시그니처만 있다.

- 기록(ws.jsonl)은 상태 전이(ws_open, ws_snap, ws_down{cause, code}, ws_regress)와
  VU별 10초 집계 wsagg{n, bins}만이다(설계 4.9).
- required: never_reconnected == 0, reconnect_p95_ms ≤ 15000, event_ts_regress == 0,
  신선도 상대 기준(tail p95 ≤ baseline p95 × 1.5). 절대 기준(p95 < 1000ms)은 선택이다.
- ws_restored(tail 연결 수 ≥ baseline 중앙값 × 0.95)는 run_start.ws_reconnect가 true일 때만
  required, 아니면 N/A다(설계 5.11).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from .model import CheckResult, Criteria, RunContext

__all__ = ["WsLog", "parse_ws_log", "ws_checks"]

RECONNECT_P95_MAX_MS = 15000
FRESHNESS_RATIO_MAX = Decimal("1.5")
FRESHNESS_ABS_P95_MS = 1000  # 선택 기준
WS_RESTORED_RATIO = Decimal("0.95")
WS_LOAD_MIN = 100  # gateway-pod-kill 가드 ws_load_present(6절 끝)
AGG_BIN_EDGES_MS = (50, 5000)  # wsagg 경계(우리 설정값)


@dataclass(frozen=True)
class WsLog:
    """ws.jsonl 파싱 결과(설계 4.9). events·agg는 파일 순서다."""

    run_id: str | None
    ws_reconnect: bool | None
    events: tuple[Mapping[str, Any], ...] = ()
    agg: tuple[Mapping[str, Any], ...] = ()
    problems: tuple[str, ...] = ()
    sha256: str = ""
    extra: Mapping[str, Any] = field(default_factory=dict, hash=False)


def parse_ws_log(path: Path) -> WsLog:
    """ws.jsonl(또는 .gz)을 읽는다. 깨진 줄은 problems에 적는다(설계 4.9)."""
    raise NotImplementedError("설계 4.9, 2차")


def ws_checks(ctx: RunContext, crit: Criteria, log: WsLog) -> list[CheckResult]:
    """WS 기준과 가드(ws_load_present, ws_restored)를 판정한다(설계 5.11, 6절 끝)."""
    raise NotImplementedError("설계 5.11, 2차")

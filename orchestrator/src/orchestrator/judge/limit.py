"""한계 찾기(설계 4.10). 2차. 시그니처만 있다.

- 계단 1 run: R_k = 30 × 1.25^k, R_max = 360(관리자 합의). 계단 60s 중 앞 15s를 버리고
  45s를 잰다. 표본 n ≥ 20/(1−q)(p95면 400).
- 계단 판정 순서
  1. p95 ≥ 300ms 또는 첫 시도 오류율 ≥ 1% → SYSTEM_LIMIT. R*는 직전 계단이다.
  2. 1이 통과하고 달성률 < 0.98만 깨짐 → GENERATOR_LIMITED(INCONCLUSIVE).
  3. 체결 비율 < 0.8 → CHEAP_PATH(INCONCLUSIVE).
- limit.json: 계단별 p95, 오류율, 달성률, 체결 비율, 거절 사유, dropped_iterations, R*.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from .model import Classified, Criteria, K6Log, RunContext

__all__ = [
    "LimitResult",
    "StepResult",
    "find_limit",
    "judge_step",
    "step_rates",
    "write_limit_json",
]

R0 = 30
STEP_FACTOR = Decimal("1.25")
R_MAX = 360
STEP_S = 60
STEP_DISCARD_S = 15
P95_LIMIT_MS = 300
ERR_RATIO_LIMIT = Decimal("0.01")
ACHIEVED_MIN = Decimal("0.98")
FILL_RATIO_MIN = Decimal("0.8")  # 우리 설정값
LIMIT_JSON_NAME = "limit.json"


@dataclass(frozen=True)
class StepResult:
    """계단 하나의 측정과 판정(설계 4.10). reasons는 reasons.py 코드."""

    k: int
    rate: Decimal
    p95_ms: Any
    err_ratio: Decimal | None
    achieved_ratio: Decimal | None
    fill_ratio: Decimal | None
    dropped_iterations: int | None = None
    reject_reasons: Mapping[str, int] = field(default_factory=dict, hash=False)
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class LimitResult:
    """limit.json 내용. r_star는 마지막으로 통과한 계단 도착률(없으면 None)."""

    steps: tuple[StepResult, ...]
    r_star: Decimal | None
    outcome: str
    reasons: tuple[str, ...] = ()


def step_rates(
    r0: int = R0, factor: Decimal = STEP_FACTOR, r_max: int = R_MAX
) -> list[Decimal]:
    """계단 도착률 R_k = r0 × factor^k(≤ r_max)(설계 4.10)."""
    raise NotImplementedError("설계 4.10, 2차")


def judge_step(
    k: int,
    log: K6Log,
    classified: Sequence[Classified],
    window: tuple[int, int],
) -> StepResult:
    """계단 하나를 판정 순서 1–3으로 본다(설계 4.10)."""
    raise NotImplementedError("설계 4.10, 2차")


def find_limit(ctx: RunContext, crit: Criteria, evidence: Path) -> LimitResult:
    """step 프로파일 run의 증거로 R*를 찾는다(설계 4.10). `judge limit`이 부른다."""
    raise NotImplementedError("설계 4.10, 2차")


def write_limit_json(result: LimitResult, run_dir: Path) -> Path:
    """run_dir/limit.json을 쓴다(설계 4.10)."""
    raise NotImplementedError("설계 4.10, 2차")

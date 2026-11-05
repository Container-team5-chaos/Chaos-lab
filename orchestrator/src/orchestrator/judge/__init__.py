"""측정과 판정(judge) 패키지(설계 5).

공개 API. judge·evaluate·collect는 pipeline 모듈 구현을 지연 import로 부른다.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .context import load_run
from .criteria import integrity_cfg, load_criteria
from .errors import (
    EXIT_FAIL,
    EXIT_INCONCLUSIVE,
    EXIT_INPUT,
    EXIT_PASS,
    InputError,
    JudgeError,
)
from .model import (
    CheckResult,
    Classified,
    Criteria,
    DbSnapshot,
    IntegrityCfg,
    K6Log,
    KState,
    OkSub,
    OrderClass,
    Outcome,
    RunContext,
    Verdict,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "EXIT_FAIL",
    "EXIT_INCONCLUSIVE",
    "EXIT_INPUT",
    "EXIT_PASS",
    "CheckResult",
    "Classified",
    "Criteria",
    "DbSnapshot",
    "InputError",
    "IntegrityCfg",
    "JudgeError",
    "K6Log",
    "KState",
    "OkSub",
    "OrderClass",
    "Outcome",
    "RunContext",
    "Verdict",
    "collect",
    "evaluate",
    "integrity_cfg",
    "judge",
    "load_criteria",
    "load_run",
]


def judge(
    run_json: str | Path, *, offline: bool = False, generated_at: int | None = None
) -> Verdict:
    """run.json 하나로 collect → evaluate를 돈다(설계 5.1). 구현은 pipeline."""
    from .pipeline import judge as _judge

    return _judge(run_json, offline=offline, generated_at=generated_at)


def evaluate(
    ctx: RunContext,
    crit: Criteria,
    evidence: str | Path,
    *,
    generated_at: int | None = None,
) -> Verdict:
    """증거만으로 판정하는 순수 함수(설계 5.1, 5.2 6단계). 구현은 pipeline."""
    from .pipeline import evaluate as _evaluate

    return _evaluate(ctx, crit, evidence, generated_at=generated_at)


def collect(ctx: RunContext, crit: Criteria, tr: Any) -> Path:
    """클러스터를 만지는 유일한 함수(설계 5.1, 5.2). 구현은 pipeline."""
    from .pipeline import collect as _collect

    return _collect(ctx, crit, tr)

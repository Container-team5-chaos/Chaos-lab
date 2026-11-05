"""k6 기록 기반 구간 SLO와 회복 시간(설계 5.2 6단계, 5.5, 5.10).

1차 구현: nearest_rank, planned_arrivals, achieved_ratio, load_window, phase_windows(순수 계산).
나머지(ack_p95_ms, sys_err_ratio, client_checks, recovery_time, precheck)는 시그니처만 있다.

- 분위수는 nearest-rank `x[ceil(q·n) − 1]`이다(설계 5.2 6단계). k6 threshold의 선형 보간과
  다르므로 SLO 판정은 judge 하나로만 한다(설계 4.7).
- 계산은 정확한 유리수(Fraction)로 한다. q·n을 float로 곱하면 0.07 × 100 = 7.000000000000001처럼
  올림 결과가 한 칸 밀린다.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from numbers import Rational
from typing import Any, TypeVar

from .criteria import duration_ms
from .errors import InputError
from .model import CheckResult, Classified, Criteria, Intent, K6Log, RunContext
from .transport import Transports

__all__ = [
    "K6_END_MARK",
    "K6_START_MARK",
    "PLAN_EXECUTOR",
    "RATIO_PLACES",
    "RecoveryResult",
    "achieved_ratio",
    "ack_p95_ms",
    "applicable_phases",
    "client_checks",
    "is_fault_run",
    "load_window",
    "nearest_rank",
    "phase_windows",
    "phases_from_marks",
    "plan_duration_ms",
    "planned_arrivals",
    "precheck",
    "recovery_time",
    "run_window",
    "sys_err_ratio",
]

T = TypeVar("T")

PLAN_EXECUTOR = "ramping-arrival-rate"  # 설계 4.5, JSONL v1 run_start.plan
RATIO_PLACES = 6  # achieved_ratio 소수 자릿수(내림)

# 5.10 회복 시간 기본값(우리 설정값)
RECOVERY_WINDOW_S = 10
RECOVERY_MIN_N = 50
RECOVERY_HOLD_S = 30
RECOVERY_DEADLINE_S = 120


# ---------------------------------------------------------------- 정확한 수


def _exact(v: Any, where: str) -> Fraction:
    """수를 Fraction으로 바꾼다. float는 최단 10진 표현(repr)을 쓴다. bool·NaN·Inf는 거부."""
    if isinstance(v, bool):
        raise TypeError(f"{where}: bool은 수가 아니다")
    if isinstance(v, float):
        if not math.isfinite(v):
            raise ValueError(f"{where}: 유한한 수가 아니다({v!r})")
        return Fraction(repr(v))
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise ValueError(f"{where}: 유한한 수가 아니다({v!r})")
        return Fraction(v)
    if isinstance(v, (int, Rational)):
        return Fraction(v)
    if isinstance(v, str):
        try:
            return Fraction(v.strip())
        except (ValueError, ZeroDivisionError) as e:
            raise ValueError(f"{where}: 수가 아니다({v!r})") from e
    raise TypeError(f"{where}: 수가 아니다({type(v).__name__})")


# ---------------------------------------------------------------- 분위수(설계 5.2 6단계)


def nearest_rank(values: Iterable[T], q: float | Decimal | Fraction | str) -> T:
    """nearest-rank 분위수 `x[ceil(q·n) − 1]`(x는 오름차순, 설계 5.2 6단계).

    - 0 < q ≤ 1. 보간하지 않고 표본 값 하나를 그대로 낸다.
    - 빈 표본·NaN은 ValueError다. 표본 수 기준(`n ≥ 20/(1−q)`, LOW_SAMPLES)은 부르는 쪽 몫이다.
    - 예: n=1이면 q와 상관없이 그 값. n=20, q=0.95면 19번째 값.
    """
    qf = _exact(q, "q")
    if not 0 < qf <= 1:
        raise ValueError(f"q는 0 < q ≤ 1이어야 한다(받음: {q!r})")
    xs = list(values)
    if not xs:
        raise ValueError("빈 표본의 분위수는 없다")
    for x in xs:
        if isinstance(x, float) and math.isnan(x):
            raise ValueError("표본에 NaN이 있다")
        if isinstance(x, Decimal) and x.is_nan():
            raise ValueError("표본에 NaN이 있다")
    xs.sort()
    rank = math.ceil(qf * len(xs))
    return xs[rank - 1]


# ---------------------------------------------------------------- 달성률(설계 4.5, 5.5)


@dataclass(frozen=True)
class _Segment:
    """계획 구간 [t0, t1) ms 동안 도착률이 r0 → r1(초당)로 선형 변한다."""

    t0: Fraction
    t1: Fraction
    r0: Fraction
    r1: Fraction


def _plan_segments(plan: Mapping[str, Any], origin_ms: int) -> list[_Segment]:
    """JSONL v1 plan을 초당 도착률 구간으로 바꾼다. 형식이 틀리면 InputError."""
    if not isinstance(plan, Mapping):
        raise InputError("run_start.plan은 객체여야 한다")
    if plan.get("executor") != PLAN_EXECUTOR:
        raise InputError(
            f"run_start.plan.executor는 {PLAN_EXECUTOR!r}여야 한다"
            f"(받음: {plan.get('executor')!r})"
        )
    try:
        unit = _exact(plan["time_unit_s"], "plan.time_unit_s")
        rate = _exact(plan["start_rate"], "plan.start_rate")
        stages = plan["stages"]
        if not isinstance(stages, Sequence) or isinstance(stages, (str, bytes)):
            raise TypeError("plan.stages는 배열이어야 한다")
        segs: list[_Segment] = []
        t = Fraction(origin_ms)
        if unit <= 0 or rate < 0:
            raise ValueError("plan.time_unit_s > 0, plan.start_rate ≥ 0이어야 한다")
        for i, st in enumerate(stages):
            if not isinstance(st, Mapping):
                raise TypeError(f"plan.stages[{i}]는 객체여야 한다")
            dur = _exact(st["duration_s"], f"plan.stages[{i}].duration_s")
            target = _exact(st["target"], f"plan.stages[{i}].target")
            if dur < 0 or target < 0:
                raise ValueError(f"plan.stages[{i}]: duration_s·target ≥ 0이어야 한다")
            if dur > 0:
                segs.append(_Segment(t, t + dur * 1000, rate / unit, target / unit))
            t += dur * 1000
            rate = target
    except KeyError as e:
        raise InputError(f"run_start.plan에 {e.args[0]!r}가 없다") from e
    except (TypeError, ValueError) as e:
        raise InputError(f"run_start.plan 형식 오류: {e}") from e
    return segs


def _segment_area(s: _Segment, a: Fraction, b: Fraction) -> Fraction:
    """구간 s에서 [a, b) ms 동안의 도착 수(사다리꼴 넓이)."""
    lo, hi = max(a, s.t0), min(b, s.t1)
    if hi <= lo:
        return Fraction(0)
    span = s.t1 - s.t0
    r_lo = s.r0 + (s.r1 - s.r0) * (lo - s.t0) / span
    r_hi = s.r0 + (s.r1 - s.r0) * (hi - s.t0) / span
    return (r_lo + r_hi) / 2 * (hi - lo) / 1000


def planned_arrivals(
    plan: Mapping[str, Any],
    *,
    origin_ms: int,
    window: tuple[int, int] | None = None,
) -> Fraction:
    """계획 도착 수 = plan 도착률의 적분(설계 4.5, 5.5).

    - plan(JSONL v1) = {executor:"ramping-arrival-rate", time_unit_s, start_rate,
      stages:[{duration_s, target}]}. target은 time_unit_s당 도착 수이고, stage 안에서는
      앞 값에서 target까지 선형으로 변한다. duration 0인 stage는 순간 점프다.
    - origin_ms: 시나리오 시작 시각(epoch ms). 보통 setup_done.t다.
    - window: [from_ms, to_ms). None이면 계획 전체. 계획 밖은 0이다.
    """
    segs = _plan_segments(plan, origin_ms)
    if window is None:
        if not segs:
            return Fraction(0)
        a, b = segs[0].t0, segs[-1].t1
    else:
        a, b = Fraction(window[0]), Fraction(window[1])
        if b < a:
            raise ValueError(f"window 끝이 시작보다 이르다: {window!r}")
    return sum((_segment_area(s, a, b) for s in segs), Fraction(0))


def plan_duration_ms(plan: Mapping[str, Any]) -> int | None:
    """계획 길이 = stage duration_s 합(ms, 올림). 형식이 틀리면 None(설계 5.7 k6 99 중단 판별)."""
    try:
        segs = _plan_segments(plan, 0)
    except InputError:
        return None
    return math.ceil(segs[-1].t1) if segs else 0


def achieved_ratio(
    intents: Iterable[Intent] | Mapping[str, Intent],
    plan: Mapping[str, Any],
    *,
    origin_ms: int,
    window: tuple[int, int] | None = None,
) -> Decimal | None:
    """달성률 = window 안 intent 수 ÷ 계획 도착 수(설계 5.5, load_coverage 가드 5.6).

    - intents: Intent들 또는 K6Log.intents(키 → Intent). intent 시각 t로 [from, to)에 넣는다.
    - 계획 도착 수가 0이면 None(평가 불가)이다.
    - 결과는 소수 6자리 내림이다. 내림이라 소수 6자리 이하 기준값과의 `≥` 비교가 바뀌지 않는다.
    """
    items = intents.values() if isinstance(intents, Mapping) else intents
    planned = planned_arrivals(plan, origin_ms=origin_ms, window=window)
    if window is None:
        count = sum(1 for _ in items)
    else:
        lo, hi = window
        count = sum(1 for it in items if lo <= it.t < hi)
    if planned == 0:
        return None
    scaled = math.floor(Fraction(count) / planned * 10**RATIO_PLACES)
    return Decimal(
        f"{scaled}E-{RATIO_PLACES}"
    )  # 정수 연산으로 내림. decimal 문맥과 무관


# ---------------------------------------------------------------- 구간(설계 5.5)

K6_START_MARK = (
    "k6_start"  # 부하 시작(setup_done.t). 기준 YAML phases에서 표지 이름으로 쓴다
)
K6_END_MARK = "k6_end"  # 부하 끝(마지막 기록 시각 + 1)
FAULT_PHASES = frozenset({"baseline", "fault", "recovery"})


def load_window(log: K6Log) -> tuple[int, int] | None:
    """부하 창 [k6_start, k6_end)(설계 5.5, 5.6 fault_in_load). k6 노드 시계다.

    k6_start는 setup_done.t(없으면 run_start.t, 그것도 없으면 첫 기록 시각)다.
    k6_end는 intent·result 시각 중 최댓값 + 1이다. 기록이 없으면 None.
    """
    times = [it.t for it in log.intents.values()]
    times += [r.t for r in log.results.values()]
    if not times:
        return None
    if log.setup_done is not None:
        start = log.setup_done.t
    elif log.run_start is not None:
        start = log.run_start.t
    else:
        start = min(times)
    return start, max(start, max(times) + 1)


def run_window(
    ctx: RunContext, log: K6Log, *, clock_offset_ms: int = 0
) -> tuple[int, int] | None:
    """판정에 쓰는 부하 창 [k6_start, k6_end). k6 노드 시계다(설계 5.5, 5.6 fault_in_load).

    기본은 load_window다. run.json marks에 k6_start·k6_end가 있으면 그 값(노트북 시계 +
    clock_offset_ms)을 쓴다. phase_windows의 모든 구간과 fault_in_load가 이 창 하나를 쓴다.
    """
    lw = load_window(log)
    if lw is None:
        return None
    start, end = lw
    if K6_START_MARK in ctx.marks:
        start = ctx.marks[K6_START_MARK] + clock_offset_ms
    if K6_END_MARK in ctx.marks:
        end = ctx.marks[K6_END_MARK] + clock_offset_ms
    return start, max(start, end)


def _fault_mark(crit: Criteria) -> str:
    return str(crit.data["phases"]["fault"]["from"])


def is_fault_run(ctx: RunContext, crit: Criteria) -> bool:
    """장애 run인가. fault 시작 표지, faults, fault_effect 항목 중 하나라도 있으면 그렇다(설계 5.5)."""
    return _fault_mark(crit) in ctx.marks or bool(ctx.faults) or bool(crit.fault_effect)


def applicable_phases(ctx: RunContext, crit: Criteria) -> frozenset[str]:
    """이 run에 해당하는 구간. 장애 run은 steady가 없고, 장애 없는 run은 baseline·fault·recovery가 없다."""
    if is_fault_run(ctx, crit):
        return frozenset({"warmup", "baseline", "fault", "recovery", "tail"})
    return frozenset({"warmup", "tail", "steady"})


def phases_from_marks(ctx: RunContext, crit: Criteria) -> frozenset[str]:
    """경계가 run.json 표지(노트북 시계)에서 오는 구간. 시계 차이가 크면 CLOCK_SKEW 대상이다(설계 5.5).

    k6_start·k6_end도 run.json marks에 있으면 표지로 본다(run_window).
    """
    ph = crit.data["phases"]
    uses = {
        "warmup": (K6_START_MARK,),
        "baseline": (ph["baseline"]["before"],),
        "fault": (ph["fault"]["from"], ph["fault"]["to"]),
        "recovery": (ph["fault"]["to"], K6_END_MARK),
        "tail": (ph["tail"]["before"],),
        "steady": (K6_START_MARK, K6_END_MARK),
    }
    return frozenset(
        p for p, names in uses.items() if any(n in ctx.marks for n in names)
    )


def phase_windows(
    ctx: RunContext,
    crit: Criteria,
    log: K6Log,
    *,
    clock_offset_ms: int = 0,
) -> dict[str, tuple[int, int]]:
    """구간 이름 → [from_ms, to_ms)(설계 5.5, 기준 YAML phases). k6 노드 시계다.

    - 표지: run.json marks(노트북 시계)에 clock_offset_ms(노드 − 노트북, 4.8)를 더해 노드 시계로 바꾼다.
      k6_start·k6_end는 run_window에서 온다(run.json에 같은 이름이 있으면 그 값을 쓴다).
      warmup·steady·recovery·tail이 모두 같은 k6_start·k6_end를 쓴다.
    - warmup [k6_start, k6_start + warmup).
    - baseline [before − duration, before − guard). 기본 [inject_start − 150s, inject_start − 2s).
    - fault [from, to + extra), recovery [fault 끝, k6_end). to 표지가 없으면 둘 다 없다.
    - tail [before − duration, before). 기본 마지막 120s.
    - steady [warmup 끝, k6_end). 장애 없는 run(is_fault_run이 거짓)에만 있다.
    - 표지가 없어 경계를 모르는 구간은 결과에 없다. 길이가 음수면 빈 창 (a, a)다.
    - 기록이 없으면 빈 dict다.
    """
    rw = run_window(ctx, log, clock_offset_ms=clock_offset_ms)
    if rw is None:
        return {}
    start, end = rw
    marks = {k: v + clock_offset_ms for k, v in ctx.marks.items()}
    marks.update({K6_START_MARK: start, K6_END_MARK: end})
    ph = crit.data["phases"]
    warm_end = start + duration_ms(ph["warmup"])

    def win(a: int, b: int) -> tuple[int, int]:
        return a, max(a, b)

    out = {"warmup": win(start, warm_end)}
    base = ph["baseline"]
    before = marks.get(base["before"])
    if before is not None:
        out["baseline"] = win(
            before - duration_ms(base["duration"]), before - duration_ms(base["guard"])
        )
    fault = ph["fault"]
    f_from, f_to = marks.get(fault["from"]), marks.get(fault["to"])
    if f_from is not None and f_to is not None:
        f_end = f_to + duration_ms(fault["extra"])
        out["fault"] = win(f_from, f_end)
        out["recovery"] = win(f_end, end)
    tail = ph["tail"]
    t_before = marks.get(tail["before"])
    if t_before is not None:
        out["tail"] = win(t_before - duration_ms(tail["duration"]), t_before)
    if not is_fault_run(ctx, crit):
        out["steady"] = win(warm_end, end)
    return dict(sorted(out.items()))


# ---------------------------------------------------------------- 구간 SLO(설계 5.5) 1차, 미구현


def ack_p95_ms(
    log: K6Log, window: tuple[int, int], *, q: Decimal = Decimal("0.95")
) -> Any:
    """ACK 받은 첫 시도의 ms로 낸 nearest-rank 분위수. 구간 배정은 t0(설계 5.5).

    표본이 `n ≥ 20/(1−q)`보다 적으면 LOW_SAMPLES다.
    """
    raise NotImplementedError("설계 5.5, 1차")


def sys_err_ratio(log: K6Log, window: tuple[int, int]) -> Decimal | None:
    """첫 시도 중 0·5xx 비율. 4xx는 분모에서 뺀다(업무 거절은 성공, 설계 5.5)."""
    raise NotImplementedError("설계 5.5, 1차")


def client_checks(
    ctx: RunContext,
    crit: Criteria,
    log: K6Log,
    classified: Sequence[Classified],
) -> list[CheckResult]:
    """kind: client 기준(ack_p95_ms, sys_err_ratio, achieved_ratio, filled, sell_filled)
    을 구간별로 판정한다(설계 5.5, 6).
    """
    raise NotImplementedError("설계 5.5, 1차")


# ---------------------------------------------------------------- 회복 시간(설계 5.10) 2차


@dataclass(frozen=True)
class RecoveryResult:
    """5.10 회복 시간 판정. 시간은 ms. reasons는 reasons.py 코드."""

    outcome: str
    rt_ms: int | None = None
    time_to_impact_ms: int | None = None
    peak: Mapping[str, Any] | None = None
    reasons: tuple[str, ...] = ()


def recovery_time(
    log: K6Log,
    ref_ms: int,
    *,
    window_s: int = RECOVERY_WINDOW_S,
    min_n: int = RECOVERY_MIN_N,
    hold_s: int = RECOVERY_HOLD_S,
    deadline_s: int = RECOVERY_DEADLINE_S,
) -> RecoveryResult:
    """첫 시도 (t0, st, ms)로 회복 시간을 잰다(설계 5.10). ref_ms는 보통 inject_start.

    LOW_TRAFFIC, RECOVERED_LATE, OBSERVATION_TOO_SHORT, NOT_RECOVERED_BY_DEADLINE.
    """
    raise NotImplementedError("설계 5.10, 2차")


def precheck(ctx: RunContext, crit: Criteria, tr: Transports) -> CheckResult:
    """주입 전 게이트(설계 1.4, 3절 3단계). INCONCLUSIVE면 주입하지 않는다(종료 코드 11)."""
    raise NotImplementedError("설계 1.4, 2차")

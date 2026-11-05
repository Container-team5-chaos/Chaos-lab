"""잔고 불변식 판정(설계 5.4). SQL 구획 결과와 I7(Python)을 CheckResult로 바꾼다.

결과 규칙(verdict.combine과 맞춘다)
- 구획(sql/inv_<name>.sql)마다 CheckResult 하나. id는 `inv_<name>`, I7은 `i7`. kind는 integrity.
- outcome은 위반 행 수를 기준의 op·value로 비교한 그대로다. on_violation 적용은 combine 몫이다.
- 구획이 없으면 INCONCLUSIVE(DB_ERROR). 다른 구획은 그대로 판정한다.
- FAIL 사유: constraints는 CONSTRAINT_MISSING, 나머지는 비운다(combine이 INTEGRITY_VIOLATION을 붙인다).
- required·on_violation·op·value는 그 구획을 sql:(I7은 check: i7)로 가리키는 기준 항목에서 온다.
  여러 항목이 가리키면 required는 하나라도 true, on_violation은 required 항목 중 하나라도 fail이면 fail.
  기준(Criteria)을 줬는데 아무 항목도 가리키지 않으면 required=false(보고만 한다).
  기준 없이 부르면 선택 검사(arith, privileges)만 required=false다.
- value는 위반 행 수, detail.examples는 위반 행 20개까지(정렬).
- invariant_counts()는 이름(i2, ..., i7) → 위반 수(평가 못 하면 None)다.
  verdict.json integrity.invariants와 verdict.evaluate_count_check의 sql 건수에 그대로 쓴다.

I7(신선도 위반 체결, 설계 5.4)
- FILLED이고 ref_age_ms × speed(received_ms) > price_max_age_ms면 위반이다(곱으로 비교해 나눗셈 오차가 없다).
  ref_age_ms NULL도 위반이다. REPLAY 아닌 종목은 배속 1이다.
- 배속 타임라인은 speed_timeline 인자(Pod 관측) → ctx.replay_speed(run.json + marks.local.json) 순이다.
- 전환 시각 ±15s(우리 설정값, 양끝 포함) 안이면 앞뒤 배속 중 느린 쪽(느슨한 상한)을 쓴다.
- 첫 점보다 이른 주문, 타임라인이 없을 때는 판정하지 못한다 → INCONCLUSIVE(NO_SPEED_TIMELINE).
  received_ms가 NULL이면 타임라인의 모든 배속에서 같은 결론일 때만 판정한다.
- 위반이 하나라도 있으면 판정 못 한 주문이 있어도 FAIL이다.
- 보고: DB 시계 나이(received_ms − ref_event_ts) 최댓값, 앱 상한 추정(FILLED 최대 나이,
  STALE_PRICE 거절 최소 나이, 배속 1 기준). judge 상한과 어긋나면 detail.warnings에 APP_MAX_AGE_DIFFERS.
  체결 순간 피드 생존(hb 나이)은 저장되지 않아 detail.feed_alive = "unverifiable"이다.
"""

from __future__ import annotations

import json
import operator
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from . import reasons as R
from .criteria import integrity_cfg
from .errors import InputError
from .model import (
    INVARIANT_SQL,
    OPTIONAL_INVARIANT_SQL,
    CheckResult,
    Criteria,
    DbOrder,
    DbSnapshot,
    IntegrityCfg,
    Outcome,
    SpeedPoint,
    dec,
)

__all__ = [
    "ADVISORY_PROBLEMS",
    "EXAMPLES_MAX",
    "I7_ID",
    "I7Eval",
    "SPEED_SLACK_MS",
    "check_invariants",
    "evaluate_i7",
    "i4_counts",
    "invariant_counts",
    "item_results",
    "speed_candidates",
]

KIND = "integrity"
I7_ID = "i7"
SPEED_SLACK_MS = 15_000  # 배속 전환 전후 느슨한 구간(우리 설정값, 설계 5.4)
EXAMPLES_MAX = 20  # 설계 5.8 examples
REPLAY_PREFIX = "REPLAY:"
STALE_REASON = "STALE_PRICE"
FILLED = "FILLED"
REJECTED = "REJECTED"

# 판정에 넣지 않고 보고만 하는 problem 값(설계 5.4 표: i6_db는 앞 둘만 required)
ADVISORY_PROBLEMS: Mapping[str, frozenset[str]] = {"i6_db": frozenset({"ref_source"})}

_OPS: Mapping[str, Callable[[Any, Any], bool]] = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}


# ---------------------------------------------------------------- 기준 항목 → 구획 규칙


@dataclass(frozen=True)
class _Rule:
    required: bool
    on_violation: str
    op: str
    value: Any
    items: tuple[str, ...]


def _criteria_of(crit: Any) -> tuple[Criteria | None, IntegrityCfg]:
    if crit is None:
        return None, IntegrityCfg(response_fields={})
    if isinstance(crit, IntegrityCfg):
        return None, crit
    if isinstance(crit, Mapping):
        crit = Criteria(data=crit)
    if not isinstance(crit, Criteria):
        raise TypeError(f"crit는 Criteria|IntegrityCfg|None이어야 한다: {type(crit)!r}")
    try:
        cfg = integrity_cfg(crit)
    except (KeyError, TypeError):
        cfg = IntegrityCfg(response_fields={})
    return crit, cfg


def _targets(item: Mapping[str, Any]) -> tuple[str, ...]:
    if item.get("kind") != KIND:
        return ()
    if "sql" in item:
        raw = item["sql"]
        return (raw,) if isinstance(raw, str) else tuple(raw)
    if item.get("check") == I7_ID:
        return (I7_ID,)
    return ()


def _rules(crit: Criteria | None) -> dict[str, _Rule]:
    names = (*INVARIANT_SQL, I7_ID)
    if crit is None:
        return {
            n: _Rule(n not in OPTIONAL_INVARIANT_SQL, "fail", "==", 0, ())
            for n in names
        }
    refs: dict[str, list[Mapping[str, Any]]] = {n: [] for n in names}
    for item in crit.checks:
        for t in _targets(item):
            if t in refs:
                refs[t].append(item)
    out: dict[str, _Rule] = {}
    for n in names:
        items = refs[n]
        if not items:
            out[n] = _Rule(False, "fail", "==", 0, ())
            continue
        required = any(bool(it.get("required", True)) for it in items)
        req_items = [it for it in items if bool(it.get("required", True))] or items
        ov = (
            "fail"
            if any(it.get("on_violation", "fail") == "fail" for it in req_items)
            else "inconclusive"
        )
        first = items[0]
        out[n] = _Rule(
            required=required,
            on_violation=ov,
            op=str(first.get("op", "==")),
            value=first.get("value", 0),
            items=tuple(str(it["id"]) for it in items),
        )
    return out


def _passes(n: int, rule: _Rule) -> bool:
    cmp = _OPS.get(rule.op)
    if cmp is None:
        raise InputError(f"모르는 op {rule.op!r}(기준 {list(rule.items)})")
    return cmp(Decimal(n), Decimal(str(rule.value)))


def _sorted_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (dict(r) for r in rows),
        key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False, default=str),
    )


# ---------------------------------------------------------------- SQL 구획


def _sql_result(db: DbSnapshot, name: str, rule: _Rule) -> CheckResult:
    sid = f"inv_{name}"
    base: dict[str, Any] = {"section": sid, "criteria": list(rule.items)}
    rows = db.invariant_violations(name)
    if rows is None:
        return CheckResult(
            id=sid,
            kind=KIND,
            outcome=Outcome.INCONCLUSIVE,
            required=rule.required,
            on_violation=rule.on_violation,
            value=None,
            reasons=(R.DB_ERROR,),
            detail={**base, "missing": True},
        )
    advisory_kinds = ADVISORY_PROBLEMS.get(name, frozenset())
    counted = [r for r in rows if r.get("problem") not in advisory_kinds]
    advisory = [r for r in rows if r.get("problem") in advisory_kinds]
    n = len(counted)
    ok = _passes(n, rule)
    reasons: tuple[str, ...] = ()
    if not ok and name == "constraints":
        reasons = (R.CONSTRAINT_MISSING,)
    detail = {
        **base,
        "rows": n,
        "examples": _sorted_rows(counted)[:EXAMPLES_MAX],
    }
    if advisory:
        detail["advisory_rows"] = len(advisory)
        detail["advisory_examples"] = _sorted_rows(advisory)[:EXAMPLES_MAX]
    return CheckResult(
        id=sid,
        kind=KIND,
        outcome=Outcome.PASS if ok else Outcome.FAIL,
        required=rule.required,
        on_violation=rule.on_violation,
        value=n,
        reasons=reasons,
        detail=detail,
    )


# ---------------------------------------------------------------- I7


def _timeline(points: Any) -> tuple[SpeedPoint, ...]:
    out: list[SpeedPoint] = []
    for p in points or ():
        if isinstance(p, SpeedPoint):
            sp = p
        elif isinstance(p, Mapping):
            try:
                sp = SpeedPoint(from_ms=int(p["from_ms"]), speed=dec(p["speed"]))
            except (KeyError, TypeError, ValueError) as e:
                raise InputError(f"배속 점 형식이 아니다: {dict(p)!r}") from e
        else:
            raise InputError(f"배속 점 형식이 아니다: {p!r}")
        if sp.speed is None or not sp.speed.is_finite() or sp.speed <= 0:
            raise InputError(f"배속은 0보다 커야 한다: {p!r}")
        out.append(sp)
    out.sort(key=lambda s: s.from_ms)
    for a, b in zip(out, out[1:]):
        if a.from_ms == b.from_ms:
            raise InputError(f"배속 타임라인에 같은 시각이 두 번 있다: {a.from_ms}")
    return tuple(out)


def speed_candidates(
    t: int, timeline: Sequence[SpeedPoint], slack_ms: int = SPEED_SLACK_MS
) -> tuple[Decimal, ...] | None:
    """시각 t에 쓸 수 있는 배속들(정렬). 첫 점보다 이르면 None(모름).

    t의 배속에 더해, |t − 전환 시각| ≤ slack_ms인 전환마다 앞뒤 배속을 넣는다.
    """
    if not timeline or t < timeline[0].from_ms:
        return None
    idx = 0
    for i, p in enumerate(timeline):
        if p.from_ms <= t:
            idx = i
    speeds = {timeline[idx].speed}
    for i in range(1, len(timeline)):
        if abs(t - timeline[i].from_ms) <= slack_ms:
            speeds.add(timeline[i - 1].speed)
            speeds.add(timeline[i].speed)
    return tuple(sorted(speeds))


@dataclass(frozen=True)
class I7Eval:
    """I7 계산 결과. 주문 id는 정렬돼 있다. 나이 추정값은 배속 1 기준 ms(Decimal)."""

    filled: int
    violations: tuple[str, ...]
    undetermined: tuple[str, ...]
    null_age: int
    loose: int  # 전환 전후 느슨한 상한으로 통과시킨 수
    db_age_max_ms: int | None
    app_filled_max: Decimal | None
    app_stale_min: Decimal | None
    warnings: tuple[str, ...]


def _candidates(
    o: DbOrder, timeline: Sequence[SpeedPoint], slack_ms: int
) -> tuple[tuple[Decimal, ...], bool] | None:
    """(배속 후보, 시각을 아는지). 시각을 모르면 후보 전부가 같은 결론일 때만 판정한다."""
    if o.symbol is not None and not o.symbol.startswith(REPLAY_PREFIX):
        return (Decimal(1),), True
    if not timeline:
        return None
    if o.received_ms is None:
        return tuple(sorted({p.speed for p in timeline})), False
    c = speed_candidates(o.received_ms, timeline, slack_ms)
    return None if c is None else (c, True)


def evaluate_i7(
    orders: Sequence[DbOrder],
    max_age_ms: int,
    timeline: Sequence[SpeedPoint],
    *,
    slack_ms: int = SPEED_SLACK_MS,
) -> I7Eval:
    """신선도 위반 체결을 센다(설계 5.4 I7). 입력 순서와 관계없이 같은 결과다."""
    limit = Decimal(max_age_ms)
    viol: list[str] = []
    und: list[str] = []
    filled = null_age = loose = 0
    db_age_max: int | None = None
    filled_max: Decimal | None = None
    stale_min: Decimal | None = None

    for o in sorted(orders, key=lambda x: x.id):
        if o.status == FILLED:
            filled += 1
            if o.received_ms is not None and o.ref_event_ts is not None:
                age = o.received_ms - o.ref_event_ts
                db_age_max = age if db_age_max is None else max(db_age_max, age)
            if o.ref_age_ms is None:
                viol.append(o.id)
                null_age += 1
                continue
            cand = _candidates(o, timeline, slack_ms)
            if cand is None:
                und.append(o.id)
                continue
            speeds, known_t = cand
            age = Decimal(o.ref_age_ms)
            if age * speeds[0] > limit:  # 가장 느슨한 상한도 넘는다
                viol.append(o.id)
            elif not known_t and age * speeds[-1] > limit:
                und.append(o.id)
            elif len(speeds) > 1 and known_t:
                loose += 1
            if known_t and len(speeds) == 1:
                norm = age * speeds[0]
                filled_max = norm if filled_max is None else max(filled_max, norm)
        elif (
            o.status == REJECTED
            and o.reject_reason == STALE_REASON
            and o.ref_age_ms is not None
        ):
            cand = _candidates(o, timeline, slack_ms)
            if cand is not None and cand[1] and len(cand[0]) == 1:
                norm = Decimal(o.ref_age_ms) * cand[0][0]
                stale_min = norm if stale_min is None else min(stale_min, norm)

    warnings: list[str] = []
    if (stale_min is not None and stale_min <= limit) or (
        filled_max is not None and filled_max > limit
    ):
        warnings.append(R.APP_MAX_AGE_DIFFERS)
    return I7Eval(
        filled=filled,
        violations=tuple(viol),
        undetermined=tuple(und),
        null_age=null_age,
        loose=loose,
        db_age_max_ms=db_age_max,
        app_filled_max=filled_max,
        app_stale_min=stale_min,
        warnings=tuple(warnings),
    )


def _i7_result(
    db: DbSnapshot,
    rule: _Rule,
    cfg: IntegrityCfg,
    timeline: tuple[SpeedPoint, ...],
) -> CheckResult:
    base: dict[str, Any] = {
        "section": "orders",
        "criteria": list(rule.items),
        "price_max_age_ms": cfg.price_max_age_ms,
        "slack_ms": SPEED_SLACK_MS,
        "speed_timeline": [
            {"from_ms": p.from_ms, "speed": str(p.speed)} for p in timeline
        ],
        "feed_alive": "unverifiable",
    }
    if "orders" not in db.sections:
        return CheckResult(
            id=I7_ID,
            kind=KIND,
            outcome=Outcome.INCONCLUSIVE,
            required=rule.required,
            on_violation=rule.on_violation,
            value=None,
            reasons=(R.DB_ERROR,),
            detail={**base, "missing": True},
        )
    ev = evaluate_i7(db.orders, cfg.price_max_age_ms, timeline)
    n = len(ev.violations)
    ok = _passes(n, rule)
    if not ok:
        outcome = Outcome.FAIL
        reasons: tuple[str, ...] = (R.NO_SPEED_TIMELINE,) if ev.undetermined else ()
    elif ev.undetermined:
        outcome = Outcome.INCONCLUSIVE
        reasons = (R.NO_SPEED_TIMELINE,)
    else:
        outcome = Outcome.PASS
        reasons = ()
    detail = {
        **base,
        "filled": ev.filled,
        "violations": n,
        "null_ref_age": ev.null_age,
        "undetermined": len(ev.undetermined),
        "loose_window": ev.loose,
        "examples": list(ev.violations[:EXAMPLES_MAX]),
        "undetermined_examples": list(ev.undetermined[:EXAMPLES_MAX]),
        "db_clock_age_max_ms": ev.db_age_max_ms,
        "app_age_estimate": {
            "filled_max": None if ev.app_filled_max is None else str(ev.app_filled_max),
            "stale_min": None if ev.app_stale_min is None else str(ev.app_stale_min),
        },
        "warnings": list(ev.warnings),
    }
    return CheckResult(
        id=I7_ID,
        kind=KIND,
        outcome=outcome,
        required=rule.required,
        on_violation=rule.on_violation,
        value=n if outcome is not Outcome.INCONCLUSIVE else None,
        reasons=reasons,
        detail=detail,
    )


# ---------------------------------------------------------------- 공개 함수


def check_invariants(
    db: DbSnapshot,
    ctx_like: Any = None,
    crit: Any = None,
    *,
    speed_timeline: Sequence[SpeedPoint] | Sequence[Mapping[str, Any]] | None = None,
) -> list[CheckResult]:
    """불변식 구획별 CheckResult(INVARIANT_SQL 순서, 끝에 i7). 설계 5.4.

    ctx_like: replay_speed 속성(SpeedPoint 목록)이 있으면 I7 배속으로 쓴다. None 가능.
    crit: Criteria(권장), IntegrityCfg, 또는 None.
    speed_timeline: Pod 관측 배속(설계 5.2 4단계). 비어 있지 않으면 ctx보다 우선한다.
    """
    criteria, cfg = _criteria_of(crit)
    rules = _rules(criteria)
    timeline = _timeline(speed_timeline) or _timeline(
        getattr(ctx_like, "replay_speed", ())
    )
    out = [_sql_result(db, n, rules[n]) for n in INVARIANT_SQL]
    out.append(_i7_result(db, rules[I7_ID], cfg, timeline))
    return out


def invariant_counts(results: Sequence[CheckResult]) -> dict[str, int | None]:
    """이름(i2, ..., arith, privileges, i7) → 위반 수. INCONCLUSIVE면 None(구획 없음 등)."""
    out: dict[str, int | None] = {}
    for r in results:
        name = r.id[len("inv_") :] if r.id.startswith("inv_") else r.id
        out[name] = None if r.outcome is Outcome.INCONCLUSIVE else r.value
    return dict(sorted(out.items()))


def i4_counts(db: DbSnapshot) -> dict[str, int] | None:
    """inv_i4 problem별 행 수(JUDGE_SELF_CHECK 교차 확인용). 구획이 없으면 None."""
    rows = db.invariant_violations("i4")
    if rows is None:
        return None
    return dict(sorted(Counter(str(r.get("problem")) for r in rows).items()))


_SEVERITY = {Outcome.PASS: 0, Outcome.NA: 0, Outcome.INCONCLUSIVE: 1, Outcome.FAIL: 2}


def item_results(results: Sequence[CheckResult], crit: Criteria) -> list[CheckResult]:
    """구획 결과를 기준 항목(sql: 또는 check: i7을 가진 integrity 항목) 결과로 묶는다.

    outcome은 가장 나쁜 구획 결과(FAIL > INCONCLUSIVE > PASS), value는 모두 평가됐을 때 합,
    reasons는 구획 사유의 합집합이다. 항목의 id·required·on_violation을 쓴다.
    verdict.evaluate_count_check(spec, invariant_counts(...))와 같은 결론이고 사유가 더 자세하다.
    """
    by_id = {r.id: r for r in results}
    out: list[CheckResult] = []
    for item in crit.checks:
        targets = _targets(item)
        if not targets:
            continue
        parts: list[CheckResult] = []
        missing: list[str] = []
        for t in targets:
            r = by_id.get(t if t == I7_ID else f"inv_{t}")
            if r is None:
                missing.append(t)
            else:
                parts.append(r)
        worst = max((_SEVERITY[p.outcome] for p in parts), default=0)
        if missing:
            worst = max(worst, 1)
        outcome = [Outcome.PASS, Outcome.INCONCLUSIVE, Outcome.FAIL][worst]
        reasons = sorted({c for p in parts for c in p.reasons})
        if missing:
            reasons = sorted({*reasons, R.NOT_EVALUATED})
        values = [p.value for p in parts]
        value = (
            None
            if missing or any(v is None for v in values)
            else sum(int(v) for v in values)
        )
        out.append(
            CheckResult(
                id=str(item["id"]),
                kind=KIND,
                outcome=outcome,
                required=bool(item.get("required", True)),
                on_violation=str(item.get("on_violation", "fail")),
                value=value,
                reasons=tuple(reasons),
                detail={
                    "parts": {
                        p.id: {
                            "outcome": p.outcome.value,
                            "value": p.value,
                            "reasons": list(p.reasons),
                        }
                        for p in parts
                    },
                    "missing": missing,
                },
            )
        )
    return out

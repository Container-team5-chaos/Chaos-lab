"""판정 집계와 verdict 생성(설계 1.4, 5.2-6, 5.7, 5.8).

- combine: 기준·가드 결과를 설계 5.7 규칙 1–5로 하나의 verdict로 모은다.
- evaluate_count_check: class·sql·class_sub 등 건수 기준을 op·value로 비교한다(설계 6).
- k6_exit_to_checks: k6 종료 코드와 summary를 기준 결과로 바꾼다(설계 5.7 하단).
- build_verdict, verdict_digest: verdict.json v1과 그 digest(설계 5.2-6, 5.8).
- exit_code_for: verdict → 종료 코드(설계 1.4).

모든 함수는 순수 함수다. 현재 시각·난수·dict 순서에 의존하지 않는다.
"""

from __future__ import annotations

import math
from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any

from . import reasons as R
from .errors import EXIT_BY_VERDICT, JudgeError
from .model import (
    DIGEST_EXCLUDED,
    CheckResult,
    OkSub,
    Outcome,
    Verdict,
    canonical_json,
    sha256_text,
)

__all__ = [
    "combine",
    "effective_outcome",
    "item_reasons",
    "evaluate_count_check",
    "requires_met",
    "k6_exit_to_checks",
    "build_verdict",
    "verdict_digest",
    "exit_code_for",
    "GUARD_KINDS",
]

# 가드 목록에 들어가는 kind. on_violation 기본값이 inconclusive다(설계 5.6, criteria.py).
GUARD_KINDS = frozenset({"guard", "fault_effect"})

# ---------------------------------------------------------------- 집계(설계 5.7)


def _as_outcome(v: Any) -> Outcome | None:
    """Outcome으로 바꾼다. 값이 없거나 모르는 값이면 None(평가하지 못함)."""
    if isinstance(v, Outcome):
        return v
    try:
        return Outcome(v)
    except ValueError:
        return None


def effective_outcome(c: CheckResult) -> Outcome | None:
    """on_violation을 적용한 결과(설계 6). None은 평가하지 못함이다.

    on_violation=inconclusive인 기준의 FAIL은 INCONCLUSIVE로 바꾼다.
    """
    o = _as_outcome(c.outcome)
    if o is Outcome.FAIL and c.on_violation == "inconclusive":
        return Outcome.INCONCLUSIVE
    return o


def item_reasons(c: CheckResult, *, guard: bool = False) -> tuple[str, ...]:
    """verdict 사유로 올릴 코드(설계 5.6, 5.7).

    기준이 낸 사유가 있으면 그대로 쓴다. 없으면 기본 사유를 붙인다.
    - 가드: GUARD_REASON[id], fault_effect kind면 FAULT_NOT_OBSERVED.
    - 평가하지 못함(모르는 outcome): NOT_EVALUATED.
    - FAIL: CHECK_FAILED. INCONCLUSIVE: CHECK_INCONCLUSIVE.
    """
    if c.reasons:
        return tuple(c.reasons)
    eff = effective_outcome(c)
    if eff is None:
        return (R.NOT_EVALUATED,)
    if guard:
        if c.id in R.GUARD_REASON:
            return (R.GUARD_REASON[c.id],)
        if c.kind == "fault_effect":
            return (R.FAULT_NOT_OBSERVED,)
    if eff is Outcome.FAIL:
        return (R.CHECK_FAILED,)
    return (R.CHECK_INCONCLUSIVE,)


def _uniq(codes: Any) -> tuple[str, ...]:
    return tuple(sorted(set(codes)))


def combine(
    checks: Sequence[CheckResult], guards: Sequence[CheckResult]
) -> tuple[Outcome, tuple[str, ...]]:
    """기준·가드 결과를 verdict와 사유로 모은다(설계 5.7).

    규칙(앞이 이긴다)
    1. checks 중 kind=integrity, required, FAIL → FAIL(INTEGRITY_VIOLATION + 그 기준 사유).
       가드보다 앞선다.
    2. required 가드가 미충족(INCONCLUSIVE 또는 평가하지 못함) → INCONCLUSIVE(가드 사유).
    3. 나머지 required FAIL(on_violation=fail인 가드 포함) → FAIL.
    4. required가 INCONCLUSIVE이거나 평가하지 못함 → INCONCLUSIVE. 조용히 건너뛰지 않는다.
    5. 그 밖은 PASS.

    - required=false는 보고만 한다. N/A(requires 불일치)는 집계에서 뺀다.
    - on_violation=inconclusive인 기준의 FAIL은 INCONCLUSIVE로 본다(effective_outcome).
    - 덧붙인 규칙: required checks가 모두 N/A이거나 없으면 공허한 PASS를 막으려고
      INCONCLUSIVE(NOT_EVALUATED)다.
    - 사유는 정렬하고 중복을 뺀다.
    """
    req_checks = [c for c in checks if c.required]
    req_guards = [g for g in guards if g.required]

    money = [
        c
        for c in req_checks
        if c.kind == "integrity" and effective_outcome(c) is Outcome.FAIL
    ]
    if money:
        codes = [R.INTEGRITY_VIOLATION]
        for c in money:
            codes.extend(c.reasons)
        return Outcome.FAIL, _uniq(codes)

    unmet = [
        g for g in req_guards if effective_outcome(g) in (Outcome.INCONCLUSIVE, None)
    ]
    if unmet:
        return Outcome.INCONCLUSIVE, _uniq(
            r for g in unmet for r in item_reasons(g, guard=True)
        )

    failed = [
        (c, is_guard)
        for group, is_guard in ((req_checks, False), (req_guards, True))
        for c in group
        if effective_outcome(c) is Outcome.FAIL
    ]
    if failed:
        return Outcome.FAIL, _uniq(
            r for c, is_guard in failed for r in item_reasons(c, guard=is_guard)
        )

    inconclusive = [
        c for c in req_checks if effective_outcome(c) in (Outcome.INCONCLUSIVE, None)
    ]
    if inconclusive:
        return Outcome.INCONCLUSIVE, _uniq(
            r for c in inconclusive for r in item_reasons(c)
        )

    if not any(effective_outcome(c) is Outcome.PASS for c in req_checks):
        return Outcome.INCONCLUSIVE, (R.NOT_EVALUATED,)
    return Outcome.PASS, ()


# ---------------------------------------------------------------- 건수 기준(설계 6)

# 건수 출처 키(앞이 우선). 값이 이름 목록이면 합산한다.
_COUNT_SOURCES = ("class", "sql", "class_sub", "check", "stat", "observe")
# 음수가 없는 건수라 일부만 알아도 결과가 정해질 수 있는 출처
_MONOTONE_SOURCES = frozenset({"class", "sql", "class_sub", "observe"})


def _num(v: Any, where: str) -> Decimal:
    """비교용 Decimal. float는 repr 문자열로 바꿔 이진 오차를 피한다."""
    if isinstance(v, bool):
        raise ValueError(f"{where}: bool은 수가 아니다")
    if isinstance(v, Decimal):
        d = v
    elif isinstance(v, int):
        d = Decimal(v)
    elif isinstance(v, float):
        if not math.isfinite(v):
            raise ValueError(f"{where}: 유한한 수가 아니다({v!r})")
        d = Decimal(repr(v))
    else:
        try:
            d = Decimal(str(v))
        except InvalidOperation as e:
            raise ValueError(f"{where}: 수가 아니다({v!r})") from e
    if not d.is_finite():
        raise ValueError(f"{where}: 유한한 수가 아니다({v!r})")
    return d


def _compare(measured: Decimal, op: str, threshold: Decimal) -> bool:
    if op == "==":
        return measured == threshold
    if op == "!=":
        return measured != threshold
    if op == "<":
        return measured < threshold
    if op == "<=":
        return measured <= threshold
    if op == ">":
        return measured > threshold
    if op == ">=":
        return measured >= threshold
    raise ValueError(f"모르는 op: {op!r}")


def _decided_by_partial(partial: Decimal, op: str, threshold: Decimal) -> bool | None:
    """음수가 아닌 미지 건수를 더해도 결과가 같으면 그 결과, 아니면 None.

    합은 아는 건수(partial) 이상이다. 그래서 partial이 이미 상한을 넘으면 FAIL,
    이미 하한을 넘으면 PASS로 정해진다.
    """
    if op in ("==", "<="):
        return False if partial > threshold else None
    if op == "<":
        return False if partial >= threshold else None
    if op in (">", "!="):
        return True if partial > threshold else None
    if op == ">=":
        return True if partial >= threshold else None
    return None


def requires_met(spec: Mapping[str, Any], features: Collection[str]) -> bool:
    """requires: 이름이 모두 features에 있으면 True(설계 5.7 규칙 5)."""
    req = spec.get("requires")
    if not req:
        return True
    names = [req] if isinstance(req, str) else list(req)
    return all(n in features for n in names)


def _spec_kind(spec: Mapping[str, Any]) -> str:
    kind = spec.get("kind")
    if kind:
        return str(kind)
    if "observe" in spec or "class_sub" in spec:
        return "fault_effect"
    return "guard"


def _spec_common(spec: Mapping[str, Any]) -> tuple[str, str, bool, str]:
    item_id = spec.get("id")
    if not isinstance(item_id, str) or not item_id:
        raise JudgeError(f"기준에 id가 없다: {dict(spec)!r}")
    kind = _spec_kind(spec)
    default_ov = "inconclusive" if kind in GUARD_KINDS else "fail"
    return (
        item_id,
        kind,
        bool(spec.get("required", True)),
        str(spec.get("on_violation", default_ov)),
    )


def _jsonable_num(v: Decimal, all_int: bool) -> Any:
    if all_int and v == v.to_integral_value():
        return int(v)
    return v


def evaluate_count_check(
    check_spec: Mapping[str, Any],
    counts: Mapping[str, Any],
    *,
    unavailable: Mapping[str, str] | None = None,
    features: Collection[str] = (),
) -> CheckResult:
    """건수·측정값을 op·value로 비교해 CheckResult를 만든다(설계 6 checks, 5.6 fault_effect).

    check_spec: 병합된 기준 항목(id, kind, class|sql|class_sub|check|stat|observe, op, value,
        min, required, on_violation, requires).
    counts: 이름 → 건수(또는 측정값). 구간(phases) 거르기는 호출자가 미리 한다.
        - class·class_sub: 키가 없으면 0건이다.
        - sql: 키가 없거나 None이면 구획이 빠진 것(INCONCLUSIVE(DB_ERROR)).
        - check·stat·observe: 키가 없거나 None이면 INCONCLUSIVE(NOT_EVALUATED).
    unavailable: 평가하지 않을 이름 → 사유(예: ACCOUNT_SET_MISMATCH면 LOST·ORPHAN, 설계 5.7 규칙 1).
        음수가 없는 건수 출처는 아는 건수만으로 결과가 정해지면 그 결과를 쓴다
        (예: '== 0'인데 다른 클래스가 이미 1건 → FAIL).
    features: requires 이름 중 충족된 것. 모자라면 N/A.

    op·value가 없으면 min이 있을 때 '>= min', fault_effect면 '>= 1'로 본다. 그 밖은 평가하지 못함.
    결과의 outcome은 비교 그대로다(on_violation 적용은 combine 몫). FAIL의 reasons는 비운다.
    """
    item_id, kind, required, on_violation = _spec_common(check_spec)
    unavailable = unavailable or {}

    def result(
        outcome: Outcome, value: Any, codes: Any, detail: Mapping[str, Any]
    ) -> CheckResult:
        return CheckResult(
            id=item_id,
            kind=kind,
            outcome=outcome,
            required=required,
            on_violation=on_violation,
            value=value,
            reasons=_uniq(codes),
            detail=dict(detail),
        )

    if not requires_met(check_spec, features):
        req = check_spec.get("requires")
        req_names = [req] if isinstance(req, str) else sorted(req)
        return result(Outcome.NA, None, (), {"requires": req_names})

    source = next((k for k in _COUNT_SOURCES if k in check_spec), None)
    raw = item_id if source is None else check_spec[source]
    names = sorted({raw} if isinstance(raw, str) else {str(x) for x in raw})

    if "op" in check_spec and "value" in check_spec:
        op, threshold_raw = str(check_spec["op"]), check_spec["value"]
    elif "min" in check_spec:
        op, threshold_raw = ">=", check_spec["min"]
    elif kind == "fault_effect":
        op, threshold_raw = ">=", 1
    else:
        return result(
            Outcome.INCONCLUSIVE,
            None,
            (R.NOT_EVALUATED,),
            {"source": source, "names": names, "note": "op/value 없음"},
        )
    threshold = _num(threshold_raw, f"{item_id}.value")

    known: dict[str, Any] = {}
    missing: dict[str, str] = {}
    for n in names:
        if n in unavailable:
            missing[n] = unavailable[n]
            continue
        v = counts.get(n)
        if v is None and source in ("class", "class_sub") and n not in counts:
            v = 0
        if v is None:
            missing[n] = R.DB_ERROR if source == "sql" else R.NOT_EVALUATED
            continue
        _num(v, f"{item_id}.counts[{n}]")
        known[n] = v

    all_int = all(
        isinstance(v, int) and not isinstance(v, bool) for v in known.values()
    )
    partial = sum((_num(v, n) for n, v in known.items()), Decimal(0))
    detail: dict[str, Any] = {
        "source": source,
        "names": names,
        "counts": {n: known.get(n) for n in names},
        "op": op,
        "threshold": threshold_raw,
    }
    if missing:
        detail["unavailable"] = dict(sorted(missing.items()))
        decided = None
        if source in _MONOTONE_SOURCES and all(
            _num(v, n) >= 0 for n, v in known.items()
        ):
            decided = _decided_by_partial(partial, op, threshold)
        if decided is None:
            return result(Outcome.INCONCLUSIVE, None, missing.values(), detail)
        detail["decided_by_partial"] = True
        outcome = Outcome.PASS if decided else Outcome.FAIL
        return result(outcome, _jsonable_num(partial, all_int), (), detail)

    if len(names) == 1:
        value: Any = known[names[0]]
    else:
        value = _jsonable_num(partial, all_int)
    outcome = Outcome.PASS if _compare(partial, op, threshold) else Outcome.FAIL
    return result(outcome, value, (), detail)


# ---------------------------------------------------------------- k6 종료 코드(설계 5.7 하단)

K6_EXIT_OK = 0
K6_EXIT_THRESHOLDS = 99  # threshold 실패, abortOnFail 중단도 99(4.7)
K6_EXIT_INVALID_CONFIG = 104
K6_EXIT_EXTERNAL_ABORT = 105  # SIGINT(judge stop). 확인 필요: 스모크
K6_EXIT_SCRIPT_EXCEPTION = 107  # init throw(4.1 안전장치)
K6_EXIT_SCRIPT_ABORTED = 108  # exec.test.abort()(setup abort)

K6_EXIT_REASON = {
    K6_EXIT_INVALID_CONFIG: R.LOAD_INVALID,
    K6_EXIT_SCRIPT_EXCEPTION: R.LOAD_INVALID,
    K6_EXIT_SCRIPT_ABORTED: R.LOAD_ABORTED,
}
# 끝까지 돈 99에서 실패한 threshold의 해석(4.7, 5.7)
K6_SLO_METRICS = frozenset(
    {"order_ack_ms", "order_sys_err", "http_req_duration", "http_req_failed"}
)
K6_DELEGATED_METRICS = {
    "resend_mismatch": ("resend_mismatch", "IDEMPOTENCY_VIOLATION"),
    "order_protocol_errors": ("protocol", "PROTOCOL"),
}


def _failed_thresholds(summary: Mapping[str, Any]) -> list[tuple[str, str]] | None:
    """handleSummary 기존 모양의 metrics에서 ok=false인 (지표, 식). metrics가 없으면 None."""
    metrics = summary.get("metrics")
    if not isinstance(metrics, Mapping):
        return None
    out: list[tuple[str, str]] = []
    for name in sorted(metrics):
        m = metrics[name]
        th = m.get("thresholds") if isinstance(m, Mapping) else None
        if not isinstance(th, Mapping):
            continue
        for expr in sorted(th):
            v = th[expr]
            ok = v.get("ok") if isinstance(v, Mapping) else v
            if ok is False:
                out.append((str(name), str(expr)))
    return out


def _run_duration_ms(summary: Mapping[str, Any]) -> Decimal | int | float | None:
    """state.testRunDurationMs. 증거는 parse_float=Decimal로 읽으므로 Decimal도 받는다."""
    state = summary.get("state")
    if not isinstance(state, Mapping):
        return None
    v = state.get("testRunDurationMs")
    if isinstance(v, Decimal):
        return v if v.is_finite() else None
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        return None
    return v


def _interpret_thresholds(
    base_id: str, failed: list[tuple[str, str]]
) -> tuple[list[CheckResult], list[str]]:
    """실패 threshold를 보고용 기준(required=false)과 해석 못 한 지표로 나눈다."""
    slo: list[str] = []
    delegated: dict[str, list[str]] = {}
    unknown: list[str] = []
    for name, expr in failed:
        base = name.split("{", 1)[0]
        label = f"{name}: {expr}"
        if base in K6_SLO_METRICS:
            slo.append(label)
        elif base in K6_DELEGATED_METRICS:
            delegated.setdefault(base, []).append(label)
        else:
            unknown.append(label)
    extra: list[CheckResult] = []
    if slo:
        extra.append(
            CheckResult(
                id=f"{base_id}_slo",
                kind="k6",
                outcome=Outcome.FAIL,
                required=False,
                reasons=(R.K6_THRESHOLD_DISAGREES,),
                detail={"thresholds": slo, "note": "SLO는 judge 구간 판정만 쓴다(4.7)"},
            )
        )
    for base in sorted(delegated):
        suffix, cls = K6_DELEGATED_METRICS[base]
        extra.append(
            CheckResult(
                id=f"{base_id}_{suffix}",
                kind="k6",
                outcome=Outcome.FAIL,
                required=False,
                detail={
                    "thresholds": delegated[base],
                    "delegated_to": f"classify:{cls}",
                },
            )
        )
    return extra, unknown


def k6_exit_to_checks(
    exit_code: int | None,
    summary: Mapping[str, Any] | None,
    plan_duration_ms: int | None,
    *,
    setup_ms: int = 0,
    stopped_by_judge: bool = False,
    spec: Mapping[str, Any] | None = None,
) -> list[CheckResult]:
    """k6 종료 코드를 기준 결과로 바꾼다(설계 5.7 하단, 4.7).

    첫 원소는 늘 k6_exit 기준(spec이 있으면 그 id·required·on_violation)이다.
    - 0 → PASS.
    - 99: state.testRunDurationMs < plan_duration_ms + setup_ms면 abortOnFail 중단(LOAD_ABORTED).
      끝까지 돌았으면 실패한 threshold로 해석한다. k6_exit는 PASS이고 해석은 required=false 기준으로
      덧붙인다. SLO 지표는 경고 K6_THRESHOLD_DISAGREES, resend_mismatch·order_protocol_errors는
      분류기 결과(IDEMPOTENCY_VIOLATION, PROTOCOL)에 맡긴다. 모르는 지표나 실패 threshold가 없는
      99는 해석할 수 없어 INCONCLUSIVE(CHECK_INCONCLUSIVE)다.
      summary·지속 시간·계획 길이가 없으면 INCONCLUSIVE(NOT_EVALUATED).
    - 104·107 → INCONCLUSIVE(LOAD_INVALID), 108 → INCONCLUSIVE(LOAD_ABORTED).
    - 105(SIGINT): stopped_by_judge면 의도한 정지라 99 끝까지 돈 경우처럼 threshold만 해석한다.
      아니면 LOAD_ABORTED.
    - None(종료 코드 모름) → INCONCLUSIVE(NOT_EVALUATED). 그 밖(OOM 137 등) → LOAD_ABORTED.

    plan_duration_ms는 stage 합이다. testRunDurationMs는 setup 시간을 포함하므로 setup_ms
    (setup_done.t − run_start.t)를 주면 중단 판별이 더 정확해진다.
    """
    spec = spec or {"id": "k6_exit", "kind": "k6"}
    item_id, kind, required, on_violation = _spec_common({"kind": "k6", **spec})

    def main(outcome: Outcome, codes: Any, detail: Mapping[str, Any]) -> CheckResult:
        return CheckResult(
            id=item_id,
            kind=kind,
            outcome=outcome,
            required=required,
            on_violation=on_violation,
            value=exit_code,
            reasons=_uniq(codes),
            detail={"exit_code": exit_code, **detail},
        )

    if exit_code is None or isinstance(exit_code, bool):
        return [
            main(Outcome.INCONCLUSIVE, (R.NOT_EVALUATED,), {"note": "종료 코드 없음"})
        ]
    if exit_code == K6_EXIT_OK:
        return [main(Outcome.PASS, (), {})]
    if exit_code in K6_EXIT_REASON:
        return [main(Outcome.INCONCLUSIVE, (K6_EXIT_REASON[exit_code],), {})]
    if exit_code == K6_EXIT_EXTERNAL_ABORT and not stopped_by_judge:
        return [main(Outcome.INCONCLUSIVE, (R.LOAD_ABORTED,), {"note": "외부 중단"})]
    if exit_code not in (K6_EXIT_THRESHOLDS, K6_EXIT_EXTERNAL_ABORT):
        return [main(Outcome.INCONCLUSIVE, (R.LOAD_ABORTED,), {})]

    if not isinstance(summary, Mapping):
        return [
            main(Outcome.INCONCLUSIVE, (R.NOT_EVALUATED,), {"note": "summary 없음"})
        ]
    detail: dict[str, Any] = {}
    if exit_code == K6_EXIT_THRESHOLDS:
        ran = _run_duration_ms(summary)
        if ran is None or plan_duration_ms is None:
            return [
                main(
                    Outcome.INCONCLUSIVE,
                    (R.NOT_EVALUATED,),
                    {"note": "testRunDurationMs 또는 계획 길이 없음"},
                )
            ]
        expected = plan_duration_ms + setup_ms
        detail = {"test_run_duration_ms": ran, "expected_min_ms": expected}
        if ran < expected:
            return [main(Outcome.INCONCLUSIVE, (R.LOAD_ABORTED,), detail)]
    else:
        detail = {"note": "judge stop(SIGINT)"}

    failed = _failed_thresholds(summary)
    if failed is None:
        return [
            main(
                Outcome.INCONCLUSIVE,
                (R.NOT_EVALUATED,),
                {**detail, "note": "metrics 없음"},
            )
        ]
    detail["failed_thresholds"] = [f"{n}: {e}" for n, e in failed]
    extra, unknown = _interpret_thresholds(item_id, failed)
    if unknown:
        detail["unknown_thresholds"] = unknown
        return [main(Outcome.INCONCLUSIVE, (R.CHECK_INCONCLUSIVE,), detail), *extra]
    if exit_code == K6_EXIT_THRESHOLDS and not failed:
        detail["note"] = "99인데 실패한 threshold가 없다"
        return [main(Outcome.INCONCLUSIVE, (R.CHECK_INCONCLUSIVE,), detail)]
    return [main(Outcome.PASS, (), detail), *extra]


# ---------------------------------------------------------------- verdict(설계 5.2-6, 5.8)


def verdict_digest(verdict_dict: Mapping[str, Any] | Verdict) -> str:
    """generated_at·run_info·verdict_digest를 뺀 canonical JSON의 sha256(설계 5.2-6).

    canonical JSON = sort_keys, separators=(',', ':'), ensure_ascii=False, Decimal은 문자열.
    NaN·Inf가 있으면 ValueError다.
    """
    d = verdict_dict.to_dict() if isinstance(verdict_dict, Verdict) else verdict_dict
    body = {k: v for k, v in d.items() if k not in DIGEST_EXCLUDED}
    return sha256_text(canonical_json(body))


def _sorted_map(v: Any) -> Any:
    """Mapping 키를 재귀로 정렬한 dict로 바꾼다(출력이 입력 dict 순서에 의존하지 않게)."""
    if isinstance(v, Mapping):
        return {k: _sorted_map(v[k]) for k in sorted(v, key=str)}
    if isinstance(v, list):
        return [_sorted_map(x) for x in v]
    if isinstance(v, tuple):
        return tuple(_sorted_map(x) for x in v)
    return v


def _ctx_get(ctx_like: Any, key: str) -> str:
    v = (
        ctx_like.get(key)
        if isinstance(ctx_like, Mapping)
        else getattr(ctx_like, key, None)
    )
    if not isinstance(v, str) or not v:
        raise JudgeError(f"verdict에 필요한 {key}가 없다")
    return v


def _derived_warnings(
    checks: Sequence[CheckResult],
    guards: Sequence[CheckResult],
    integrity: Mapping[str, Any],
) -> set[str]:
    """기준 사유 중 경고 코드와 after_503 경고(설계 5.3)."""
    out = {r for c in (*checks, *guards) for r in c.reasons if r in R.WARNINGS}
    subs = integrity.get("subs")
    if isinstance(subs, Mapping):
        n = subs.get(OkSub.AFTER_503)
        if isinstance(n, int) and not isinstance(n, bool) and n > 0:
            out.add(R.ROLLBACK_REPLY_COMMITTED)
    return out


def build_verdict(
    ctx_like: Mapping[str, Any] | Any,
    checks: Sequence[CheckResult],
    guards: Sequence[CheckResult],
    integrity: Mapping[str, Any],
    client: Mapping[str, Any],
    load: Mapping[str, Any],
    inputs: Mapping[str, Any],
    warnings: Collection[str],
    run_info: Mapping[str, Any],
    generated_at: int | None,
) -> Verdict:
    """verdict.json v1을 만든다(설계 5.8).

    ctx_like: run_id·experiment를 가진 Mapping 또는 RunContext.
    verdict·reasons는 combine으로 정한다. checks·guards 순서는 그대로 둔다(기준 파일 순서).
    Mapping 입력은 키를 정렬해 담는다. warnings는 주어진 것 + 기준 사유 속 경고 코드 +
    subs.after_503 > 0이면 ROLLBACK_REPLY_COMMITTED(정렬, 중복 제거).
    verdict_digest는 verdict_digest()로 채운다. generated_at은 epoch ms(digest 밖)다.
    """
    if generated_at is not None and (
        isinstance(generated_at, bool) or not isinstance(generated_at, int)
    ):
        raise JudgeError("generated_at은 epoch ms 정수여야 한다")
    checks = tuple(checks)
    guards = tuple(guards)
    outcome, why = combine(checks, guards)
    warn = set(warnings) | _derived_warnings(checks, guards, integrity)
    v = Verdict(
        run_id=_ctx_get(ctx_like, "run_id"),
        experiment=_ctx_get(ctx_like, "experiment"),
        verdict=outcome,
        reasons=why,
        checks=checks,
        guards=guards,
        integrity=_sorted_map(integrity),
        client=_sorted_map(client),
        load=_sorted_map(load),
        inputs=_sorted_map(inputs),
        warnings=tuple(sorted(warn)),
        run_info=_sorted_map(run_info),
        generated_at=generated_at,
    )
    return replace(v, verdict_digest=verdict_digest(v.to_dict()))


def exit_code_for(outcome: Outcome | str) -> int:
    """verdict → 종료 코드(설계 1.4). PASS 0, FAIL 10, INCONCLUSIVE 11.

    N/A나 모르는 값은 verdict가 될 수 없어 JudgeError(종료 코드 1)다.
    """
    o = _as_outcome(outcome)
    if o is None or o not in EXIT_BY_VERDICT:
        raise JudgeError(f"종료 코드로 바꿀 수 없는 verdict: {outcome!r}")
    return EXIT_BY_VERDICT[o]

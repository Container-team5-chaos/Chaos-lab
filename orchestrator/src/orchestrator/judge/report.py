"""report.md 조각(설계 5.8, 9 시연 최소선).

fragment(verdict)는 Verdict만 읽는 순수 함수다. 현재 시각·generated_at·run_info는 쓰지 않는다.
첫 줄은 시연 화면 한 줄 요약이다.
    유실 0 · 불일치 0 · 중복 0 · 고아 0 · 잔고 불일치 0 · 신선도 위반 체결 0 → PASS

읽는 verdict 필드(값이 없으면 '?'로 쓴다)
- integrity.intents: {"total": int, "ack": int, ...} 또는 int(의도 수만).
- integrity.log_complete: bool.
- integrity.classes: OrderClass 값 → 건수. integrity.subs: OkSub 값 → 건수.
- integrity.not_evaluated: 평가하지 않은 클래스 → 사유("*"는 전부). 그 클래스가 든 수치는
  확정 값처럼 쓰지 않는다. 아는 부분만 있으면 '≥n(사유)', 없으면 '?(사유)'다(설계 5.3, 5.7 규칙 1).
- integrity.invariants: 불변식 id(cash, qty, avg, nonneg, arith, constraints, fresh, 명세 7) → 위반 수.
  None은 평가하지 못함(DB_ERROR, SCHEMA_DRIFT, ticks 없음 등). 값이 {"count"|"violations": n} 맵이어도
  읽는다. 잔고 불일치 = cash + qty + avg. fresh가 없으면 checks의 stale_fill 값을 쓴다.
- client.phases: 구간 → {"n", "ack_p95_ms", "sys_err_ratio", "achieved_ratio"}.
  phases 키가 없으면 client 최상위의 맵 값을 구간으로 본다.
- inputs: criteria_sha256, run_json_sha256, evidence_manifest_sha256, judge_version,
  response_fields_confirmed.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from . import reasons as R
from .model import (
    DUPLICATE_CLASSES,
    LOSS_CLASSES,
    MISMATCH_CLASSES,
    ORPHAN_CLASSES,
    USER_IMPACT_CLASSES,
    CheckResult,
    OkSub,
    OrderClass,
    Outcome,
    Verdict,
    canonical_json,
)
from .verdict import effective_outcome, item_reasons

__all__ = ["fragment", "summary_line", "REMEDY", "BALANCE_INVARIANTS"]

# 잔고 불일치 = cash + qty + avg(명세 7. 현금 보존식, 수량 보존식, 평균가 재생)
BALANCE_INVARIANTS = ("cash", "qty", "avg")
STALE_FILL_INVARIANT = "fresh"  # 명세 7 fresh(선택)
STALE_FILL_CHECK_ID = "stale_fill"  # default.yaml 기준 id

PHASE_ORDER = ("warmup", "baseline", "fault", "recovery", "tail", "steady")

# INCONCLUSIVE 사유 → 해소 방법(설계 4.1–4.8, 5.2–5.7)
REMEDY = {
    R.SAFETY_VIOLATION: "intent의 market·user_id·client_order_id(k6-<run_id>-…), 1 VU = 1 user, 기록의 token·authorization 키를 확인하고 k6 스크립트를 고친 뒤 새 run_id로 다시 돈다.",
    R.ACCOUNT_SET_MISMATCH: "JSONL의 user_id가 DB accounts에 없다. psql 대상(run.json db)과 seed.sql 적용을 확인하고 다시 돈다.",
    R.LOW_FILL_COVERAGE: "구간 체결 수가 기준보다 적다. 시세 수집(Redis price:<market>)과 stale_price·price_unavailable 거절 수를 확인하고 다시 돈다.",
    R.GENERATOR_LIMITED: "달성률이 기준보다 낮다. k6 자원, VU 수, dropped_iterations를 확인한다.",
    R.FAULT_OUTSIDE_LOAD: "장애 시각이 부하 창 밖이다. 주입을 부하 창 안으로 옮기거나 DURATION_S를 늘린다.",
    R.FAULT_NOT_OBSERVED: "기대한 장애 효과가 관측되지 않았다. 주입 대상·시각과 raw/k8s Pod 관측을 확인한다.",
    R.DB_NOT_QUIESCENT: "스냅샷 중 DB가 바뀌었다. 부하가 멈춘 것을 확인하고 judge collect를 다시 돈다.",
    R.SENT_LOG_INCOMPLETE: "orders.jsonl이 불완전하다. k6 종료 코드, Counter, sha256을 확인한다. 고아·unresolved 기준만 영향받는다.",
    R.SENT_LOG_MISSING: "기록을 회수하지 못했다. Job을 지우지 말고 judge fetch를 다시 돈다.",
    R.RESPONSE_SCHEMA_UNCONFIRMED: "응답 필드를 앱 팀과 확정하고 response_fields_confirmed: true로 judge evaluate --offline을 다시 돈다.",
    R.LOAD_ABORTED: "k6가 중간에 멈췄다. k6/pod-status.json과 summary.json을 확인하고 다시 돈다.",
    R.LOAD_INVALID: "k6 스크립트·설정 오류다(104·107). env와 안전장치 조건을 고쳐 다시 돈다.",
    R.DB_ERROR: "psql 구획이 빠졌다. raw/db 출력과 psql 대상(run.json db: pod·container·user·database)을 확인하고 judge collect를 다시 돈다.",
    R.EXTRA_ACCOUNTS_MISSING: "발표자 계좌가 없다. judge mark --extra-account로 넣고 judge evaluate를 다시 돈다.",
    R.CLOCK_SKEW: "노드와 노트북 시계 차이가 크다. derived.json을 확인하고 시간 동기화 뒤 다시 돈다.",
    R.LOW_SAMPLES: "구간 표본이 모자란다. 구간을 늘리거나 RATE를 올린다.",
    R.JUDGE_SELF_CHECK: "스냅샷 run 키 주문 수와 분류기 D1 수가 다르거나 스냅샷 행이 지문과 다르다. classified.csv와 raw/db를 대조한다(judge 결함 가능).",
    R.SCHEMA_DRIFT: "DB 스키마가 서비스 schema.sql과 다르다(테이블·열·UNIQUE·CHECK). 앱 팀과 스키마를 맞추고 다시 돈다.",
    R.REJECT_AFTER_INFLIGHT: "결과 불명 시도 뒤 재전송이 확정 거절을 받았는데 앞 시도가 늦게 체결됐다(서비스 find_order가 잠금 밖). 클라이언트는 거절로 안내받았으므로 유실 기준 FAIL이다. 앱 팀에 멱등 조회를 잠금 안으로 옮기도록 요청한다.",
    R.SNAPSHOT_LARGE: "orders 행이 많아 스냅샷이 크다. 판정은 그대로다. 실험 전 DB를 seed로 다시 채운다.",
    R.CHECK_INCONCLUSIVE: "기준을 판정할 수 없었다. 해당 기준의 detail을 확인한다.",
    R.NOT_EVALUATED: "평가하지 못한 기준이 있다. 입력과 증거 파일이 모두 있는지 확인한다.",
    R.LOW_TRAFFIC: "회복 판정 구간의 트래픽이 모자란다. RATE를 올리거나 구간을 늘린다.",
    R.OBSERVATION_TOO_SHORT: "회복 관찰 시간이 짧다. tail을 늘려 다시 돈다.",
    R.PROM_ERROR: "Prometheus 조회가 실패했다. 접근 경로와 쿼리를 확인한다.",
    R.CRITERIA_ERROR: "PromQL 기준이 잘못됐다. 기준 파일의 쿼리를 고친다.",
    R.BUCKET_BOUNDARY_MISSING: "히스토그램 경계가 없다. 앱 팀에 버킷 경계를 요청한다.",
    R.CHEAP_PATH: "체결 비율이 낮아 한계가 부풀려졌다. 시세와 주문 가격을 확인한다.",
}
DEFAULT_REMEDY = "설계 문서(b-measure-judge.md)에서 해당 사유를 찾아 확인한다."


# ---------------------------------------------------------------- 값 읽기


def _count(v: Any) -> int | None:
    """건수로 읽는다. 없거나 모르는 모양이면 None."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, (Decimal, float)):
        return int(v) if v == int(v) else None
    if isinstance(v, Mapping):
        for k in ("count", "violations", "n"):
            if k in v:
                return _count(v[k])
        rows = v.get("rows")
        return len(rows) if isinstance(rows, (list, tuple)) else None
    if isinstance(v, (list, tuple)):
        return len(v)
    return None


def _section(v: Verdict, name: str) -> Mapping[str, Any] | None:
    sec = v.integrity.get(name)
    return sec if isinstance(sec, Mapping) else None


def _sum_classes(v: Verdict, names: frozenset[OrderClass]) -> int | None:
    classes = _section(v, "classes")
    if classes is None:
        return None
    total = 0
    for n in sorted(names):
        c = _count(classes.get(n, 0))
        if c is None:
            return None
        total += c
    return total


def _not_evaluated(v: Verdict) -> Mapping[str, Any]:
    ne = v.integrity.get("not_evaluated")
    return ne if isinstance(ne, Mapping) else {}


def _class_text(v: Verdict, names: frozenset[OrderClass]) -> str:
    """클래스 묶음 수치. 평가하지 않은 클래스가 있으면 확정 값으로 쓰지 않는다.

    평가하지 않은 클래스가 없으면 합계, 있으면 아는 부분 합이 양수일 때 '≥n(사유)', 아니면 '?(사유)'.
    """
    ne = _not_evaluated(v)
    skipped = frozenset(names) if "*" in ne else frozenset(n for n in names if n in ne)
    if not skipped:
        return _n(_sum_classes(v, names))
    why = ",".join(sorted({str(ne.get(n, ne.get("*"))) for n in skipped}))
    part = _sum_classes(v, frozenset(names) - skipped)
    return f"≥{part}({why})" if part else f"?({why})"


def _sub(v: Verdict, sub: OkSub) -> int | None:
    subs = _section(v, "subs")
    if subs is None:
        return None
    return _count(subs.get(sub, 0))


def _invariant(v: Verdict, name: str) -> int | None:
    inv = _section(v, "invariants")
    if inv is None or name not in inv:
        return None
    return _count(inv[name])


def _balance(v: Verdict) -> tuple[int | None, dict[str, int | None]]:
    parts = {n: _invariant(v, n) for n in BALANCE_INVARIANTS}
    if any(p is None for p in parts.values()):
        return None, parts
    return sum(p for p in parts.values() if p is not None), parts


def _find_check(v: Verdict, item_id: str) -> CheckResult | None:
    for c in (*v.checks, *v.guards):
        if c.id == item_id:
            return c
    return None


def _stale_fills(v: Verdict) -> int | None:
    n = _invariant(v, STALE_FILL_INVARIANT)
    if n is not None:
        return n
    c = _find_check(v, STALE_FILL_CHECK_ID)
    if c is None or effective_outcome(c) in (Outcome.NA, None):
        return None
    return _count(c.value)


def _intents(v: Verdict) -> tuple[int | None, int | None]:
    it = v.integrity.get("intents")
    if isinstance(it, Mapping):
        return _count(it.get("total")), _count(it.get("ack"))
    return _count(it), None


def _phases(v: Verdict) -> dict[str, Mapping[str, Any]]:
    src = (
        v.client.get("phases")
        if isinstance(v.client.get("phases"), Mapping)
        else v.client
    )
    out = {str(k): p for k, p in src.items() if isinstance(p, Mapping)}
    order = {name: i for i, name in enumerate(PHASE_ORDER)}
    return {k: out[k] for k in sorted(out, key=lambda k: (order.get(k, len(order)), k))}


# ---------------------------------------------------------------- 서식


def _n(v: int | None) -> str:
    return "?" if v is None else str(v)


def _fmt(v: Any) -> str:
    """결정적인 짧은 표기. None은 '—'."""
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "예" if v else "아니오"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v == int(v):
            return str(int(v))
        return f"{v:.4f}".rstrip("0").rstrip(".")
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (Mapping, list, tuple, set, frozenset)):
        s = canonical_json(v)
        return s if len(s) <= 60 else s[:59] + "…"
    return str(v)


def _cell(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ")


def _row(*cells: str) -> str:
    return "| " + " | ".join(_cell(c) for c in cells) + " |"


def _codes(codes: Any) -> str:
    codes = list(codes)
    return ", ".join(f"`{c}`" for c in codes) if codes else "없음"


# ---------------------------------------------------------------- 조각


def summary_line(verdict: Verdict) -> str:
    """시연 화면 한 줄 요약(설계 9). 모르는 값은 '?'."""
    balance, _ = _balance(verdict)
    parts = [
        f"유실 {_class_text(verdict, LOSS_CLASSES)}",
        f"불일치 {_class_text(verdict, MISMATCH_CLASSES)}",
        f"중복 {_class_text(verdict, DUPLICATE_CLASSES)}",
        f"고아 {_class_text(verdict, ORPHAN_CLASSES)}",
        f"잔고 불일치 {_n(balance)}",
        f"신선도 위반 체결 {_n(_stale_fills(verdict))}",
    ]
    return " · ".join(parts) + f" → {verdict.verdict}"


def _recovered_line(v: Verdict) -> str:
    """커밋 후 회수(명세 4): 결과 불명 시도가 커밋됐고 같은 키 재전송이 replayed로 돌려받았다."""
    return f"커밋 후 회수 {_n(_sub(v, OkSub.AFTER_COMMIT))}건(after_commit)"


def _names(classes: frozenset[OrderClass]) -> str:
    """클래스 묶음 이름(OrderClass 정의 순서, '+'로 잇는다)."""
    return "+".join(c.value for c in OrderClass if c in classes)


def _orders_section(v: Verdict) -> list[str]:
    total, ack = _intents(v)
    log_complete = v.integrity.get("log_complete")
    rows = [
        ("의도(intent)", _n(total)),
        ("ACK(200, order_id)", _n(ack)),
        (f"유실({_names(LOSS_CLASSES)})", _class_text(v, LOSS_CLASSES)),
        (f"내용 불일치({_names(MISMATCH_CLASSES)})", _class_text(v, MISMATCH_CLASSES)),
        (f"중복({_names(DUPLICATE_CLASSES)})", _class_text(v, DUPLICATE_CLASSES)),
        (f"고아({_names(ORPHAN_CLASSES)})", _class_text(v, ORPHAN_CLASSES)),
        (
            f"결과 불명·중단(사용자 영향: {_names(USER_IMPACT_CLASSES)})",
            _class_text(v, USER_IMPACT_CLASSES),
        ),
        (
            "기록 완전성(log_complete)",
            _fmt(log_complete) if log_complete is not None else "?",
        ),
    ]
    out = ["### 주문", "", _row("항목", "건수"), _row("---", "---:")]
    out += [_row(a, b) for a, b in rows]
    classes = _section(v, "classes")
    if classes:
        nonzero = [
            (c.value, _count(classes.get(c)))
            for c in OrderClass
            if _count(classes.get(c, 0)) not in (0, None)
        ]
        if nonzero:
            out += ["", "클래스별: " + ", ".join(f"{k} {_n(n)}" for k, n in nonzero)]
    return out


def _balance_section(v: Verdict) -> list[str]:
    balance, parts = _balance(v)
    detail = ", ".join(f"{k} {_n(n)}" for k, n in parts.items())
    out = [
        "### 잔고와 신선도",
        "",
        _row("항목", "위반"),
        _row("---", "---:"),
        _row(f"잔고 불일치({detail})", _n(balance)),
        _row(f"신선도 위반 체결({STALE_FILL_INVARIANT})", _n(_stale_fills(v))),
    ]
    inv = _section(v, "invariants")
    if inv:
        others = [
            k
            for k in sorted(inv, key=str)
            if k not in BALANCE_INVARIANTS and k != STALE_FILL_INVARIANT
        ]
        if others:
            out += [
                "",
                "그 밖 불변식: "
                + ", ".join(f"{k} {_n(_invariant(v, k))}" for k in others),
            ]
    return out


def _recovery_section(v: Verdict) -> list[str]:
    out = ["### 커밋 후 회수", "", _recovered_line(v)]
    rows = [(s.value, _n(_sub(v, s))) for s in OkSub]
    out += ["", _row("OK 세부", "건수"), _row("---", "---:")]
    out += [_row(a, b) for a, b in rows]
    after_503 = _sub(v, OkSub.AFTER_503)
    if after_503:
        out += [
            "",
            f"after_503 {after_503}건은 경고 `{R.ROLLBACK_REPLY_COMMITTED}`이고 장애 효과 증거에서 뺀다.",
        ]
    return out


def _client_section(v: Verdict) -> list[str]:
    phases = _phases(v)
    out = ["### 구간 p95(k6 기록, nearest-rank)", ""]
    if not phases:
        return out + ["구간 측정값 없음."]
    out += [
        _row("구간", "n", "ack p95(ms)", "첫 시도 오류율", "달성률"),
        _row("---", "---:", "---:", "---:", "---:"),
    ]
    for name, p in phases.items():
        out.append(
            _row(
                name,
                _fmt(p.get("n")),
                _fmt(p.get("ack_p95_ms")),
                _fmt(p.get("sys_err_ratio")),
                _fmt(p.get("achieved_ratio")),
            )
        )
    return out


def _outcome_label(c: CheckResult) -> str:
    raw, eff = c.outcome, effective_outcome(c)
    if eff is None:
        return f"평가 못 함({raw!s})"
    if raw == Outcome.FAIL and eff is Outcome.INCONCLUSIVE:
        return "FAIL→INCONCLUSIVE"
    return str(eff)


def _items_section(v: Verdict) -> list[str]:
    out = [
        "### 기준과 가드",
        "",
        _row("구분", "id", "종류", "결과", "필수", "값", "사유"),
        _row("---", "---", "---", "---", "---", "---:", "---"),
    ]
    for label, is_guard, group in (("기준", False, v.checks), ("가드", True, v.guards)):
        for c in group:
            eff = effective_outcome(c)
            shown = (
                item_reasons(c, guard=is_guard)
                if eff in (Outcome.FAIL, Outcome.INCONCLUSIVE, None)
                else c.reasons
            )
            out.append(
                _row(
                    label,
                    f"`{c.id}`",
                    c.kind,
                    _outcome_label(c),
                    "예" if c.required else "아니오(보고만)",
                    _fmt(c.value),
                    _codes(shown),
                )
            )
    return out


def _inconclusive_section(v: Verdict) -> list[str]:
    by_reason: dict[str, set[str]] = {}
    for is_guard, group in ((False, v.checks), (True, v.guards)):
        for c in group:
            if effective_outcome(c) not in (Outcome.INCONCLUSIVE, None):
                continue
            tag = c.id if c.required else f"{c.id}(선택)"
            for r in item_reasons(c, guard=is_guard):
                by_reason.setdefault(r, set()).add(tag)
    if v.verdict == Outcome.INCONCLUSIVE:
        for r in v.reasons:
            by_reason.setdefault(r, set())
    if not by_reason:
        return []
    out = [
        "### INCONCLUSIVE 사유와 해소 방법",
        "",
        _row("사유", "기준", "해소 방법"),
        _row("---", "---", "---"),
    ]
    for r in sorted(by_reason):
        ids = ", ".join(f"`{i}`" for i in sorted(by_reason[r])) or "—"
        out.append(_row(f"`{r}`", ids, REMEDY.get(r, DEFAULT_REMEDY)))
    return out


def _evidence_section(v: Verdict) -> list[str]:
    keys = (
        "criteria_sha256",
        "run_json_sha256",
        "evidence_manifest_sha256",
        "judge_version",
        "response_fields_confirmed",
    )
    out = ["### 증거 해시", ""]
    for k in keys:
        val = v.inputs.get(k)
        out.append(f"- {k}: `{_fmt(val)}`" if val is not None else f"- {k}: —")
    out.append(f"- verdict_digest: `{v.verdict_digest or '—'}`")
    return out


def fragment(verdict: Verdict) -> str:
    """report.md 조각(설계 5.8). 첫 줄은 시연 한 줄 요약(설계 9)."""
    v = verdict
    lines = [summary_line(v), _recovered_line(v), ""]
    if R.SAFETY_VIOLATION in v.warnings or R.SAFETY_VIOLATION in v.reasons:
        lines += [
            f"> **경고 `{R.SAFETY_VIOLATION}`**: 안전장치 위반이 있다. 이 run의 결과를 쓰지 않는다(4.1).",
            "",
        ]
    lines += [
        f"## 판정: {v.verdict}",
        "",
        f"- run_id: `{v.run_id}` · 실험: `{v.experiment}`",
        f"- 사유: {_codes(v.reasons)}",
        f"- 경고: {_codes(v.warnings)}",
    ]
    if v.verdict == Outcome.FAIL and R.INTEGRITY_VIOLATION in v.reasons:
        lines.append("- 돈 불변식 FAIL은 가드보다 앞선다(설계 5.7 규칙 1).")
    for section in (
        _orders_section(v),
        _balance_section(v),
        _recovery_section(v),
        _client_section(v),
        _items_section(v),
        _inconclusive_section(v),
        _evidence_section(v),
    ):
        if section:
            lines += ["", *section]
    return "\n".join(lines) + "\n"

"""판정 파이프라인: collect → evaluate(설계 5.1, 5.2, 5.6, 5.7, 5.8).

- collect(ctx, crit, tr): 클러스터를 만지는 유일한 함수다. 1차 스텁이다.
- evaluate(ctx, crit, evidence): 증거 폴더만 읽는 순수 함수다. 네트워크와 현재 시각을 쓰지 않는다.
  시각은 증거와 run.json 값만 쓴다. generated_at만 호출자가 넘긴다(digest 밖).
- assess: evaluate와 같고 classified 행도 돌려준다. render_outputs는 verdict.json·report.md·
  classified.csv 내용을 만든다. 파일 쓰기는 CLI(__main__)만 한다.
- judge(run_json, offline): run.json 하나로 collect(오프라인이면 건너뜀) → evaluate.

증거 폴더(runs/<run_id>/, 설계 4.8, 5.2)
    k6/orders.jsonl.gz | k6/orders.jsonl  주문 기록(JSONL v1). 둘 다 있으면 .gz를 읽는다
    k6/summary.json                       handleSummary. 없거나 깨지면(잘림) 기록 불완전(OOM 등)
    k6/pod-status.json                    k6 컨테이너 종료 코드
    k6/sha256sum.txt                      원격 sha256sum 출력. 없으면 sha256 미검증
    k6/derived.json                       노드 − 노트북 시계 차이. 없으면 표지 기준 구간은 NOT_EVALUATED
    raw/db/snapshot.txt                   psql 출력(--@ 구획 + COPY CSV, 설계 5.4)

판정 순서
1. 증거를 읽는다. judge가 만드는 파일(pod-status, derived, sha256sum, snapshot)의 형식이 틀리면
   InputError(12)다. k6가 Pod 안에서 쓰는 파일(orders, summary)의 형식 오류는 기록 불완전으로
   다룬다(설계 5.7 하단). 기록의 run 값이 run_id와 달라도 InputError다.
2. 계좌 집합(설계 5.3): JSONL account 계좌 + 보조 계좌(replay, nickname ^k6-<run_id>-[0-9]+$),
   extra 계좌(DB 전용 검사만).
3. 분류(classify), 불변식(check_invariants → item_results), 기록 완전성(log_complete).
4. 기준(checks)을 기준 파일 순서로 평가한다.
   - class: 분류 건수. phases가 있으면 그 구간(합집합)의 행만 센다.
     평가하지 않는 클래스(설계 5.3, 5.7 규칙 1): ACCOUNT_SET_MISMATCH면 LOST와 ORPHAN,
     기록 불완전이면 ORPHAN과 UNRESOLVED_*. 분류를 못 하면(기록 없음, DB 구획 없음) 전부다.
     PASS만 막는 불확실성: TRANSPORT_REPLAY_POSSIBLE·재송신 비교 못 함(IDEMPOTENCY_VIOLATION),
     RESPONSE_SCHEMA_UNCONFIRMED·응답 원문 못 읽음(MISMATCH). 이미 FAIL이면 FAIL이 남는다
     (설계 4.4, 5.3, 5.7 규칙 4).
   - sql·check i7: invariants.item_results. k6 exit_code: verdict.k6_exit_to_checks.
   - client·prom: 1차 미구현(설계 5.5, 5.9)이라 INCONCLUSIVE(NOT_EVALUATED). required: false면 보고만 한다.
5. 가드(설계 5.6). 구현한 것: safety(1 VU = 1 계좌 재검사 포함, 설계 4.1), account_set,
   fill_coverage, sell_coverage, load_coverage, fault_in_load, fault_effect(class_sub는 분류 결과,
   observe는 run.json·marks.local의 obs 값), db_quiescent. 그 밖(2차 WS 가드)은 NOT_EVALUATED다.
   표지 기준 구간과 fault_in_load는 시계 차이를 모르면(derived.json 없음) NOT_EVALUATED다.
   log_complete는 required=false로 덧붙인다.
   judge_self_check(분류기 대 inv_i4)는 required=false 기준으로 덧붙인다.
6. combine → build_verdict(설계 5.7, 5.8).
"""

from __future__ import annotations

import csv
import io
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import __version__
from . import reasons as R
from .classify import (
    ClassifyResult,
    classes_of,
    classify,
    count_filled,
    count_subs,
    summarize,
)
from .client import (
    achieved_ratio,
    applicable_phases,
    is_fault_run,
    phase_windows,
    phases_from_marks,
    plan_duration_ms,
    run_window,
)
from .context import load_run
from .criteria import RESOLVED_NAME, integrity_cfg, load_criteria
from .db import parse_bundle_output
from .errors import InputError
from .fetch import (
    CLOCK_SKEW_MAX_MS,
    DERIVED_NAME,
    K6_CONTAINER,
    LOCAL_K6_DIR,
    LOCAL_ORDERS_GZ,
    ORDERS_LOG,
    POD_STATUS_NAME,
    REMOTE_SHA_NAME,
    SUMMARY,
)
from .invariants import (
    I7_ID,
    check_invariants,
    i4_counts,
    invariant_counts,
    item_results,
)
from .k6log import (
    COND_COUNTERS,
    LOG_COMPLETE_ID,
    LOG_COMPLETE_KIND,
    find_token_keys,
    log_complete,
    parse_k6log,
    sha_matches,
)
from .model import (
    CheckResult,
    Classified,
    Criteria,
    DbSnapshot,
    K6Log,
    OrderClass,
    Outcome,
    RunContext,
    Verdict,
    canonical_json,
    sha256_bytes,
    sha256_text,
)
from .report import fragment
from .transport import Transports
from .verdict import (
    build_verdict,
    evaluate_count_check,
    k6_exit_to_checks,
    requires_met,
)

__all__ = [
    "Assessment",
    "AccountScope",
    "CLASSIFIED_NAME",
    "DB_SNAPSHOT",
    "Evidence",
    "REPORT_NAME",
    "VERDICT_NAME",
    "account_scope",
    "assess",
    "classified_csv",
    "collect",
    "evaluate",
    "judge",
    "load_evidence",
    "render_outputs",
]

# 증거 폴더 안 경로(posix, 설계 4.8, 5.2)
ORDERS_GZ = f"{LOCAL_K6_DIR}/{LOCAL_ORDERS_GZ}"
ORDERS_PLAIN = f"{LOCAL_K6_DIR}/{ORDERS_LOG}"
SUMMARY_PATH = f"{LOCAL_K6_DIR}/{SUMMARY}"
POD_STATUS_PATH = f"{LOCAL_K6_DIR}/{POD_STATUS_NAME}"
REMOTE_SHA_PATH = f"{LOCAL_K6_DIR}/{REMOTE_SHA_NAME}"
DERIVED_PATH = f"{LOCAL_K6_DIR}/{DERIVED_NAME}"
DB_SNAPSHOT = "raw/db/snapshot.txt"

# runs/<run_id>/ 산출물(설계 5.2). CLI가 쓴다
VERDICT_NAME = "verdict.json"
REPORT_NAME = "report.md"
CLASSIFIED_NAME = "classified.csv"

# 분류에 필요한 DB 구획. 하나라도 빠지면 모든 클래스가 DB_ERROR다
CLASSIFY_SECTIONS = frozenset({"accounts", "orders", "fills"})
AUX_ACCOUNT_RATIO = Decimal("0.1")  # 보조 계좌 상한 N × 0.1(설계 5.3)
DEFAULT_LOAD_PHASES = ("baseline", "tail", "steady")  # load_coverage(설계 5.6)
SELF_CHECK_ID = "judge_self_check"
EXAMPLES_MAX = 20

# inv_i4 problem → 분류 클래스(JUDGE_SELF_CHECK, 설계 5.4)
_I4_CLASS = {
    "dup_key": OrderClass.DUP_ORDER.value,
    "dup_fill": OrderClass.DUP_FILL.value,
    "filled_without_fill": OrderClass.FILL_STATE.value,
    "rejected_with_fill": OrderClass.FILL_STATE.value,
    "fill_without_order": OrderClass.FILL_STATE.value,
    "fill_field": OrderClass.FILL_STATE.value,
}
_SHA_LINE = re.compile(r"^([0-9a-fA-F]{64})\s+\*?(.+)$")

# PASS를 막는 불확실성(설계 4.4, 5.3, 5.7 규칙 4): 출처(marks 키 또는 클래스) → (낮출 클래스, 사유)
_UNCERTAIN_SOURCES = {
    R.TRANSPORT_REPLAY_POSSIBLE: (
        OrderClass.IDEMPOTENCY_VIOLATION.value,
        R.TRANSPORT_REPLAY_POSSIBLE,
    ),
    "resend_uncompared": (OrderClass.IDEMPOTENCY_VIOLATION.value, R.NOT_EVALUATED),
    OrderClass.RESPONSE_SCHEMA_UNCONFIRMED.value: (
        OrderClass.MISMATCH.value,
        R.RESPONSE_SCHEMA_UNCONFIRMED,
    ),
    "response_unparsed": (OrderClass.MISMATCH.value, R.NOT_EVALUATED),
}
# ACCOUNT_SET_MISMATCH면 평가하지 않는 클래스(설계 5.3, 5.7 규칙 1: LOST·ORPHAN)
ACCOUNT_SET_UNAVAILABLE = (OrderClass.LOST.value, OrderClass.ORPHAN.value)
CLOCK_UNKNOWN_NOTE = "시계 차이 미측정(k6/derived.json 없음, 설계 4.8 4단계·5.5)"

CLASSIFIED_COLUMNS = (
    "key",
    "account_id",
    "k_state",
    "cls",
    "classes",
    "sub",
    "idem",
    "resend",
    "t",
    "order_ids",
    "detail",
)


# ---------------------------------------------------------------- 증거 읽기


@dataclass(frozen=True)
class Evidence:
    """증거 폴더를 읽은 결과. 없는 파일은 None이다.

    manifest: 읽으려 한 파일(증거 폴더 기준 posix 경로) → 원본 바이트 sha256(없으면 None).
    remote_sha: sha256sum.txt의 orders.jsonl 줄 값(16진 소문자).
    summary_error: summary.json이 있는데 JSON 객체로 읽지 못한 사유(잘림 등). 이때 summary는 None.
    summary_token_key: 읽지 못한 summary 원문에서 "...token": 키를 찾았다(설계 5.6 safety).
    clock_offset_ms: None이면 시계 차이를 모른다(derived.json 없음).
    """

    log: K6Log | None
    summary: Mapping[str, Any] | None = field(default=None, hash=False)
    k6_exit_code: int | None = None
    k6_exit_reason: str | None = None
    remote_sha: str | None = None
    clock_offset_ms: int | None = None
    db: DbSnapshot | None = None
    manifest: Mapping[str, str | None] = field(default_factory=dict, hash=False)
    summary_error: str | None = None
    summary_token_key: bool = False

    @property
    def manifest_sha256(self) -> str:
        return sha256_text(canonical_json(dict(sorted(self.manifest.items()))))


def _read(root: Path, rel: str, manifest: dict[str, str | None]) -> bytes | None:
    p = root / rel
    if not p.is_file():
        manifest[rel] = None
        return None
    try:
        data = p.read_bytes()
    except OSError as e:
        raise InputError(f"{rel}: 읽을 수 없다({e.strerror or e})") from e
    manifest[rel] = sha256_bytes(data)
    return data


def _text(data: bytes, rel: str) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as e:
        raise InputError(f"{rel}: UTF-8이 아니다") from e


def _json_obj(data: bytes, rel: str) -> dict[str, Any]:
    try:
        doc = json.loads(_text(data, rel), parse_float=Decimal)
    except (ValueError, RecursionError) as e:
        raise InputError(f"{rel}: JSON이 아니다") from e
    if not isinstance(doc, dict):
        raise InputError(f"{rel}: 최상위가 객체가 아니다")
    return doc


def _plain_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _pod_exit(doc: Mapping[str, Any], rel: str) -> tuple[int | None, str | None]:
    """pod-status.json의 k6 컨테이너 exitCode·reason(설계 4.8 1단계)."""
    containers = doc.get("containers")
    if not isinstance(containers, Mapping):
        raise InputError(f"{rel}: containers 객체가 없다")
    c = containers.get(K6_CONTAINER)
    if c is None:
        return None, None
    if not isinstance(c, Mapping):
        raise InputError(f"{rel}: containers.{K6_CONTAINER}가 객체가 아니다")
    code, reason = c.get("exitCode"), c.get("reason")
    if code is not None and not _plain_int(code):
        raise InputError(f"{rel}: exitCode가 정수가 아니다")
    if reason is not None and not isinstance(reason, str):
        raise InputError(f"{rel}: reason이 문자열이 아니다")
    return code, reason


def _remote_sha(text: str, name: str) -> str | None:
    """sha256sum 출력에서 파일 이름이 name인 줄의 해시. 없으면 None."""
    for line in text.splitlines():
        m = _SHA_LINE.match(line.strip())
        if m and m.group(2).strip().rsplit("/", 1)[-1] == name:
            return m.group(1).lower()
    return None


def _summary(data: bytes | None) -> tuple[dict[str, Any] | None, str | None, bool]:
    """summary.json을 읽는다. (summary, 읽지 못한 사유, 원문의 token 키 여부).

    k6가 Pod 안에서 쓰는 파일이라 OOM·SIGKILL로 잘린 채 남을 수 있다. 형식 오류는 입력 오류가
    아니라 기록 불완전이다(설계 5.7 하단). token 키는 원문 텍스트에서 찾는다(설계 5.6, 7.4).
    """
    if data is None:
        return None, None, False
    try:
        return _json_obj(data, SUMMARY_PATH), None, False
    except InputError as e:
        text = data.decode("utf-8", errors="replace")
        return None, str(e), bool(find_token_keys(text))


def load_evidence(evidence: str | Path) -> Evidence:
    """증거 폴더를 읽는다(설계 5.2 6단계).

    judge가 만드는 파일의 형식이 틀리면 InputError다. k6가 쓰는 orders·summary의 형식 오류는
    기록 불완전으로 남긴다(parse_k6log problems, Evidence.summary_error).
    """
    root = Path(evidence)
    if not root.is_dir():
        raise InputError(f"증거 폴더가 없다: {root}")
    manifest: dict[str, str | None] = {}

    summary_raw = _read(root, SUMMARY_PATH, manifest)
    summary, summary_error, summary_token = _summary(summary_raw)

    sha_raw = _read(root, REMOTE_SHA_PATH, manifest)
    remote = (
        None
        if sha_raw is None
        else _remote_sha(_text(sha_raw, REMOTE_SHA_PATH), ORDERS_LOG)
    )

    rel = ORDERS_GZ if (root / ORDERS_GZ).is_file() else ORDERS_PLAIN
    orders_raw = _read(root, rel, manifest)
    log = (
        None
        if orders_raw is None
        else parse_k6log(orders_raw, summary=summary, expected_sha256=remote)
    )

    code: int | None = None
    reason: str | None = None
    pod_raw = _read(root, POD_STATUS_PATH, manifest)
    if pod_raw is not None:
        code, reason = _pod_exit(_json_obj(pod_raw, POD_STATUS_PATH), POD_STATUS_PATH)

    offset: int | None = None
    derived_raw = _read(root, DERIVED_PATH, manifest)
    if derived_raw is not None:
        v = _json_obj(derived_raw, DERIVED_PATH).get("offset_ms")
        if v is not None and not _plain_int(v):
            raise InputError(f"{DERIVED_PATH}: offset_ms가 정수가 아니다")
        offset = v

    db_raw = _read(root, DB_SNAPSHOT, manifest)
    db = None if db_raw is None else parse_bundle_output(_text(db_raw, DB_SNAPSHOT))

    return Evidence(
        log=log,
        summary=summary,
        k6_exit_code=code,
        k6_exit_reason=reason,
        remote_sha=remote,
        clock_offset_ms=offset,
        db=db,
        manifest=dict(sorted(manifest.items())),
        summary_error=summary_error,
        summary_token_key=summary_token,
    )


# ---------------------------------------------------------------- 계좌 집합(설계 5.3)


@dataclass(frozen=True)
class AccountScope:
    """이번 run 계좌. evaluable은 기록과 DB accounts 구획이 모두 있을 때 True다.

    jsonl: JSONL account 계좌. aux: 서버에만 생긴 보조 계좌(nickname 정규식). extra: 발표자 계좌.
    missing_jsonl·missing_extra: DB에 없는 계좌(정렬).
    """

    jsonl: frozenset[str]
    aux: frozenset[str]
    extra: frozenset[str]
    missing_jsonl: tuple[str, ...] = ()
    missing_extra: tuple[str, ...] = ()
    evaluable: bool = False

    @property
    def run_ids(self) -> frozenset[str]:
        return self.jsonl | self.aux

    @property
    def mismatch(self) -> bool:
        """ACCOUNT_SET_MISMATCH(설계 5.3): JSONL 계좌가 DB에 없거나 보조 계좌가 N × 0.1을 넘는다."""
        if not self.evaluable:
            return False
        too_many = Decimal(len(self.aux)) > Decimal(len(self.jsonl)) * AUX_ACCOUNT_RATIO
        return not self.jsonl or bool(self.missing_jsonl) or too_many


def account_scope(
    ctx: RunContext, log: K6Log | None, db: DbSnapshot | None
) -> AccountScope:
    """계좌 집합을 만든다(설계 5.3). 보조 계좌는 replay이고 nickname이 정확히 맞는 계좌다."""
    extra = frozenset(ctx.extra_accounts)
    jsonl = log.account_ids if log is not None else frozenset()
    if log is None or db is None or "accounts" not in db.sections:
        return AccountScope(jsonl=jsonl, aux=frozenset(), extra=extra)
    nick = re.compile(rf"k6-{re.escape(ctx.run_id)}-[0-9]+")
    db_ids = {a.id for a in db.accounts}
    aux = frozenset(
        a.id
        for a in db.accounts
        if a.id not in jsonl
        and a.id not in extra
        and a.mode == "replay"
        and a.nickname is not None
        and nick.fullmatch(a.nickname)
    )
    return AccountScope(
        jsonl=jsonl,
        aux=aux,
        extra=extra,
        missing_jsonl=tuple(sorted(jsonl - db_ids)),
        missing_extra=tuple(sorted(extra - db_ids)),
        evaluable=True,
    )


# ---------------------------------------------------------------- 판정 상태


@dataclass(frozen=True)
class _State:
    ctx: RunContext
    crit: Criteria
    ev: Evidence
    scope: AccountScope
    result: ClassifyResult | None
    cls_reason: str | None
    lc: CheckResult
    windows: Mapping[str, tuple[int, int]] = field(hash=False)
    lw: tuple[int, int] | None
    applicable: frozenset[str]
    mark_phases: frozenset[str]
    offset: int
    offset_known: bool
    skew: bool
    features: frozenset[str]
    unavailable: Mapping[str, str] = field(hash=False)
    uncertain: Mapping[str, Mapping[str, int]] = field(hash=False)

    @property
    def log(self) -> K6Log | None:
        return self.ev.log

    @property
    def db(self) -> DbSnapshot | None:
        return self.ev.db

    @property
    def stopped_by_judge(self) -> bool:
        return self.ctx.obs.get("stopped_by_judge") is True


def _meta(spec: Mapping[str, Any], kind: str, ov: str) -> tuple[str, str, bool, str]:
    return (
        str(spec["id"]),
        str(spec.get("kind", kind)),
        bool(spec.get("required", True)),
        str(spec.get("on_violation", ov)),
    )


def _result(
    spec: Mapping[str, Any],
    outcome: Outcome,
    *,
    kind: str = "guard",
    ov: str = "inconclusive",
    value: Any = None,
    reasons: Iterable[str] = (),
    detail: Mapping[str, Any] | None = None,
) -> CheckResult:
    item_id, k, required, on_violation = _meta(spec, kind, ov)
    return CheckResult(
        id=item_id,
        kind=k,
        outcome=outcome,
        required=required,
        on_violation=on_violation,
        value=value,
        reasons=tuple(sorted(set(reasons))),
        detail=dict(detail or {}),
    )


def _requires(spec: Mapping[str, Any]) -> list[str]:
    req = spec.get("requires")
    return [req] if isinstance(req, str) else sorted(req or ())


def _examples(keys: Iterable[str]) -> list[str]:
    return sorted(keys)[:EXAMPLES_MAX]


def _in_windows(t: int | None, windows: Sequence[tuple[int, int]]) -> bool:
    return t is not None and any(a <= t < b for a, b in windows)


def _phase_scope(
    st: _State, spec: Mapping[str, Any], phases: Sequence[str], kind: str, ov: str
) -> tuple[list[tuple[int, int]], dict[str, tuple[int, int]], CheckResult | None]:
    """phases → 창 목록. 해당 구간이 없으면 N/A, 경계를 모르면 NOT_EVALUATED, 시계 차이면 CLOCK_SKEW.

    표지 기준 구간인데 시계 차이를 모르면(derived.json 없음) NOT_EVALUATED다(설계 5.5, 5.7 규칙 4).
    """
    used = [p for p in phases if p in st.applicable]
    base = {"phases": list(phases)}
    if not used:
        note = "이 run에 해당 구간이 없다"
        return (
            [],
            {},
            _result(spec, Outcome.NA, kind=kind, ov=ov, detail={**base, "note": note}),
        )
    missing = [p for p in used if p not in st.windows]
    if missing:
        return (
            [],
            {},
            _result(
                spec,
                Outcome.INCONCLUSIVE,
                kind=kind,
                ov=ov,
                reasons=(R.NOT_EVALUATED,),
                detail={**base, "phases_undefined": missing},
            ),
        )
    if st.skew and any(p in st.mark_phases for p in used):
        return (
            [],
            {},
            _result(
                spec,
                Outcome.INCONCLUSIVE,
                kind=kind,
                ov=ov,
                reasons=(R.CLOCK_SKEW,),
                detail={**base, "clock_offset_ms": st.offset},
            ),
        )
    if not st.offset_known and any(p in st.mark_phases for p in used):
        return (
            [],
            {},
            _result(
                spec,
                Outcome.INCONCLUSIVE,
                kind=kind,
                ov=ov,
                reasons=(R.NOT_EVALUATED,),
                detail={**base, "clock_offset_ms": None, "note": CLOCK_UNKNOWN_NOTE},
            ),
        )
    chosen = {p: st.windows[p] for p in used}
    return list(chosen.values()), chosen, None


# ---------------------------------------------------------------- 기준(설계 6 checks)


def _class_counts(
    result: ClassifyResult,
    names: Sequence[str],
    windows: Sequence[tuple[int, int]] | None,
) -> dict[str, int]:
    if windows is None:
        return {n: int(result.counts.get(n, 0)) for n in names}
    out = dict.fromkeys(names, 0)
    for c in result.classified:
        if not _in_windows(c.t, windows):
            continue
        for n in classes_of(c):
            if n in out:
                out[n] += 1
    return out


def _class_check(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    kind = str(spec["kind"])
    names = sorted({str(x) for x in spec["class"]})
    windows: list[tuple[int, int]] | None = None
    chosen: dict[str, tuple[int, int]] = {}
    if spec.get("phases"):
        windows, chosen, early = _phase_scope(st, spec, spec["phases"], kind, "fail")
        if early is not None:
            return early
    if st.result is None:
        counts: dict[str, int] = {}
        unavailable = {n: str(st.cls_reason) for n in names}
    else:
        counts = _class_counts(st.result, names, windows)
        unavailable = {n: st.unavailable[n] for n in names if n in st.unavailable}
    r = evaluate_count_check(
        spec, counts, unavailable=unavailable, features=st.features
    )
    detail = dict(r.detail)
    if chosen:
        detail["windows"] = {p: list(w) for p, w in chosen.items()}
    sources = {
        src: rows for n in names for src, rows in st.uncertain.get(n, {}).items()
    }
    blockers = sorted({_UNCERTAIN_SOURCES[src][1] for src in sources})
    if r.outcome is Outcome.PASS and blockers:
        detail["uncertain"] = blockers
        detail["uncertain_rows"] = dict(sorted(sources.items()))
        return replace(
            r, outcome=Outcome.INCONCLUSIVE, reasons=tuple(blockers), detail=detail
        )
    return replace(r, detail=detail)


def _k6_checks(st: _State, spec: Mapping[str, Any]) -> list[CheckResult]:
    log = st.log
    rs = log.run_start if log is not None else None
    plan_ms = plan_duration_ms(rs.plan) if rs is not None else None
    setup_ms = 0
    if rs is not None and log is not None and log.setup_done is not None:
        setup_ms = max(0, log.setup_done.t - rs.t)
    return k6_exit_to_checks(
        st.ev.k6_exit_code,
        st.ev.summary,
        plan_ms,
        setup_ms=setup_ms,
        stopped_by_judge=st.stopped_by_judge,
        spec=spec,
    )


def _checks(st: _State, inv_items: Mapping[str, CheckResult]) -> list[CheckResult]:
    out: list[CheckResult] = []
    for spec in st.crit.checks:
        kind = str(spec["kind"])
        if not requires_met(spec, st.features):
            out.append(
                _result(
                    spec,
                    Outcome.NA,
                    kind=kind,
                    ov="fail",
                    detail={"requires": _requires(spec)},
                )
            )
        elif "class" in spec:
            out.append(_class_check(st, spec))
        elif kind == "integrity":
            r = inv_items.get(str(spec["id"]))
            if r is None:
                r = _result(
                    spec,
                    Outcome.INCONCLUSIVE,
                    kind=kind,
                    ov="fail",
                    reasons=(R.NOT_EVALUATED,),
                )
            out.append(r)
        elif kind == "k6":
            out.extend(_k6_checks(st, spec))
        else:
            section = "설계 5.5, 1차" if kind == "client" else "설계 5.9, 2차"
            if kind == "client" and spec.get("stat") == "recovery":
                section = "설계 5.10, 2차"
            out.append(
                _result(
                    spec,
                    Outcome.INCONCLUSIVE,
                    kind=kind,
                    ov="fail",
                    reasons=(R.NOT_EVALUATED,),
                    detail={"note": f"미구현({section})"},
                )
            )
    return out


def _self_check(st: _State) -> CheckResult | None:
    """분류기 대 inv_i4(설계 5.4 JUDGE_SELF_CHECK). 클래스별 '있음' 여부를 비교한다. 보고만 한다."""
    if st.result is None or st.db is None:
        return None
    i4 = i4_counts(st.db)
    if i4 is None:
        return None
    names = sorted(set(_I4_CLASS.values()))
    sql = {
        n: any(_I4_CLASS.get(p) == n and k > 0 for p, k in i4.items()) for n in names
    }
    cls = {n: st.result.counts.get(n, 0) > 0 for n in names}
    differs = [n for n in names if sql[n] != cls[n]]
    return CheckResult(
        id=SELF_CHECK_ID,
        kind="integrity",
        outcome=Outcome.INCONCLUSIVE if differs else Outcome.PASS,
        required=False,
        on_violation="inconclusive",
        value=len(differs),
        reasons=(R.JUDGE_SELF_CHECK,) if differs else (),
        detail={"inv_i4": i4, "classifier": cls, "sql": sql, "differs": differs},
    )


# ---------------------------------------------------------------- 가드(설계 5.6)


def _g_safety(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    log = st.log
    if log is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(R.SENT_LOG_MISSING,))
    sec = st.crit.data["safety"]
    prefix, mode = str(sec["symbol_prefix"]), str(sec["account_mode"])
    bad_symbol = [
        k
        for k, it in log.intents.items()
        if not isinstance(it.body.get("symbol"), str)
        or not it.body["symbol"].startswith(prefix)
    ]
    bad_account = [k for k, it in log.intents.items() if it.acct not in st.scope.jsonl]
    vu_multi, acct_multi, vu_range = _vu_account_violations(log)
    rs = log.run_start
    bad_run_symbols = sorted(
        s for s in (rs.symbols if rs else ()) if not s.startswith(prefix)
    )
    detail: dict[str, Any] = {"symbol_prefix": prefix, "account_mode": mode}
    bad_mode: list[str] = []
    if st.db is not None and "accounts" in st.db.sections:
        bad_mode = sorted(
            a.id for a in st.db.accounts if a.id in st.scope.jsonl and a.mode != mode
        )
    else:
        detail["note"] = "DB accounts 구획이 없어 계좌 mode는 보지 않았다"
    token_key = log.has_token_key or st.ev.summary_token_key
    if st.ev.summary_error is not None:
        detail["summary_token_scan"] = "text"  # 깨진 summary는 원문 텍스트로 찾았다
    value = {
        "bad_symbol": len(bad_symbol),
        "bad_account": len(bad_account),
        "bad_run_symbols": len(bad_run_symbols),
        "bad_mode": len(bad_mode),
        "vu_multi_account": len(vu_multi),
        "account_multi_vu": len(acct_multi),
        "vu_out_of_range": len(vu_range),
        "token_key": token_key,
    }
    counted = (
        "bad_symbol",
        "bad_account",
        "bad_run_symbols",
        "bad_mode",
        "vu_multi_account",
        "account_multi_vu",
        "vu_out_of_range",
    )
    violated = any(value[k] for k in counted) or token_key
    detail["examples"] = {
        "bad_symbol": _examples(bad_symbol),
        "bad_account": _examples(bad_account),
        "bad_mode": bad_mode[:EXAMPLES_MAX],
        "vu_multi_account": vu_multi[:EXAMPLES_MAX],
        "account_multi_vu": acct_multi[:EXAMPLES_MAX],
        "vu_out_of_range": vu_range[:EXAMPLES_MAX],
    }
    if violated:
        return _result(
            spec,
            Outcome.FAIL,
            value=value,
            reasons=(R.SAFETY_VIOLATION,),
            detail=detail,
        )
    return _result(spec, Outcome.PASS, value=value, detail=detail)


def _vu_account_violations(log: K6Log) -> tuple[list[int], list[str], list[int]]:
    """1 VU = 1 계좌(설계 4.1, 4.2) 재검사. intent의 vu·acct로 본다.

    (계좌가 둘 이상인 vu, vu가 둘 이상인 계좌, 1..계좌 수 밖의 vu). 모두 정렬.
    ACCOUNTS == preAllocatedVUs == maxVUs라 vu.idInTest는 1..계좌 수다. idx는 실패 시 건너뛰어
    vu − 1과 같다는 보장이 없어(4.2) 보지 않는다.
    """
    vu_accts: dict[int, set[str]] = {}
    acct_vus: dict[str, set[int]] = {}
    for it in log.intents.values():
        vu_accts.setdefault(it.vu, set()).add(it.acct)
        acct_vus.setdefault(it.acct, set()).add(it.vu)
    n = len(log.accounts)
    vu_multi = sorted(vu for vu, a in vu_accts.items() if len(a) > 1)
    acct_multi = sorted(a for a, v in acct_vus.items() if len(v) > 1)
    vu_range = sorted(vu for vu in vu_accts if not 1 <= vu <= n)
    return vu_multi, acct_multi, vu_range


def _g_account_set(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    if st.log is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(R.SENT_LOG_MISSING,))
    sc = st.scope
    if not sc.evaluable:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(R.DB_ERROR,))
    codes: list[str] = []
    if sc.mismatch:
        codes.append(R.ACCOUNT_SET_MISMATCH)
    need_extra = bool(st.crit.data.get("require_extra_accounts")) and not sc.extra
    if sc.missing_extra or need_extra:
        codes.append(R.EXTRA_ACCOUNTS_MISSING)
    value = {
        "jsonl": len(sc.jsonl),
        "aux": len(sc.aux),
        "missing": len(sc.missing_jsonl),
        "extra": len(sc.extra),
        "extra_missing": len(sc.missing_extra),
    }
    detail = {
        "aux_limit_ratio": str(AUX_ACCOUNT_RATIO),
        "require_extra_accounts": bool(st.crit.data.get("require_extra_accounts")),
        "examples": {
            "aux": _examples(sc.aux),
            "missing": list(sc.missing_jsonl[:EXAMPLES_MAX]),
            "extra_missing": list(sc.missing_extra[:EXAMPLES_MAX]),
        },
    }
    outcome = Outcome.FAIL if codes else Outcome.PASS
    return _result(spec, outcome, value=value, reasons=codes, detail=detail)


def _g_fill_coverage(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    if st.result is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(str(st.cls_reason),))
    mins = st.crit.data["coverage"]["min_filled"]
    phases = [p for p in (spec.get("phases") or sorted(mins)) if p in mins]
    _, chosen, early = _phase_scope(st, spec, phases, "guard", "inconclusive")
    if early is not None:
        return early
    filled = {
        p: count_filled(st.result, start=a, end=b, accounts=st.scope.run_ids)
        for p, (a, b) in chosen.items()
    }
    low = [p for p in chosen if filled[p] < mins[p]]
    detail = {
        "min_filled": {p: mins[p] for p in chosen},
        "windows": {p: list(w) for p, w in chosen.items()},
        "low": low,
    }
    if low:
        return _result(
            spec,
            Outcome.FAIL,
            value=filled,
            reasons=(R.LOW_FILL_COVERAGE,),
            detail=detail,
        )
    return _result(spec, Outcome.PASS, value=filled, detail=detail)


def _g_sell_coverage(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    if st.result is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(str(st.cls_reason),))
    need = st.crit.data["coverage"]["min_sell_filled"]
    n = count_filled(st.result, side="sell", accounts=st.scope.run_ids)
    detail = {"min_sell_filled": need, "scope": "run"}
    if n < need:
        return _result(
            spec, Outcome.FAIL, value=n, reasons=(R.LOW_FILL_COVERAGE,), detail=detail
        )
    return _result(spec, Outcome.PASS, value=n, detail=detail)


def _num(v: Any) -> Decimal:
    """YAML 수(int·float)를 Decimal로. float는 repr로 바꿔 이진 오차를 피한다."""
    return Decimal(repr(v)) if isinstance(v, float) else Decimal(v)


def _ratio(st: _State, window: tuple[int, int]) -> Decimal | None:
    log = st.log
    if log is None or log.run_start is None:
        return None
    origin = log.setup_done.t if log.setup_done is not None else log.run_start.t
    try:
        return achieved_ratio(
            log.intents, log.run_start.plan, origin_ms=origin, window=window
        )
    except InputError:
        return None


def _g_load_coverage(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    if st.log is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(R.SENT_LOG_MISSING,))
    phases = list(spec.get("phases") or DEFAULT_LOAD_PHASES)
    _, chosen, early = _phase_scope(st, spec, phases, "guard", "inconclusive")
    if early is not None:
        return early
    need = _num(st.crit.data["coverage"]["min_achieved_ratio"])
    ratios = {p: _ratio(st, w) for p, w in chosen.items()}
    low = [p for p, r in ratios.items() if r is not None and r < need]
    unknown = [p for p, r in ratios.items() if r is None]
    detail = {"min_achieved_ratio": str(need), "low": low, "unknown": unknown}
    if low:
        return _result(
            spec,
            Outcome.FAIL,
            value=ratios,
            reasons=(R.GENERATOR_LIMITED,),
            detail=detail,
        )
    if unknown:
        detail["note"] = "run_start.plan이 없거나 형식이 틀리다(계획 도착 수 0 포함)"
        return _result(
            spec,
            Outcome.INCONCLUSIVE,
            value=ratios,
            reasons=(R.NOT_EVALUATED,),
            detail=detail,
        )
    return _result(spec, Outcome.PASS, value=ratios, detail=detail)


def _fault_times(st: _State) -> list[tuple[str, int]]:
    """장애 시각(노트북 시계). run.json faults와 fault 표지(inject_start·inject_end)."""
    out: list[tuple[str, int]] = []
    for i, f in enumerate(st.ctx.faults):
        out.append((f"faults[{i}].start_ms", f.start_ms))
        if f.end_ms is not None:
            out.append((f"faults[{i}].end_ms", f.end_ms))
    ph = st.crit.data["phases"]["fault"]
    for name in (ph["from"], ph["to"]):
        if name in st.ctx.marks:
            out.append((f"marks.{name}", st.ctx.marks[name]))
    return out


def _g_fault_in_load(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    times = _fault_times(st)
    if not times:
        if is_fault_run(st.ctx, st.crit):
            note = "장애 시각이 없다(run.json marks·faults 또는 judge mark)"
            return _result(
                spec,
                Outcome.INCONCLUSIVE,
                reasons=(R.NOT_EVALUATED,),
                detail={"note": note},
            )
        return _result(spec, Outcome.NA, detail={"note": "장애 없는 run"})
    if st.log is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(R.SENT_LOG_MISSING,))
    lw = st.lw
    if lw is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(R.NOT_EVALUATED,))
    if not st.offset_known:
        return _result(
            spec,
            Outcome.INCONCLUSIVE,
            reasons=(R.NOT_EVALUATED,),
            detail={"clock_offset_ms": None, "note": CLOCK_UNKNOWN_NOTE},
        )
    if st.skew:
        return _result(
            spec,
            Outcome.INCONCLUSIVE,
            reasons=(R.CLOCK_SKEW,),
            detail={"clock_offset_ms": st.offset},
        )
    outside = [name for name, t in times if not lw[0] <= t + st.offset <= lw[1]]
    detail = {"load_window": list(lw), "clock_offset_ms": st.offset, "outside": outside}
    if outside:
        return _result(
            spec,
            Outcome.FAIL,
            value=len(outside),
            reasons=(R.FAULT_OUTSIDE_LOAD,),
            detail=detail,
        )
    return _result(spec, Outcome.PASS, value=0, detail=detail)


def _g_db_quiescent(st: _State, spec: Mapping[str, Any]) -> CheckResult:
    db = st.db
    if db is None or db.fingerprint1 is None or db.fingerprint2 is None:
        return _result(spec, Outcome.INCONCLUSIVE, reasons=(R.DB_ERROR,))
    value = {"f1": list(db.fingerprint1), "f2": list(db.fingerprint2)}
    if not db.quiescent:
        return _result(spec, Outcome.FAIL, value=value, reasons=(R.DB_NOT_QUIESCENT,))
    return _result(spec, Outcome.PASS, value=value)


def _fault_effect_item(st: _State, item: Mapping[str, Any]) -> CheckResult:
    kind = "fault_effect"
    if not requires_met(item, st.features):
        return _result(
            item, Outcome.NA, kind=kind, detail={"requires": _requires(item)}
        )
    if "class_sub" in item:
        if st.result is None:
            return _result(
                item, Outcome.INCONCLUSIVE, kind=kind, reasons=(str(st.cls_reason),)
            )
        sel = str(item.get("account", "any"))
        accounts: list[str] | None = None
        if sel.startswith("idx"):
            acct = st.log.account_by_idx(int(sel[3:])) if st.log is not None else None
            if acct is None:
                note = f"JSONL에 계좌 {sel}가 없다"
                return _result(
                    item,
                    Outcome.INCONCLUSIVE,
                    kind=kind,
                    reasons=(R.NOT_EVALUATED,),
                    detail={"note": note},
                )
            accounts = [acct]
        elif sel == "extra":
            accounts = sorted(st.scope.extra)
        counts = {
            str(s): count_subs(st.result, [s], accounts=accounts)
            for s in item["class_sub"]
        }
        r = evaluate_count_check(item, counts, features=st.features)
        return replace(r, detail={**r.detail, "account": sel})
    observe = str(item["observe"])
    v = st.ctx.obs.get(observe)
    if isinstance(v, bool):
        v = int(v)
    if not _plain_int(v):
        note = (
            f"Pod 관측값 obs.{observe}가 없다(설계 5.2 4단계 collect는 1차 미구현). "
            f"judge mark --obs {observe}=<건수>로 넣는다"
        )
        return _result(
            item,
            Outcome.INCONCLUSIVE,
            kind=kind,
            reasons=(R.NOT_EVALUATED,),
            detail={"note": note, "target": item.get("target")},
        )
    r = evaluate_count_check(item, {observe: v}, features=st.features)
    return replace(
        r, detail={**r.detail, "target": item.get("target"), "source": "obs"}
    )


def _fault_effects(st: _State, guard: Mapping[str, Any] | None) -> list[CheckResult]:
    """fault_effect 가드(설계 5.6, 6). 항목마다 결과 하나다.

    판정은 병합 결과(criteria.resolved.yaml과 같은 데이터)만으로 정한다(설계 5.2-6, 6).
    guards에 fault_effect가 있고 required면 항목은 자기 required를 따른다. 가드가 없거나
    (disable 포함) required: false면 항목은 보고만 한다(required=false). Criteria.disabled는
    resolved에 남지 않아 쓰지 않는다. 항목이 없고 가드만 있으면 N/A 하나다(기대 효과 없음).
    """
    items = st.crit.fault_effect
    active = guard is not None and bool(guard.get("required", True))
    if not items:
        if guard is None:
            return []
        return [_result(guard, Outcome.NA, detail={"note": "fault_effect 항목이 없다"})]
    out = []
    for item in items:
        r = _fault_effect_item(st, item)
        out.append(r if active else replace(r, required=False))
    return out


_GUARDS = {
    "safety": _g_safety,
    "account_set": _g_account_set,
    "fill_coverage": _g_fill_coverage,
    "sell_coverage": _g_sell_coverage,
    "load_coverage": _g_load_coverage,
    "fault_in_load": _g_fault_in_load,
    "db_quiescent": _g_db_quiescent,
}


def _guards(st: _State) -> list[CheckResult]:
    out: list[CheckResult] = []
    fault_done = False
    for spec in st.crit.guards:
        gid = str(spec["id"])
        if gid == "fault_effect":
            fault_done = True
            if requires_met(spec, st.features):
                out.extend(_fault_effects(st, spec))
            else:
                out.append(
                    _result(spec, Outcome.NA, detail={"requires": _requires(spec)})
                )
            continue
        if not requires_met(spec, st.features):
            out.append(_result(spec, Outcome.NA, detail={"requires": _requires(spec)}))
            continue
        fn = _GUARDS.get(gid)
        if fn is None:
            out.append(
                _result(
                    spec,
                    Outcome.INCONCLUSIVE,
                    reasons=(R.NOT_EVALUATED,),
                    detail={"note": "미구현 가드(2차)"},
                )
            )
        else:
            out.append(fn(st, spec))
    if not fault_done:
        out.extend(_fault_effects(st, None))
    out.append(st.lc)
    return out


# ---------------------------------------------------------------- verdict 본문(설계 5.8)


def _jsonable_summary(s: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in s.items():
        if isinstance(v, float):
            v = Decimal(repr(v))
        out[k] = v
    return out


def _integrity(st: _State, inv_results: Sequence[CheckResult]) -> dict[str, Any]:
    log = st.log
    intents: dict[str, Any] = {"total": None, "ack": None, "results": None}
    if log is not None:
        intents = {
            "total": len(log.intents),
            "ack": sum(
                1 for r in log.results.values() if any(a.is_ack for a in r.atts)
            ),
            "results": len(log.results),
        }
    out: dict[str, Any] = {
        "intents": intents,
        "log_complete": st.lc.value is True,
        "classes": None,
        "subs": None,
        "invariants": invariant_counts(inv_results),
        "examples": {},
        "summary": None,
    }
    if st.result is None:
        out["not_evaluated"] = {"*": st.cls_reason}
        return out
    r = st.result
    out["classes"] = dict(r.counts)
    out["subs"] = dict(r.subs)
    out["examples"] = {k: list(v) for k, v in r.examples.items()}
    out["summary"] = _jsonable_summary(
        summarize(
            r, log_complete=st.lc.value is True, account_set_ok=not st.scope.mismatch
        )
    )
    if st.unavailable:
        out["not_evaluated"] = dict(sorted(st.unavailable.items()))
    return out


def _client(st: _State) -> dict[str, Any]:
    """구간별 n(intent 수), 달성률, FILLED 수(설계 5.5). p95·오류율은 1차 미구현이다."""
    phases: dict[str, Any] = {}
    log = st.log
    if log is None:
        return {"phases": phases}
    for p, (a, b) in st.windows.items():
        entry: dict[str, Any] = {
            "from_ms": a,
            "to_ms": b,
            "n": sum(1 for it in log.intents.values() if a <= it.t < b),
            "achieved_ratio": _ratio(st, (a, b)),
        }
        if st.result is not None:
            entry["filled"] = count_filled(
                st.result, start=a, end=b, accounts=st.scope.run_ids
            )
            entry["sell_filled"] = count_filled(
                st.result, side="sell", start=a, end=b, accounts=st.scope.run_ids
            )
        phases[p] = entry
    return {"phases": phases}


def _load(st: _State) -> dict[str, Any]:
    log = st.log
    rs = log.run_start if log is not None else None
    lw = st.lw
    duration = None
    state = st.ev.summary.get("state") if st.ev.summary is not None else None
    if isinstance(state, Mapping):
        duration = state.get("testRunDurationMs")
    setup_ms = None
    if rs is not None and log is not None and log.setup_done is not None:
        setup_ms = log.setup_done.t - rs.t
    return {
        "k6_exit_code": st.ev.k6_exit_code,
        "k6_exit_reason": st.ev.k6_exit_reason,
        "stopped_by_judge": st.stopped_by_judge,
        "profile": rs.profile if rs is not None else None,
        "conn_reuse": rs.conn_reuse if rs is not None else None,
        "plan_duration_ms": plan_duration_ms(rs.plan) if rs is not None else None,
        "setup_ms": setup_ms,
        "test_run_duration_ms": duration,
        "summary_error": st.ev.summary_error,
        "window": None if lw is None else {"from_ms": lw[0], "to_ms": lw[1]},
        "clock_offset_ms": st.ev.clock_offset_ms,
        "clock_skew": st.skew,
        "sha256": log.sha256 if log is not None else None,
        "sha256_remote": st.ev.remote_sha,
    }


def _features(crit: Criteria, log: K6Log | None) -> frozenset[str]:
    """requires: 이름 중 충족된 것(설계 5.7 규칙 5). prometheus, ws_reconnect."""
    out = set()
    prom = crit.data.get("prometheus")
    if isinstance(prom, Mapping) and prom.get("enabled") is True:
        out.add("prometheus")
    if log is not None and log.run_start is not None:
        if log.run_start.extra.get("ws_reconnect") is True:
            out.add("ws_reconnect")
    return frozenset(out)


def _missing_log_check() -> CheckResult:
    return CheckResult(
        id=LOG_COMPLETE_ID,
        kind=LOG_COMPLETE_KIND,
        outcome=Outcome.INCONCLUSIVE,
        required=False,
        on_violation="inconclusive",
        value=False,
        reasons=(R.SENT_LOG_MISSING,),
        detail={
            "failed": ["lines_parsed"],
            "why": {"lines_parsed": "orders.jsonl이 없다"},
        },
    )


def _unavailable(scope: AccountScope, lc_ok: bool) -> dict[str, str]:
    """평가하지 않는 클래스 → 사유(설계 5.3 기록 완전성, 5.7 규칙 1)."""
    out: dict[str, str] = {}
    if scope.mismatch:
        for c in ACCOUNT_SET_UNAVAILABLE:
            out[c] = R.ACCOUNT_SET_MISMATCH
    if not lc_ok:
        for c in (
            OrderClass.ORPHAN,
            OrderClass.UNRESOLVED_COMMITTED,
            OrderClass.UNRESOLVED_ABSENT,
        ):
            out.setdefault(str(c), R.SENT_LOG_INCOMPLETE)
    return dict(sorted(out.items()))


def _uncertain(result: ClassifyResult | None) -> dict[str, dict[str, int]]:
    """PASS를 INCONCLUSIVE로 낮추는 클래스 → {출처: 행 수}(설계 4.4, 5.3, 5.7 규칙 4).

    출처는 _UNCERTAIN_SOURCES 키다. 응답·재송신 원문을 비교하지 못한 행이 있으면 그 클래스는
    PASS가 아니다(조용히 건너뛰지 않는다).
    """
    if result is None:
        return {}
    out: dict[str, dict[str, int]] = {}
    for src, (cls, _reason) in sorted(_UNCERTAIN_SOURCES.items()):
        rows = result.marks.get(src, 0) or result.counts.get(src, 0)
        if rows > 0:
            out.setdefault(cls, {})[src] = int(rows)
    return dict(sorted(out.items()))


# ---------------------------------------------------------------- 공개 함수


@dataclass(frozen=True)
class Assessment:
    """evaluate 결과와 classified.csv 행."""

    verdict: Verdict
    classified: tuple[Classified, ...] = ()


def assess(
    ctx: RunContext,
    crit: Criteria,
    evidence: str | Path,
    *,
    generated_at: int | None = None,
) -> Assessment:
    """증거만으로 판정한다(설계 5.2 6단계). 순수 함수다. 모듈 docstring의 판정 순서를 따른다."""
    ev = load_evidence(evidence)
    log, db = ev.log, ev.db
    if log is not None and log.run_id is not None and log.run_id != ctx.run_id:
        raise InputError(
            f"증거 기록의 run 값 {log.run_id!r}가 run.json run_id {ctx.run_id!r}와 다르다"
        )
    cfg = integrity_cfg(crit)
    scope = account_scope(ctx, log, db)

    cls_reason: str | None = None
    if log is None:
        cls_reason = R.SENT_LOG_MISSING
    elif db is None or not CLASSIFY_SECTIONS <= db.sections:
        cls_reason = R.DB_ERROR
    result = None
    if cls_reason is None and log is not None and db is not None:
        result = classify(
            log,
            db,
            cfg,
            account_ids=scope.run_ids,
            extra_account_ids=scope.extra,
        )

    if log is None:
        lc = _missing_log_check()
    else:
        sha_ok = None if ev.remote_sha is None else sha_matches(log, ev.remote_sha)
        lc = log_complete(log, ev.summary, ev.k6_exit_code, sha_ok)
        if ev.summary_error is not None:
            why = dict(lc.detail.get("why", {}))
            why[COND_COUNTERS] = f"summary.json을 읽지 못했다({ev.summary_error})"
            lc = replace(
                lc, detail={**lc.detail, "why": why, "summary_error": ev.summary_error}
            )
    lc_ok = lc.value is True

    offset_known = ev.clock_offset_ms is not None
    offset = ev.clock_offset_ms if ev.clock_offset_ms is not None else 0
    windows = (
        phase_windows(ctx, crit, log, clock_offset_ms=offset) if log is not None else {}
    )
    lw = run_window(ctx, log, clock_offset_ms=offset) if log is not None else None
    st = _State(
        ctx=ctx,
        crit=crit,
        ev=ev,
        scope=scope,
        result=result,
        cls_reason=cls_reason,
        lc=lc,
        windows=windows,
        lw=lw,
        applicable=applicable_phases(ctx, crit),
        mark_phases=phases_from_marks(ctx, crit),
        offset=offset,
        offset_known=offset_known,
        skew=abs(offset) > CLOCK_SKEW_MAX_MS,
        features=_features(crit, log),
        unavailable=_unavailable(scope, lc_ok) if result is not None else {},
        uncertain=_uncertain(result),
    )

    snapshot = db if db is not None else DbSnapshot((), (), (), (), None, None)
    inv_results = check_invariants(snapshot, ctx, crit)
    inv_items = {r.id: r for r in item_results(inv_results, crit)}
    checks = _checks(st, inv_items)
    self_check = _self_check(st)
    if self_check is not None:
        checks.append(self_check)
    guards = _guards(st)

    warnings: set[str] = set()
    for r in inv_results:
        if r.id == I7_ID:
            warnings.update(str(w) for w in r.detail.get("warnings", ()))
    integrity = _integrity(st, inv_results)
    summary = integrity.get("summary")
    if isinstance(summary, Mapping):
        warnings.update(summary.get("warnings", ()))

    inputs = {
        "criteria_sha256": crit.sha256,
        "run_json_sha256": ctx.run_json_sha256,
        "marks_local_sha256": ctx.marks_local_sha256,
        "evidence_manifest_sha256": ev.manifest_sha256,
        "evidence_manifest": dict(ev.manifest),
        "judge_version": __version__,
        "response_fields_confirmed": cfg.response_fields_confirmed,
    }
    verdict = build_verdict(
        ctx,
        checks,
        guards,
        integrity,
        _client(st),
        _load(st),
        inputs,
        warnings,
        {},
        generated_at,
    )
    classified = result.classified if result is not None else ()
    return Assessment(verdict=verdict, classified=tuple(classified))


def evaluate(
    ctx: RunContext,
    crit: Criteria,
    evidence: str | Path,
    *,
    generated_at: int | None = None,
) -> Verdict:
    """증거만으로 판정하는 순수 함수(설계 5.1, 5.2 6단계). generated_at은 digest 밖이다."""
    return assess(ctx, crit, evidence, generated_at=generated_at).verdict


def _join(values: Iterable[Any]) -> str:
    return ";".join(str(v) for v in values)


def classified_csv(rows: Iterable[Classified]) -> str:
    """classified.csv(설계 5.2, 5.8). 한 줄 = 키 하나. detail은 canonical JSON이다."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(CLASSIFIED_COLUMNS)
    for c in rows:
        w.writerow(
            [
                c.key,
                c.account_id,
                "" if c.k_state is None else str(c.k_state),
                str(c.cls),
                _join(classes_of(c)),
                "" if c.sub is None else str(c.sub),
                _join(c.idem),
                "" if c.resend is None else str(c.resend),
                "" if c.t is None else str(c.t),
                _join(c.order_ids),
                canonical_json(c.detail),
            ]
        )
    return buf.getvalue()


def render_outputs(a: Assessment) -> dict[str, str]:
    """runs/<run_id>/에 쓸 파일 이름 → 내용(설계 5.2, 5.8). 쓰기는 CLI가 한다."""
    v = a.verdict
    text = json.dumps(v.to_dict(), ensure_ascii=False, indent=2, allow_nan=False)
    return {
        VERDICT_NAME: text + "\n",
        REPORT_NAME: fragment(v),
        CLASSIFIED_NAME: classified_csv(a.classified),
    }


def collect(ctx: RunContext, crit: Criteria, tr: Transports) -> Path:
    """클러스터를 만지는 유일한 함수(설계 5.1, 5.2). 1차 스텁이다.

    순서(설계 5.2)
    1. fetch(4.8): k6 Pod를 찾고 terminated를 기다린다. k6/pod-status.json, k6/sha256sum.txt,
       k6/orders.jsonl.gz, k6/summary.json, k6/derived.json을 쓴다.
    2. 정지 확인: k6 종료 뒤 10s 기다린다.
    3. DB: db.build_bundle → db.exec_bundle(psql 1회). 출력은 raw/db/snapshot.txt.
       F1 ≠ F2면 30s 뒤 한 번 더. psql 실패(TransportError)는 잡아 빠진 구획(DB_ERROR)으로 남긴다.
    4. Pod 관측: kubectl get pods -o json을 raw/k8s/에 남기고 pod_replaced·crash_restart·배속을 만든다.
    5. RELEASE(fetch.release), evidence.json(파일별 sha256), criteria.resolved.yaml.
    돌려주는 값은 증거 폴더(runs/<run_id>/)다.
    """
    raise NotImplementedError("설계 5.2, 1차")


def _criteria_for(ctx: RunContext, *, prefer_resolved: bool) -> Criteria:
    if prefer_resolved and ctx.run_dir is not None:
        resolved = ctx.run_dir / RESOLVED_NAME
        if resolved.is_file():
            return load_criteria(resolved)
    return load_criteria(ctx.criteria)


def judge(
    run_json: str | Path,
    *,
    offline: bool = False,
    generated_at: int | None = None,
) -> Verdict:
    """run.json 하나로 판정한다(설계 5.1). 파일은 쓰지 않는다.

    offline이면 collect 없이 run.json 폴더를 증거로 쓰고, criteria.resolved.yaml이 있으면 그것을 쓴다.
    """
    ctx = load_run(run_json)
    crit = _criteria_for(ctx, prefer_resolved=offline)
    if offline:
        if ctx.run_dir is None:
            raise InputError("run 디렉터리를 알 수 없다")
        evidence = ctx.run_dir
    else:
        evidence = collect(ctx, crit, Transports())
    return evaluate(ctx, crit, evidence, generated_at=generated_at)

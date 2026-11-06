"""주문 분류기(계약 정렬 명세 3–5, 설계 5.3).

k6 기록(K6Log, JSONL v2)과 DB 스냅샷(DbSnapshot)을 client_order_id 단위로 맞춰 본다.

- 시도 성격(ACK·REJECT·UNKNOWN·PROTOCOL)은 model.attempt_kind, 키별 K는 k_state다.
  ACK가 없고 마지막 시도가 PROTOCOL이거나 시도가 없으면 K_UNKNOWN이다(명세 3이 정하지 않은 칸.
  클라이언트가 결과를 해석하지 못했다). 이때 PROTOCOL이 함께 붙는다.
- 행렬(명세 4): K × D. D1 = 그 client_order_id의 orders 행이 있다. K_ACK × D1의 일치는
  첫 ACK 본문 대 DB 행이다(명세 5 매핑, cfg.response_fields).
- 행 하나 = 보낸 키 하나(intent 또는 result가 있는 키), 또는 DB 전용 행(ORPHAN, 범위 안 계좌의 이상).
- 주 클래스는 DUP_ORDER → PROTOCOL → 행렬 칸(OK 아님) → BODY_MISMATCH → FILL_STATE →
  IDEMPOTENCY_VIOLATION 중 처음 나오는 것이다. 하나도 없으면 OK다. 나머지는 detail["also"]에 둔다.
  IDEMPOTENCY_VIOLATION은 also가 아니라 idem으로 센다. 세는 규칙은 classes_of 한 곳에 있다.
- 응답 필드 매핑이 미확정이면(response_fields_confirmed=False) 응답 대 DB 불일치는 MISMATCH 대신
  RESPONSE_SCHEMA_UNCONFIRMED다. 보낸 본문 비교(BODY_MISMATCH)는 늘 한다.
- 비교는 model.order_value로 정규화한다. 숫자는 Decimal, 시각은 instant(µs), id는 int다.
  바꿀 수 없는 값은 불일치다. 단 응답 시각(filled_at 등)은 응답에 적힌 소수 자릿수까지만 DB 값을
  내림해 비교하고(밀리초 Z, 마이크로초 +00:00 모두 읽는다), 못 읽으면 불일치가 아니라
  불확실성이다(명세 11, _same_instant).
- 불확실성은 marks로 센다. PASS만 막는다(판정 몫, 설계 5.7 규칙 4). 200인데 원문 또는 응답
  시각을 못 읽음(response_unparsed·lost_unknown), replayed를 못 읽어 멱등 검사를 다 못 함
  (resend_uncompared), 보낸 본문이 없어 본문 비교를 못 함(body_missing).
- 결정성: 출력은 키 순서다. 입력 줄 순서·JSON 키 순서·현재 시각·난수에 기대지 않는다.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from . import reasons
from .model import (
    DEFAULT_RESPONSE_FIELDS,
    DUPLICATE_CLASSES,
    EXAMPLES_MAX,
    LOSS_CLASSES,
    MISMATCH_CLASSES,
    ORDER_BODY_FIELDS,
    ORDER_ID_FIELD,
    ORDER_TIME_COLUMNS,
    ORPHAN_CLASSES,
    REPLAYED_FIELD,
    RESEND_COMPARE_FIELDS,
    STATUS_FILLED,
    UNRESOLVED_CLASSES,
    USER_IMPACT_CLASSES,
    Attempt,
    Classified,
    DbOrder,
    DbSnapshot,
    IdemViolation,
    Intent,
    IntegrityCfg,
    K6Log,
    KState,
    OkSub,
    OrderClass,
    Result,
    is_run_key,
    iso_us,
    order_value,
    ts_digits,
    ts_us,
)

__all__ = [
    "EXAMPLES_MAX",
    "FIRST_REPLAYED_PATH",
    "MARK_KEYS",
    "ClassifyResult",
    "ack_path",
    "classes_of",
    "classify",
    "count_classes",
    "count_filled",
    "count_subs",
    "k_state",
    "run_order_count",
    "summarize",
]

CONFLICT_STATUS = 409  # idempotency_conflict(명세 1)
# OkSub가 아닌 ACK 경로. 첫 ACK rp=true인데 앞에 결과 불명 시도가 없다(FIRST_REPLAYED)
FIRST_REPLAYED_PATH = "first_replayed"
OK_SUB_VALUES = frozenset(s.value for s in OkSub)

# marks 키. 대문자는 사유 코드(reasons.py), 소문자는 진단 수치(행 수)다.
MARK_KEYS = (
    reasons.ROLLBACK_REPLY_COMMITTED,  # after_503 경로(경고)
    reasons.REJECT_AFTER_INFLIGHT,  # REJECT_COMMITTED 중 앞 시도가 늦게 커밋된 행(경고)
    "body_missing",  # D1인데 보낸 본문이 없어 BODY_MISMATCH를 보지 못함
    "intent_missing",  # result는 있는데 intent가 없음(기록 손상)
    "lost_unknown",  # 200 원문을 못 읽었고 DB 행이 없음(LOST인지 모름)
    "response_unparsed",  # 200 원문 또는 응답 시각을 못 읽었고 DB 행이 있음(비교 못 함)
    "resend_uncompared",  # 200 원문·replayed를 못 읽어 멱등 검사를 다 하지 못함
)

# summarize 멱등 위반 키(IdemViolation 값 → 요약 키)
_IDEM_SUMMARY = {
    IdemViolation.DOUBLE_CREATE: "idem_double_create",
    IdemViolation.FIRST_REPLAYED: "idem_first_replayed",
    IdemViolation.RESEND_DIFF: "idem_resend_diff",
    IdemViolation.SAME_BODY_409: "idem_409_same_body",
}


@dataclass(frozen=True)
class ClassifyResult:
    """분류 결과(설계 5.3, 5.8 integrity.classes·subs·examples).

    classified: 키 순서로 정렬된 행.
    counts: OrderClass 값 → 행 수(모든 클래스, 0 포함). classes_of 규칙으로 센다.
    subs: OkSub 값 → cls가 OK인 행 수(모든 세부, 0 포함).
    examples: 행이 있는 클래스 → 키(정렬, 최대 EXAMPLES_MAX).
    marks: MARK_KEYS → 행 수. idem: IdemViolation 값 → 행 수(0 포함).
    intents: intent 줄 수.
    """

    classified: tuple[Classified, ...]
    counts: Mapping[str, int] = field(default_factory=dict, hash=False)
    subs: Mapping[str, int] = field(default_factory=dict, hash=False)
    examples: Mapping[str, tuple[str, ...]] = field(default_factory=dict, hash=False)
    marks: Mapping[str, int] = field(default_factory=dict, hash=False)
    idem: Mapping[str, int] = field(default_factory=dict, hash=False)
    intents: int = 0


# ---------------------------------------------------------------- 공개 함수


def classify(
    log: K6Log,
    db: DbSnapshot,
    cfg: IntegrityCfg,
    *,
    account_ids: frozenset[str] | None = None,
    extra_account_ids: frozenset[str] | None = None,
) -> ClassifyResult:
    """k6 기록과 DB를 키 단위로 분류한다(명세 4, 설계 5.3).

    account_ids: 이번 run 계좌(user_id). None이면 log.account_ids(intent의 user_id).
      run 키가 아닌 이 계좌 주문은 DB 전용 이상(DUP_ORDER, FILL_STATE)이 있을 때만 행을 만든다
      (detail.scope "account"). ORPHAN은 계좌가 아니라 run 키 형식으로 정한다(명세 4).
    extra_account_ids: 발표자 계좌(run.json extra_accounts). None이면 없다.
      이 계좌 주문은 DB 전용 이상이 있을 때만 행을 만든다(detail.scope "extra").
    ACCOUNT_SET_MISMATCH일 때 LOST·ORPHAN을 빼는 일은 판정(summarize, pipeline) 몫이다.
    """
    run_ids = log.account_ids if account_ids is None else frozenset(account_ids)
    extra = frozenset() if extra_account_ids is None else frozenset(extra_account_ids)
    run_id = log.run_id
    if run_id is None and log.run_start is not None:
        run_id = log.run_start.run
    by_key = db.orders_by_key()
    pairs = _resend_pairs(cfg)

    rows: list[Classified] = []
    sent = sorted(set(log.intents) | set(log.results))
    matched: set[str] = set()
    for key in sent:
        intent = log.intents.get(key)
        db_key = _db_key(key, intent)
        matched.add(db_key)
        rows.append(
            _classify_sent(
                key,
                intent,
                log.results.get(key),
                by_key.get(db_key, ()),
                cfg,
                pairs,
                db_key=db_key,
            )
        )
    for key, group in by_key.items():
        if key in matched:
            continue
        row = _classify_db_only(
            key,
            group,
            run_id=run_id,
            run_ids=run_ids,
            extra=extra,
            log_problems=len(log.problems),
        )
        if row is not None:
            rows.append(row)

    rows.sort(key=lambda c: (c.key, c.account_id, c.order_ids))
    return _aggregate(rows, intents=len(log.intents))


def classes_of(c: Classified) -> tuple[str, ...]:
    """행이 세는 클래스: cls, detail["also"], idem이 있으면 IDEMPOTENCY_VIOLATION."""
    out = [str(c.cls)]
    for x in c.detail.get("also", ()):
        if x not in out:
            out.append(str(x))
    if c.idem and OrderClass.IDEMPOTENCY_VIOLATION not in out:
        out.append(str(OrderClass.IDEMPOTENCY_VIOLATION))
    return tuple(out)


def count_classes(
    result: ClassifyResult,
    classes: Iterable[str],
    *,
    start: int | None = None,
    end: int | None = None,
    accounts: Iterable[str] | None = None,
) -> int:
    """classes 중 하나라도 세는 행 수. 구간은 start ≤ t < end(t가 None인 행은 뺀다)."""
    want = {str(x) for x in classes}
    return sum(
        1
        for c in _select(result, start, end, accounts)
        if want.intersection(classes_of(c))
    )


def count_subs(
    result: ClassifyResult,
    subs: Iterable[str],
    *,
    start: int | None = None,
    end: int | None = None,
    accounts: Iterable[str] | None = None,
) -> int:
    """cls가 OK이고 sub가 subs 중 하나인 행 수(fault_effect class_sub, 설계 6)."""
    want = {str(x) for x in subs}
    return sum(
        1
        for c in _select(result, start, end, accounts)
        if c.cls == OrderClass.OK and c.sub is not None and str(c.sub) in want
    )


def count_filled(
    result: ClassifyResult,
    *,
    side: str | None = None,
    start: int | None = None,
    end: int | None = None,
    accounts: Iterable[str] | None = None,
) -> int:
    """status 'filled' 주문 수(설계 5.5 filled·sell_filled, 명세 8). side는 'bid' 또는 'ask'.

    범위 밖 계좌의 이상 행(detail.scope extra·account)은 이번 run 주문이 아니라 뺀다.
    """
    n = 0
    for c in _select(result, start, end, accounts):
        if "scope" in c.detail:
            continue
        pairs = zip(c.detail.get("db_status", ()), c.detail.get("db_side", ()))
        n += sum(1 for st, sd in pairs if st == STATUS_FILLED and side in (None, sd))
    return n


def run_order_count(result: ClassifyResult, run_id: str) -> int:
    """run 키 형식 행에 매칭된 DB 주문 수(분류기의 D1 수). judge_self_check가
    스냅샷 run 키 주문 수(DbSnapshot.run_orders)와 맞춰 본다(명세 7)."""
    return sum(len(c.order_ids) for c in result.classified if is_run_key(c.key, run_id))


def summarize(
    result: ClassifyResult,
    *,
    log_complete: bool = True,
    account_set_ok: bool = True,
) -> dict[str, Any]:
    """완료 기준 용어로 모은다(설계 5.3 "완료 기준 용어 대응").

    유실 = LOST + REJECT_COMMITTED. 중복 = DUP_ORDER + FILL_STATE + IDEMPOTENCY_VIOLATION.
    내용 불일치 = MISMATCH + BODY_MISMATCH. 고아 = ORPHAN. 클래스별 행 수를 더한다
    (한 행이 두 클래스를 세면 2다).
    inconclusive: 수치를 믿을 수 없는 항목 → 사유 코드(설계 5.3 기록 완전성, 5.7 규칙 1·4).
    """
    c = result.counts
    m = result.marks

    def total(classes: Iterable[str]) -> int:
        return sum(c.get(str(x), 0) for x in classes)

    user_impact = total(USER_IMPACT_CLASSES)
    ratio = round(user_impact / result.intents, 6) if result.intents else None
    inconclusive: dict[str, set[str]] = {}

    def flag(metric: str, reason: str) -> None:
        inconclusive.setdefault(metric, set()).add(reason)

    if not account_set_ok:
        flag("lost", reasons.ACCOUNT_SET_MISMATCH)
        flag("orphans", reasons.ACCOUNT_SET_MISMATCH)
    if not log_complete:
        flag("orphans", reasons.SENT_LOG_INCOMPLETE)
        flag("unresolved", reasons.SENT_LOG_INCOMPLETE)
    if m.get("resend_uncompared", 0):
        flag("duplicates", reasons.NOT_EVALUATED)
    if c.get(OrderClass.RESPONSE_SCHEMA_UNCONFIRMED, 0):
        flag("mismatch", reasons.RESPONSE_SCHEMA_UNCONFIRMED)
    if m.get("response_unparsed", 0) or m.get("body_missing", 0):
        flag("mismatch", reasons.NOT_EVALUATED)
    if m.get("lost_unknown", 0):
        flag("lost", reasons.NOT_EVALUATED)

    warnings = []
    if m.get(reasons.ROLLBACK_REPLY_COMMITTED, 0):
        warnings.append(reasons.ROLLBACK_REPLY_COMMITTED)
    if m.get(reasons.REJECT_AFTER_INFLIGHT, 0):
        warnings.append(reasons.REJECT_AFTER_INFLIGHT)

    out: dict[str, Any] = {
        "intents": result.intents,
        "ok": c.get(OrderClass.OK, 0),
        "lost": total(LOSS_CLASSES),
        "reject_committed": c.get(OrderClass.REJECT_COMMITTED, 0),
        "duplicates": total(DUPLICATE_CLASSES),
        "mismatch": total(MISMATCH_CLASSES),
        "orphans": total(ORPHAN_CLASSES),
        "unresolved": total(UNRESOLVED_CLASSES),
        "interrupted": c.get(OrderClass.INTERRUPTED, 0),
        "user_impact": user_impact,
        "intent_unresolved_ratio": ratio,
        "protocol": c.get(OrderClass.PROTOCOL, 0),
        "response_unconfirmed": c.get(OrderClass.RESPONSE_SCHEMA_UNCONFIRMED, 0),
        "response_unparsed": m.get("response_unparsed", 0),
        "lost_unknown": m.get("lost_unknown", 0),
        "resend_uncompared": m.get("resend_uncompared", 0),
        "body_missing": m.get("body_missing", 0),
        "recovered_after_commit": result.subs.get(OkSub.AFTER_COMMIT, 0),
        "rejected": result.subs.get(OkSub.REJECTED, 0),
    }
    for v, name in _IDEM_SUMMARY.items():
        out[name] = result.idem.get(v.value, 0)
    out["warnings"] = tuple(warnings)
    out["inconclusive"] = {k: tuple(sorted(v)) for k, v in sorted(inconclusive.items())}
    return out


# ---------------------------------------------------------------- 시도 해석(명세 3, 4)


def k_state(result: Result | None) -> KState:
    """키별 클라이언트 상태 K(명세 3).

    ACK가 없고 마지막 시도가 PROTOCOL이거나 시도가 없으면 K_UNKNOWN이다(모듈 docstring).
    """
    if result is None:
        return KState.INTERRUPTED
    if any(a.is_ack for a in result.atts):
        return KState.K_ACK
    last = result.last
    if last is not None and last.is_reject:
        return KState.K_REJECT
    return KState.K_UNKNOWN


def ack_path(atts: Sequence[Attempt]) -> str | None:
    """첫 ACK까지의 시도 경로. OkSub 값 또는 FIRST_REPLAYED_PATH. ACK가 없거나 rp를 모르면 None.

    앞 시도 중 확정 거절이 아닌 것(UNKNOWN, PROTOCOL)은 커밋됐을 수 있는 시도다.
    - 앞 시도 없음: rp=false → clean, rp=true → first_replayed.
    - rp=false: not_committed(앞이 결과 불명이든 확정 거절이든).
    - rp=true이고 커밋됐을 수 있는 앞 시도가 있다: 503 not_ready가 아닌 것이 하나라도 있으면
      after_commit, not_ready만 있으면 after_503.
    - rp=true인데 앞 시도가 모두 확정 거절: first_replayed. 확정 거절은 그 시도 자체를 저장하지
      않으므로 서버가 "이미 있다"고 한 것은 n=1 rp=true와 같은 뜻이다. 키 전체로는 다르다.
      같은 키의 처리 중인 앞 시도(결과 불명)는 확정 거절 뒤에 커밋될 수 있다(app/orders.py:
      find_order가 잠금 밖이라 재전송이 앞 시도의 잠금을 기다린 뒤 바뀐 잔고로 422를 받는다).
      그래서 [결과 불명, 확정 거절] × D1도 K_REJECT라 REJECT_COMMITTED다. 클라이언트는 거절로
      안내받고 돈은 움직였으므로 FAIL로 두고, detail.after_inflight와 경고 REJECT_AFTER_INFLIGHT로
      원인(서비스 경합)을 표시한다(명세 4).
    - 겹친 시도: 첫 ACK가 rp=true면, 첫 ACK가 끝나기 전에 시작한(t0 < 첫 ACK t1) 뒤 번호 시도도
      커밋됐을 수 있는 시도로 본다(rp=true ACK와 확정 거절은 뺀다). 같은 키 동시 전송(서비스 ④)이면
      t0 순서의 n=1이 진 쪽일 수 있다(실서비스 e2e). k6는 같은 키를 순차로만 보내므로(명세 2)
      순차 기록의 결과는 같다.
    """
    idx = next((i for i, a in enumerate(atts) if a.is_ack), None)
    if idx is None:
        return None
    first = atts[idx]
    rp, _ = _replayed(first)
    if rp is None:
        return None
    prior = atts[:idx]
    if not rp:
        return OkSub.NOT_COMMITTED.value if prior else OkSub.CLEAN.value
    overlapping = [
        a
        for a in atts[idx + 1 :]
        if a.t0 < first.t1 and not (a.is_ack and _replayed(a)[0] is True)
    ]
    maybe_committed = [a for a in (*prior, *overlapping) if not a.is_reject]
    if not maybe_committed:
        return FIRST_REPLAYED_PATH
    if any(not a.is_not_ready for a in maybe_committed):
        return OkSub.AFTER_COMMIT.value
    return OkSub.AFTER_503.value


def _replayed(a: Attempt) -> tuple[bool | None, bool]:
    """ACK의 replayed. (값, rp 충돌). 원문 replayed가 bool이면 그것, 아니면 Attempt.rp.

    둘 다 bool인데 다르면 충돌이다(k6 기록 결함, PROTOCOL). 둘 다 없으면 None이다.
    """
    body = a.json()
    v = body.get(REPLAYED_FIELD) if body is not None else None
    rp = a.rp if isinstance(a.rp, bool) else None
    if isinstance(v, bool):
        return v, rp is not None and rp != v
    return rp, False


def _idem(
    atts: Sequence[Attempt], pairs: tuple[tuple[str, str], ...]
) -> tuple[tuple[IdemViolation, ...], dict[str, Any], list[str]]:
    """멱등 위반(명세 4). (위반, detail, 비교 못 한 시도 메모).

    RESEND_DIFF는 두 번째 이후 ACK의 의미 필드가 첫 ACK와 다를 때다. 첫 ACK 뒤 확정 거절도
    재요청 결과가 원 응답과 다른 것으로 본다(이미 있는 키라 서비스는 replay해야 한다).
    """
    found: set[IdemViolation] = set()
    detail: dict[str, Any] = {}
    uncompared: list[str] = []
    acks = [(a, _replayed(a)[0]) for a in atts if a.is_ack]
    for a in atts:
        if a.st == 200 and not a.is_ack:
            uncompared.append(f"att{a.n}:unparsed")
    for a, rp in acks:
        if rp is None:
            uncompared.append(f"att{a.n}:rp_unknown")

    if ack_path(atts) == FIRST_REPLAYED_PATH:
        found.add(IdemViolation.FIRST_REPLAYED)

    created = [a for a, rp in acks if rp is False]
    created_ids = {_norm("id", _field(a, ORDER_ID_FIELD)) for a in created}
    if len(created) >= 2 and len(created_ids) >= 2:
        found.add(IdemViolation.DOUBLE_CREATE)
        detail["created_ids"] = [_show(_field(a, ORDER_ID_FIELD)) for a in created]

    idx = next((i for i, a in enumerate(atts) if a.is_ack), None)
    if idx is not None:
        first = atts[idx]
        orig = first.json() or {}
        rdiff: dict[str, Any] = {}
        for a in atts[idx + 1 :]:
            if a.is_ack:
                d = _diff_fields(orig, a.json() or {}, pairs)
                if d:
                    rdiff[str(a.n)] = d
            elif a.is_reject:
                rdiff[str(a.n)] = {"st": [first.st, a.st]}
        if rdiff:
            found.add(IdemViolation.RESEND_DIFF)
            detail["resend_diff"] = rdiff

    conflicts = [a.n for a in atts if a.st == CONFLICT_STATUS]
    if conflicts:
        found.add(IdemViolation.SAME_BODY_409)  # k6는 늘 같은 본문을 보낸다(명세 2)
        detail["conflict_atts"] = conflicts
    return tuple(v for v in IdemViolation if v in found), detail, uncompared


def _db_key(key: str, intent: Intent | None) -> str:
    """DB와 맞춰 볼 키. 서버가 저장한 것은 본문의 client_order_id다(명세 4).

    intent.key와 본문 키가 다르면 k6 기록 결함이다. 본문 키로 DB를 찾아 서버 쪽 결과를 그대로
    판정하고, 결함은 PROTOCOL(intent_key)과 safety 가드가 잡는다. intent 키로 찾으면 정상 체결이
    LOST와 ORPHAN으로 세어져 클라이언트 결함이 FAIL(INTEGRITY_VIOLATION)이 된다.
    """
    if intent is not None:
        body_key = intent.body.get("client_order_id")
        if isinstance(body_key, str) and body_key:
            return body_key
    return key


def _protocol_notes(intent: Intent | None, result: Result | None) -> list[str]:
    """형식 위반(명세 3 "그 밖" 시도, 명세 2 intent 규칙). 비면 없음."""
    notes: list[str] = []
    if intent is not None:
        b = intent.body
        if b.get("client_order_id") != intent.key:
            notes.append("intent_key")
        if b.get("user_id") != intent.acct:
            notes.append("intent_acct")
        if _body_missing(intent):
            notes.append("intent_body")
    if result is not None:
        atts = result.atts
        if not atts:
            notes.append("atts_empty")
        elif [a.n for a in atts] != list(range(1, len(atts) + 1)):
            notes.append("att_order")
        for a in atts:
            if a.is_protocol:
                notes.append(f"att{a.n}:st{a.st}")
            elif a.is_ack and _replayed(a)[1]:
                notes.append(f"att{a.n}:rp_conflict")
    return notes


# ---------------------------------------------------------------- 보낸 키


def _classify_sent(
    key: str,
    intent: Intent | None,
    result: Result | None,
    orders: tuple[DbOrder, ...],
    cfg: IntegrityCfg,
    pairs: tuple[tuple[str, str], ...],
    *,
    db_key: str | None = None,
) -> Classified:
    """보낸 키 하나(intent 또는 result가 있는 키)를 분류한다. orders는 db_key(_db_key)의 행이다."""
    k = k_state(result)
    atts = result.atts if result is not None else ()
    detail: dict[str, Any] = {"db_rows": len(orders)}
    if db_key is not None and db_key != key:
        detail["db_key"] = db_key
    detail.update(_db_facts(orders))
    if result is not None:
        detail["statuses"] = [a.st for a in atts]
        ecs = [a.ec for a in atts]
        if any(e is not None for e in ecs):
            detail["ecs"] = ecs
    if intent is None:
        detail["intent_missing"] = True
    if k is KState.INTERRUPTED:
        detail["committed"] = bool(orders)

    protocol = _protocol_notes(intent, result)
    if protocol:
        detail["protocol"] = protocol

    path = ack_path(atts) if k is KState.K_ACK else None
    if path is not None:
        detail["path"] = path
    idem, idetail, uncompared = _idem(atts, pairs)
    detail.update(idetail)
    if uncompared:
        detail["idem_uncompared"] = uncompared

    # 보낸 본문 대 DB(K와 무관하게, 명세 4)
    body_diff: dict[str, Any] = {}
    if orders:
        if intent is None or _body_missing(intent):
            detail["body"] = "missing"
        else:
            for o in orders:
                d = _body_diff(intent, o)
                if d:
                    body_diff[str(o.id)] = d
    if body_diff:
        detail["body_diff"] = body_diff

    fill = _fill_state(orders)
    if fill:
        detail["fill"] = fill

    # 원 응답 대 DB(K_ACK × D1, 명세 5)
    resp_diff: dict[str, Any] = {}
    first_ack = result.first_ack if result is not None else None
    if first_ack is not None and orders:
        resp = first_ack.json() or {}
        resp_diff, unread = _response_diff(resp, _target(orders, resp), cfg)
        if resp_diff:
            detail["response"] = "mismatch"
            detail["response_diff"] = resp_diff
        else:
            # 못 읽은 시각만 남으면 일치를 확인하지 못했다(명세 11, 원문 못 읽음과 같은 불확실성)
            detail["response"] = "unparsed" if unread else "match"
        if unread:
            detail["response_unparsed"] = unread
    elif first_ack is None and any(a.st == 200 for a in atts):
        detail["response"] = "unparsed"  # 200을 받았지만 원문을 못 읽었다

    if k is KState.INTERRUPTED:
        cell = OrderClass.INTERRUPTED
    elif k is KState.K_ACK:
        if not orders:
            cell = OrderClass.LOST
        elif resp_diff:
            cell = (
                OrderClass.MISMATCH
                if cfg.response_fields_confirmed
                else OrderClass.RESPONSE_SCHEMA_UNCONFIRMED
            )
        else:
            cell = OrderClass.OK
    elif k is KState.K_REJECT:
        cell = OrderClass.REJECT_COMMITTED if orders else OrderClass.OK
        if not orders:
            path = OkSub.REJECTED.value
            detail["path"] = path
        elif atts[-1].st != CONFLICT_STATUS and any(not a.is_reject for a in atts[:-1]):
            # 앞의 결과 불명 시도가 재전송의 거절 뒤에 커밋됐다(ack_path docstring).
            # 클라이언트는 거절로 안내받았으므로 FAIL은 그대로 두고 원인을 표시한다.
            # 409는 서버가 이미 있는 행을 찾았다는 뜻이라 경합이 아니다(멱등 위반 409_SAME_BODY).
            detail["after_inflight"] = True
    elif orders:
        cell = OrderClass.UNRESOLVED_COMMITTED
    else:
        cell = OrderClass.UNRESOLVED_ABSENT

    cls, also = _compose(
        cell,
        dup=len(orders) >= 2,
        protocol=bool(protocol),
        body=bool(body_diff),
        fill=bool(fill),
        idem=bool(idem),
    )
    if also:
        detail["also"] = also
    sub = OkSub(path) if cls == OrderClass.OK and path in OK_SUB_VALUES else None

    if intent is not None:
        acct = intent.acct
        t = intent.t
    else:
        acct = orders[0].user_id if orders else ""
        t = _first_seen(orders)
        if t is None and atts:
            t = atts[0].t0
    return Classified(
        key=key,
        account_id=acct,
        k_state=k,
        cls=cls,
        sub=sub,
        idem=idem,
        t=t,
        order_ids=tuple(o.id for o in orders),
        detail=detail,
    )


def _compose(
    cell: OrderClass,
    *,
    dup: bool,
    protocol: bool,
    body: bool,
    fill: bool,
    idem: bool,
) -> tuple[OrderClass, list[str]]:
    """주 클래스와 also(모듈 docstring 순서). IDEMPOTENCY_VIOLATION은 also에 넣지 않는다."""
    found: list[OrderClass] = []
    if dup:
        found.append(OrderClass.DUP_ORDER)
    if protocol:
        found.append(OrderClass.PROTOCOL)
    if cell != OrderClass.OK:
        found.append(cell)
    if body:
        found.append(OrderClass.BODY_MISMATCH)
    if fill:
        found.append(OrderClass.FILL_STATE)
    if idem:
        found.append(OrderClass.IDEMPOTENCY_VIOLATION)
    if not found:
        return OrderClass.OK, []
    also = [x.value for x in found[1:] if x != OrderClass.IDEMPOTENCY_VIOLATION]
    return found[0], also


# ---------------------------------------------------------------- DB 전용(명세 4)


def _classify_db_only(
    key: str,
    group: tuple[DbOrder, ...],
    *,
    run_id: str | None,
    run_ids: frozenset[str],
    extra: frozenset[str],
    log_problems: int,
) -> Classified | None:
    """보낸 키가 아닌 DB 행. run 키 형식이면 ORPHAN, 범위 안 계좌면 이상이 있을 때만 행을 만든다.

    범위: run 키 형식 → ORPHAN. 아니면 extra 계좌 → scope "extra", run 계좌 → scope "account".
    그 밖(다른 사용자, 다른 run의 키)은 분류하지 않는다(불변식이 모든 계좌를 본다).
    """
    orphan = run_id is not None and is_run_key(key, run_id)
    users = {o.user_id for o in group}
    scope = None
    if not orphan:
        if users & extra:
            scope = "extra"
        elif users & run_ids:
            scope = "account"
        else:
            return None
    fill = _fill_state(group)
    dup = len(group) >= 2
    if scope is not None and not dup and not fill:
        return None  # 범위 안 계좌의 정상 주문은 분류하지 않는다
    detail: dict[str, Any] = {"db_rows": len(group)}
    detail.update(_db_facts(group))
    if fill:
        detail["fill"] = fill
    if scope is not None:
        detail["scope"] = scope
    else:
        if log_problems:
            detail["log_problems"] = log_problems
        if not users & run_ids:
            detail["foreign_account"] = True  # run 키인데 이번 run 계좌가 아니다
    cls, also = _compose(
        OrderClass.ORPHAN if orphan else OrderClass.OK,
        dup=dup,
        protocol=False,
        body=False,
        fill=bool(fill),
        idem=False,
    )
    if also:
        detail["also"] = also
    return Classified(
        key=key,
        account_id=group[0].user_id,
        k_state=None,
        cls=cls,
        t=_first_seen(group),
        order_ids=tuple(o.id for o in group),
        detail=detail,
    )


def _fill_state(orders: Iterable[DbOrder]) -> list[str]:
    """FILL_STATE 메모. status가 filled가 아님, 또는 filled인데 체결 열이 NULL."""
    notes: list[str] = []
    for o in orders:
        if o.status != STATUS_FILLED:
            notes.append(f"{o.id}:status_{o.status or 'null'}")
            continue
        for col, v in (
            ("fill_price", o.fill_price),
            ("amount", o.amount),
            ("filled_at", o.filled_us),
        ):
            if v is None:
                notes.append(f"{o.id}:filled_null_{col}")
    return notes


# ---------------------------------------------------------------- 비교 도우미(명세 5)


def _norm(column: str, v: Any) -> Any:
    """비교용 값(order_value). 바꿀 수 없으면 표식 튜플(어떤 정상 값과도 다르다)."""
    try:
        return order_value(column, v)
    except (TypeError, ValueError):
        return ("unparsable", repr(v))


def _field(a: Attempt, name: str) -> Any:
    body = a.json()
    return body.get(name) if body is not None else None


def _show(v: Any) -> Any:
    """detail용 표시값. bool·None은 그대로, 나머지는 문자열(JSON 직렬화와 결정성)."""
    if v is None or isinstance(v, bool):
        return v
    return str(v)


def _show_db(column: str, v: Any) -> Any:
    if column in ORDER_TIME_COLUMNS and isinstance(v, int):
        return iso_us(v)
    return _show(v)


def _resend_pairs(cfg: IntegrityCfg) -> tuple[tuple[str, str], ...]:
    """RESEND_DIFF 비교 (응답 필드, 열). 열은 RESEND_COMPARE_FIELDS의 기본 매핑 열이고,
    응답 필드 이름은 cfg.response_fields가 그 열에 매핑한 이름이다(없으면 기본 이름)."""
    by_col: dict[str, list[str]] = {}
    for f, col in cfg.response_fields.items():
        by_col.setdefault(col, []).append(f)
    out: list[tuple[str, str]] = []
    for f in RESEND_COMPARE_FIELDS:
        col = DEFAULT_RESPONSE_FIELDS[f]
        for name in sorted(by_col.get(col, [f])):
            out.append((name, col))
    return tuple(out)


def _diff_fields(
    a: Mapping[str, Any], b: Mapping[str, Any], pairs: Iterable[tuple[str, str]]
) -> dict[str, list[Any]]:
    diff: dict[str, list[Any]] = {}
    for f, col in pairs:
        if _norm(col, a.get(f)) != _norm(col, b.get(f)):
            diff[f] = [_show(a.get(f)), _show(b.get(f))]
    return diff


def _target(orders: tuple[DbOrder, ...], resp: Mapping[str, Any]) -> DbOrder:
    """응답과 비교할 행. 행이 여럿이면(DUP_ORDER) id가 응답 order_id인 행, 없으면 첫 행."""
    if len(orders) > 1:
        rid = _norm("id", resp.get(ORDER_ID_FIELD))
        for o in orders:
            if o.id == rid:
                return o
    return orders[0]


def _response_diff(
    resp: Mapping[str, Any], order: DbOrder, cfg: IntegrityCfg
) -> tuple[dict[str, list[Any]], list[str]]:
    """원 응답 대 DB 행(cfg.response_fields: 응답 필드 → 열). (불일치, 못 읽은 시각 필드).

    응답에 필드가 없으면 null로 본다. 시각 열은 _same_instant로 비교한다(명세 11).
    """
    diff: dict[str, list[Any]] = {}
    unread: list[str] = []
    for f, col in sorted(cfg.response_fields.items()):
        try:
            want = order.value(col)
        except KeyError:
            continue  # 모르는 열(criteria 검증이 막는다)
        got = resp.get(f)
        if col in ORDER_TIME_COLUMNS:
            same = _same_instant(got, want)
            if same is None:
                unread.append(f)
                continue
        else:
            same = _norm(col, got) == want
        if not same:
            diff[f] = [_show(got), _show_db(col, want)]
    return diff, unread


def _same_instant(got: Any, want: int | None) -> bool | None:
    """응답 시각 대 DB 시각(epoch µs). 응답에 적힌 초 소수 자릿수 p까지만 비교한다(명세 11).

    둘 다 10^(6−p) µs 단위로 내림한다(밀리초 응답이면 밀리초까지, 7자리 이상은 µs까지).
    없거나 빈 값은 null이다. 문자열인데 초까지 적지 않았거나 시간대가 없는 등 못 읽으면 None
    (불확실성, PASS만 막는다). int는 이미 µs로 본다(order_value와 같다).
    """
    if isinstance(got, int) and not isinstance(got, bool):
        return got == want
    if got is not None and not isinstance(got, str):
        return None
    try:
        us = ts_us(got)
    except ValueError:
        return None
    if us is None or want is None:
        return us == want
    digits = ts_digits(got)
    if digits is None:
        return None
    unit = 10 ** (6 - min(digits, 6))
    return us - us % unit == want - want % unit


def _body_missing(intent: Intent) -> bool:
    return any(intent.body.get(f) in (None, "") for f in ORDER_BODY_FIELDS)


def _body_diff(intent: Intent, order: DbOrder) -> dict[str, list[Any]]:
    """보낸 본문 대 DB 행(user_id, market, side, volume). volume은 Decimal로 비교한다."""
    diff: dict[str, list[Any]] = {}
    for f in ORDER_BODY_FIELDS:
        sent = intent.body.get(f)
        got = order.value(f)
        if _norm(f, sent) != got:
            diff[f] = [_show(sent), _show(got)]
    return diff


# ---------------------------------------------------------------- 집계


def _db_facts(orders: Iterable[DbOrder]) -> dict[str, list[str | None]]:
    """주문별 status·side(id 순). 체결 커버리지 집계에 쓴다(설계 5.5, 명세 8)."""
    rows = sorted(orders, key=lambda o: o.id)
    if not rows:
        return {}
    return {
        "db_status": [o.status for o in rows],
        "db_side": [o.side for o in rows],
    }


def _first_seen(orders: Iterable[DbOrder]) -> int | None:
    ts = [
        o.created_ms if o.created_ms is not None else o.filled_ms
        for o in orders
        if o.created_ms is not None or o.filled_ms is not None
    ]
    return min(ts) if ts else None


def _select(
    result: ClassifyResult,
    start: int | None,
    end: int | None,
    accounts: Iterable[str] | None,
) -> Iterable[Classified]:
    accs = None if accounts is None else frozenset(accounts)
    for c in result.classified:
        if accs is not None and c.account_id not in accs:
            continue
        if start is not None or end is not None:
            if c.t is None:
                continue
            if start is not None and c.t < start:
                continue
            if end is not None and c.t >= end:
                continue
        yield c


def _aggregate(rows: list[Classified], *, intents: int) -> ClassifyResult:
    counts = {c.value: 0 for c in sorted(OrderClass, key=lambda x: x.value)}
    subs = {s.value: 0 for s in sorted(OkSub, key=lambda x: x.value)}
    idem = {v.value: 0 for v in sorted(IdemViolation, key=lambda x: x.value)}
    marks = {k: 0 for k in sorted(MARK_KEYS)}
    examples: dict[str, list[str]] = {}
    for c in rows:
        for name in classes_of(c):
            counts[name] += 1
            ex = examples.setdefault(name, [])
            if len(ex) < EXAMPLES_MAX and (not ex or ex[-1] != c.key):
                ex.append(c.key)
        if c.cls == OrderClass.OK and c.sub is not None:
            subs[c.sub.value] += 1
        for v in c.idem:
            idem[v.value] += 1
        d = c.detail
        if d.get("path") == OkSub.AFTER_503.value:
            marks[reasons.ROLLBACK_REPLY_COMMITTED] += 1
        if d.get("after_inflight"):
            marks[reasons.REJECT_AFTER_INFLIGHT] += 1
        if d.get("body") == "missing":
            marks["body_missing"] += 1
        if d.get("intent_missing"):
            marks["intent_missing"] += 1
        if d.get("response") == "unparsed" or d.get("response_unparsed"):
            marks["response_unparsed" if d.get("db_rows") else "lost_unknown"] += 1
        if d.get("idem_uncompared"):
            marks["resend_uncompared"] += 1
    return ClassifyResult(
        classified=tuple(rows),
        counts=counts,
        subs=subs,
        examples={k: tuple(v) for k, v in sorted(examples.items())},
        marks=marks,
        idem=idem,
        intents=intents,
    )

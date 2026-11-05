"""주문 분류기(설계 5.3).

k6 기록(K6Log)과 DB 스냅샷(DbSnapshot)을 키 단위로 맞춰 본다.

- 1단계 K 상태, 2단계 DB와 무관한 시도 이상, 3단계 K × DB 행렬, 4단계 DB 전용.
- 행 하나 = (account_id, idem_key) 하나. 주 클래스는 cls, 함께 세는 클래스는
  detail["also"](클래스 값 목록)에 둔다. idem이 비어 있지 않으면 IDEMPOTENCY_VIOLATION도
  센다. 세는 규칙은 classes_of 한 곳에 있다.
- 주 클래스 규칙: DB 행이 2건 이상이면 DUP_ORDER(설계 5.3 표 주석). 아니면 행렬 칸.
  행렬 칸이 OK인데 DB 전용 이상(DUP_FILL, FILL_STATE)이 있으면 그 이상이 주 클래스다.
- 판정이 응답 필드 이름에 기대는데 이름이 미확정이면(response_fields_confirmed=False)
  MISMATCH 대신 RESPONSE_SCHEMA_UNCONFIRMED다. 보낸 본문 비교는 늘 한다.
- 응답 원문을 JSON 객체로 읽지 못하면(2KB 잘림 등, 설계 4.6) 응답 대 DB 비교를 하지 않고
  marks["response_unparsed"]로 센다. 재송신 응답을 비교하지 못하면 resend는 None이고
  marks["resend_uncompared"]로 센다. 둘 다 PASS를 막는 불확실성이다(summarize, 판정 몫, 설계 5.7 규칙 4).
- 결정성: 출력은 키 순서다. 현재 시각·난수·dict 순서에 기대지 않는다.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from . import reasons
from .model import (
    DUPLICATE_CLASSES,
    LOSS_CLASSES,
    MISMATCH_CLASSES,
    ORPHAN_CLASSES,
    USER_IMPACT_CLASSES,
    Ack,
    Attempt,
    Classified,
    DbFill,
    DbOrder,
    DbSnapshot,
    IdemViolation,
    Intent,
    IntegrityCfg,
    K6Log,
    KState,
    Lookup,
    OkSub,
    OrderClass,
    ResendOutcome,
    Result,
    dec,
)

EXAMPLES_MAX = 20  # 설계 5.8 클래스별 예시 키 상한
BODY_FIELDS = ("symbol", "side", "qty", "limit_price")  # acct는 따로 본다
DECIMAL_BODY_FIELDS = frozenset({"qty", "limit_price"})
RESPONSE_FIELDS = ("status", "reject_reason", "price", "fee")  # 의미 이름(설계 5.3)
DECIMAL_RESPONSE_FIELDS = frozenset({"price", "fee"})
FILL_FIELDS = ("account_id", "symbol", "side", "qty")  # 주문과 같아야 하는 체결 열
# 커밋 여부를 모르는 응답. 이 뒤의 200은 after_commit이다(설계 5.3 OK 세부).
COMMIT_UNKNOWN_STATUSES = frozenset({0, 500, 502, 504})
OK_SUB_VALUES = frozenset(s.value for s in OkSub)
UNRESOLVED_CLASSES = frozenset(
    {OrderClass.UNRESOLVED_COMMITTED, OrderClass.UNRESOLVED_ABSENT}
)
# 본문 불일치가 행렬 칸에 드러나지 않는 칸. 이때 BODY_MISMATCH를 함께 센다("늘 FAIL").
_BODY_MISMATCH_ALSO = frozenset(
    {OrderClass.FALSE_NOT_FOUND, OrderClass.PROTOCOL, OrderClass.INTERRUPTED}
)
_D0_CLASS = {
    KState.K_ACK: OrderClass.LOST,
    KState.K_FOUND: OrderClass.PHANTOM_ACK,
    KState.K_404: OrderClass.NOT_ENTERED,
    KState.K_UNKNOWN: OrderClass.UNRESOLVED_ABSENT,
}

# marks 키. 대문자는 사유 코드(reasons.py), 소문자는 진단 수치다.
MARK_KEYS = (
    reasons.ROLLBACK_REPLY_COMMITTED,  # after_503 경로(경고)
    reasons.TRANSPORT_REPLAY_POSSIBLE,  # conn_reuse가 꺼지지 않은 run의 FIRST_200
    "body_missing",  # intent 본문이 비어 본문 비교를 못 함
    "intent_missing",  # result는 있는데 intent가 없음(기록 손상)
    "response_unparsed",  # 응답 원문이 없거나 JSON 객체가 아님(응답 대 DB 비교 못 함)
    "resend_uncompared",  # 재송신 200을 원 응답과 비교하지 못함(원문 깨짐, 원 ACK 없음)
)
# 재송신 비교를 못 한 사유(detail["resend_note"])
RESEND_UNCOMPARED_NOTES = frozenset({"unparsed", "no_original_ack"})


@dataclass(frozen=True)
class ClassifyResult:
    """분류 결과(설계 5.3, 5.8 integrity.classes·subs·examples).

    classified: 키 순서로 정렬된 행.
    counts: OrderClass 값 → 행 수(모든 클래스, 0 포함). classes_of 규칙으로 센다.
    subs: OkSub 값 → cls가 OK인 행 수(모든 세부, 0 포함).
    examples: 행이 있는 클래스 → 키(정렬, 최대 EXAMPLES_MAX).
    marks: MARK_KEYS → 행 수. resend: ResendOutcome 값 → 행 수.
    intents: intent 줄 수.
    """

    classified: tuple[Classified, ...]
    counts: Mapping[str, int] = field(default_factory=dict, hash=False)
    subs: Mapping[str, int] = field(default_factory=dict, hash=False)
    examples: Mapping[str, tuple[str, ...]] = field(default_factory=dict, hash=False)
    marks: Mapping[str, int] = field(default_factory=dict, hash=False)
    resend: Mapping[str, int] = field(default_factory=dict, hash=False)
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
    """k6 기록과 DB를 키 단위로 분류한다(설계 5.1, 5.3).

    account_ids: 이번 run의 k6 계좌(JSONL account + 보조 nickname 계좌). ORPHAN 범위다.
      None이면 JSONL account 계좌 + DB k6 열이 참인 계좌다(설계 5.1 classify(log, db, cfg)).
    extra_account_ids: 발표자 계좌. DB 전용 검사(DUP_ORDER, DUP_FILL, FILL_STATE)만 한다.
      None이면 DB 스냅샷에서 k6 열이 거짓인 계좌다(ra − k6 = extra, 설계 5.4).
    ACCOUNT_SET_MISMATCH일 때 LOST·ORPHAN을 빼는 일은 판정(summarize, verdict) 몫이다.
    반환은 ClassifyResult다. 행 목록(설계 5.1의 list[Classified])은 .classified에 있다.
    """
    if account_ids is None:
        account_ids = log.account_ids | db.k6_account_ids
    if extra_account_ids is None:
        extra_account_ids = frozenset(
            a.id for a in db.accounts if not a.k6 and a.id not in account_ids
        )
    by_idem = _orders_by_idem(db.orders)
    fills_by_order = db.fills_by_order()
    conn_reuse_off = log.run_start is not None and log.run_start.conn_reuse is False
    consumed: set[str] = set()
    rows: list[Classified] = []

    for key in sorted(set(log.intents) | set(log.results)):
        intent = log.intents.get(key)
        result = log.results.get(key)
        orders = _match(by_idem.get(key, ()), intent, account_ids)
        consumed.update(o.id for o in orders)
        rows.append(
            _classify_sent(
                key,
                intent,
                result,
                orders,
                fills_by_order,
                cfg,
                conn_reuse_off=conn_reuse_off,
            )
        )

    leftover: dict[tuple[str, str], list[DbOrder]] = {}
    for o in db.orders:
        if o.id in consumed:
            continue
        if o.account_id in account_ids or o.account_id in extra_account_ids:
            leftover.setdefault((o.account_id, o.idem_key), []).append(o)
    for (acct, key), group in sorted(leftover.items()):
        row = _classify_db_only(
            key,
            acct,
            tuple(sorted(group, key=lambda o: o.id)),
            fills_by_order,
            orphan=acct in account_ids,
            log_problems=len(log.problems),
        )
        if row is not None:
            rows.append(row)

    rows.extend(
        _fills_without_order(db, account_ids | extra_account_ids, fills_by_order)
    )
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
    """FILLED 주문 수(설계 5.5 filled·sell_filled). extra 계좌 행은 뺀다."""
    n = 0
    for c in _select(result, start, end, accounts):
        if c.detail.get("scope") == "extra":
            continue
        pairs = zip(c.detail.get("db_status", ()), c.detail.get("db_side", ()))
        n += sum(1 for st, sd in pairs if st == "FILLED" and side in (None, sd))
    return n


def summarize(
    result: ClassifyResult,
    *,
    log_complete: bool = True,
    account_set_ok: bool = True,
) -> dict[str, Any]:
    """완료 기준 용어로 모은다(설계 5.3 "완료 기준 용어 대응").

    유실 = LOST + PHANTOM_ACK + FALSE_NOT_FOUND. 중복 = DUP_ORDER + DUP_FILL + FILL_STATE
    + IDEMPOTENCY_VIOLATION. 내용 불일치 = MISMATCH + BODY_MISMATCH. 고아 = ORPHAN.
    inconclusive: 수치를 믿을 수 없는 항목 → 사유 코드(설계 5.3 기록 완전성, 5.7 규칙 1).
    """
    c = result.counts
    m = result.marks
    r = result.resend

    def total(classes: Iterable[str]) -> int:
        return sum(c.get(str(x), 0) for x in classes)

    user_impact = total(USER_IMPACT_CLASSES)
    ratio = round(user_impact / result.intents, 6) if result.intents else None
    inconclusive: dict[str, list[str]] = {}

    def flag(metric: str, reason: str) -> None:
        inconclusive.setdefault(metric, []).append(reason)

    if not account_set_ok:
        flag("lost", reasons.ACCOUNT_SET_MISMATCH)
        flag("orphans", reasons.ACCOUNT_SET_MISMATCH)
    if not log_complete:
        flag("orphans", reasons.SENT_LOG_INCOMPLETE)
        flag("unresolved", reasons.SENT_LOG_INCOMPLETE)
    if m.get(reasons.TRANSPORT_REPLAY_POSSIBLE, 0):
        flag("duplicates", reasons.TRANSPORT_REPLAY_POSSIBLE)
    if m.get("resend_uncompared", 0):
        flag("duplicates", reasons.NOT_EVALUATED)
    if c.get(OrderClass.RESPONSE_SCHEMA_UNCONFIRMED, 0):
        flag("mismatch", reasons.RESPONSE_SCHEMA_UNCONFIRMED)
    if m.get("response_unparsed", 0):
        flag("mismatch", reasons.NOT_EVALUATED)

    warnings = []
    if m.get(reasons.ROLLBACK_REPLY_COMMITTED, 0):
        warnings.append(reasons.ROLLBACK_REPLY_COMMITTED)

    return {
        "intents": result.intents,
        "ok": c.get(OrderClass.OK, 0),
        "lost": total(LOSS_CLASSES),
        "duplicates": total(DUPLICATE_CLASSES),
        "mismatch": total(MISMATCH_CLASSES),
        "orphans": total(ORPHAN_CLASSES),
        "unresolved": total(UNRESOLVED_CLASSES),
        "not_entered": c.get(OrderClass.NOT_ENTERED, 0),
        "interrupted": c.get(OrderClass.INTERRUPTED, 0),
        "user_impact": user_impact,
        "intent_unresolved_ratio": ratio,
        "protocol": c.get(OrderClass.PROTOCOL, 0),
        "response_unconfirmed": c.get(OrderClass.RESPONSE_SCHEMA_UNCONFIRMED, 0),
        "response_unparsed": m.get("response_unparsed", 0),
        "resend_uncompared": m.get("resend_uncompared", 0),
        "recovered_after_commit": result.subs.get(OkSub.AFTER_COMMIT, 0)
        + result.subs.get(OkSub.LOOKUP, 0),
        "resend_ok": r.get(ResendOutcome.OK, 0),
        "resend_double_create": r.get(ResendOutcome.DOUBLE_CREATE, 0),
        "resend_diff": r.get(ResendOutcome.RESEND_DIFF, 0),
        "resend_unresolved": r.get(ResendOutcome.RESEND_UNRESOLVED, 0),
        "transport_replay_possible": m.get(reasons.TRANSPORT_REPLAY_POSSIBLE, 0),
        "warnings": tuple(warnings),
        "inconclusive": {k: tuple(sorted(v)) for k, v in sorted(inconclusive.items())},
    }


# ---------------------------------------------------------------- 1·2단계


def k_state(result: Result | None) -> KState:
    """1단계 클라이언트 상태(설계 5.3 표)."""
    if result is None:
        return KState.INTERRUPTED
    atts = result.atts
    if any(a.is_ack for a in atts):
        return KState.K_ACK
    if atts and atts[-1].is_4xx:
        return KState.K_4XX
    lk = result.lookup
    if lk is not None and lk.st == 200:
        return KState.K_FOUND
    if lk is not None and lk.st == 404:
        return KState.K_404
    return KState.K_UNKNOWN


def ack_path(atts: tuple[Attempt, ...]) -> str | None:
    """K_ACK의 시도 경로. OkSub 값 또는 "first_200"(첫 시도 200은 세부가 없다)."""
    idx = next((i for i, a in enumerate(atts) if a.is_ack), None)
    if idx is None:
        return None
    first = atts[idx]
    prior = [a.st for a in atts[:idx]]
    if not prior:
        return OkSub.CLEAN.value if first.st == 201 else "first_200"
    if first.st == 201:
        return OkSub.NOT_COMMITTED.value
    if any(s in COMMIT_UNKNOWN_STATUSES for s in prior):
        return OkSub.AFTER_COMMIT.value
    if 503 in prior:
        return OkSub.AFTER_503.value  # 명세상 503은 롤백이다(경고)
    return OkSub.AFTER_COMMIT.value


def _idem(
    result: Result | None, resend: ResendOutcome | None, *, conn_reuse_off: bool
) -> tuple[tuple[IdemViolation, ...], tuple[str, ...]]:
    """2단계 시도 이상(설계 5.3). (FAIL로 세는 것, INCONCLUSIVE로 낮춘 것)."""
    if result is None:
        return (), ()
    orig = result.atts
    rs = result.resend.atts if result.resend is not None else ()
    found: set[IdemViolation] = set()
    down: list[str] = []
    if sum(a.st == 201 for a in orig + rs) >= 2 or any(a.st == 201 for a in rs):
        found.add(IdemViolation.DOUBLE_CREATE)
    if orig and orig[0].st == 200:
        if conn_reuse_off:
            found.add(IdemViolation.FIRST_200)
        else:
            down.append(IdemViolation.FIRST_200.value)  # 4.4 TRANSPORT_REPLAY_POSSIBLE
    if resend == ResendOutcome.RESEND_DIFF:
        found.add(IdemViolation.RESEND_DIFF)
    if any(a.st == 409 for a in orig + rs):
        found.add(IdemViolation.SAME_BODY_409)  # k6는 늘 같은 바이트를 보낸다
    return tuple(v for v in IdemViolation if v in found), tuple(down)


def _resend(
    result: Result | None, cfg: IntegrityCfg
) -> tuple[ResendOutcome | None, dict[str, Any]]:
    """재송신 결과(설계 4.4). 의미 필드는 cfg.response_fields로 읽는다.

    RESEND_UNRESOLVED는 끝까지 2xx가 없을 때만이다. 200을 받았지만 원 응답과 비교하지 못하면
    (원문이 JSON 객체가 아님, 원 ACK 없음) None과 detail["resend_note"]를 돌려준다.
    """
    if result is None or result.resend is None:
        return None, {}
    rs = result.resend
    sts = [a.st for a in rs.atts]
    if 201 in sts:
        return ResendOutcome.DOUBLE_CREATE, {"resend_statuses": sts}
    if rs.ack is None:
        return ResendOutcome.RESEND_UNRESOLVED, {"resend_statuses": sts}
    if rs.ack.st != 200:
        return ResendOutcome.RESEND_DIFF, {"resend_ack_st": rs.ack.st}
    if result.ack is None:
        return None, {"resend_note": "no_original_ack"}
    if result.ack.raw == rs.ack.raw:
        return ResendOutcome.OK, {}
    a, b = result.ack.json(), rs.ack.json()
    if a is None or b is None:
        return None, {"resend_note": "unparsed"}
    diff = {}
    for name in RESPONSE_FIELDS:
        f = cfg.response_fields.get(name)
        if f is None:
            continue
        if not _same(name, a.get(f), b.get(f)):
            diff[name] = [_show(a.get(f)), _show(b.get(f))]
    if diff:
        return ResendOutcome.RESEND_DIFF, {"resend_diff": diff}
    return ResendOutcome.OK, {}


# ---------------------------------------------------------------- 3단계: 보낸 주문


def _classify_sent(
    key: str,
    intent: Intent | None,
    result: Result | None,
    orders: tuple[DbOrder, ...],
    fills_by_order: Mapping[str, tuple[DbFill, ...]],
    cfg: IntegrityCfg,
    *,
    conn_reuse_off: bool,
) -> Classified:
    """보낸 키 하나(intent 또는 result가 있는 키)를 1–3단계와 4단계 검사로 분류한다."""
    k = k_state(result)
    detail: dict[str, Any] = {"db_rows": len(orders)}
    detail.update(_db_facts(orders))
    if result is not None:
        detail["statuses"] = [a.st for a in result.atts]
        if result.lookup is not None:
            detail["lookup_st"] = result.lookup.st
    if intent is None:
        detail["intent_missing"] = True

    path = None
    if k == KState.K_ACK:
        path = ack_path(result.atts)
    elif k == KState.K_FOUND:
        path = OkSub.LOOKUP.value
    if path is not None:
        detail["path"] = path

    resend, rdetail = _resend(result, cfg)
    detail.update(rdetail)
    idem, down = _idem(result, resend, conn_reuse_off=conn_reuse_off)
    if down:
        detail["downgraded"] = list(down)

    # 보낸 본문 대 DB(늘 비교한다)
    body_diff: dict[str, Any] = {}
    if intent is not None and orders:
        if intent.body:
            for o in orders:
                d = _body_diff(intent, o)
                if d:
                    body_diff[o.id] = d
        else:
            detail["body"] = "missing"
    if body_diff:
        detail["body_diff"] = body_diff

    # DB 전용 이상(4단계)
    anomalies, fdetail = _fill_anomalies(orders, fills_by_order)
    if fdetail:
        detail["fill"] = fdetail

    # 응답 대 DB(행이 1건일 때만)
    resp_diff: dict[str, Any] = {}
    if len(orders) == 1 and k in (KState.K_ACK, KState.K_FOUND):
        src: Ack | Lookup | None = result.ack if k == KState.K_ACK else result.lookup
        resp = src.json() if src is not None else None
        if resp is None:
            detail["response"] = "unparsed"
        else:
            o = orders[0]
            resp_diff = _response_diff(
                resp, o, fills_by_order.get(o.id, ()), cfg, fill_ok=not anomalies
            )
            detail["response"] = "mismatch" if resp_diff else "match"
            if resp_diff:
                detail["response_diff"] = resp_diff

    # 행렬 칸
    if k == KState.INTERRUPTED:
        cell = OrderClass.INTERRUPTED
    elif k == KState.K_4XX:
        cell = OrderClass.PROTOCOL
    elif not orders:
        cell = _D0_CLASS[k]
    elif k == KState.K_404:
        cell = OrderClass.FALSE_NOT_FOUND
    elif k == KState.K_UNKNOWN:
        cell = (
            OrderClass.BODY_MISMATCH if body_diff else OrderClass.UNRESOLVED_COMMITTED
        )
    elif body_diff:
        cell = OrderClass.MISMATCH
    elif resp_diff:
        cell = (
            OrderClass.MISMATCH
            if cfg.response_fields_confirmed
            else OrderClass.RESPONSE_SCHEMA_UNCONFIRMED
        )
    else:
        cell = OrderClass.OK

    extra = list(anomalies)
    if body_diff and cell in _BODY_MISMATCH_ALSO:
        extra.append(OrderClass.BODY_MISMATCH)
    cls, also = _compose(cell, extra, dup=len(orders) >= 2)
    if also:
        detail["also"] = also

    sub = None
    if cls == OrderClass.OK and path in OK_SUB_VALUES:
        sub = OkSub(path)
    acct = (
        intent.acct if intent is not None else (orders[0].account_id if orders else "")
    )
    if intent is not None:
        t = intent.t
    else:
        t = _first_received(orders)
        if t is None and result is not None and result.atts:
            t = result.atts[0].t0
    return Classified(
        key=key,
        account_id=acct,
        k_state=k,
        cls=cls,
        sub=sub,
        idem=idem,
        resend=resend,
        t=t,
        order_ids=tuple(sorted(o.id for o in orders)),
        detail=detail,
    )


def _compose(
    cell: OrderClass | None, extra: list[OrderClass], *, dup: bool
) -> tuple[OrderClass, list[str]]:
    """주 클래스와 함께 세는 클래스를 정한다(모듈 docstring 규칙)."""
    found: list[OrderClass] = []
    if dup:
        found.append(OrderClass.DUP_ORDER)
    if cell is not None and cell != OrderClass.OK:
        found.append(cell)
    for x in extra:
        if x not in found:
            found.append(x)
    if not found:
        return OrderClass.OK, []
    return found[0], [x.value for x in found[1:]]


def _match(
    candidates: tuple[DbOrder, ...],
    intent: Intent | None,
    account_ids: frozenset[str],
) -> tuple[DbOrder, ...]:
    """키에 맞는 DB 주문. 같은 계좌 행이 먼저, 없으면 다른 계좌 행(acct 불일치)."""
    if not candidates:
        return ()
    if intent is not None:
        same = tuple(o for o in candidates if o.account_id == intent.acct)
        return same or candidates
    scoped = tuple(o for o in candidates if o.account_id in account_ids)
    return scoped or candidates


# ---------------------------------------------------------------- 4단계: DB 전용


def _classify_db_only(
    key: str,
    acct: str,
    orders: tuple[DbOrder, ...],
    fills_by_order: Mapping[str, tuple[DbFill, ...]],
    *,
    orphan: bool,
    log_problems: int,
) -> Classified | None:
    """intent 없는 DB 행. k6 계좌면 ORPHAN, extra 계좌면 이상이 있을 때만 행을 만든다."""
    anomalies, fdetail = _fill_anomalies(orders, fills_by_order)
    detail: dict[str, Any] = {"db_rows": len(orders)}
    if fdetail:
        detail["fill"] = fdetail
    if orphan:
        if log_problems:
            detail["log_problems"] = log_problems
    else:
        detail["scope"] = "extra"
    detail.update(_db_facts(orders))
    dup = len(orders) >= 2
    if not orphan and not dup and not anomalies:
        return None  # extra 계좌의 정상 주문은 분류하지 않는다
    cell = OrderClass.ORPHAN if orphan else None
    cls, also = _compose(cell, list(anomalies), dup=dup)
    if also:
        detail["also"] = also
    return Classified(
        key=key,
        account_id=acct,
        k_state=None,
        cls=cls,
        t=_first_received(orders),
        order_ids=tuple(o.id for o in orders),
        detail=detail,
    )


def _fills_without_order(
    db: DbSnapshot,
    scope: frozenset[str],
    fills_by_order: Mapping[str, tuple[DbFill, ...]],
) -> list[Classified]:
    """주문 행이 없는 체결(FK 부재 시). FILL_STATE로 센다."""
    known = {o.id for o in db.orders}
    out = []
    for order_id, fl in fills_by_order.items():
        if order_id in known:
            continue
        acct = fl[0].account_id or ""
        if acct not in scope:
            continue
        out.append(
            Classified(
                key=f"(fill-order {order_id})",
                account_id=acct,
                k_state=None,
                cls=OrderClass.FILL_STATE,
                t=fl[0].created_ms,
                order_ids=(order_id,),
                detail={"fill": ["fill_without_order"], "fills": [f.id for f in fl]},
            )
        )
    return out


def _fill_anomalies(
    orders: tuple[DbOrder, ...],
    fills_by_order: Mapping[str, tuple[DbFill, ...]],
) -> tuple[tuple[OrderClass, ...], list[str]]:
    """DUP_FILL, FILL_STATE(FILLED인데 fill≠1, REJECTED인데 fill 있음, 필드 불일치)."""
    found: set[OrderClass] = set()
    notes: list[str] = []
    for o in orders:
        fl = fills_by_order.get(o.id, ())
        if len(fl) >= 2:
            found.add(OrderClass.DUP_FILL)
            notes.append(f"{o.id}:fills={len(fl)}")
            continue
        if o.status == "FILLED" and not fl:
            found.add(OrderClass.FILL_STATE)
            notes.append(f"{o.id}:filled_without_fill")
        elif o.status != "FILLED" and fl:
            found.add(OrderClass.FILL_STATE)
            notes.append(f"{o.id}:{(o.status or 'NULL').lower()}_with_fill")
        elif fl:
            f = fl[0]
            for name in FILL_FIELDS:
                if getattr(f, name) != getattr(o, name):
                    found.add(OrderClass.FILL_STATE)
                    notes.append(f"{o.id}:fill_{name}")
    order = (OrderClass.DUP_FILL, OrderClass.FILL_STATE)
    return tuple(x for x in order if x in found), notes


# ---------------------------------------------------------------- 비교 도우미


def _body_diff(intent: Intent, order: DbOrder) -> dict[str, list[Any]]:
    """보낸 본문 대 DB(acct, symbol, side, qty, limit_price). 수량·가격은 Decimal."""
    diff: dict[str, list[Any]] = {}
    if intent.acct != order.account_id:
        diff["acct"] = [intent.acct, order.account_id]
    for name in BODY_FIELDS:
        sent = intent.body.get(name)
        got = getattr(order, name)
        if not _same(name, sent, got, decimal_fields=DECIMAL_BODY_FIELDS):
            diff[name] = [_show(sent), _show(got)]
    return diff


def _response_diff(
    resp: Mapping[str, Any],
    order: DbOrder,
    fills: tuple[DbFill, ...],
    cfg: IntegrityCfg,
    *,
    fill_ok: bool,
) -> dict[str, list[Any]]:
    """응답 대 DB(status, reject_reason, fill의 price·fee). 필드 이름은 cfg 매핑.

    price·fee는 체결 상태가 맞을 때만 본다(FILLED + fill 1건, REJECTED + fill 0건).
    어긋나면 FILL_STATE가 따로 잡는다. 응답에 키가 없으면 null로 본다.
    """
    expect: dict[str, Any] = {
        "status": order.status,
        "reject_reason": order.reject_reason,
    }
    if fill_ok and order.status == "FILLED" and len(fills) == 1:
        expect["price"] = fills[0].price
        expect["fee"] = fills[0].fee
    elif fill_ok and order.status == "REJECTED" and not fills:
        expect["price"] = None
        expect["fee"] = None
    diff: dict[str, list[Any]] = {}
    for name in RESPONSE_FIELDS:
        f = cfg.response_fields.get(name)
        if f is None or name not in expect:
            continue
        got = resp.get(f)
        if not _same(name, got, expect[name]):
            diff[name] = [_show(got), _show(expect[name])]
    return diff


def _same(
    name: str,
    a: Any,
    b: Any,
    *,
    decimal_fields: frozenset[str] = DECIMAL_RESPONSE_FIELDS,
) -> bool:
    if name in decimal_fields:
        return _as_decimal(a) == _as_decimal(b)
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    return str(a) == str(b)


def _as_decimal(v: Any) -> Any:
    """Decimal로 바꾼다. 못 바꾸면 표식 튜플(어떤 Decimal과도 다르다)."""
    try:
        return dec(v)
    except (TypeError, ValueError):
        return ("unparsable", repr(v))


def _show(v: Any) -> str | None:
    """detail용 표시값. Decimal·int도 문자열로 남긴다(JSON 직렬화와 결정성)."""
    return None if v is None else str(v)


# ---------------------------------------------------------------- 집계


def _orders_by_idem(orders: Iterable[DbOrder]) -> dict[str, tuple[DbOrder, ...]]:
    out: dict[str, list[DbOrder]] = {}
    for o in orders:
        out.setdefault(o.idem_key, []).append(o)
    return {
        k: tuple(sorted(v, key=lambda o: (o.account_id, o.id)))
        for k, v in sorted(out.items())
    }


def _db_facts(orders: Iterable[DbOrder]) -> dict[str, list[str]]:
    """주문별 상태·side(order_ids 순서). 체결 커버리지 집계에 쓴다(설계 5.5, 5.6)."""
    rows = sorted(orders, key=lambda o: o.id)
    if not rows:
        return {}
    return {
        "db_status": [o.status or "NULL" for o in rows],
        "db_side": [o.side or "NULL" for o in rows],
    }


def _first_received(orders: Iterable[DbOrder]) -> int | None:
    ts = [o.received_ms for o in orders if o.received_ms is not None]
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
    resend = {r.value: 0 for r in sorted(ResendOutcome, key=lambda x: x.value)}
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
        if c.resend is not None:
            resend[c.resend.value] += 1
        d = c.detail
        if d.get("path") == OkSub.AFTER_503.value:
            marks[reasons.ROLLBACK_REPLY_COMMITTED] += 1
        if d.get("downgraded"):
            marks[reasons.TRANSPORT_REPLAY_POSSIBLE] += 1
        if d.get("body") == "missing":
            marks["body_missing"] += 1
        if d.get("intent_missing"):
            marks["intent_missing"] += 1
        if d.get("response") == "unparsed":
            marks["response_unparsed"] += 1
        if d.get("resend_note") in RESEND_UNCOMPARED_NOTES:
            marks["resend_uncompared"] += 1
    return ClassifyResult(
        classified=tuple(rows),
        counts=counts,
        subs=subs,
        examples={k: tuple(v) for k, v in sorted(examples.items())},
        marks=marks,
        resend=resend,
        intents=intents,
    )

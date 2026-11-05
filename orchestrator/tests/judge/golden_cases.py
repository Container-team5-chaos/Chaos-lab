"""분류기 골든 케이스(설계 8 "골든(최소 22건)", 5.3).

케이스마다 k6 기록과 DB 스냅샷을 빌더로 만들고, 기대 클래스·sub·summarize 수치를 둔다.
summary에 없는 핵심 수치(CORE_METRICS)는 0이어야 한다.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from orchestrator.judge import reasons
from orchestrator.judge.model import (
    DbSnapshot,
    IdemViolation,
    IntegrityCfg,
    K6Log,
    KState,
    OkSub,
    OrderClass,
    ResendOutcome,
)

from .factories import (
    ACCT_A,
    ACCT_EXTRA,
    make_db,
    make_fill,
    make_intent,
    make_log,
    make_order,
    make_resend,
    make_result,
    response_raw,
)

RESPONSE_FIELDS = {
    "status": "status",
    "reject_reason": "reason",
    "price": "price",
    "fee": "fee",
}
CFG_CONFIRMED = IntegrityCfg(
    response_fields=RESPONSE_FIELDS, response_fields_confirmed=True
)
CFG_UNCONFIRMED = IntegrityCfg(
    response_fields=RESPONSE_FIELDS, response_fields_confirmed=False
)

# summary에 따로 적지 않으면 0이어야 하는 수치
CORE_METRICS = (
    "lost",
    "duplicates",
    "mismatch",
    "orphans",
    "unresolved",
    "not_entered",
    "interrupted",
    "protocol",
    "response_unconfirmed",
    "transport_replay_possible",
)

RETRY_FAIL = [(0, "request timeout", 5000)] * 4  # 첫 시도 + 재시도 3회 모두 전송 오류
LEDGER_BUSY = [(503, "LEDGER_BUSY", 3000)] * 4

Build = Callable[[], tuple[K6Log, DbSnapshot]]


@dataclass(frozen=True)
class Golden:
    name: str
    build: Build
    cls: OrderClass
    k_state: KState | None
    key: str = "k1"
    sub: OkSub | None = None
    idem: tuple[IdemViolation, ...] = ()
    resend: ResendOutcome | None = None
    also: tuple[OrderClass, ...] = ()
    summary: Mapping[str, Any] = field(default_factory=dict)
    marks: Mapping[str, int] = field(default_factory=dict)
    detail: Mapping[str, Any] = field(default_factory=dict)
    cfg: IntegrityCfg = CFG_CONFIRMED
    log_complete: bool = True


# ---------------------------------------------------------------- 빌더 도우미


def _filled_db(*orders):
    return make_db(orders=list(orders), fills=[make_fill(o) for o in orders])


def _one(atts=((201, None, 12),), db=None, *, conn_reuse=False, **result_kw):
    """intent k1(ACCT_A) 한 건 + result. db 기본값은 본문이 같은 FILLED 주문 1건."""
    it = make_intent("k1", ACCT_A)
    rs = make_result("k1", atts=atts, **result_kw)
    log = make_log([it], [rs], conn_reuse=conn_reuse)
    if db is None:
        db = _filled_db(make_order(ACCT_A, "k1"))
    return log, db


def _no_rows():
    return make_db(accounts=[ACCT_A])


# ---------------------------------------------------------------- 3단계 행렬 15칸


def ack_d1_match():
    return _one()


def ack_d1_mismatch():
    return _one(db=_filled_db(make_order(ACCT_A, "k1", qty="0.00020000")))


def ack_d0():
    return _one(db=_no_rows())


def found_d1_match():
    return _one(RETRY_FAIL, lookup=200)


def found_d1_mismatch():
    return _one(
        RETRY_FAIL, lookup=200, db=_filled_db(make_order(ACCT_A, "k1", side="sell"))
    )


def found_d0():
    return _one(RETRY_FAIL, lookup=200, db=_no_rows())


def k404_d1_match():
    return _one([(500, None, 40)], lookup=404)


def k404_d1_mismatch():
    db = _filled_db(make_order(ACCT_A, "k1", limit_price="99000000"))
    return _one([(500, None, 40)], lookup=404, db=db)


def k404_d0():
    return _one([(500, None, 40)], lookup=404, db=_no_rows())


def unknown_d1_match():
    return _one(RETRY_FAIL, lookup=0)


def unknown_d1_mismatch():
    db = _filled_db(make_order(ACCT_A, "k1", symbol="REPLAY:UPBIT:KRW-ETH"))
    return _one(RETRY_FAIL, lookup=0, db=db)


def unknown_d0():
    return _one(RETRY_FAIL, lookup=None, db=_no_rows())


def k4xx_d1_match():
    return _one([(422, "INVALID_QTY", 9)])


def k4xx_d1_mismatch():
    db = _filled_db(make_order(ACCT_A, "k1", qty="0.00030000"))
    return _one([(401, "UNAUTHORIZED", 9)], db=db)


def k4xx_d0():
    return _one([(422, "INVALID_QTY", 9)], db=_no_rows())


# ---------------------------------------------------------------- 2단계 시도 이상


def double_create():
    return _one(resend=[(201, None, 4)])


def first_200_reuse_off():
    return _one([(200, None, 12)], conn_reuse=False)


def first_200_reuse_on():
    return _one([(200, None, 12)], conn_reuse=True)


def first_200_reuse_unknown():
    return _one([(200, None, 12)], conn_reuse=None)


def resend_ok():
    return _one(resend=[(200, None, 4)])


def resend_diff():
    return _one(resend=make_resend([(200, None, 4)], raw=response_raw(fee=6)))


def resend_unresolved():
    return _one(resend=LEDGER_BUSY)


def resend_unparsed():
    """재송신 200의 원문이 잘려 원 응답과 비교하지 못한다(설계 4.6 raw ≤2KB)."""
    return _one(resend=make_resend([(200, None, 4)], raw='{"status":"FILLED","fe'))


def same_body_409():
    return _one([(503, "LEDGER_BUSY", 3000), (409, "IDEMPOTENCY_KEY_REUSED", 7)])


# ---------------------------------------------------------------- 4단계 DB 전용


def dup_order():
    a = make_order(ACCT_A, "k1")
    b = make_order(ACCT_A, "k1", n=1, received_ms=a.received_ms + 3000)
    return _one(db=_filled_db(a, b))


def dup_fill():
    o = make_order(ACCT_A, "k1")
    return _one(db=make_db(orders=[o], fills=[make_fill(o), make_fill(o)]))


def fill_state_filled_without_fill():
    return _one(db=make_db(orders=[make_order(ACCT_A, "k1")]))


def fill_state_rejected_with_fill():
    o = make_order(ACCT_A, "k1", status="REJECTED", reject_reason="STALE_PRICE")
    return _one(
        db=make_db(orders=[o], fills=[make_fill(o)]),
        raw=response_raw("REJECTED", "STALE_PRICE"),
    )


def fill_state_field_mismatch():
    o = make_order(ACCT_A, "k1")
    f = replace(make_fill(o), symbol="REPLAY:UPBIT:KRW-ETH")
    return _one(db=make_db(orders=[o], fills=[f]))


def _orphan(problems=()):
    it = make_intent("k1", ACCT_A)
    rs = make_result("k1")
    log = make_log([it], [rs], problems=problems)
    return log, _filled_db(make_order(ACCT_A, "k1"), make_order(ACCT_A, "k2"))


def orphan_complete():
    return _orphan()


def orphan_incomplete():
    return _orphan(problems=("line 9: JSON 파싱 실패",))


def result_without_intent():
    """intent 줄을 잃고 result만 남은 기록. DB 행은 ORPHAN이 아니다."""
    log = make_log([], [make_result("k2")], accounts=[ACCT_A])
    return log, _filled_db(make_order(ACCT_A, "k2"))


def extra_account_dup_order():
    it = make_intent("k1", ACCT_A)
    log = make_log([it], [make_result("k1")])
    a = make_order(ACCT_A, "k1")
    p1 = make_order(ACCT_EXTRA, "p1")
    p2 = make_order(ACCT_EXTRA, "p1", n=1)
    p3 = make_order(ACCT_EXTRA, "p3")  # 정상 주문은 분류하지 않는다
    db = make_db(
        orders=[a, p1, p2, p3],
        fills=[make_fill(a), make_fill(p1), make_fill(p2), make_fill(p3)],
        extra_accounts=[ACCT_EXTRA],
    )
    return log, db


# ---------------------------------------------------------------- INTERRUPTED, OK 세부, 응답


def _interrupted(db):
    log = make_log([make_intent("k1", ACCT_A)], [])
    return log, db


def interrupted_no_row():
    return _interrupted(_no_rows())


def interrupted_with_row():
    return _interrupted(_filled_db(make_order(ACCT_A, "k1")))


def after_commit():
    return _one([(0, "EOF", 800), (200, None, 15)])


def after_503():
    return _one([(503, "LEDGER_BUSY", 3000), (200, None, 15)])


def not_committed():
    return _one([(503, "LEDGER_BUSY", 3000), (201, None, 15)])


def response_diff():
    return _one(raw=response_raw(fee=999))


def response_truncated():
    return _one(raw='{"status":"FILLED","reason":null,"pri')


# ---------------------------------------------------------------- 케이스 표

S = OrderClass
K = KState
V = IdemViolation

CASES: tuple[Golden, ...] = (
    # 3단계 K × DB 행렬(설계 5.3 표 15칸)
    Golden("ack_d1_match", ack_d1_match, S.OK, K.K_ACK, sub=OkSub.CLEAN),
    Golden(
        "ack_d1_mismatch",
        ack_d1_mismatch,
        S.MISMATCH,
        K.K_ACK,
        summary={"mismatch": 1},
        detail={"path": "clean"},
    ),
    Golden("ack_d0_lost", ack_d0, S.LOST, K.K_ACK, summary={"lost": 1}),
    Golden(
        "found_d1_match",
        found_d1_match,
        S.OK,
        K.K_FOUND,
        sub=OkSub.LOOKUP,
        summary={"recovered_after_commit": 1},
    ),
    Golden(
        "found_d1_mismatch",
        found_d1_mismatch,
        S.MISMATCH,
        K.K_FOUND,
        summary={"mismatch": 1},
    ),
    Golden("found_d0_phantom", found_d0, S.PHANTOM_ACK, K.K_FOUND, summary={"lost": 1}),
    Golden(
        "k404_d1_match",
        k404_d1_match,
        S.FALSE_NOT_FOUND,
        K.K_404,
        summary={"lost": 1},
    ),
    Golden(
        "k404_d1_mismatch",
        k404_d1_mismatch,
        S.FALSE_NOT_FOUND,
        K.K_404,
        also=(S.BODY_MISMATCH,),
        summary={"lost": 1, "mismatch": 1},
    ),
    Golden(
        "k404_d0_not_entered",
        k404_d0,
        S.NOT_ENTERED,
        K.K_404,
        summary={"not_entered": 1, "user_impact": 1, "intent_unresolved_ratio": 1.0},
    ),
    Golden(
        "unknown_d1_committed",
        unknown_d1_match,
        S.UNRESOLVED_COMMITTED,
        K.K_UNKNOWN,
        summary={"unresolved": 1, "user_impact": 1},
    ),
    Golden(
        "unknown_d1_body_mismatch",
        unknown_d1_mismatch,
        S.BODY_MISMATCH,
        K.K_UNKNOWN,
        summary={"mismatch": 1},
    ),
    Golden(
        "unknown_d0_absent",
        unknown_d0,
        S.UNRESOLVED_ABSENT,
        K.K_UNKNOWN,
        summary={"unresolved": 1, "user_impact": 1},
    ),
    Golden(
        "k4xx_d1_match",
        k4xx_d1_match,
        S.PROTOCOL,
        K.K_4XX,
        summary={"protocol": 1},
    ),
    Golden(
        "k4xx_d1_mismatch",
        k4xx_d1_mismatch,
        S.PROTOCOL,
        K.K_4XX,
        also=(S.BODY_MISMATCH,),
        summary={"protocol": 1, "mismatch": 1},
    ),
    Golden("k4xx_d0", k4xx_d0, S.PROTOCOL, K.K_4XX, summary={"protocol": 1}),
    # 2단계 시도 이상
    Golden(
        "double_create",
        double_create,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        idem=(V.DOUBLE_CREATE,),
        resend=ResendOutcome.DOUBLE_CREATE,
        summary={"duplicates": 1, "resend_double_create": 1},
    ),
    Golden(
        "first_200_conn_reuse_off_fail",
        first_200_reuse_off,
        S.OK,
        K.K_ACK,
        idem=(V.FIRST_200,),
        summary={"duplicates": 1, "inconclusive": {}},
        detail={"path": "first_200"},
    ),
    Golden(
        "first_200_conn_reuse_on_inconclusive",
        first_200_reuse_on,
        S.OK,
        K.K_ACK,
        summary={
            "transport_replay_possible": 1,
            "inconclusive": {"duplicates": (reasons.TRANSPORT_REPLAY_POSSIBLE,)},
        },
        marks={reasons.TRANSPORT_REPLAY_POSSIBLE: 1},
        detail={"downgraded": ["FIRST_200"]},
    ),
    Golden(
        "first_200_conn_reuse_unknown_inconclusive",
        first_200_reuse_unknown,
        S.OK,
        K.K_ACK,
        summary={
            "transport_replay_possible": 1,
            "inconclusive": {"duplicates": (reasons.TRANSPORT_REPLAY_POSSIBLE,)},
        },
        marks={reasons.TRANSPORT_REPLAY_POSSIBLE: 1},
    ),
    Golden(
        "resend_ok",
        resend_ok,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        resend=ResendOutcome.OK,
        summary={"resend_ok": 1},
    ),
    Golden(
        "resend_diff",
        resend_diff,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        idem=(V.RESEND_DIFF,),
        resend=ResendOutcome.RESEND_DIFF,
        summary={"duplicates": 1, "resend_diff": 1},
        detail={"resend_diff": {"fee": ["5", "6"]}},
    ),
    Golden(
        "resend_unresolved",
        resend_unresolved,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        resend=ResendOutcome.RESEND_UNRESOLVED,
        summary={"resend_unresolved": 1},
    ),
    Golden(
        # 200을 받았으니 RESEND_UNRESOLVED(끝까지 2xx 없음)가 아니다. 비교 못 함은 PASS를 막는다
        "resend_unparsed_not_compared",
        resend_unparsed,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        resend=None,
        summary={
            "resend_uncompared": 1,
            "inconclusive": {"duplicates": (reasons.NOT_EVALUATED,)},
        },
        marks={"resend_uncompared": 1},
        detail={"resend_note": "unparsed"},
    ),
    Golden(
        "same_body_409",
        same_body_409,
        S.PROTOCOL,
        K.K_4XX,
        idem=(V.SAME_BODY_409,),
        summary={"protocol": 1, "duplicates": 1},
    ),
    # 4단계 DB 전용
    Golden(
        "dup_order",
        dup_order,
        S.DUP_ORDER,
        K.K_ACK,
        summary={"duplicates": 1},
        detail={"db_rows": 2},
    ),
    Golden("dup_fill", dup_fill, S.DUP_FILL, K.K_ACK, summary={"duplicates": 1}),
    Golden(
        "fill_state_filled_without_fill",
        fill_state_filled_without_fill,
        S.FILL_STATE,
        K.K_ACK,
        summary={"duplicates": 1},
    ),
    Golden(
        "fill_state_rejected_with_fill",
        fill_state_rejected_with_fill,
        S.FILL_STATE,
        K.K_ACK,
        summary={"duplicates": 1},
    ),
    Golden(
        "fill_state_field_mismatch",
        fill_state_field_mismatch,
        S.FILL_STATE,
        K.K_ACK,
        summary={"duplicates": 1},
    ),
    Golden(
        "orphan_complete_log",
        orphan_complete,
        S.ORPHAN,
        None,
        key="k2",
        summary={"orphans": 1, "ok": 1, "inconclusive": {}},
    ),
    Golden(
        "orphan_incomplete_log",
        orphan_incomplete,
        S.ORPHAN,
        None,
        key="k2",
        log_complete=False,
        summary={
            "orphans": 1,
            "inconclusive": {
                "orphans": (reasons.SENT_LOG_INCOMPLETE,),
                "unresolved": (reasons.SENT_LOG_INCOMPLETE,),
            },
        },
        detail={"log_problems": 1},
    ),
    Golden(
        "result_without_intent_not_orphan",
        result_without_intent,
        S.OK,
        K.K_ACK,
        key="k2",
        sub=OkSub.CLEAN,
        summary={"intents": 0, "ok": 1},
        marks={"intent_missing": 1},
        detail={"intent_missing": True},
    ),
    Golden(
        "extra_account_dup_order",
        extra_account_dup_order,
        S.DUP_ORDER,
        None,
        key="p1",
        summary={"duplicates": 1, "ok": 1},
        detail={"scope": "extra"},
    ),
    # INTERRUPTED
    Golden(
        "interrupted_no_row",
        interrupted_no_row,
        S.INTERRUPTED,
        K.INTERRUPTED,
        summary={"interrupted": 1, "user_impact": 1},
        detail={"db_rows": 0},
    ),
    Golden(
        "interrupted_with_row",
        interrupted_with_row,
        S.INTERRUPTED,
        K.INTERRUPTED,
        summary={"interrupted": 1, "user_impact": 1},
        detail={"db_rows": 1},
    ),
    # OK 세부
    Golden(
        "after_commit",
        after_commit,
        S.OK,
        K.K_ACK,
        sub=OkSub.AFTER_COMMIT,
        summary={"recovered_after_commit": 1},
    ),
    Golden(
        "after_503_warning",
        after_503,
        S.OK,
        K.K_ACK,
        sub=OkSub.AFTER_503,
        summary={
            "recovered_after_commit": 0,
            "warnings": (reasons.ROLLBACK_REPLY_COMMITTED,),
        },
        marks={reasons.ROLLBACK_REPLY_COMMITTED: 1},
    ),
    Golden(
        "not_committed",
        not_committed,
        S.OK,
        K.K_ACK,
        sub=OkSub.NOT_COMMITTED,
    ),
    # 응답 대 DB
    Golden(
        "response_schema_unconfirmed",
        response_diff,
        S.RESPONSE_SCHEMA_UNCONFIRMED,
        K.K_ACK,
        cfg=CFG_UNCONFIRMED,
        summary={
            "response_unconfirmed": 1,
            "inconclusive": {"mismatch": (reasons.RESPONSE_SCHEMA_UNCONFIRMED,)},
        },
        detail={"response": "mismatch", "response_diff": {"fee": ["999", "5"]}},
    ),
    Golden(
        "response_mismatch_confirmed",
        response_diff,
        S.MISMATCH,
        K.K_ACK,
        cfg=CFG_CONFIRMED,
        summary={"mismatch": 1},
    ),
    Golden(
        "response_match_unconfirmed_is_ok",
        ack_d1_match,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        cfg=CFG_UNCONFIRMED,
        detail={"response": "match"},
    ),
    Golden(
        "response_unparsed_not_compared",
        response_truncated,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        summary={
            "response_unparsed": 1,
            "inconclusive": {"mismatch": (reasons.NOT_EVALUATED,)},
        },
        marks={"response_unparsed": 1},
        detail={"response": "unparsed"},
    ),
)

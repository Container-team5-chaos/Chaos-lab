"""분류기 골든 케이스(계약 명세 3–5, 10 "골든 최소 22건", 설계 5.3).

케이스마다 k6 기록(JSONL v2)과 DB 스냅샷을 빌더로 만들고, 기대 클래스·K·sub·idem·summarize 수치를 둔다.
summary에 적지 않은 핵심 수치(CORE_METRICS)는 0이어야 한다.

응답 원문은 실제 주문 서비스가 보내는 바이트 그대로 만든다(workloads/order, 커밋 6e8f38b).
- 200 본문 = app/schemas.py order_to_dict(row, replayed). 필드 순서는 ORDER_TO_DICT_FIELDS다.
  Decimal은 지수 없는 문자열(DB 열 자릿수), filled_at은 datetime.isoformat()(UTC, +00:00)이다.
- 거절 본문 = app/main.py rejected_body(reason, client_order_id). 400은 client_order_id가 null이다.
- 직렬화 = starlette JSONResponse.render(json.dumps, separators=(",", ":"), ensure_ascii=False).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    iso_us,
)

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_EXTRA,
    FILLED_US,
    att,
    default_body,
    make_db,
    make_intent,
    make_log,
    make_order,
    make_result,
    order_id_for,
    response_body,
    run_key,
)

CFG = IntegrityCfg()  # 기본 매핑(DEFAULT_RESPONSE_FIELDS), 확정
CFG_UNCONFIRMED = IntegrityCfg(response_fields_confirmed=False)

K1 = run_key(1)  # k6-r-test-1-1
K2 = run_key(2)
ID1 = order_id_for(K1)
P1 = "presenter-1"  # 발표자 주문 키(run 키 형식이 아니다)

# schemas.order_to_dict 반환 dict의 키 순서
ORDER_TO_DICT_FIELDS = (
    "order_id",
    "client_order_id",
    "user_id",
    "market",
    "side",
    "volume",
    "status",
    "fill_price",
    "amount",
    "filled_at",
    "replayed",
)
# main.rejected_body 반환 dict의 키 순서
REJECTED_BODY_FIELDS = ("status", "reason", "client_order_id")

# summary에 따로 적지 않으면 0이어야 하는 수치
CORE_METRICS = (
    "lost",
    "reject_committed",
    "duplicates",
    "mismatch",
    "orphans",
    "unresolved",
    "interrupted",
    "user_impact",
    "protocol",
    "response_unconfirmed",
    "response_unparsed",
    "lost_unknown",
    "resend_uncompared",
    "body_missing",
    "recovered_after_commit",
    "rejected",
    "idem_double_create",
    "idem_first_replayed",
    "idem_resend_diff",
    "idem_409_same_body",
)

Build = Callable[[], tuple[K6Log, DbSnapshot]]


@dataclass(frozen=True)
class Golden:
    name: str
    build: Build
    cls: OrderClass
    k_state: KState | None
    key: str = K1
    sub: OkSub | None = None
    idem: tuple[IdemViolation, ...] = ()
    also: tuple[OrderClass, ...] = ()
    summary: Mapping[str, Any] = field(default_factory=dict)
    marks: Mapping[str, int] = field(default_factory=dict)
    detail: Mapping[str, Any] = field(default_factory=dict)
    cfg: IntegrityCfg = CFG
    log_complete: bool = True
    extra: tuple[str, ...] = ()  # extra_account_ids


# ---------------------------------------------------------------- 서비스 응답 원문


def service_json(obj: Mapping[str, Any], order: Sequence[str] = ()) -> str:
    """서비스 응답 바이트. order의 키를 먼저(그 순서로), 나머지는 원래 순서로 둔다."""
    d = {k: obj[k] for k in order if k in obj}
    d.update((k, v) for k, v in obj.items() if k not in d)
    return json.dumps(d, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def ack(
    rp: bool | None = False,
    *,
    key: str = K1,
    acct: str = ACCT_A,
    body: Mapping[str, Any] | None = None,
    drop: Sequence[str] = (),
    **resp: Any,
) -> dict[str, Any]:
    """200 시도 명세. 원문 = order_to_dict 바이트, rp = 본문 replayed.

    resp: response_body 인자(fill_price, amount, order_id 등). drop: 원문에서 뺄 필드.
    rp=None은 k6가 replayed를 기록하지 못한 것이다(원문 replayed는 False로 둔다. drop으로 뺀다).
    """
    b = response_body(key, acct, body=body, replayed=bool(rp), **resp)
    for f in drop:
        b.pop(f, None)
    return att(200, rp=rp, raw=service_json(b, ORDER_TO_DICT_FIELDS))


def rej(st: int, reason: str, *, key: str = K1, ms: float = 8) -> dict[str, Any]:
    """거절 시도 명세. 원문 = rejected_body(reason, client_order_id) 바이트."""
    cid = None if st == 400 else key
    raw = service_json(
        {"status": "rejected", "reason": reason, "client_order_id": cid},
        REJECTED_BODY_FIELDS,
    )
    return att(st, reason, ms, raw=raw)


def not_ready(key: str = K1) -> dict[str, Any]:
    """503 not_ready(종료 중 또는 DB·Redis 연결 오류, 결과 불명)."""
    return rej(503, "not_ready", key=key)


TIMEOUT = att(0, "1050", 5000)  # k6 전송 오류(error_code 1050 = request timeout)
GW502 = att(502, None, 30)  # 인그레스가 낸 502(본문 없음)
GW504 = att(504, None, 30)
BARE503 = att(503, None, 3)  # reason을 못 읽은 503(결과 불명)
UNKNOWN_ALL = [TIMEOUT] * 4  # 첫 전송 + 재전송 3회 모두 전송 오류


def iso_at(us: int, *, hours: int = 0, timespec: str = "microseconds") -> str:
    """epoch µs → ISO 8601(UTC+hours). 같은 시각의 다른 표기를 만든다."""
    t = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=us)
    return t.astimezone(timezone(timedelta(hours=hours))).isoformat(timespec=timespec)


def truncated_ack(key: str = K1) -> dict[str, Any]:
    """200인데 원문이 잘려 JSON으로 읽지 못한 시도(rp도 기록하지 못함)."""
    raw = service_json(response_body(key), ORDER_TO_DICT_FIELDS)[:48]
    return att(200, rp=None, raw=raw)


# ---------------------------------------------------------------- 빌더 도우미


def _one(
    atts: Sequence[Any] | None = None,
    db: DbSnapshot | None = None,
    *,
    intent: Any = None,
) -> tuple[K6Log, DbSnapshot]:
    """intent K1(ACCT_A) 한 건 + result. db 기본값은 본문이 같은 filled 주문 1건."""
    it = intent if intent is not None else make_intent(K1, ACCT_A)
    log = make_log([it], [make_result(K1, list(atts or [ack()]))])
    if db is None:
        db = make_db(orders=[make_order(ACCT_A, K1)])
    return log, db


def _row(**kw: Any) -> DbSnapshot:
    return make_db(orders=[make_order(ACCT_A, K1, **kw)])


def _no_rows() -> DbSnapshot:
    return make_db(accounts=[ACCT_A])


BODY_V2 = default_body(K1, ACCT_A, volume="0.00020000")  # 보낸 것과 다른 수량의 본문


# ---------------------------------------------------------------- 행렬 12칸(계약 명세 4)


def ack_d1_match():
    return _one()


def ack_d1_mismatch():
    return _one([ack(amount="9999.0000")])


def ack_d0():
    return _one(db=_no_rows())


def reject_d1_match():
    return _one([rej(422, "insufficient_balance")])


def reject_d1_mismatch():
    return _one([rej(503, "stale_price")], db=_row(volume="0.00020000"))


def reject_d0():
    return _one([rej(422, "insufficient_balance")], db=_no_rows())


def unknown_d1_match():
    return _one(UNKNOWN_ALL)


def unknown_d1_mismatch():
    return _one([TIMEOUT, GW502, GW504, BARE503], db=_row(side="ask"))


def unknown_d0():
    return _one(UNKNOWN_ALL, db=_no_rows())


def _interrupted(db):
    return make_log([make_intent(K1, ACCT_A)], []), db


def interrupted_d1_match():
    return _interrupted(_row())


def interrupted_d1_mismatch():
    return _interrupted(_row(volume="0.00020000"))


def interrupted_d0():
    return _interrupted(_no_rows())


# ---------------------------------------------------------------- OK 세부


def after_commit():
    return _one([TIMEOUT, ack(True)])


def after_commit_mixed():
    return _one([not_ready(), TIMEOUT, ack(True)])


def not_committed():
    return _one([TIMEOUT, ack(False)])


def after_503():
    return _one([not_ready(), ack(True)])


def stale_price_retry_filled():
    """서비스 테스트 test_orders.py 111행: stale_price 거절은 저장 안 됨 → 재시도가 새로 체결된다."""
    return _one([rej(503, "stale_price"), ack(False)])


def not_ready_retry_filled():
    """pod-kill: 종료 중 503 not_ready 뒤 새 Pod가 받아 체결한다."""
    return _one([not_ready(), ack(False)])


# ---------------------------------------------------------------- 본문·멱등 위반


def ack_body_mismatch():
    """서비스는 저장한 행을 돌려준다. 응답 = DB지만 보낸 수량과 다르다."""
    return _one([ack(body=BODY_V2)], db=_row(body=BODY_V2))


def double_create():
    return _one([ack(False), ack(False, order_id=order_id_for(K1, 1))])


def first_replayed():
    return _one([ack(True)])


def first_replayed_after_reject():
    """확정 거절은 그 시도를 저장하지 않고 앞 시도도 없다. 그 뒤 첫 ACK가 replayed=true면 키 충돌이다."""
    return _one([rej(503, "stale_price"), ack(True)])


def resend_diff():
    return _one([ack(False), ack(True, amount="10001.0000")])


def same_body_409():
    return _one([TIMEOUT, rej(409, "idempotency_conflict")])


# ---------------------------------------------------------------- DB 전용


def dup_order():
    a = make_order(ACCT_A, K1)
    b = make_order(ACCT_A, K1, n=1)
    return _one(db=make_db(orders=[a, b]))


def fill_state_null_amount():
    return _one([ack(amount=None)], db=_row(amount=None))


def fill_state_status():
    return _one([ack(status="pending")], db=_row(status="pending"))


def _orphan(problems=()):
    log = make_log(
        [make_intent(K1, ACCT_A)], [make_result(K1, [ack()])], problems=problems
    )
    return log, make_db(orders=[make_order(ACCT_A, K1), make_order(ACCT_A, K2)])


def orphan_complete():
    return _orphan()


def orphan_incomplete():
    return _orphan(problems=("line 9: JSON 파싱 실패",))


def result_without_intent():
    """intent 줄을 잃고 result만 남은 기록. DB 행은 ORPHAN이 아니다."""
    log = make_log([], [make_result(K2, [ack(key=K2)])])
    return log, make_db(orders=[make_order(ACCT_A, K2)])


def extra_account_dup_order():
    log = make_log([make_intent(K1, ACCT_A)], [make_result(K1, [ack()])])
    orders = [
        make_order(ACCT_A, K1),
        make_order(ACCT_EXTRA, P1),
        make_order(ACCT_EXTRA, P1, n=1),
        make_order(ACCT_EXTRA, "presenter-3"),  # 정상 주문은 분류하지 않는다
    ]
    return log, make_db(orders=orders, extra_accounts=[ACCT_EXTRA])


# ---------------------------------------------------------------- PROTOCOL, 원문 못 읽음


def protocol_status_201():
    raw = service_json(response_body(K1), ORDER_TO_DICT_FIELDS)
    return _one([att(201, None, 12, raw=raw)])


def protocol_intent_acct():
    """intent.acct ≠ body.user_id. 서비스는 본문 user_id(ACCT_B)로 저장한다."""
    it = make_intent(K1, ACCT_A, body=default_body(K1, ACCT_B))
    db = make_db(orders=[make_order(ACCT_B, K1)])
    return _one([ack(acct=ACCT_B)], db=db, intent=it)


def response_unparsed_committed():
    return _one([truncated_ack()])


def response_unparsed_absent():
    return _one([truncated_ack()], db=_no_rows())


def rp_unknown():
    """원문에 replayed가 없고 k6도 rp를 기록하지 못했다. 멱등 검사를 다 하지 못한다."""
    return _one([ack(None, drop=("replayed",))])


# ---------------------------------------------------------------- 응답 대 DB 정규화(계약 명세 5)


def response_notation_normalized():
    """표기만 다르고 값이 같은 응답(문자열 id, 자릿수, 지수, 다른 시간대)은 일치다."""
    b = response_body(K1)
    b.update(
        order_id=str(ID1),
        volume="0.0001",
        fill_price="1E+8",
        amount="10000",
        filled_at=iso_at(FILLED_US, hours=9),
    )
    return _one([att(200, rp=False, raw=service_json(b, ORDER_TO_DICT_FIELDS))])


# 통일본 형식(밀리초, Z). FILLED_US는 ms 아래 자리(123µs)가 있어 내림해야 DB와 맞는다
FILLED_AT_MS = iso_at(FILLED_US, timespec="milliseconds").replace("+00:00", "Z")
FILLED_AT_MS_OFF = iso_at(FILLED_US + 1000, timespec="milliseconds")  # 1ms 다른 값
FILLED_AT_UNREADABLE = "06/11/2026 09:00"


def response_filled_at_ms_only():
    """filled_at을 ms까지만 돌려주면(통일본 형식) DB 값도 ms까지 내림해 비교한다(계약 명세 11)."""
    return _one([ack(filled_at=FILLED_AT_MS)])


def response_filled_at_ms_differs():
    """ms 응답이라도 그 자릿수 안에서 DB와 다르면 불일치다."""
    return _one([ack(filled_at=FILLED_AT_MS_OFF)])


def response_filled_at_unreadable():
    """filled_at 형식을 못 읽으면 불일치가 아니라 원문 못 읽음과 같은 불확실성이다(계약 명세 11)."""
    return _one([ack(filled_at=FILLED_AT_UNREADABLE)])


# ---------------------------------------------------------------- 같은 키 재전송 경합(app/orders.py)


def reject_after_inflight_commit():
    """결과 불명 뒤 재전송이 422를 받았는데 DB에 행이 있다(검증된 지적 #1).

    서비스는 find_order를 잠금 밖에서 부른다. 첫 전송(타임아웃)이 accounts 잠금을 쥔 동안 재전송은
    행을 못 찾고 잠금을 기다린다. 첫 전송이 전량 매도를 커밋한 뒤 재전송은 바뀐 보유로 422
    insufficient_volume을 받는다. 클라이언트는 거절로 안내받았는데 돈이 움직였으므로
    REJECT_COMMITTED(lost 기준 FAIL)로 두고, detail.after_inflight와 경고 REJECT_AFTER_INFLIGHT로
    원인을 표시한다(계약 명세 4).
    """
    body = default_body(K1, ACCT_A, side="ask")
    return _one(
        [TIMEOUT, rej(422, "insufficient_volume")],
        db=make_db(orders=[make_order(ACCT_A, K1, body=body)]),
        intent=make_intent(K1, ACCT_A, body=body),
    )


# ---------------------------------------------------------------- 케이스 표

S = OrderClass
K = KState
V = IdemViolation
NE = (reasons.NOT_EVALUATED,)

CASES: tuple[Golden, ...] = (
    # 행렬(계약 명세 4) K × D
    Golden(
        "ack_d1_match",
        ack_d1_match,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        detail={"path": "clean", "response": "match"},
    ),
    Golden(
        "ack_d1_mismatch",
        ack_d1_mismatch,
        S.MISMATCH,
        K.K_ACK,
        summary={"mismatch": 1},
        detail={
            "path": "clean",
            "response": "mismatch",
            "response_diff": {"amount": ["9999.0000", "10000.0000"]},
        },
    ),
    Golden("ack_d0_lost", ack_d0, S.LOST, K.K_ACK, summary={"lost": 1}),
    Golden(
        "reject_d1_match",
        reject_d1_match,
        S.REJECT_COMMITTED,
        K.K_REJECT,
        summary={"lost": 1, "reject_committed": 1},
        detail={"statuses": [422], "ecs": ["insufficient_balance"]},
    ),
    Golden(
        "reject_d1_body_mismatch",
        reject_d1_mismatch,
        S.REJECT_COMMITTED,
        K.K_REJECT,
        also=(S.BODY_MISMATCH,),
        summary={"lost": 1, "reject_committed": 1, "mismatch": 1},
        detail={"body_diff": {str(ID1): {"volume": ["0.00010000", "0.00020000"]}}},
    ),
    Golden(
        "reject_d0_rejected",
        reject_d0,
        S.OK,
        K.K_REJECT,
        sub=OkSub.REJECTED,
        summary={"rejected": 1},
        detail={"path": "rejected", "db_rows": 0},
    ),
    Golden(
        "unknown_d1_committed",
        unknown_d1_match,
        S.UNRESOLVED_COMMITTED,
        K.K_UNKNOWN,
        summary={"unresolved": 1, "user_impact": 1, "intent_unresolved_ratio": 1.0},
    ),
    Golden(
        "unknown_d1_body_mismatch",
        unknown_d1_mismatch,
        S.UNRESOLVED_COMMITTED,
        K.K_UNKNOWN,
        also=(S.BODY_MISMATCH,),
        summary={"unresolved": 1, "user_impact": 1, "mismatch": 1},
        detail={
            "statuses": [0, 502, 504, 503],
            "body_diff": {str(ID1): {"side": ["bid", "ask"]}},
        },
    ),
    Golden(
        "unknown_d0_absent",
        unknown_d0,
        S.UNRESOLVED_ABSENT,
        K.K_UNKNOWN,
        summary={"unresolved": 1, "user_impact": 1},
    ),
    Golden(
        "interrupted_d1_committed",
        interrupted_d1_match,
        S.INTERRUPTED,
        K.INTERRUPTED,
        summary={"interrupted": 1, "user_impact": 1},
        detail={"committed": True, "db_rows": 1},
    ),
    Golden(
        "interrupted_d1_body_mismatch",
        interrupted_d1_mismatch,
        S.INTERRUPTED,
        K.INTERRUPTED,
        also=(S.BODY_MISMATCH,),
        summary={"interrupted": 1, "user_impact": 1, "mismatch": 1},
        detail={"committed": True},
    ),
    Golden(
        "interrupted_d0",
        interrupted_d0,
        S.INTERRUPTED,
        K.INTERRUPTED,
        summary={"interrupted": 1, "user_impact": 1},
        detail={"committed": False, "db_rows": 0},
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
        # not_ready와 다른 결과 불명이 함께 있으면 after_commit이다(설계 5.3)
        "after_commit_mixed_not_ready",
        after_commit_mixed,
        S.OK,
        K.K_ACK,
        sub=OkSub.AFTER_COMMIT,
        summary={"recovered_after_commit": 1},
    ),
    Golden(
        "not_committed",
        not_committed,
        S.OK,
        K.K_ACK,
        sub=OkSub.NOT_COMMITTED,
    ),
    Golden(
        "after_503_warning",
        after_503,
        S.OK,
        K.K_ACK,
        sub=OkSub.AFTER_503,
        summary={"warnings": (reasons.ROLLBACK_REPLY_COMMITTED,)},
        marks={reasons.ROLLBACK_REPLY_COMMITTED: 1},
    ),
    Golden(
        "stale_price_retry_filled",
        stale_price_retry_filled,
        S.OK,
        K.K_ACK,
        sub=OkSub.NOT_COMMITTED,
        detail={"ecs": ["stale_price", None]},
    ),
    Golden(
        "not_ready_retry_filled",
        not_ready_retry_filled,
        S.OK,
        K.K_ACK,
        sub=OkSub.NOT_COMMITTED,
    ),
    # 본문 불일치
    Golden(
        "ack_body_mismatch",
        ack_body_mismatch,
        S.BODY_MISMATCH,
        K.K_ACK,
        summary={"mismatch": 1},
        detail={
            "response": "match",
            "body_diff": {str(ID1): {"volume": ["0.00010000", "0.00020000"]}},
        },
    ),
    # 멱등 위반(계약 명세 4)
    Golden(
        "double_create",
        double_create,
        S.IDEMPOTENCY_VIOLATION,
        K.K_ACK,
        idem=(V.DOUBLE_CREATE, V.RESEND_DIFF),
        summary={"duplicates": 1, "idem_double_create": 1, "idem_resend_diff": 1},
        detail={
            "created_ids": [str(ID1), str(order_id_for(K1, 1))],
            "resend_diff": {"2": {"order_id": [str(ID1), str(order_id_for(K1, 1))]}},
        },
    ),
    Golden(
        "first_replayed",
        first_replayed,
        S.IDEMPOTENCY_VIOLATION,
        K.K_ACK,
        idem=(V.FIRST_REPLAYED,),
        summary={"duplicates": 1, "idem_first_replayed": 1},
        detail={"path": "first_replayed"},
    ),
    Golden(
        "first_replayed_after_reject",
        first_replayed_after_reject,
        S.IDEMPOTENCY_VIOLATION,
        K.K_ACK,
        idem=(V.FIRST_REPLAYED,),
        summary={"duplicates": 1, "idem_first_replayed": 1},
        detail={"path": "first_replayed"},
    ),
    Golden(
        "resend_diff",
        resend_diff,
        S.IDEMPOTENCY_VIOLATION,
        K.K_ACK,
        idem=(V.RESEND_DIFF,),
        summary={"duplicates": 1, "idem_resend_diff": 1},
        detail={
            "path": "clean",
            "resend_diff": {"2": {"amount": ["10000.0000", "10001.0000"]}},
        },
    ),
    Golden(
        # 결과 불명 뒤 409: 같은 본문인데 충돌이라 했고(서비스 결함), DB에는 커밋됐다
        "same_body_409",
        same_body_409,
        S.REJECT_COMMITTED,
        K.K_REJECT,
        idem=(V.SAME_BODY_409,),
        summary={
            "lost": 1,
            "reject_committed": 1,
            "duplicates": 1,
            "idem_409_same_body": 1,
        },
        detail={"conflict_atts": [2], "statuses": [0, 409]},
    ),
    # DB 전용(계약 명세 4)
    Golden(
        "dup_order",
        dup_order,
        S.DUP_ORDER,
        K.K_ACK,
        summary={"duplicates": 1},
        detail={"db_rows": 2, "response": "match"},
    ),
    Golden(
        "fill_state_null_amount",
        fill_state_null_amount,
        S.FILL_STATE,
        K.K_ACK,
        summary={"duplicates": 1},
        detail={"fill": [f"{ID1}:filled_null_amount"], "response": "match"},
    ),
    Golden(
        "fill_state_status_not_filled",
        fill_state_status,
        S.FILL_STATE,
        K.K_ACK,
        summary={"duplicates": 1},
        detail={"fill": [f"{ID1}:status_pending"], "db_status": ["pending"]},
    ),
    Golden(
        "orphan_complete_log",
        orphan_complete,
        S.ORPHAN,
        None,
        key=K2,
        summary={"orphans": 1, "ok": 1},
        detail={"db_rows": 1, "log_problems": None, "foreign_account": None},
    ),
    Golden(
        "orphan_incomplete_log",
        orphan_incomplete,
        S.ORPHAN,
        None,
        key=K2,
        log_complete=False,
        summary={
            "orphans": 1,
            "ok": 1,
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
        key=K2,
        sub=OkSub.CLEAN,
        summary={
            "intents": 0,
            "ok": 1,
            "body_missing": 1,
            "intent_unresolved_ratio": None,
            "inconclusive": {"mismatch": NE},
        },
        marks={"intent_missing": 1, "body_missing": 1},
        detail={"intent_missing": True, "body": "missing"},
    ),
    Golden(
        "extra_account_dup_order",
        extra_account_dup_order,
        S.DUP_ORDER,
        None,
        key=P1,
        extra=(ACCT_EXTRA,),
        summary={"duplicates": 1, "ok": 1},
        detail={"scope": "extra", "db_rows": 2},
    ),
    # PROTOCOL(계약 명세 3 "그 밖", 계약 명세 2 intent 규칙)
    Golden(
        "protocol_status_201",
        protocol_status_201,
        S.PROTOCOL,
        K.K_UNKNOWN,
        also=(S.UNRESOLVED_COMMITTED,),
        summary={"protocol": 1, "unresolved": 1, "user_impact": 1},
        detail={"protocol": ["att1:st201"]},
    ),
    Golden(
        "protocol_intent_acct",
        protocol_intent_acct,
        S.PROTOCOL,
        K.K_ACK,
        summary={"protocol": 1},
        detail={"protocol": ["intent_acct"], "response": "match"},
    ),
    # 응답 원문 못 읽음(PASS만 막는 불확실성)
    Golden(
        "response_unparsed_committed",
        response_unparsed_committed,
        S.PROTOCOL,
        K.K_UNKNOWN,
        also=(S.UNRESOLVED_COMMITTED,),
        summary={
            "protocol": 1,
            "unresolved": 1,
            "user_impact": 1,
            "response_unparsed": 1,
            "resend_uncompared": 1,
            "inconclusive": {"duplicates": NE, "mismatch": NE},
        },
        marks={"response_unparsed": 1, "resend_uncompared": 1},
        detail={
            "response": "unparsed",
            "protocol": ["att1:st200"],
            "idem_uncompared": ["att1:unparsed"],
        },
    ),
    Golden(
        "response_unparsed_absent",
        response_unparsed_absent,
        S.PROTOCOL,
        K.K_UNKNOWN,
        also=(S.UNRESOLVED_ABSENT,),
        summary={
            "protocol": 1,
            "unresolved": 1,
            "user_impact": 1,
            "lost_unknown": 1,
            "resend_uncompared": 1,
            "inconclusive": {"duplicates": NE, "lost": NE},
        },
        marks={"lost_unknown": 1, "resend_uncompared": 1},
        detail={"response": "unparsed"},
    ),
    Golden(
        "rp_unknown_not_compared",
        rp_unknown,
        S.OK,
        K.K_ACK,
        summary={"resend_uncompared": 1, "inconclusive": {"duplicates": NE}},
        marks={"resend_uncompared": 1},
        detail={"path": None, "idem_uncompared": ["att1:rp_unknown"]},
    ),
    # 응답 대 DB(계약 명세 5)
    Golden(
        "response_schema_unconfirmed",
        ack_d1_mismatch,
        S.RESPONSE_SCHEMA_UNCONFIRMED,
        K.K_ACK,
        cfg=CFG_UNCONFIRMED,
        summary={
            "response_unconfirmed": 1,
            "inconclusive": {"mismatch": (reasons.RESPONSE_SCHEMA_UNCONFIRMED,)},
        },
        detail={"response_diff": {"amount": ["9999.0000", "10000.0000"]}},
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
        "response_notation_normalized",
        response_notation_normalized,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        detail={"response": "match"},
    ),
    # filled_at 자릿수(계약 명세 11)
    Golden(
        "response_filled_at_ms_only",
        response_filled_at_ms_only,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        detail={"response": "match", "response_diff": None},
    ),
    Golden(
        "response_filled_at_ms_differs",
        response_filled_at_ms_differs,
        S.MISMATCH,
        K.K_ACK,
        summary={"mismatch": 1},
        detail={
            "response": "mismatch",
            "response_diff": {"filled_at": [FILLED_AT_MS_OFF, iso_us(FILLED_US)]},
        },
    ),
    Golden(
        "response_filled_at_unreadable",
        response_filled_at_unreadable,
        S.OK,
        K.K_ACK,
        sub=OkSub.CLEAN,
        summary={"response_unparsed": 1, "inconclusive": {"mismatch": NE}},
        marks={"response_unparsed": 1},
        detail={
            "response": "unparsed",
            "response_unparsed": ["filled_at"],
            "response_diff": None,
        },
    ),
    # 같은 키 재전송 경합(검증된 지적. 분류 정책 미정, 지금 규칙을 고정한다)
    Golden(
        "reject_after_inflight_commit",
        reject_after_inflight_commit,
        S.REJECT_COMMITTED,
        K.K_REJECT,
        summary={
            "lost": 1,
            "reject_committed": 1,
            "warnings": (reasons.REJECT_AFTER_INFLIGHT,),
        },
        marks={reasons.REJECT_AFTER_INFLIGHT: 1},
        detail={
            "statuses": [0, 422],
            "ecs": ["1050", "insufficient_volume"],
            "db_side": ["ask"],
            "body_diff": None,
            "after_inflight": True,
        },
    ),
)

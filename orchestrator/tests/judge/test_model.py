"""공용 모델·직렬화·테스트 빌더(설계 5.1, 5.8, 계약 정렬 명세 1–8)."""

import dataclasses
import json
from decimal import Decimal

import pytest

import orchestrator.judge as J
from orchestrator.judge import reasons
from orchestrator.judge.errors import EXIT_BY_VERDICT, InputError, JudgeError
from orchestrator.judge.model import (
    DEFAULT_RESPONSE_FIELDS,
    DUPLICATE_CLASSES,
    INTENT_BODY_FIELDS,
    INVARIANT_IDS,
    LOSS_CLASSES,
    ORDER_COLUMNS,
    USER_IMPACT_CLASSES,
    Attempt,
    AttemptKind,
    CheckResult,
    Classified,
    DbTarget,
    Fingerprint,
    IdemViolation,
    IntegrityCfg,
    Intent,
    KState,
    OkSub,
    OrderClass,
    Outcome,
    RejectReason,
    Result,
    SafetyCfg,
    Verdict,
    attempt_kind,
    canonical_json,
    compute_fingerprint,
    dec,
    derive_accounts,
    is_run_key,
    iso_us,
    order_value,
    run_key_prefix,
    ts_digits,
    ts_us,
)

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_EXTRA,
    AMOUNT,
    FILLED_US,
    INITIAL_KRW,
    MARKET,
    PRICE,
    RUN_ID,
    VOLUME,
    att,
    default_schema,
    fill_amount,
    make_account,
    make_db,
    make_intent,
    make_log,
    make_order,
    make_position,
    make_result,
    make_tick,
    reject_raw,
    replay_positions,
    response_raw,
    run_key,
)


def test_public_api():
    assert J.__version__ == "0.1.0"
    assert J.EXIT_PASS == 0 and J.EXIT_FAIL == 10
    assert J.EXIT_INCONCLUSIVE == 11 and J.EXIT_INPUT == 12
    assert callable(J.judge) and callable(J.evaluate) and callable(J.collect)
    assert callable(J.safety_cfg) and J.SafetyCfg is SafetyCfg
    assert J.DbTarget is DbTarget


def test_exit_codes():
    assert InputError("x").exit_code == 12
    assert JudgeError("x").exit_code == 1
    assert issubclass(InputError, JudgeError)
    assert EXIT_BY_VERDICT[Outcome.PASS] == 0
    assert EXIT_BY_VERDICT[Outcome.FAIL] == 10
    assert EXIT_BY_VERDICT[Outcome.INCONCLUSIVE] == 11


# ---------------------------------------------------------------- Enum·클래스 묶음(명세 3, 4)


def test_kstate_values():
    assert [k.value for k in KState] == [
        "K_ACK",
        "K_REJECT",
        "K_UNKNOWN",
        "INTERRUPTED",
    ]


def test_order_class_values():
    names = {c.value for c in OrderClass}
    assert "REJECT_COMMITTED" in names
    for gone in ("PHANTOM_ACK", "FALSE_NOT_FOUND", "NOT_ENTERED", "DUP_FILL"):
        assert gone not in names
    assert f"{OrderClass.LOST}" == "LOST"


def test_ok_sub_and_idem_values():
    assert [s.value for s in OkSub] == [
        "clean",
        "after_commit",
        "not_committed",
        "after_503",
        "rejected",
    ]
    assert [v.value for v in IdemViolation] == [
        "DOUBLE_CREATE",
        "FIRST_REPLAYED",
        "RESEND_DIFF",
        "409_SAME_BODY",
    ]


def test_reject_reasons_match_service():
    """workloads/order main.py·orders.py의 reason 문자열 그대로다."""
    assert {r.value for r in RejectReason} == {
        "invalid_request",
        "user_not_found",
        "idempotency_conflict",
        "insufficient_balance",
        "insufficient_volume",
        "price_unavailable",
        "stale_price",
        "not_ready",
    }


def test_class_groups():
    assert LOSS_CLASSES == {OrderClass.LOST, OrderClass.REJECT_COMMITTED}
    assert DUPLICATE_CLASSES == {
        OrderClass.DUP_ORDER,
        OrderClass.FILL_STATE,
        OrderClass.IDEMPOTENCY_VIOLATION,
    }
    assert OrderClass.INTERRUPTED in USER_IMPACT_CLASSES


def test_removed_types_are_gone():
    import orchestrator.judge.model as M

    for name in ("Lookup", "Resend", "ResendOutcome", "Ack", "SetupDone", "DbFill"):
        assert not hasattr(M, name), name


def test_reason_constants():
    for name in ("INTEGRITY_VIOLATION", "SAFETY_VIOLATION", "SCHEMA_DRIFT"):
        assert getattr(reasons, name) == name
        assert name in reasons.ALL_REASONS
    for gone in (
        "TRANSPORT_REPLAY_POSSIBLE",
        "APP_MAX_AGE_DIFFERS",
        "NO_SPEED_TIMELINE",
        "CONSTRAINT_MISSING",
    ):
        assert not hasattr(reasons, gone)
        assert gone not in reasons.ALL_REASONS
    assert set(reasons.GUARD_REASON.values()) <= reasons.ALL_REASONS
    assert reasons.WARNINGS <= reasons.ALL_REASONS
    assert reasons.SNAPSHOT_LARGE in reasons.WARNINGS


# ---------------------------------------------------------------- 값 변환


def test_dec_rejects_float():
    assert dec("0.00010000") == Decimal("0.0001")
    assert dec(None) is None and dec("") is None
    assert dec(5) == Decimal(5)
    with pytest.raises(TypeError):
        dec(0.1)
    with pytest.raises(ValueError):
        dec("abc")


def test_ts_us_reads_psql_and_service_forms():
    us = 1_762_300_000_005_123
    assert ts_us("2025-11-04 23:46:40.005123+00") == us  # psql(SET TIME ZONE 'UTC')
    assert ts_us(iso_us(us)) == us  # 서비스 isoformat
    assert iso_us(us) == "2025-11-04T23:46:40.005123+00:00"
    assert ts_us("2025-11-05T08:46:40.005123+09:00") == us  # 같은 instant
    assert ts_us("2025-11-04 23:46:40+00") == us - 5123
    assert ts_us(None) is None and ts_us("") is None
    for bad in ("2025-11-04 23:46:40", "yesterday", 5):
        with pytest.raises(ValueError):
            ts_us(bad)


@pytest.mark.parametrize(
    "text, digits",
    [
        ("2025-11-04T23:46:40.005Z", 3),  # 통일본(명세 11)
        ("2025-11-04T23:46:40.005123+00:00", 6),  # 서비스 isoformat
        ("2025-11-04T23:46:40+00:00", 0),  # isoformat은 µs가 0이면 소수부를 뺀다
        ("2025-11-04 23:46:40.5+00", 1),  # psql
        ("2025-11-04T23:46:40,25Z", 2),
        ("20251104T234640.123Z", 3),  # 기본 형식
        ("2025-11-04T23:46:40.005123456Z", 9),
        ("2025-11-04T23:46Z", None),  # 초가 없다
        ("06/11/2026 09:00", None),
        ("yesterday", None),
    ],
)
def test_ts_digits(text, digits):
    assert ts_digits(text) == digits


def test_ts_us_reads_unified_ms_z_form():
    """명세 11 통일본 형식(밀리초, Z)도 읽는다."""
    assert ts_us("2025-11-04T23:46:40.005Z") == 1_762_300_000_005_000


def test_order_value_normalizes_both_sides():
    assert order_value("volume", "0.0001") == order_value(
        "volume", Decimal("0.00010000")
    )
    assert order_value("amount", "10000.0000") == Decimal("10000")
    assert (
        order_value("id", 7) == order_value("id", "7") == order_value("id", Decimal(7))
    )
    assert order_value("filled_at", iso_us(FILLED_US)) == FILLED_US
    assert order_value("filled_at", FILLED_US) == FILLED_US
    assert order_value("side", "bid") == "bid"
    assert order_value("status", None) is None
    with pytest.raises(TypeError):
        order_value("id", True)
    with pytest.raises(ValueError):
        order_value("id", "7a")
    with pytest.raises(TypeError):
        order_value("fill_price", 1.5)


def test_run_key_exact_match():
    assert run_key_prefix("r1117-crash") == "k6-r1117-crash-"
    assert is_run_key("k6-r1117-crash-3-17", "r1117-crash")
    # 접두어는 같지만 다른 run의 키다
    assert not is_run_key("k6-r1117-crash-2-3-17", "r1117-crash")
    assert is_run_key("k6-r1117-crash-2-3-17", "r1117-crash-2")
    for bad in ("k6-r1117-crash-3", "k6-r1117-crash-a-1", "x-k6-r1117-crash-1-1", None):
        assert not is_run_key(bad, "r1117-crash")


# ---------------------------------------------------------------- 시도 성격(명세 3)

_ACK_RAW = response_raw("k1")


@pytest.mark.parametrize(
    "st, ec, raw, kind",
    [
        (200, None, _ACK_RAW, AttemptKind.ACK),
        (200, None, None, AttemptKind.PROTOCOL),  # 원문 없음: order_id 확인 못 함
        (200, None, '{"order_id": 1', AttemptKind.PROTOCOL),  # 잘림
        (200, None, '{"status": "filled"}', AttemptKind.PROTOCOL),  # 주문이 아님
        (200, None, '{"order_id": null}', AttemptKind.PROTOCOL),
        (200, None, "[1]", AttemptKind.PROTOCOL),
        (201, None, _ACK_RAW, AttemptKind.PROTOCOL),
        (400, "invalid_request", None, AttemptKind.REJECT),
        (404, "user_not_found", None, AttemptKind.REJECT),
        (409, "idempotency_conflict", None, AttemptKind.REJECT),
        (422, "insufficient_balance", None, AttemptKind.REJECT),
        (422, "insufficient_volume", None, AttemptKind.REJECT),
        (503, "price_unavailable", None, AttemptKind.REJECT),
        (503, "stale_price", None, AttemptKind.REJECT),
        (503, "not_ready", None, AttemptKind.UNKNOWN),
        (503, None, None, AttemptKind.UNKNOWN),
        (503, "LEDGER_BUSY", None, AttemptKind.UNKNOWN),
        (0, "1050", None, AttemptKind.UNKNOWN),
        (500, None, "Internal Server Error", AttemptKind.UNKNOWN),
        (502, None, None, AttemptKind.UNKNOWN),
        (504, None, None, AttemptKind.UNKNOWN),
        (599, None, None, AttemptKind.UNKNOWN),
        (405, None, None, AttemptKind.PROTOCOL),
        (429, None, None, AttemptKind.PROTOCOL),
        (302, None, None, AttemptKind.PROTOCOL),
    ],
)
def test_attempt_kind(st, ec, raw, kind):
    a = Attempt(n=1, t0=1, t1=2, st=st, ec=ec, ms=1.0, rp=None, raw=raw)
    assert attempt_kind(st, ec, raw) is kind
    assert a.kind is kind
    flags = (a.is_ack, a.is_reject, a.is_unknown, a.is_protocol)
    assert sum(flags) == 1


def test_attempt_json_and_not_ready():
    a = Attempt(n=1, t0=1, t1=2, st=200, ec=None, ms=1.0, rp=False, raw=_ACK_RAW)
    j = a.json()
    assert j["fill_price"] == "100000000.00000000" and j["replayed"] is False
    assert Attempt(1, 1, 2, 200, None, 1.0, raw='{"x": 1.5}').json()["x"] == Decimal(
        "1.5"
    )
    assert Attempt(1, 1, 2, 503, "not_ready", 1.0).is_not_ready
    assert not Attempt(1, 1, 2, 503, "stale_price", 1.0).is_not_ready


def test_result_helpers():
    k = run_key(1)
    rs = make_result(k, [(0, "1050", 5000), (503, "not_ready"), 200, 200])
    assert [a.n for a in rs.atts] == [1, 2, 3, 4]
    assert [a.st for a in rs.atts] == [0, 503, 200, 200]
    assert rs.atts[1].t0 > rs.atts[0].t1
    assert rs.first_ack is rs.atts[2]
    assert rs.acks == rs.atts[2:]
    assert rs.last is rs.atts[3]
    assert not rs.has_protocol
    # rp 기본값: 첫 200은 False, 뒤 200은 True. 본문 replayed도 같다
    assert [a.rp for a in rs.atts] == [None, None, False, True]
    assert rs.atts[3].json()["replayed"] is True
    assert Result(key="k", atts=()).first_ack is None
    assert Result(key="k", atts=()).last is None


def test_intent_consistency():
    k = run_key(1)
    it = make_intent(k)
    assert set(it.body) == set(INTENT_BODY_FIELDS)
    assert it.consistent and it.vu == 1 and it.acct == ACCT_A
    assert not make_intent(k, body={**it.body, "client_order_id": "other"}).consistent
    assert not make_intent(k, body={**it.body, "user_id": ACCT_B}).consistent


# ---------------------------------------------------------------- 계좌(명세 2, 8)


def test_derive_accounts_by_vu():
    its = [
        make_intent(run_key(1, vu=2), ACCT_B, vu=2),
        make_intent(run_key(1, vu=1), ACCT_A, vu=1),
        make_intent(run_key(2, vu=2), ACCT_B, vu=2),
        make_intent(run_key(1, vu=3), ACCT_B, vu=3),  # 한 user를 두 VU가 쓴다
    ]
    accs = derive_accounts(its)
    assert [(a.idx, a.acct, a.vus) for a in accs] == [
        (0, ACCT_A, (1,)),
        (1, ACCT_B, (2, 3)),
    ]
    log = make_log(its, [])
    assert log.accounts == accs
    assert log.account_ids == {ACCT_A, ACCT_B}
    assert log.account_idx(ACCT_B) == 1 and log.account_idx("nobody") is None
    assert log.account_by_idx(0) == ACCT_A and log.account_by_idx(2) is None
    assert log.accounts_by_vu == {1: (ACCT_A,), 2: (ACCT_B,), 3: (ACCT_B,)}
    assert log.run_start.vus == 3 and log.run_start.v == 2


def test_k6log_vu_with_two_users():
    log = make_log(
        [make_intent(run_key(1), ACCT_A, vu=5), make_intent(run_key(2), ACCT_B, vu=5)],
        [],
    )
    assert log.accounts_by_vu == {5: (ACCT_A, ACCT_B)}


# ---------------------------------------------------------------- 설정 타입


def test_integrity_cfg_defaults():
    cfg = IntegrityCfg()
    assert dict(cfg.response_fields) == DEFAULT_RESPONSE_FIELDS
    assert set(DEFAULT_RESPONSE_FIELDS.values()) <= set(ORDER_COLUMNS)
    assert DEFAULT_RESPONSE_FIELDS["order_id"] == "id"
    assert cfg.response_fields_confirmed is True
    assert cfg.initial_krw == Decimal("10000000") and cfg.stale_ms == 5000
    assert IntegrityCfg().response_fields is not cfg.response_fields  # 공유하지 않는다
    assert hash(cfg) == hash(IntegrityCfg())


def test_safety_cfg():
    s = SafetyCfg()
    assert s.markets == ("KRW-BTC",)
    assert s.market_ok("KRW-BTC") and not s.market_ok("KRW-ETH")
    assert not s.market_ok(None)
    assert s.user_max == 1000
    assert s.user_ok("user-0001") and s.user_ok("user-1000")
    for bad in ("user-1", "user-00001", "user-0001\n", "admin", None):
        assert not s.user_ok(bad)
    assert SafetyCfg(user_pattern="user-[0-9]+").user_ok("user-7")  # 전체 일치


@pytest.mark.parametrize(
    "user_id, ok",
    [
        ("user-0000", False),  # 명세 11: 범위 1..user_max(seed user-0001..1000)
        ("user-0001", True),
        ("user-1000", True),
        ("user-1001", False),
        ("user-9999", False),
    ],
)
def test_safety_user_range(user_id, ok):
    assert SafetyCfg().user_ok(user_id) is ok


def test_safety_user_max_configurable():
    s = SafetyCfg(user_max=2)
    assert s.user_ok("user-0002") and not s.user_ok("user-0003")
    # 끝이 숫자가 아니면 범위를 확인할 수 없어 위반이다
    assert not SafetyCfg(user_pattern="presenter-[a-z]+").user_ok("presenter-a")


def test_db_target_defaults():
    t = DbTarget()
    assert (t.pod, t.container, t.user, t.database) == (
        "postgres-0",
        None,
        "exchange",
        "exchange",
    )


def test_invariant_ids():
    assert INVARIANT_IDS == (
        "cash",
        "qty",
        "avg",
        "nonneg",
        "arith",
        "constraints",
        "fresh",
    )


# ---------------------------------------------------------------- verdict·직렬화


def _verdict(**kw):
    base = dict(
        run_id="r-test",
        experiment="demo",
        verdict=Outcome.PASS,
        checks=(
            CheckResult(id="lost", kind="integrity", outcome=Outcome.PASS, value=0),
            CheckResult(
                id="p95",
                kind="client",
                outcome=Outcome.NA,
                required=False,
                detail={"p95": Decimal("12.5")},
            ),
        ),
        integrity={"classes": {OrderClass.OK: 3}},
        inputs={"criteria_sha256": "a" * 64},
    )
    base.update(kw)
    return Verdict(**base)


def test_verdict_to_dict_and_digest():
    v = _verdict()
    d = v.to_dict()
    assert list(d)[0] == "schema" and d["schema"] == "papertrade.verdict/v1"
    assert d["verdict"] == "PASS"
    assert d["checks"][1]["outcome"] == "N/A"
    assert d["checks"][1]["detail"] == {"p95": "12.5"}
    assert d["integrity"] == {"classes": {"OK": 3}}
    json.dumps(d)  # 직렬화 가능
    dg = v.compute_digest()
    # generated_at, run_info는 digest에 들어가지 않는다(5.2 6단계)
    v2 = _verdict(generated_at=1_762_300_000_000, run_info={"elapsed_s": 12})
    assert v2.compute_digest() == dg
    assert _verdict(verdict=Outcome.FAIL).compute_digest() != dg
    assert v.with_digest().verdict_digest == dg
    assert v.with_digest().with_digest().verdict_digest == dg


def test_canonical_json_sorted_and_strict():
    assert (
        canonical_json({"b": 1, "a": frozenset({"y", "x"})}) == '{"a":["x","y"],"b":1}'
    )
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def test_snapshot_serializable():
    db = make_db(orders=[make_order(ACCT_A, run_key(1))], ticks=[make_tick()])
    d = json.loads(canonical_json(db))
    assert d["fingerprint1"]["krw_sum"] == "9990000.0000"
    assert d["orders"][0]["volume"] == "0.00010000"


def test_dataclasses_frozen_and_hashable():
    c = Classified(
        key="k",
        account_id=ACCT_A,
        k_state=KState.K_ACK,
        cls=OrderClass.OK,
        sub=OkSub.CLEAN,
        order_ids=(1,),
        detail={"x": [1]},
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        c.cls = OrderClass.LOST
    assert len({c, c}) == 1
    it = make_intent("k1", ACCT_A)
    assert hash(it) == hash(make_intent("k1", ACCT_A))
    log = make_log([it], [make_result("k1")])
    assert hash(log) == hash(log)
    assert log.accounts[0].acct == ACCT_A  # cached_property가 frozen에서도 된다
    assert hash(make_db()) == hash(make_db())


# ---------------------------------------------------------------- factories


def test_factories_default_ok_case_is_consistent():
    k = run_key(1)
    it = make_intent(k)
    rs = make_result(k)
    log = make_log([it], [rs])
    o = make_order(ACCT_A, k)
    db = make_db(orders=[o])

    assert isinstance(it, Intent) and it.key == k == it.body["client_order_id"]
    assert rs.atts[0].is_ack and rs.atts[0].rp is False
    # 원 응답 = DB 행(명세 5 매핑, order_value 비교)
    resp = rs.first_ack.json()
    for field, col in DEFAULT_RESPONSE_FIELDS.items():
        assert order_value(col, resp[field]) == o.value(col), field
    # 보낸 본문 = DB 행
    for f in ("user_id", "market", "side", "volume"):
        assert order_value(f, it.body[f]) == o.value(f), f
    assert o.amount == Decimal(AMOUNT) == fill_amount(PRICE, VOLUME)
    assert o.filled_us == FILLED_US and o.created_us == FILLED_US
    assert o.filled_ms == FILLED_US // 1000
    # 계좌·보유 = 서비스 공식
    assert db.account(ACCT_A).krw == Decimal(INITIAL_KRW) - Decimal(AMOUNT)
    p = db.position(ACCT_A, MARKET)
    assert p.volume == Decimal(VOLUME) and p.avg_price == Decimal(PRICE)
    assert db.quiescent
    assert db.fingerprint1 == Fingerprint(
        orders=1,
        max_order_id=o.id,
        krw_sum=Decimal("9990000"),
        positions=1,
        volume_sum=Decimal(VOLUME),
    )
    assert db.orders_by_key() == {k: (o,)}
    assert db.orders_by_user() == {ACCT_A: (o,)}
    assert db.run_orders(RUN_ID) == (o,)
    assert db.run_orders("other") == ()
    assert db.invariant_violations("cash") == ()
    assert db.has_section("schema") and db.schema == default_schema()
    assert log.kind_counts == {"run_start": 1, "intent": 1, "result": 1}


def test_replay_positions_service_formula():
    """매수 평균가는 quantize(1e-8) HALF_EVEN, 매도는 평균가 유지(0이 돼도)."""
    o1 = make_order(ACCT_A, run_key(1), fill_price="100000000", volume="0.001")
    o2 = make_order(ACCT_A, run_key(2), fill_price="100000001", volume="0.002")
    o3 = make_order(
        ACCT_A, run_key(3), side="ask", fill_price="90000000", volume="0.003"
    )
    st = replay_positions([o3, o2, o1])  # id 순으로 재생한다
    vol, avg = st[(ACCT_A, MARKET)]
    assert vol == Decimal(0)
    # (0.001·1e8 + 0.002·100000001) / 0.003 = 100000000.666… → .66666667
    assert avg == Decimal("100000000.66666667")
    db = make_db(orders=[o1, o2, o3])
    expected = (
        Decimal(INITIAL_KRW)
        - fill_amount("100000000", "0.001")
        - fill_amount("100000001", "0.002")
        + fill_amount("90000000", "0.003")
    )
    assert db.account(ACCT_A).krw == expected
    assert db.position(ACCT_A, MARKET).volume == 0


def test_attempt_spec_defaults():
    k = run_key(7)
    rs = make_result(
        k,
        [
            (503, "stale_price"),
            400,
            (422, "insufficient_volume"),
            (409,),
            att(200, rp=True, fill_price="1"),
            att(200, raw=None),
        ],
    )
    a = rs.atts
    assert a[0].raw == reject_raw("stale_price", k) and a[0].is_reject
    assert a[1].ec == "invalid_request" and a[1].raw == reject_raw(
        "invalid_request", None
    )
    assert a[2].ec == "insufficient_volume"
    assert a[3].ec == "idempotency_conflict"
    assert a[4].rp is True and a[4].json()["fill_price"] == "1.00000000"
    assert a[5].raw is None and a[5].is_protocol
    assert make_result(k, [(503,)]).atts[0].is_unknown  # ec 없는 503은 불명


def test_make_db_overrides_and_dups():
    k = run_key(1)
    o1 = make_order(ACCT_A, k)
    o2 = make_order(ACCT_A, k, n=1)
    ox = make_order(ACCT_EXTRA, "presenter-1")
    db = make_db(
        accounts=[make_account(ACCT_A, krw="1"), ACCT_EXTRA],
        orders=[o2, ox, o1],
        positions=[make_position(ACCT_A, volume="5")],
        fingerprint2=None,
        sections={"accounts", "orders"},
    )
    assert o1.id != o2.id
    assert [o.id for o in db.orders] == sorted(o.id for o in (o1, o2, ox))
    assert len(db.orders_by_key()[k]) == 2
    assert db.account(ACCT_A).krw == Decimal("1")
    assert db.account(ACCT_EXTRA).krw == Decimal(INITIAL_KRW) - Decimal(AMOUNT)
    assert db.position(ACCT_A, MARKET).volume == Decimal("5")
    assert not db.quiescent and db.fingerprint1 is None
    assert db.invariant_violations("cash") is None
    assert db.run_orders(RUN_ID) == (o1, o2)


def test_compute_fingerprint_empty():
    fp = compute_fingerprint((), (), ())
    assert fp == Fingerprint(
        orders=0, max_order_id=None, krw_sum=None, positions=0, volume_sum=None
    )
    assert Fingerprint(1, 2, Decimal("10.0"), 1, None) == Fingerprint(
        1, 2, Decimal("10.00"), 1, None
    )

"""공용 모델·직렬화·테스트 빌더(설계 4.6, 5.1, 5.8)."""

import dataclasses
import json
from decimal import Decimal

import pytest

import orchestrator.judge as J
from orchestrator.judge import reasons
from orchestrator.judge.errors import EXIT_BY_VERDICT, InputError, JudgeError
from orchestrator.judge.model import (
    Ack,
    CheckResult,
    Classified,
    IntegrityCfg,
    KState,
    OkSub,
    OrderClass,
    Outcome,
    Verdict,
    canonical_json,
    dec,
)

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_EXTRA,
    PRICE,
    QTY,
    make_db,
    make_fill,
    make_intent,
    make_log,
    make_order,
    make_result,
)


def test_public_api():
    assert J.__version__ == "0.1.0"
    assert J.EXIT_PASS == 0 and J.EXIT_FAIL == 10
    assert J.EXIT_INCONCLUSIVE == 11 and J.EXIT_INPUT == 12
    assert callable(J.judge) and callable(J.evaluate) and callable(J.collect)


def test_exit_codes():
    assert InputError("x").exit_code == 12
    assert JudgeError("x").exit_code == 1
    assert issubclass(InputError, JudgeError)
    assert EXIT_BY_VERDICT[Outcome.PASS] == 0
    assert EXIT_BY_VERDICT[Outcome.FAIL] == 10
    assert EXIT_BY_VERDICT[Outcome.INCONCLUSIVE] == 11


def test_enum_values_are_contract():
    assert Outcome.NA == "N/A"
    assert OkSub.AFTER_COMMIT == "after_commit"
    assert OrderClass.FALSE_NOT_FOUND == "FALSE_NOT_FOUND"
    assert str(KState.K_404) == "K_404"
    assert f"{OrderClass.LOST}" == "LOST"


def test_reason_constants_unique_and_complete():
    for name in (
        "INTEGRITY_VIOLATION",
        "SAFETY_VIOLATION",
        "TRANSPORT_REPLAY_POSSIBLE",
    ):
        assert getattr(reasons, name) == name
        assert name in reasons.ALL_REASONS
    assert set(reasons.GUARD_REASON.values()) <= reasons.ALL_REASONS


def test_dec_rejects_float():
    assert dec("0.00010000") == Decimal("0.0001")
    assert dec(None) is None and dec("") is None
    assert dec(5) == Decimal(5)
    with pytest.raises(TypeError):
        dec(0.1)
    with pytest.raises(ValueError):
        dec("abc")


def test_ack_json_uses_decimal():
    a = Ack(st=201, raw='{"status":"FILLED","price":100000000.5,"fee":5}')
    j = a.json()
    assert j["price"] == Decimal("100000000.5") and isinstance(j["price"], Decimal)
    assert Ack(st=201, raw='{"status": "FIL').json() is None  # 2KB에서 잘림
    assert Ack(st=201, raw="[1]").json() is None
    assert Ack(st=201, raw=None).json() is None


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


def test_dataclasses_frozen_and_hashable():
    c = Classified(
        key="k",
        account_id=ACCT_A,
        k_state=KState.K_ACK,
        cls=OrderClass.OK,
        sub=OkSub.CLEAN,
        detail={"x": [1]},
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        c.cls = OrderClass.LOST
    assert len({c, c}) == 1
    it = make_intent("k1", ACCT_A)
    assert hash(it) == hash(make_intent("k1", ACCT_A))


def test_integrity_cfg_default_mapping_matches_design():
    """IntegrityCfg() 기본 매핑은 설계 6 default.yaml과 같다. 빈 매핑이면 비교가 사라진다."""
    cfg = IntegrityCfg()
    assert dict(cfg.response_fields) == {
        "status": "status",
        "reject_reason": "reason",
        "price": "price",
        "fee": "fee",
    }
    assert cfg.response_fields_confirmed is False
    assert cfg.price_max_age_ms == 60000
    assert IntegrityCfg().response_fields is not cfg.response_fields  # 공유하지 않는다
    assert hash(cfg) == hash(IntegrityCfg())


# ---------------------------------------------------------------- factories


def test_factories_default_ok_case_is_consistent():
    it = make_intent("k1", ACCT_A)
    rs = make_result("k1", atts=[(201, None, 12)])
    log = make_log([it], [rs])
    o = make_order(ACCT_A, "k1")
    f = make_fill(o)
    db = make_db(orders=[o], fills=[f])

    assert log.run_start.conn_reuse is False
    assert log.account_ids == {ACCT_A}
    assert log.account_idx(ACCT_A) == 0
    assert log.intents["k1"].body["qty"] == QTY
    assert rs.ack.st == 201 and rs.atts[0].is_ack
    # 본문 = DB 주문
    assert o.qty == Decimal(it.body["qty"]) and o.symbol == it.body["symbol"]
    assert o.limit_price == Decimal(it.body["limit_price"])
    # 응답 = DB 체결
    resp = rs.ack.json()
    assert resp["status"] == o.status == "FILLED"
    assert Decimal(resp["price"]) == db.fills[0].price == Decimal(PRICE)
    assert resp["fee"] == db.fills[0].fee == 5
    assert db.fills[0].amount == 10_000 and db.fills[0].realized_pnl == -5
    assert db.fills[0].id == 1
    assert db.quiescent and db.fingerprint1 == (1, 1, 0)
    assert db.k6_account_ids == {ACCT_A}
    assert db.orders_by_key() == {(ACCT_A, "k1"): (o,)}
    assert db.fills_by_order() == {o.id: (db.fills[0],)}
    assert db.invariant_violations("i2") == ()


def test_factories_attempts_lookup_resend():
    rs = make_result(
        "k2",
        atts=[(0, "1050", 5000), (503, "LEDGER_BUSY"), (504,)],
        lookup=404,
    )
    assert [a.n for a in rs.atts] == [1, 2, 3]
    assert [a.st for a in rs.atts] == [0, 503, 504]
    assert rs.atts[1].t0 > rs.atts[0].t1
    assert rs.ack is None
    assert rs.lookup.st == 404 and rs.lookup.raw is None
    rs2 = make_result("k3", atts=[(201, None, 9)], resend=[(201, None, 4)])
    assert rs2.resend.ack.st == 201


def test_make_db_extra_and_dups():
    o1 = make_order(ACCT_A, "k1")
    o2 = make_order(ACCT_A, "k1", n=1)
    ox = make_order(ACCT_EXTRA, "kx")
    db = make_db(
        orders=[o1, o2, ox],
        fills=[make_fill(o1), make_fill(o1)],
        extra_accounts=[ACCT_EXTRA],
        fingerprint2=(9, 9, 9),
        sections={"accounts", "orders"},
    )
    assert o1.id != o2.id
    assert len(db.orders_by_key()[(ACCT_A, "k1")]) == 2
    assert [f.id for f in db.fills] == [1, 2]
    assert db.k6_account_ids == {ACCT_A}
    assert db.account(ACCT_EXTRA).k6 is False
    assert not db.quiescent
    assert db.invariant_violations("i2") is None


def test_make_log_accounts_order():
    log = make_log(
        [
            make_intent("k1", ACCT_B),
            make_intent("k2", ACCT_A),
            make_intent("k3", ACCT_B),
        ],
        [],
    )
    assert [a.acct for a in log.accounts] == [ACCT_B, ACCT_A]
    assert log.account_by_idx(1) == ACCT_A
    assert log.kind_counts["intent"] == 3 and log.kind_counts["result"] == 0

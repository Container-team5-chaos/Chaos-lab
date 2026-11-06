"""invariants.py: 잔고 불변식 7개와 기준 항목 묶기(계약 정렬 명세 7). DB 없이 factories로 돈다.

기본 make_db는 서비스 공식으로 krw·positions를 계산하므로 모든 불변식이 맞는다.
위반은 값을 하나씩 바꿔 심고, 그 불변식만 잡히는지 본다.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.judge import db as D
from orchestrator.judge import reasons as R
from orchestrator.judge.criteria import load_criteria
from orchestrator.judge.invariants import (
    EXPECTED_COLUMNS,
    EXPECTED_CONSTRAINTS,
    EXPECTED_TABLES,
    ROW_BASED,
    Scope,
    check_arith,
    check_avg,
    check_constraints,
    check_fresh,
    check_invariants,
    invariant_counts,
    item_results,
    replay_position,
    run_key_counts,
    snapshot_consistent,
)
from orchestrator.judge.model import (
    ACCOUNT_COLUMNS,
    EXAMPLES_MAX,
    INVARIANT_IDS,
    ORDER_COLUMNS,
    POSITION_COLUMNS,
    SNAPSHOT_SECTIONS,
    Fingerprint,
    IntegrityCfg,
    InvariantResult,
    Outcome,
    SchemaItem,
)

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_C,
    ACCT_EXTRA,
    FILLED_US,
    INITIAL_KRW,
    PRICE,
    RUN_ID,
    default_schema,
    make_account,
    make_db,
    make_order,
    make_position,
    make_tick,
    run_key,
    vu_of,
)

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_YAML = ROOT / "experiments" / "criteria" / "default.yaml"
CTX = SimpleNamespace(run_id=RUN_ID, extra_accounts=(ACCT_EXTRA,))
S1 = 1_000_000  # 1초(µs)


def by_id(results):
    return {r.id: r for r in results}


def flagged(results):
    """위반이 있거나 평가하지 못한 불변식 id."""
    return {r.id for r in results if r.violations != 0}


def run(db, ctx=CTX, crit=None):
    return by_id(check_invariants(db, ctx, crit))


def order(acct, seq, side="bid", volume="0.00010000", price=PRICE, **kw):
    """같은 VU의 seq 순서 = id 순서(factories.order_id_for)."""
    key = run_key(seq, vu=vu_of(acct))
    return make_order(acct, key, side=side, volume=volume, fill_price=price, **kw)


def clean_db(**kw):
    """사용자 셋 + 발표자. 매수·매도·전량 매도·다시 매수가 섞인 정상 스냅샷."""
    orders = [
        order(ACCT_A, 1, "bid", "0.00010000", "100000000"),
        order(ACCT_A, 2, "bid", "0.00030000", "101234567.12345678"),
        order(ACCT_A, 3, "ask", "0.00020000", "102000000"),
        order(ACCT_B, 1, "bid", "0.00012345", "99999999"),
        order(ACCT_B, 2, "bid", "0.00000001", "100000001"),
        order(ACCT_C, 1, "bid", "0.00050000", "100000000"),
        order(ACCT_C, 2, "ask", "0.00050000", "100500000"),  # 전량 매도. avg는 남는다
        make_order(ACCT_EXTRA, "demo-1", volume="0.00100000"),
    ]
    ticks = [make_tick(price=o.fill_price) for o in orders]
    return make_db(orders=orders, ticks=ticks, **kw)


# ---------------------------------------------------------------- 정상


def test_clean_snapshot_has_no_violations():
    res = check_invariants(clean_db(), CTX, None)
    assert [r.id for r in res] == list(INVARIANT_IDS)
    for r in res:
        assert r.violations == 0, (r.id, r.reasons, r.examples)
        assert r.source == "python" and r.examples == () and r.reasons == ()


def test_clean_snapshot_without_ctx_checks_every_order():
    res = run(clean_db(), ctx=None)
    assert flagged(res.values()) == set()
    assert res["arith"].detail["orders"] == 8 and res["arith"].detail["run_id"] is None


def test_expected_schema_covers_model_columns():
    assert EXPECTED_TABLES == ("accounts", "positions", "orders")
    assert tuple(EXPECTED_COLUMNS["accounts"]) == ACCOUNT_COLUMNS
    assert tuple(EXPECTED_COLUMNS["positions"]) == POSITION_COLUMNS
    assert tuple(EXPECTED_COLUMNS["orders"]) == ORDER_COLUMNS
    assert set(EXPECTED_CONSTRAINTS) == set(EXPECTED_TABLES)
    assert ROW_BASED == set(INVARIANT_IDS) - {"constraints"}


# ---------------------------------------------------------------- cash


def test_cash_catches_changed_krw_only():
    db = clean_db()
    accts = [
        dataclasses.replace(a, krw=a.krw + Decimal("0.0001"))
        if a.user_id == ACCT_B
        else a
        for a in db.accounts
    ]
    res = run(make_db(orders=db.orders, ticks=db.ticks, accounts=accts))
    assert flagged(res.values()) == {"cash"}
    [v] = res["cash"].examples
    assert v.subject == ACCT_B and v.detail["problem"] == "mismatch"
    assert Decimal(v.actual) - Decimal(v.expected) == Decimal("0.0001")
    assert res["cash"].detail["initial_krw"] == INITIAL_KRW


def test_cash_covers_every_account_not_only_run_accounts():
    other = make_order("user-0500", "manual-1")  # run 키가 아닌 주문
    accts = [make_account("user-0500", krw=INITIAL_KRW)]  # 매수 금액이 빠지지 않았다
    res = run(make_db(orders=[other], accounts=accts))
    assert res["cash"].violations == 1
    assert res["cash"].examples[0].subject == "user-0500"


def test_cash_uses_initial_krw_from_criteria():
    db = clean_db()
    assert (
        run(db, crit=IntegrityCfg(initial_krw=Decimal("5000")))["cash"].violations == 4
    )
    assert (
        run(db, crit=IntegrityCfg(initial_krw=Decimal(INITIAL_KRW)))["cash"].violations
        == 0
    )


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ("krw_null", "krw_null"),
        ("amount_null", "order_null"),
        ("no_account", "no_account"),
    ],
)
def test_cash_null_and_missing_rows_are_violations(change, problem):
    o = order(ACCT_A, 1)
    if change == "krw_null":
        db = make_db(orders=[o], accounts=[make_account(ACCT_A, krw=None)])
    elif change == "amount_null":
        db = make_db(orders=[dataclasses.replace(o, amount=None)])
    else:
        db = make_db(orders=[o], accounts=[ACCT_B])
    r = run(db)["cash"]
    assert r.violations >= 1
    assert problem in {v.detail["problem"] for v in r.examples}


# ---------------------------------------------------------------- qty


def test_qty_catches_changed_volume_only():
    db = clean_db()
    poss = [
        dataclasses.replace(p, volume=p.volume + Decimal("0.00000001"))
        if p.user_id == ACCT_B
        else p
        for p in db.positions
    ]
    res = run(make_db(orders=db.orders, ticks=db.ticks, positions=poss))
    assert flagged(res.values()) == {"qty"}
    [v] = res["qty"].examples
    assert v.subject == f"{ACCT_B}/KRW-BTC"
    assert (v.expected, v.actual) == ("0.00012346", "0.00012347")


def test_missing_position_row_breaks_qty_and_avg():
    db = clean_db()
    poss = [p for p in db.positions if p.user_id != ACCT_A]
    res = run(make_db(orders=db.orders, ticks=db.ticks, positions=poss))
    assert flagged(res.values()) == {"qty", "avg"}
    assert res["avg"].examples[0].detail["problem"] == "position_missing"


def test_position_without_orders():
    empty = make_position(ACCT_B, volume="0", avg_price="0")
    assert flagged(run(make_db(accounts=[ACCT_B], positions=[empty])).values()) == set()
    held = make_position(ACCT_B, volume="0.1", avg_price="0")
    res = run(make_db(accounts=[ACCT_B], positions=[held]))
    assert flagged(res.values()) == {"qty"}


# ---------------------------------------------------------------- avg


def test_avg_replay_uses_half_even_and_28_digits():
    db = clean_db()
    a = next(p for p in db.positions if p.user_id == ACCT_A)
    # (0.0001·100000000 + 0.0003·101234567.12345678) / 0.0004 = 100925925.342592585 → HALF_EVEN
    assert a.avg_price == Decimal("100925925.34259258")
    assert a.volume == Decimal("0.00020000")  # 매도 뒤 avg 유지
    b = next(p for p in db.positions if p.user_id == ACCT_B)
    # 몫 99999999.00016199578810950915는 28자리에서 끊긴 값이다
    assert b.avg_price == Decimal("99999999.00016200")
    c = next(p for p in db.positions if p.user_id == ACCT_C)
    assert c.volume == 0 and c.avg_price == Decimal(
        "100000000"
    )  # 전량 매도 뒤에도 유지


@pytest.mark.parametrize(
    ("user", "avg", "why"),
    [
        (ACCT_A, "100925925.34259259", "ROUND_HALF_UP으로 계산했다"),
        (ACCT_C, "0", "전량 매도 때 avg를 0으로 되돌렸다"),
        (ACCT_B, "99999999.00016199", "자릿수를 더 써서 내림했다"),
    ],
)
def test_avg_catches_wrong_formula_only(user, avg, why):
    db = clean_db()
    poss = [
        dataclasses.replace(p, avg_price=Decimal(avg)) if p.user_id == user else p
        for p in db.positions
    ]
    res = run(make_db(orders=db.orders, ticks=db.ticks, positions=poss))
    assert flagged(res.values()) == {"avg"}, why
    [v] = res["avg"].examples
    assert v.subject == f"{user}/KRW-BTC" and v.actual == avg


def test_replay_position_rules():
    rp = replay_position(
        [order(ACCT_A, 1, "bid", "0.5", "100"), order(ACCT_A, 2, "ask", "0.5", "999")]
    )
    assert (rp.volume, rp.avg_price, rp.orders, rp.oversold, rp.error) == (
        Decimal("0E-8"),
        Decimal("100.00000000"),
        2,
        (),
        None,
    )
    again = replay_position(
        [
            order(ACCT_A, 1, "bid", "0.5", "100"),
            order(ACCT_A, 2, "ask", "0.5", "999"),
            order(ACCT_A, 3, "bid", "0.25", "300"),
        ]
    )
    assert again.avg_price == Decimal("300")  # 보유 0에서 다시 사면 그 가격


def test_replay_position_orders_by_id_and_skips_unfilled():
    a = order(ACCT_A, 1, "bid", "1", "100")
    b = order(ACCT_A, 2, "bid", "1", "200")
    pending = order(ACCT_A, 3, "bid", "1", "900", status="pending")
    assert replay_position([b, pending, a]).avg_price == Decimal("150")


def test_replay_position_errors_and_oversell():
    sell = order(ACCT_A, 1, "ask", "1", "100")
    over = replay_position([sell])
    assert over.oversold == (sell.id,) and over.error is None
    zero = replay_position(
        [order(ACCT_A, 1, "ask", "1", "100"), order(ACCT_A, 2, "bid", "1", "100")]
    )
    assert zero.error is not None and "DivisionByZero" in zero.error
    null = replay_position([order(ACCT_A, 1, price=None)])
    assert null.error is not None
    side = replay_position([order(ACCT_A, 1, side="hold")])
    assert side.error is not None


def test_avg_replay_error_is_a_violation():
    o = dataclasses.replace(order(ACCT_A, 1), side="hold")
    db = make_db(orders=[o], positions=[make_position(ACCT_A)])
    r = check_avg(db)
    assert r.violations == 1 and r.examples[0].detail["problem"] == "replay_error"


# ---------------------------------------------------------------- nonneg


def test_nonneg_catches_consistent_overspend_only():
    # 1,000만 원 계좌가 2,000만 원어치를 샀다. cash·qty·avg는 맞고 krw만 음수다
    big = order(ACCT_A, 1, "bid", "0.20000000", "100000000")
    res = run(make_db(orders=[big], ticks=[make_tick()]), ctx=None)
    assert flagged(res.values()) == {"nonneg"}
    [v] = res["nonneg"].examples
    assert (v.subject, v.actual, v.detail["column"]) == (
        ACCT_A,
        "-10000000.0000",
        "krw",
    )


def test_nonneg_positions_and_null():
    poss = [
        make_position(ACCT_A, volume="-0.1", avg_price="1"),
        make_position(ACCT_B, volume="0", avg_price=None),
    ]
    r = run(make_db(accounts=[ACCT_A, ACCT_B], positions=poss))["nonneg"]
    got = {(v.subject, v.detail["column"], v.detail["problem"]) for v in r.examples}
    assert got == {
        (f"{ACCT_A}/KRW-BTC", "volume", "negative"),
        (f"{ACCT_B}/KRW-BTC", "avg_price", "null"),
    }


# ---------------------------------------------------------------- arith


def test_arith_catches_half_up_amount_only():
    # 99999999 × 0.00012345 = 12344.99987655 → ROUND_DOWN 12344.9998(HALF_UP이면 12344.9999)
    o = order(ACCT_B, 1, "bid", "0.00012345", "99999999", amount="12344.9999")
    res = run(make_db(orders=[o], ticks=[make_tick(price=o.fill_price)]))
    assert flagged(res.values()) == {"arith"}  # krw는 그 amount로 맞춰 두었다
    [v] = res["arith"].examples
    assert (v.expected, v.actual, v.detail["problem"]) == (
        "12344.9998",
        "12344.9999",
        "mismatch",
    )


def test_arith_scope_is_run_orders_plus_extra():
    bad = {"amount": "1.0000"}
    run_order = order(ACCT_A, 1, **bad)
    twin = make_order(ACCT_B, f"k6-{RUN_ID}-2-1-1", **bad)  # 다른 run(r-test-2)의 키
    manual = make_order(ACCT_C, "manual-1", **bad)
    extra = make_order(ACCT_EXTRA, "demo-1", **bad)
    db = make_db(orders=[run_order, twin, manual, extra])
    scoped = check_arith(db, Scope(run_id=RUN_ID, extra=frozenset({ACCT_EXTRA})))
    assert {v.subject for v in scoped.examples} == {run_order.client_order_id, "demo-1"}
    assert check_arith(db).violations == 4  # ctx 없음 = 전부


def test_arith_null_columns_are_violations():
    o = dataclasses.replace(order(ACCT_A, 1), fill_price=None)
    r = check_arith(make_db(orders=[o]), Scope(run_id=RUN_ID))
    assert r.violations == 1 and r.examples[0].detail["problem"] == "null"


# ---------------------------------------------------------------- constraints


def schema_without(pred):
    return tuple(s for s in default_schema() if not pred(s))


def test_constraints_pass_on_service_schema():
    r = check_constraints(make_db())
    assert r.violations == 0 and r.reasons == ()


@pytest.mark.parametrize(
    ("schema", "problem", "subject"),
    [
        (
            schema_without(lambda s: s.name == "orders_client_order_id_key"),
            "constraint_missing",
            "orders: UNIQUE (client_order_id)",
        ),
        (
            schema_without(lambda s: s.name == "accounts_krw_check"),
            "constraint_missing",
            "accounts: CHECK ((krw >= (0)::numeric))",
        ),
        (
            schema_without(lambda s: s.table == "positions"),
            "table_missing",
            "positions",
        ),
        (
            schema_without(lambda s: s.kind == "column" and s.name == "filled_at"),
            "column_missing",
            "orders.filled_at",
        ),
        (
            tuple(
                dataclasses.replace(s, detail="numeric(30,4) not null")
                if (s.table, s.name) == ("accounts", "krw")
                else s
                for s in default_schema()
            ),
            "column_type",
            "accounts.krw",
        ),
        (
            tuple(
                dataclasses.replace(s, detail="numeric(24,8) not null")
                if (s.table, s.name) == ("orders", "fill_price")
                else s
                for s in default_schema()
            ),
            "column_type",
            "orders.fill_price",
        ),
    ],
)
def test_constraints_drift_is_inconclusive_not_a_count(schema, problem, subject):
    r = check_constraints(make_db(schema=schema))
    assert r.violations is None and r.reasons == (R.SCHEMA_DRIFT,)
    assert (subject, problem) in {(v.subject, v.detail["problem"]) for v in r.examples}


def test_constraints_not_valid_check_is_drift_and_shows_near_definition():
    schema = tuple(
        dataclasses.replace(s, detail=s.detail + " NOT VALID")
        if s.name == "accounts_krw_check"
        else s
        for s in default_schema()
    )
    r = check_constraints(make_db(schema=schema))
    [v] = r.examples
    assert v.actual == "CHECK ((krw >= (0)::numeric)) NOT VALID"


def test_constraints_ignore_names_extras_whitespace_and_ticks():
    schema = [
        dataclasses.replace(s, name=s.name + "_renamed", detail="  " + s.detail + " ")
        if s.kind == "constraint"
        else s
        for s in default_schema()
        if s.table != "ticks"
    ]
    schema += [
        SchemaItem("column", "orders", "note", "text"),
        SchemaItem("constraint", "orders", "orders_note_check", "CHECK (true)"),
        SchemaItem("table", "audit", "audit"),
    ]
    assert check_constraints(make_db(schema=schema)).violations == 0


def test_constraints_schema_section_missing_is_db_error():
    db = make_db(sections=set(SNAPSHOT_SECTIONS) - {"schema"})
    r = check_constraints(db)
    assert r.violations is None and r.reasons == (R.DB_ERROR,)


# ---------------------------------------------------------------- fresh

STALE = 5000


def fresh_of(ticks, *, filled_us=FILLED_US, stale_ms=STALE, sections=None):
    o = order(ACCT_A, 1, filled_us=filled_us)
    db = make_db(orders=[o], ticks=ticks, sections=sections)
    return check_fresh(db, Scope(run_id=RUN_ID), stale_ms)


@pytest.mark.parametrize(
    ("offset_us", "ok"),
    [
        (-(STALE + 1000) * 1000, True),  # 창 아래 끝(포함)
        (-(STALE + 1000) * 1000 - 1, False),
        (1000 * 1000, True),  # 창 위 끝(포함)
        (1000 * 1000 + 1, False),
        (0, True),
    ],
)
def test_fresh_window_bounds(offset_us, ok):
    r = fresh_of([make_tick(received_us=FILLED_US + offset_us)])
    assert r.violations == (0 if ok else 1)


def test_fresh_price_must_match_and_stale_ms_widens_window():
    r = fresh_of([make_tick(price="100000001")])
    assert r.violations == 1
    v = r.examples[0]
    assert v.detail["problem"] == "no_tick" and v.detail["ticks_in_window"] == 1
    old = [make_tick(received_us=FILLED_US - 9 * S1)]
    assert fresh_of(old).violations == 1
    assert fresh_of(old, stale_ms=10_000).violations == 0


def test_fresh_without_ticks_is_not_evaluated():
    r = fresh_of([])
    assert r.violations is None and r.reasons == (R.NOT_EVALUATED,)
    assert r.detail["unevaluated"] == 1


def test_fresh_other_market_ticks_do_not_count():
    r = fresh_of([make_tick(market="KRW-ETH")])
    assert r.violations is None and r.reasons == (R.NOT_EVALUATED,)


def test_fresh_violation_wins_over_unevaluated_orders():
    a = order(ACCT_A, 1)
    b = order(ACCT_B, 1, market="KRW-ETH")
    db = make_db(orders=[a, b], ticks=[make_tick(price="1")])
    r = check_fresh(db, Scope(run_id=RUN_ID), STALE)
    assert r.violations == 1 and r.detail["unevaluated"] == 1


def test_fresh_no_targets_is_zero_and_null_columns_are_skipped():
    other = make_order(ACCT_A, "manual-1")
    assert check_fresh(make_db(orders=[other]), Scope(run_id=RUN_ID)).violations == 0
    o = order(ACCT_A, 1, filled_us=None)
    r = check_fresh(make_db(orders=[o], ticks=[make_tick()]), Scope(run_id=RUN_ID))
    assert r.violations == 0 and r.detail["skipped"] == 1


def test_fresh_ticks_section_missing_is_db_error():
    r = fresh_of([make_tick()], sections=set(SNAPSHOT_SECTIONS) - {"ticks"})
    assert r.violations is None and r.reasons == (R.DB_ERROR,)


def test_fresh_uses_stale_ms_from_criteria():
    o = order(ACCT_A, 1)
    db = make_db(orders=[o], ticks=[make_tick(received_us=FILLED_US - 9 * S1)])
    assert run(db)["fresh"].violations == 1
    assert run(db, crit=IntegrityCfg(stale_ms=10_000))["fresh"].violations == 0


# ---------------------------------------------------------------- 구획 없음·자기 검사


def test_missing_sections_only_affect_their_invariants():
    db = clean_db(sections=set(SNAPSHOT_SECTIONS) - {"orders"})
    res = run(db)
    for i in ("cash", "qty", "avg", "arith", "fresh"):
        assert res[i].violations is None and res[i].reasons == (R.DB_ERROR,), i
        assert "orders" in res[i].detail["missing_sections"]
    assert res["nonneg"].violations == 0 and res["constraints"].violations == 0


def test_none_snapshot_is_all_db_error():
    res = check_invariants(None)
    assert all(r.violations is None and r.reasons == (R.DB_ERROR,) for r in res)


def test_snapshot_self_check():
    db = clean_db()
    assert snapshot_consistent(db) is True
    moving = dataclasses.replace(db, fingerprint2=Fingerprint(0, None, None, 0, None))
    assert (
        snapshot_consistent(moving) is None
    )  # 정지가 아니면 판단하지 않는다(db_quiescent 몫)
    fp = dataclasses.replace(db.fingerprint1, orders=db.fingerprint1.orders + 1)
    lost = dataclasses.replace(db, fingerprint1=fp, fingerprint2=fp)
    assert snapshot_consistent(lost) is False
    res = run(lost)
    for i in ROW_BASED:
        assert res[i].violations is None, i
        assert R.JUDGE_SELF_CHECK in res[i].reasons, i
        assert res[i].detail["computed_violations"] == 0
    assert res["constraints"].violations == 0


# ---------------------------------------------------------------- 기준 항목·건수


def test_invariant_counts():
    res = check_invariants(clean_db(sections=set(SNAPSHOT_SECTIONS) - {"ticks"}), CTX)
    assert invariant_counts(res) == {
        "cash": 0,
        "qty": 0,
        "avg": 0,
        "nonneg": 0,
        "arith": 0,
        "constraints": 0,
        "fresh": None,
    }


@pytest.fixture(scope="module")
def default_crit():
    return load_criteria(DEFAULT_YAML)


def items(db, crit, ctx=CTX):
    return by_id(item_results(check_invariants(db, ctx, crit), crit))


def test_item_results_clean_default_yaml(default_crit):
    got = items(clean_db(), default_crit)
    assert set(got) == {"invariants", "stale_fill"}  # class·k6 항목은 다루지 않는다
    inv = got["invariants"]
    assert (inv.outcome, inv.value, inv.kind, inv.required) == (
        Outcome.PASS,
        0,
        "integrity",
        True,
    )
    assert set(inv.detail["parts"]) == {
        "cash",
        "qty",
        "avg",
        "nonneg",
        "arith",
        "constraints",
    }
    sf = got["stale_fill"]
    assert (sf.outcome, sf.value, sf.required) == (Outcome.PASS, 0, False)


def test_item_results_violation_fails(default_crit):
    db = clean_db()
    accts = [dataclasses.replace(a, krw=Decimal("1")) for a in db.accounts]
    inv = items(
        make_db(orders=db.orders, ticks=db.ticks, accounts=accts), default_crit
    )["invariants"]
    assert (inv.outcome, inv.value, inv.reasons) == (Outcome.FAIL, 4, ())
    assert len(inv.detail["parts"]["cash"]["examples"]) == 4


def test_item_results_drift_is_inconclusive_unless_already_failed(default_crit):
    drift = schema_without(lambda s: s.name == "orders_client_order_id_key")
    inv = items(clean_db(schema=drift), default_crit)["invariants"]
    assert (inv.outcome, inv.value, inv.reasons) == (
        Outcome.INCONCLUSIVE,
        None,
        (R.SCHEMA_DRIFT,),
    )
    db = clean_db(schema=drift)
    accts = [dataclasses.replace(a, krw=Decimal("1")) for a in db.accounts]
    both = make_db(orders=db.orders, ticks=db.ticks, accounts=accts, schema=drift)
    inv = items(both, default_crit)["invariants"]
    assert inv.outcome is Outcome.FAIL and inv.detail.get("decided_by_partial") is True


def test_item_results_fresh_not_evaluated_and_db_error(default_crit):
    db = clean_db()
    sf = items(dataclasses.replace(db, ticks=()), default_crit)["stale_fill"]
    assert (sf.outcome, sf.reasons, sf.required) == (
        Outcome.INCONCLUSIVE,
        (R.NOT_EVALUATED,),
        False,
    )
    no_orders = clean_db(sections=set(SNAPSHOT_SECTIONS) - {"orders"})
    inv = items(no_orders, default_crit)["invariants"]
    assert (inv.outcome, inv.reasons) == (Outcome.INCONCLUSIVE, (R.DB_ERROR,))


def test_item_results_unknown_name_is_not_evaluated(default_crit):
    partial = [r for r in check_invariants(clean_db(), CTX) if r.id != "avg"]
    inv = by_id(item_results(partial, default_crit))["invariants"]
    assert (inv.outcome, inv.reasons) == (Outcome.INCONCLUSIVE, (R.NOT_EVALUATED,))
    assert inv.detail["parts"]["avg"]["missing"] is True


def test_crit_forms(default_crit):
    db = clean_db()
    assert flagged(check_invariants(db, CTX, default_crit)) == set()
    assert flagged(check_invariants(db, CTX, dict(default_crit.data))) == set()
    assert flagged(check_invariants(db, CTX, {"schema_version": 1})) == set()
    with pytest.raises(TypeError):
        check_invariants(db, CTX, 3)


# ---------------------------------------------------------------- 예시·자기 검사 건수


def test_examples_are_capped_and_sorted_but_count_is_full():
    users = [f"user-{i:04d}" for i in range(1, EXAMPLES_MAX + 6)]
    accts = [make_account(u, krw="1") for u in users]
    r = run(make_db(accounts=accts))["cash"]
    assert r.violations == len(users)
    assert [v.subject for v in r.examples] == users[:EXAMPLES_MAX]
    assert isinstance(r, InvariantResult)


def test_run_key_counts():
    rows = [
        order(ACCT_A, 1),
        order(ACCT_A, 2, status="pending", price=None, filled_us=None),
        make_order(ACCT_B, f"k6-{RUN_ID}-2-1-1"),  # 다른 run(r-test-2)
        make_order(ACCT_C, "manual-1"),
    ]
    dup = dataclasses.replace(rows[0], id=rows[0].id + 7)
    db = make_db(orders=[*rows, dup])
    assert run_key_counts(db, RUN_ID) == {"orders": 3, "keys": 2, "filled": 2}
    assert run_key_counts(db, f"{RUN_ID}-2") == {"orders": 1, "keys": 1, "filled": 1}
    no_orders = make_db(sections=set(SNAPSHOT_SECTIONS) - {"orders"})
    assert run_key_counts(no_orders, RUN_ID) is None


def test_render_parse_roundtrip_gives_same_results():
    db = clean_db()
    again = D.parse_bundle_output(D.render_snapshot(db))
    assert check_invariants(again, CTX) == check_invariants(db, CTX)

"""invariants.py: 구획별 불변식 결과와 I7 신선도(설계 5.4, 8 "invariants CSV 픽스처")."""

from __future__ import annotations

import random
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.judge import db as D
from orchestrator.judge import reasons as R
from orchestrator.judge.criteria import load_criteria
from orchestrator.judge.invariants import (
    SPEED_SLACK_MS,
    check_invariants,
    evaluate_i7,
    i4_counts,
    invariant_counts,
    item_results,
    speed_candidates,
)
from orchestrator.judge.model import (
    INVARIANT_SQL,
    SNAPSHOT_SECTIONS,
    IntegrityCfg,
    Outcome,
    SpeedPoint,
)

from .factories import ACCT_A, ACCT_B, T0, make_db, make_fill, make_order

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_YAML = ROOT / "experiments" / "criteria" / "default.yaml"
MAX_AGE = 60_000


def sp(from_ms, speed):
    return SpeedPoint(from_ms=from_ms, speed=Decimal(str(speed)))


SPEED1 = (sp(T0 - 3_600_000, 1),)


def by_id(results):
    return {r.id: r for r in results}


def filled(key, *, age=500, at=T0 + 5, acct=ACCT_A, **kw):
    return make_order(acct, key, ref_age_ms=age, received_ms=at, **kw)


def i7(orders, timeline=SPEED1, **kw):
    db = make_db(orders=orders)
    return by_id(check_invariants(db, None, None, speed_timeline=timeline, **kw))["i7"]


# ---------------------------------------------------------------- 구획 결과


def test_clean_snapshot_passes_everything():
    o = filled("k1")
    db = make_db(orders=[o], fills=[make_fill(o)])
    res = check_invariants(db, SimpleNamespace(replay_speed=SPEED1), None)
    assert [r.id for r in res] == [f"inv_{n}" for n in INVARIANT_SQL] + ["i7"]
    assert all(r.outcome is Outcome.PASS for r in res), [
        (r.id, r.outcome, r.reasons) for r in res
    ]
    assert all(r.kind == "integrity" for r in res)
    assert all(r.value == 0 for r in res)


def test_missing_section_is_db_error_only_for_that_section():
    secs = (set(SNAPSHOT_SECTIONS) | {f"inv_{n}" for n in INVARIANT_SQL}) - {
        "inv_i5_cash"
    }
    db = make_db(orders=[filled("k1")], sections=secs)
    res = by_id(check_invariants(db, None, None, speed_timeline=SPEED1))
    r = res["inv_i5_cash"]
    assert r.outcome is Outcome.INCONCLUSIVE
    assert r.reasons == (R.DB_ERROR,) and r.value is None
    assert r.detail["missing"] is True
    others = [x for k, x in res.items() if k != "inv_i5_cash"]
    assert all(x.outcome is Outcome.PASS for x in others)


def test_violation_rows_fail_with_count_and_sorted_examples():
    rows = [
        {"account_id": f"00000000-0000-4000-8000-{i:012d}", "field": "cash"}
        for i in range(25, 0, -1)
    ]
    db = make_db(orders=[filled("k1")], invariant_rows={"i2": rows})
    r = by_id(check_invariants(db, None, None, speed_timeline=SPEED1))["inv_i2"]
    assert r.outcome is Outcome.FAIL and r.value == 25
    assert r.reasons == ()  # combine이 INTEGRITY_VIOLATION을 붙인다
    ex = r.detail["examples"]
    assert len(ex) == 20
    assert ex == sorted(ex, key=lambda x: x["account_id"])


def test_constraints_violation_reason_is_constraint_missing():
    db = make_db(
        invariant_rows={
            "constraints": [
                {"kind": "unique", "table_name": "fills", "columns": "order_id"}
            ]
        }
    )
    r = by_id(check_invariants(db, None, None, speed_timeline=SPEED1))[
        "inv_constraints"
    ]
    assert r.outcome is Outcome.FAIL and r.reasons == (R.CONSTRAINT_MISSING,)


def test_i6_ref_source_rows_are_advisory_only():
    rows = [{"problem": "ref_source", "order_id": "o1", "ref_source": "upbit"}]
    db = make_db(invariant_rows={"i6_db": rows})
    r = by_id(check_invariants(db, None, None, speed_timeline=SPEED1))["inv_i6_db"]
    assert r.outcome is Outcome.PASS and r.value == 0
    assert r.detail["advisory_rows"] == 1
    rows.append({"problem": "price_ne_ref", "order_id": "o2", "ref_source": "replay"})
    db = make_db(invariant_rows={"i6_db": rows})
    r = by_id(check_invariants(db, None, None, speed_timeline=SPEED1))["inv_i6_db"]
    assert r.outcome is Outcome.FAIL and r.value == 1


def test_without_criteria_only_optional_sql_is_not_required():
    res = by_id(check_invariants(make_db(), None, None, speed_timeline=SPEED1))
    assert res["inv_i2"].required and res["i7"].required
    assert not res["inv_arith"].required and not res["inv_privileges"].required


def test_default_criteria_maps_required_and_items():
    crit = load_criteria(DEFAULT_YAML)
    res = by_id(check_invariants(make_db(), None, crit, speed_timeline=SPEED1))
    assert res["inv_i2"].required and res["inv_i2"].detail["criteria"] == ["invariants"]
    assert res["inv_constraints"].required
    assert not res["inv_arith"].required  # arithmetic: required false
    assert res["inv_arith"].detail["criteria"] == ["arithmetic"]
    assert not res["inv_privileges"].required  # 어떤 기준도 가리키지 않는다
    assert res["i7"].required and res["i7"].detail["criteria"] == ["i7_stale_fill"]
    assert res["i7"].detail["price_max_age_ms"] == 60000


def test_on_violation_is_carried_not_applied():
    data = {
        "integrity": {
            "response_fields": {
                "status": "status",
                "reject_reason": "reason",
                "price": "price",
                "fee": "fee",
            },
            "response_fields_confirmed": False,
            "price_max_age_ms": 30000,
        },
        "checks": [
            {
                "id": "money",
                "kind": "integrity",
                "sql": ["i2"],
                "op": "==",
                "value": 0,
                "required": True,
                "on_violation": "inconclusive",
            },
            {
                "id": "money2",
                "kind": "integrity",
                "sql": ["i2", "i5_cash"],
                "op": "==",
                "value": 0,
                "required": False,
                "on_violation": "fail",
            },
        ],
    }
    db = make_db(invariant_rows={"i2": [{"account_id": ACCT_A}]})
    res = by_id(check_invariants(db, None, data, speed_timeline=SPEED1))
    r = res["inv_i2"]
    assert r.outcome is Outcome.FAIL
    assert r.on_violation == "inconclusive"  # required 항목(money)의 값
    assert r.required and r.detail["criteria"] == ["money", "money2"]
    assert not res["inv_i5_cash"].required
    assert res["i7"].detail["price_max_age_ms"] == 30000


def test_integrity_cfg_sets_max_age():
    o = filled("k1", age=31_000)
    db = make_db(orders=[o])
    cfg = IntegrityCfg(response_fields={}, price_max_age_ms=30_000)
    r = by_id(check_invariants(db, None, cfg, speed_timeline=SPEED1))["i7"]
    assert r.outcome is Outcome.FAIL


def test_invariant_counts_and_item_results():
    crit = load_criteria(DEFAULT_YAML)
    secs = (set(SNAPSHOT_SECTIONS) | {f"inv_{n}" for n in INVARIANT_SQL}) - {"inv_i8"}
    db = make_db(
        orders=[filled("k1")],
        sections=secs,
        invariant_rows={"i5_qty": [{"account_id": ACCT_A}, {"account_id": ACCT_B}]},
    )
    res = check_invariants(db, None, crit, speed_timeline=())
    counts = invariant_counts(res)
    assert counts["i5_qty"] == 2 and counts["i2"] == 0
    assert counts["i8"] is None  # 구획 없음
    assert counts["i7"] is None  # 배속 모름
    items = by_id(item_results(res, crit))
    assert set(items) == {"invariants", "i7_stale_fill", "arithmetic"}
    inv = items["invariants"]
    assert inv.outcome is Outcome.FAIL and inv.value is None
    assert R.DB_ERROR in inv.reasons
    assert inv.detail["parts"]["inv_i5_qty"]["value"] == 2
    st = items["i7_stale_fill"]
    assert st.outcome is Outcome.INCONCLUSIVE and st.reasons == (R.NO_SPEED_TIMELINE,)
    assert items["arithmetic"].outcome is Outcome.PASS
    assert not items["arithmetic"].required


def test_i4_counts():
    rows = [{"problem": "dup_key"}, {"problem": "dup_fill"}, {"problem": "dup_key"}]
    assert i4_counts(make_db(invariant_rows={"i4": rows})) == {
        "dup_fill": 1,
        "dup_key": 2,
    }
    assert i4_counts(make_db(sections=set(SNAPSHOT_SECTIONS))) is None


def test_csv_fixture_through_parser():
    """psql과 같은 --@ + CSV 텍스트에서 시작한다(설계 8)."""
    o = filled("k1", age=6_001, at=T0 + 1_000)
    text = D.render_snapshot(
        make_db(
            orders=[o],
            fills=[make_fill(o)],
            invariant_rows={"i5_cash": [{"account_id": ACCT_A, "cash": "1"}]},
        )
    )
    db = D.parse_bundle_output(text)
    res = by_id(check_invariants(db, None, None, speed_timeline=(sp(T0, 10),)))
    assert res["inv_i5_cash"].outcome is Outcome.FAIL
    assert res["inv_i5_cash"].detail["examples"][0]["cash"] == "1"
    assert res["i7"].outcome is Outcome.FAIL and res["i7"].value == 1


# ---------------------------------------------------------------- I7


@pytest.mark.parametrize(
    ("speed", "age", "outcome"),
    [
        (1, 60_000, Outcome.PASS),  # 정확히 상한
        (1, 60_001, Outcome.FAIL),
        (10, 6_000, Outcome.PASS),
        (10, 6_001, Outcome.FAIL),
        (7, 8_571, Outcome.PASS),  # 60000/7 = 8571.43, 곱으로 비교
        (7, 8_572, Outcome.FAIL),
        ("0.5", 120_000, Outcome.PASS),
        ("0.5", 120_001, Outcome.FAIL),
    ],
)
def test_i7_exact_limit(speed, age, outcome):
    r = i7([filled("k1", age=age)], (sp(T0 - 1_000, speed),))
    assert r.outcome is outcome
    assert r.value == (1 if outcome is Outcome.FAIL else 0)


@pytest.mark.parametrize(
    ("offset", "outcome"),
    [
        (-SPEED_SLACK_MS - 1, Outcome.PASS),  # 전환 전, 배속 1
        (-SPEED_SLACK_MS, Outcome.PASS),
        (0, Outcome.PASS),  # 전환 순간, 느슨한 값(배속 1)
        (SPEED_SLACK_MS, Outcome.PASS),  # 전환 뒤 15s, 아직 느슨한 값
        (SPEED_SLACK_MS + 1, Outcome.FAIL),  # 배속 10 상한 6000
    ],
)
def test_i7_speed_switch_up_uses_loose_limit_within_15s(offset, outcome):
    switch = T0 + 100_000
    timeline = (sp(T0 - 100_000, 1), sp(switch, 10))
    r = i7([filled("k1", age=50_000, at=switch + offset)], timeline)
    assert r.outcome is outcome


@pytest.mark.parametrize(
    ("offset", "outcome"),
    [
        (-SPEED_SLACK_MS - 1, Outcome.FAIL),  # 전환 전, 배속 10
        (-SPEED_SLACK_MS, Outcome.PASS),
        (SPEED_SLACK_MS + 1, Outcome.PASS),  # 전환 뒤, 배속 1
    ],
)
def test_i7_speed_switch_down(offset, outcome):
    switch = T0 + 100_000
    timeline = (sp(T0 - 100_000, 10), sp(switch, 1))
    r = i7([filled("k1", age=50_000, at=switch + offset)], timeline)
    assert r.outcome is outcome


def test_i7_loose_window_is_reported():
    switch = T0 + 100_000
    timeline = (sp(T0 - 100_000, 1), sp(switch, 10))
    r = i7([filled("k1", age=50_000, at=switch + 1)], timeline)
    assert r.outcome is Outcome.PASS and r.detail["loose_window"] == 1


def test_i7_null_ref_age_is_violation_even_without_timeline():
    r = i7([filled("k1", age=None)], ())
    assert r.outcome is Outcome.FAIL and r.value == 1
    assert r.detail["null_ref_age"] == 1 and r.reasons == ()


def test_i7_violation_wins_over_undetermined():
    r = i7([filled("k1", age=None), filled("k2", age=100)], ())
    assert r.outcome is Outcome.FAIL and r.value == 1
    assert r.reasons == (R.NO_SPEED_TIMELINE,)
    assert r.detail["undetermined"] == 1


def test_i7_without_timeline_is_inconclusive():
    r = i7([filled("k1", age=100)], ())
    assert r.outcome is Outcome.INCONCLUSIVE
    assert r.reasons == (R.NO_SPEED_TIMELINE,) and r.value is None


def test_i7_order_before_first_speed_point_is_inconclusive():
    r = i7([filled("k1", age=100, at=T0 - 1)], (sp(T0, 1),))
    assert r.outcome is Outcome.INCONCLUSIVE


def test_i7_no_filled_orders_passes():
    rej = make_order(ACCT_A, "k1", status="REJECTED", reject_reason="MARKET_CLOSED")
    r = i7([rej], ())
    assert r.outcome is Outcome.PASS and r.detail["filled"] == 0


@pytest.mark.parametrize(
    ("age", "kind"),
    [(5_000, "ok"), (7_000, "undetermined"), (70_000, "violation")],
)
def test_i7_unknown_received_time_decides_only_when_all_speeds_agree(age, kind):
    timeline = (sp(T0 - 100_000, 1), sp(T0, 10))
    ev = evaluate_i7([filled("k1", age=age, at=None)], MAX_AGE, timeline)
    assert (len(ev.violations), len(ev.undetermined)) == {
        "ok": (0, 0),
        "undetermined": (0, 1),
        "violation": (1, 0),
    }[kind]


def test_i7_live_symbol_uses_speed_one():
    live = "UPBIT:KRW-BTC"
    ok = i7([filled("k1", age=59_000, symbol=live)], ())
    bad = i7([filled("k1", age=61_000, symbol=live)], (sp(T0 - 1_000, 10),))
    assert ok.outcome is Outcome.PASS and bad.outcome is Outcome.FAIL


def test_i7_rejected_orders_are_not_violations_but_estimate_app_limit():
    stale = make_order(
        ACCT_A,
        "k2",
        status="REJECTED",
        reject_reason="STALE_PRICE",
        ref_age_ms=5_000,
        received_ms=T0 + 10,
    )
    r = i7([filled("k1", age=500), stale], (sp(T0 - 1_000, 10),))
    assert r.outcome is Outcome.PASS
    # 배속 10에서 5000ms = 배속 1 기준 50000ms ≤ 60000인데 앱이 거절했다
    assert r.detail["warnings"] == [R.APP_MAX_AGE_DIFFERS]
    assert r.detail["app_age_estimate"] == {"filled_max": "5000", "stale_min": "50000"}
    stale2 = make_order(
        ACCT_A,
        "k3",
        status="REJECTED",
        reject_reason="STALE_PRICE",
        ref_age_ms=7_000,
        received_ms=T0 + 10,
    )
    r2 = i7([filled("k1", age=500), stale2], (sp(T0 - 1_000, 10),))
    assert r2.detail["warnings"] == []


def test_i7_reports_db_clock_age():
    o = filled("k1", age=500, at=T0 + 5)  # factories: ref_event_ts = received − age
    r = i7([o])
    assert r.detail["db_clock_age_max_ms"] == 500
    assert r.detail["feed_alive"] == "unverifiable"


def test_i7_speed_timeline_argument_overrides_context():
    o = filled("k1", age=6_001)
    db = make_db(orders=[o])
    ctx = SimpleNamespace(replay_speed=(sp(T0 - 1_000, 1),))
    by_ctx = by_id(check_invariants(db, ctx, None))["i7"]
    by_pod = by_id(
        check_invariants(db, ctx, None, speed_timeline=(sp(T0 - 1_000, 10),))
    )["i7"]
    assert by_ctx.outcome is Outcome.PASS and by_pod.outcome is Outcome.FAIL


def test_i7_accepts_mapping_points_and_rejects_bad_ones():
    r = i7([filled("k1", age=6_001)], [{"from_ms": T0 - 1_000, "speed": "10"}])
    assert r.outcome is Outcome.FAIL
    from orchestrator.judge.errors import InputError

    with pytest.raises(InputError):
        i7([filled("k1")], [{"from_ms": T0, "speed": "0"}])
    with pytest.raises(InputError):
        i7([filled("k1")], [sp(T0, 1), sp(T0, 2)])


def test_i7_missing_orders_section_is_db_error():
    db = make_db(sections={"accounts"})
    r = by_id(check_invariants(db, None, None, speed_timeline=SPEED1))["i7"]
    assert r.outcome is Outcome.INCONCLUSIVE and r.reasons == (R.DB_ERROR,)


def test_speed_candidates_boundaries():
    tl = (sp(T0, 1), sp(T0 + 100_000, 10), sp(T0 + 110_000, 2))
    assert speed_candidates(T0 - 1, tl) is None
    assert speed_candidates(T0, tl) == (Decimal(1),)
    # 두 전환이 모두 15s 안
    assert speed_candidates(T0 + 105_000, tl) == (Decimal(1), Decimal(2), Decimal(10))
    assert speed_candidates(T0 + 126_000, tl) == (Decimal(2),)


def test_i7_is_deterministic_under_order_shuffle():
    orders = [filled(f"k{i}", age=1_000 * i, at=T0 + i) for i in range(1, 15)]
    tl = (sp(T0 - 1_000, 10),)
    base = evaluate_i7(orders, MAX_AGE, tl)
    rng = random.Random(7)
    for _ in range(5):
        shuffled = orders[:]
        rng.shuffle(shuffled)
        assert evaluate_i7(shuffled, MAX_AGE, tl) == base
    assert len(base.violations) == 8  # 7000..14000

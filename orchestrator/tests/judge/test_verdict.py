"""집계 규칙, 건수 기준, k6 종료 코드, verdict digest(설계 1.4, 5.2-6, 5.7, 5.8)."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from orchestrator.judge import reasons as R
from orchestrator.judge.criteria import load_criteria
from orchestrator.judge.errors import JudgeError
from orchestrator.judge.model import CheckResult, Outcome, Verdict
from orchestrator.judge.verdict import (
    build_verdict,
    combine,
    effective_outcome,
    evaluate_count_check,
    exit_code_for,
    k6_exit_to_checks,
    requires_met,
    verdict_digest,
)

REPO = Path(__file__).resolve().parents[3]
DEFAULT_YAML = REPO / "experiments" / "criteria" / "default.yaml"

PASS, FAIL, INC, NA = Outcome.PASS, Outcome.FAIL, Outcome.INCONCLUSIVE, Outcome.NA


def chk(item_id, kind, outcome, *, required=True, on_violation="fail", reasons=()):
    return CheckResult(
        id=item_id,
        kind=kind,
        outcome=outcome,
        required=required,
        on_violation=on_violation,
        reasons=tuple(reasons),
    )


def grd(item_id, outcome, *, kind="guard", required=True, on_violation="inconclusive"):
    return CheckResult(
        id=item_id,
        kind=kind,
        outcome=outcome,
        required=required,
        on_violation=on_violation,
    )


def base_checks():
    return [
        chk("lost", "integrity", PASS),
        chk("protocol", "unresolved", PASS, on_violation="inconclusive"),
        chk("order_p95_k6", "client", PASS),
        chk("k6_exit", "k6", PASS),
    ]


def base_guards():
    return [grd("safety", PASS), grd("fill_coverage", PASS), grd("db_quiescent", PASS)]


# ---------------------------------------------------------------- combine(설계 5.7)


def test_all_pass():
    assert combine(base_checks(), base_guards()) == (PASS, ())


def test_rule1_money_fail_beats_unmet_guard():
    checks = base_checks()
    checks[0] = chk("lost", "integrity", FAIL)
    guards = [grd("fill_coverage", FAIL), grd("safety", INC)]
    assert combine(checks, guards) == (FAIL, (R.INTEGRITY_VIOLATION,))


def test_rule1_keeps_check_reasons_sorted_and_unique():
    checks = [
        chk("lost", "integrity", FAIL, reasons=[R.JUDGE_SELF_CHECK]),
        chk("invariants", "integrity", FAIL, reasons=[R.JUDGE_SELF_CHECK]),
        chk("order_p95_k6", "client", FAIL, reasons=[R.LOW_SAMPLES]),
    ]
    outcome, why = combine(checks, base_guards())
    assert outcome is FAIL
    assert why == (R.INTEGRITY_VIOLATION, R.JUDGE_SELF_CHECK)


def test_rule2_unmet_guard_beats_other_fail():
    checks = base_checks()
    checks[2] = chk("order_p95_k6", "client", FAIL)
    guards = [grd("safety", PASS), grd("fill_coverage", FAIL)]
    assert combine(checks, guards) == (INC, (R.LOW_FILL_COVERAGE,))


def test_rule2_guard_reasons_default_and_own():
    guards = [
        grd("load_coverage", INC),
        grd("crash_restart", FAIL, kind="fault_effect"),
        CheckResult(id="account_set", kind="guard", outcome=INC, reasons=(R.DB_ERROR,)),
        grd("custom_guard", INC),
    ]
    outcome, why = combine(base_checks(), guards)
    assert outcome is INC
    assert why == tuple(
        sorted(
            {
                R.GENERATOR_LIMITED,
                R.FAULT_NOT_OBSERVED,
                R.DB_ERROR,
                R.CHECK_INCONCLUSIVE,
            }
        )
    )


def test_rule3_required_fail():
    checks = base_checks()
    checks[2] = chk("order_p95_k6", "client", FAIL)
    assert combine(checks, base_guards()) == (FAIL, (R.CHECK_FAILED,))


def test_rule3_guard_with_on_violation_fail():
    guards = [grd("fault_in_load", FAIL, on_violation="fail")]
    assert combine(base_checks(), guards) == (FAIL, (R.FAULT_OUTSIDE_LOAD,))


def test_rule4_required_inconclusive_not_skipped():
    checks = base_checks()
    checks[2] = chk("order_p95_k6", "client", INC, reasons=[R.LOW_SAMPLES])
    assert combine(checks, base_guards()) == (INC, (R.LOW_SAMPLES,))


def test_rule4_unknown_outcome_is_not_evaluated():
    checks = base_checks()
    checks[3] = CheckResult(id="k6_exit", kind="k6", outcome=None)
    assert combine(checks, base_guards()) == (INC, (R.NOT_EVALUATED,))


def test_required_false_is_report_only():
    checks = base_checks() + [
        chk("arithmetic", "integrity", FAIL, required=False),
        chk("order_recovery_k6", "client", INC, required=False),
    ]
    guards = base_guards() + [grd("sell_coverage", FAIL, required=False)]
    assert combine(checks, guards) == (PASS, ())


def test_na_is_ignored():
    checks = base_checks() + [chk("ws_restored", "client", NA)]
    guards = base_guards() + [grd("ws_load_present", NA)]
    assert combine(checks, guards) == (PASS, ())


def test_all_na_is_not_vacuous_pass():
    checks = [chk("a", "client", NA), chk("b", "k6", NA, required=False)]
    assert combine(checks, base_guards()) == (INC, (R.NOT_EVALUATED,))
    assert combine([], []) == (INC, (R.NOT_EVALUATED,))


def test_on_violation_inconclusive_downgrades_fail():
    checks = base_checks()
    checks[1] = chk("protocol", "unresolved", FAIL, on_violation="inconclusive")
    assert combine(checks, base_guards()) == (INC, (R.CHECK_INCONCLUSIVE,))
    assert effective_outcome(checks[1]) is INC
    # 돈 불변식도 on_violation을 따른다: 규칙 1에 들지 않는다.
    money = [chk("lost", "integrity", FAIL, on_violation="inconclusive")]
    assert combine(money + base_checks()[1:], base_guards())[0] is INC


def test_string_outcomes_are_accepted():
    checks = [chk("lost", "integrity", "FAIL")]
    assert combine(checks, []) == (FAIL, (R.INTEGRITY_VIOLATION,))
    assert combine([chk("x", "client", "N/A"), chk("y", "client", "PASS")], []) == (
        PASS,
        (),
    )


# ---------------------------------------------------------------- 건수 기준(설계 6)


def specs_by_id():
    crit = load_criteria(DEFAULT_YAML)
    return {c["id"]: c for c in crit.checks}


def test_count_check_class_sum_fail():
    spec = specs_by_id()["lost"]
    r = evaluate_count_check(spec, {"LOST": 1, "REJECT_COMMITTED": 2, "OK": 50})
    assert r.outcome is FAIL and r.value == 3 and r.kind == "integrity"
    assert r.required is True and r.on_violation == "fail" and r.reasons == ()
    assert r.detail["counts"] == {"LOST": 1, "REJECT_COMMITTED": 2}


def test_count_check_absent_classes_are_zero():
    r = evaluate_count_check(specs_by_id()["duplicates"], {"OK": 10})
    assert r.outcome is PASS and r.value == 0


def test_interrupted_and_user_impact_do_not_fail():
    """INTERRUPTED·UNRESOLVED_*는 돈 문제가 아니다(설계 5.3, 명세 4)."""
    counts = {
        "OK": 100,
        "INTERRUPTED": 7,
        "UNRESOLVED_ABSENT": 2,
        "UNRESOLVED_COMMITTED": 1,
    }
    specs = specs_by_id()
    ids = ("lost", "mismatch", "duplicates", "orphans", "protocol")
    checks = [evaluate_count_check(specs[i], counts) for i in ids]
    assert all(c.outcome is PASS for c in checks)
    assert combine(checks, base_guards()) == (PASS, ())


@pytest.mark.parametrize(
    "measured,op,threshold,expected",
    [
        (0, "==", 0, PASS),
        (1, "==", 0, FAIL),
        (1, "!=", 0, PASS),
        (299, "<", 300, PASS),
        (300, "<", 300, FAIL),
        (300, "<=", 300, PASS),
        (301, "<=", 300, FAIL),
        (2, ">", 1, PASS),
        (1, ">", 1, FAIL),
        (1, ">=", 1, PASS),
        (0, ">=", 1, FAIL),
        (0.0099, "<", 0.01, PASS),
        (0.01, "<", 0.01, FAIL),
        (Decimal("0.3"), "==", 0.3, PASS),
        (0.1 + 0.2, "<=", 0.3, FAIL),  # float 합은 0.30000000000000004다
    ],
)
def test_count_check_ops(measured, op, threshold, expected):
    spec = {
        "id": "x",
        "kind": "client",
        "stat": "ack_p95_ms",
        "op": op,
        "value": threshold,
    }
    r = evaluate_count_check(spec, {"ack_p95_ms": measured})
    assert r.outcome is expected
    assert r.value == measured


def test_count_check_sql_missing_section_is_db_error():
    spec = specs_by_id()["invariants"]
    counts = {n: 0 for n in spec["sql"]}
    assert evaluate_count_check(spec, counts).outcome is PASS
    del counts["cash"]
    r = evaluate_count_check(spec, counts)
    assert r.outcome is INC and r.reasons == (R.DB_ERROR,)
    assert r.detail["unavailable"] == {"cash": R.DB_ERROR}
    counts["cash"] = None
    assert evaluate_count_check(spec, counts).reasons == (R.DB_ERROR,)


def test_count_check_partial_violation_still_fails():
    """일부만 알아도 이미 위반이면 FAIL(돈 FAIL 우선을 지킨다)."""
    spec = specs_by_id()["invariants"]
    counts = {n: 0 for n in spec["sql"]}
    counts["arith"] = 2
    del counts["qty"]
    r = evaluate_count_check(spec, counts)
    assert r.outcome is FAIL and r.value == 2 and r.detail["decided_by_partial"]


def test_count_check_unavailable_names():
    spec = specs_by_id()["lost"]
    unavailable = {"LOST": R.ACCOUNT_SET_MISMATCH}
    r = evaluate_count_check(spec, {"LOST": 5}, unavailable=unavailable)
    assert r.outcome is INC and r.reasons == (R.ACCOUNT_SET_MISMATCH,)
    r = evaluate_count_check(spec, {"REJECT_COMMITTED": 1}, unavailable=unavailable)
    assert r.outcome is FAIL and r.value == 1


def test_count_check_requires():
    spec = {"id": "ws_x", "kind": "client", "stat": "s", "op": "<", "value": 1}
    spec["requires"] = "ws_scenario"
    assert evaluate_count_check(spec, {"s": 0}).outcome is NA
    assert (
        evaluate_count_check(spec, {"s": 0}, features={"ws_scenario"}).outcome is PASS
    )
    assert requires_met({"requires": ["a", "b"]}, {"a"}) is False
    assert requires_met({}, ()) is True


def test_count_check_fault_effect_forms():
    sub = {
        "id": "crash_seen_by_k6",
        "class_sub": ["after_commit", "after_503"],
        "op": ">=",
        "value": 1,
    }
    r = evaluate_count_check(sub, {"after_commit": 1})
    assert (
        r.outcome is PASS
        and r.kind == "fault_effect"
        and r.on_violation == "inconclusive"
    )
    assert evaluate_count_check(sub, {}).outcome is FAIL
    obs = {"id": "crash_restart", "observe": "crash_restart", "min": 2}
    assert evaluate_count_check(obs, {"crash_restart": 2}).outcome is PASS
    assert evaluate_count_check(obs, {"crash_restart": 1}).outcome is FAIL
    r = evaluate_count_check(obs, {})
    assert r.outcome is INC and r.reasons == (R.NOT_EVALUATED,)
    replaced = {"id": "pod_replaced", "observe": "pod_replaced", "within_s": 120}
    assert evaluate_count_check(replaced, {"pod_replaced": 1}).outcome is PASS


def test_count_check_stat_missing_and_no_op():
    spec = {
        "id": "p95",
        "kind": "client",
        "stat": "ack_p95_ms",
        "op": "<",
        "value": 300,
    }
    r = evaluate_count_check(spec, {})
    assert r.outcome is INC and r.reasons == (R.NOT_EVALUATED,)
    rec = {"id": "order_recovery_k6", "kind": "client", "stat": "recovery"}
    assert evaluate_count_check(rec, {"recovery": 3}).reasons == (R.NOT_EVALUATED,)


def test_count_check_rejects_bad_values():
    spec = {"id": "p95", "kind": "client", "stat": "s", "op": "<", "value": 1}
    with pytest.raises(ValueError):
        evaluate_count_check(spec, {"s": True})
    with pytest.raises(ValueError):
        evaluate_count_check(spec, {"s": float("nan")})
    with pytest.raises(JudgeError):
        evaluate_count_check({"kind": "client"}, {})


# ---------------------------------------------------------------- k6 종료 코드(설계 5.7 하단)

PLAN_MS = 600_000


def summary(duration_ms, failed=()):
    metrics = {
        "order_intents": {"type": "counter", "values": {"count": 10}},
        "order_ack_ms": {"type": "trend", "thresholds": {"p(95)<300": {"ok": True}}},
    }
    for name, expr in failed:
        metrics.setdefault(name, {"type": "rate"}).setdefault("thresholds", {})[
            expr
        ] = {"ok": False}
    return {"metrics": metrics, "state": {"testRunDurationMs": duration_ms}}


def test_k6_exit_zero():
    (r,) = k6_exit_to_checks(0, None, PLAN_MS)
    assert (r.id, r.kind, r.outcome, r.value) == ("k6_exit", "k6", PASS, 0)


def test_k6_99_short_run_is_load_aborted():
    s = summary(PLAN_MS - 1, [("order_sys_err{step:3}", "rate<0.2")])
    (r,) = k6_exit_to_checks(99, s, PLAN_MS)
    assert r.outcome is INC and r.reasons == (R.LOAD_ABORTED,)


def test_k6_99_setup_ms_extends_expected_length():
    s = summary(PLAN_MS + 10_000, [("order_ack_ms", "p(95)<300")])
    assert k6_exit_to_checks(99, s, PLAN_MS)[0].outcome is PASS
    r = k6_exit_to_checks(99, s, PLAN_MS, setup_ms=60_000)[0]
    assert r.reasons == (R.LOAD_ABORTED,)


def test_k6_99_full_run_slo_is_warning_only():
    s = summary(
        PLAN_MS + 5, [("order_ack_ms", "p(95)<300"), ("order_sys_err", "rate<0.01")]
    )
    main, slo = k6_exit_to_checks(99, s, PLAN_MS)
    assert main.outcome is PASS and main.reasons == ()
    assert slo.id == "k6_exit_slo" and slo.required is False
    assert slo.reasons == (R.K6_THRESHOLD_DISAGREES,)
    assert combine([*base_checks()[:3], main, slo], base_guards()) == (PASS, ())


def test_k6_99_accepts_decimal_duration():
    """증거 summary는 parse_float=Decimal로 읽는다. 실수 지속 시간도 해석해야 한다(설계 5.7)."""
    full = summary(Decimal(PLAN_MS) + Decimal("0.5"), [("order_ack_ms", "p(95)<300")])
    main, slo = k6_exit_to_checks(99, full, PLAN_MS)
    assert main.outcome is PASS
    assert main.detail["test_run_duration_ms"] == Decimal(PLAN_MS) + Decimal("0.5")
    assert slo.reasons == (R.K6_THRESHOLD_DISAGREES,)
    short = summary(Decimal(PLAN_MS) - Decimal("0.5"), [("order_ack_ms", "p(95)<300")])
    assert k6_exit_to_checks(99, short, PLAN_MS)[0].reasons == (R.LOAD_ABORTED,)
    for bad in (Decimal("NaN"), Decimal("Infinity")):
        r = k6_exit_to_checks(99, summary(bad), PLAN_MS)[0]
        assert r.reasons == (R.NOT_EVALUATED,)


def test_k6_99_delegated_thresholds():
    s = summary(
        PLAN_MS,
        [("resend_mismatch", "count<1"), ("order_protocol_errors", "count<1")],
    )
    main, *extra = k6_exit_to_checks(99, s, PLAN_MS)
    assert main.outcome is PASS
    by_id = {c.id: c for c in extra}
    assert set(by_id) == {"k6_exit_resend_mismatch", "k6_exit_protocol"}
    assert by_id["k6_exit_protocol"].detail["delegated_to"] == "classify:PROTOCOL"
    assert all(not c.required for c in extra)


def test_k6_99_unknown_or_missing_threshold_is_inconclusive():
    s = summary(PLAN_MS, [("checks", "rate>0.99")])
    r = k6_exit_to_checks(99, s, PLAN_MS)[0]
    assert r.outcome is INC and r.reasons == (R.CHECK_INCONCLUSIVE,)
    r = k6_exit_to_checks(99, summary(PLAN_MS), PLAN_MS)[0]
    assert r.outcome is INC and r.reasons == (R.CHECK_INCONCLUSIVE,)


def test_k6_99_without_evidence_is_not_evaluated():
    for s, plan in ((None, PLAN_MS), ({"metrics": {}}, PLAN_MS), (summary(1), None)):
        r = k6_exit_to_checks(99, s, plan)[0]
        assert r.outcome is INC and r.reasons == (R.NOT_EVALUATED,)


@pytest.mark.parametrize(
    "code,reason",
    [
        (104, R.LOAD_INVALID),
        (107, R.LOAD_INVALID),
        (108, R.LOAD_ABORTED),
        (105, R.LOAD_ABORTED),
        (137, R.LOAD_ABORTED),
        (None, R.NOT_EVALUATED),
    ],
)
def test_k6_other_exit_codes(code, reason):
    (r,) = k6_exit_to_checks(code, summary(PLAN_MS), PLAN_MS)
    assert r.outcome is INC and r.reasons == (reason,)


def test_k6_105_from_judge_stop_is_intended():
    (r,) = k6_exit_to_checks(105, summary(10_000), PLAN_MS, stopped_by_judge=True)
    assert r.outcome is PASS
    s = summary(10_000, [("order_ack_ms", "p(95)<300")])
    main, slo = k6_exit_to_checks(105, s, None, stopped_by_judge=True)
    assert main.outcome is PASS and slo.reasons == (R.K6_THRESHOLD_DISAGREES,)


def test_k6_spec_sets_id_required_and_on_violation():
    spec = {"id": "k6_exit", "kind": "k6", "check": "exit_code", "required": False}
    (r,) = k6_exit_to_checks(108, None, PLAN_MS, spec=spec)
    assert r.required is False and r.on_violation == "fail"


# ---------------------------------------------------------------- verdict와 digest(설계 5.2-6, 5.8)

CTX = {"run_id": "r1117-crash", "experiment": "order-api-crash-after-commit"}


def make_verdict(**kw):
    args = dict(
        ctx_like=CTX,
        checks=base_checks(),
        guards=base_guards(),
        integrity={
            "intents": {"total": 100, "ack": 98},
            "log_complete": True,
            "classes": {"OK": 98, "UNRESOLVED_ABSENT": 2},
            "subs": {"clean": 90, "after_commit": 8},
            "invariants": {"cash": 0, "qty": 0},
            "examples": {"UNRESOLVED_ABSENT": ["k1", "k2"]},
        },
        client={"phases": {"baseline": {"n": 4000, "ack_p95_ms": Decimal("41.5")}}},
        load={"plan_arrivals": 18000, "achieved_ratio": 0.995},
        inputs={"criteria_sha256": "c" * 64, "run_json_sha256": "a" * 64},
        warnings=(),
        run_info={"duration_ms": 1234, "transport": "exec"},
        generated_at=1_763_000_000_000,
    )
    args.update(kw)
    return build_verdict(**args)


def test_build_verdict_shape():
    v = make_verdict()
    assert v.verdict is PASS and v.reasons == ()
    assert v.run_id == CTX["run_id"] and v.experiment == CTX["experiment"]
    d = v.to_dict()
    assert next(iter(d)) == "schema" and d["schema"] == "papertrade.verdict/v1"
    assert d["verdict_digest"] == v.verdict_digest == v.compute_digest()
    json.dumps(d)  # 직렬화 가능


def test_digest_is_deterministic():
    assert make_verdict().verdict_digest == make_verdict().verdict_digest


def test_digest_ignores_dict_order():
    a = make_verdict()
    b = make_verdict(
        integrity=dict(reversed(list(make_verdict().integrity.items()))),
        load={"achieved_ratio": 0.995, "plan_arrivals": 18000},
        inputs={"run_json_sha256": "a" * 64, "criteria_sha256": "c" * 64},
    )
    assert a.verdict_digest == b.verdict_digest
    assert json.dumps(a.to_dict()) == json.dumps(b.to_dict())  # 출력도 순서 무관


def test_digest_ignores_generated_at_and_run_info():
    a = make_verdict()
    b = make_verdict(generated_at=1_999_999_999_999, run_info={"duration_ms": 9})
    c = make_verdict(generated_at=None)
    assert a.verdict_digest == b.verdict_digest == c.verdict_digest
    assert a.to_dict() != b.to_dict()


def test_digest_changes_with_evidence():
    a = make_verdict()
    b = make_verdict(load={"plan_arrivals": 18001, "achieved_ratio": 0.995})
    assert a.verdict_digest != b.verdict_digest


def test_verdict_digest_canonical_form():
    d = {
        "z": Decimal("1.50"),
        "a": "한글",
        "generated_at": 1,
        "run_info": {"x": 1},
        "verdict_digest": "old",
    }
    expected_text = '{"a":"한글","z":"1.50"}'
    assert (
        verdict_digest(d) == hashlib.sha256(expected_text.encode("utf-8")).hexdigest()
    )
    with pytest.raises(ValueError):
        verdict_digest({"x": float("nan")})


def test_verdict_digest_accepts_verdict_object():
    v = make_verdict()
    assert verdict_digest(v) == verdict_digest(v.to_dict()) == v.verdict_digest


def test_build_verdict_combines_and_merges_warnings():
    checks = base_checks()
    checks[0] = chk("lost", "integrity", FAIL)
    k6 = k6_exit_to_checks(
        99, summary(PLAN_MS, [("order_ack_ms", "p(95)<300")]), PLAN_MS
    )
    v = make_verdict(
        checks=[*checks[:3], *k6],
        guards=[grd("fill_coverage", FAIL)],
        warnings=[R.SNAPSHOT_LARGE, R.SNAPSHOT_LARGE],
        integrity={"subs": {"after_503": 1}},
    )
    assert v.verdict is FAIL and v.reasons == (R.INTEGRITY_VIOLATION,)
    assert v.warnings == (
        R.K6_THRESHOLD_DISAGREES,
        R.ROLLBACK_REPLY_COMMITTED,
        R.SNAPSHOT_LARGE,
    )


def test_build_verdict_accepts_object_context_and_validates():
    class Ctx:
        run_id = "r-obj"
        experiment = "exp"

    assert make_verdict(ctx_like=Ctx()).run_id == "r-obj"
    with pytest.raises(JudgeError):
        make_verdict(ctx_like={"run_id": "r"})
    with pytest.raises(JudgeError):
        make_verdict(generated_at=1.5)


def test_verdict_type_is_model_verdict():
    assert isinstance(make_verdict(), Verdict)


# ---------------------------------------------------------------- 종료 코드(설계 1.4)


def test_exit_code_for():
    assert exit_code_for(PASS) == 0
    assert exit_code_for(FAIL) == 10
    assert exit_code_for(INC) == 11
    assert exit_code_for("FAIL") == 10
    for bad in (NA, "N/A", "maybe", None):
        with pytest.raises(JudgeError):
            exit_code_for(bad)

"""주문 분류기 골든 테스트(설계 5.3, 8)."""

from pathlib import Path

import pytest

from orchestrator.judge import reasons
from orchestrator.judge.classify import (
    EXAMPLES_MAX,
    ack_path,
    classes_of,
    classify,
    count_classes,
    count_filled,
    count_subs,
    k_state,
    summarize,
)
from orchestrator.judge.criteria import integrity_cfg, load_criteria
from orchestrator.judge.model import (
    IntegrityCfg,
    KState,
    OkSub,
    OrderClass,
    ResendOutcome,
    canonical_json,
)

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_C,
    T0,
    make_attempts,
    make_db,
    make_fill,
    make_intent,
    make_log,
    make_order,
    make_result,
)
from .golden_cases import (
    CASES,
    CFG_CONFIRMED,
    CFG_UNCONFIRMED,
    CORE_METRICS,
    RETRY_FAIL,
)

REPO = Path(__file__).resolve().parents[3]

# 설계 8 "골든(최소 22건)" 목록 → 케이스 이름
REQUIRED = {
    "K_ACK×D1 일치": "ack_d1_match",
    "K_ACK×D1 불일치": "ack_d1_mismatch",
    "K_ACK×D0": "ack_d0_lost",
    "K_FOUND×D1 일치": "found_d1_match",
    "K_FOUND×D1 불일치": "found_d1_mismatch",
    "K_FOUND×D0": "found_d0_phantom",
    "K_404×D1 일치": "k404_d1_match",
    "K_404×D1 불일치": "k404_d1_mismatch",
    "K_404×D0": "k404_d0_not_entered",
    "K_UNKNOWN×D1 일치": "unknown_d1_committed",
    "K_UNKNOWN×D1 불일치": "unknown_d1_body_mismatch",
    "K_UNKNOWN×D0": "unknown_d0_absent",
    "K_4XX×D1 일치": "k4xx_d1_match",
    "K_4XX×D1 불일치": "k4xx_d1_mismatch",
    "K_4XX×D0": "k4xx_d0",
    "DOUBLE_CREATE": "double_create",
    "FIRST_200 conn_reuse 꺼짐": "first_200_conn_reuse_off_fail",
    "FIRST_200 conn_reuse 켜짐": "first_200_conn_reuse_on_inconclusive",
    "RESEND_DIFF": "resend_diff",
    "RESEND_UNRESOLVED": "resend_unresolved",
    "409_SAME_BODY": "same_body_409",
    "DUP_ORDER": "dup_order",
    "DUP_FILL": "dup_fill",
    "FILL_STATE": "fill_state_filled_without_fill",
    "ORPHAN 완전 기록": "orphan_complete_log",
    "ORPHAN 불완전 기록": "orphan_incomplete_log",
    "INTERRUPTED": "interrupted_no_row",
    "after_503": "after_503_warning",
    "응답 스키마 미확정": "response_schema_unconfirmed",
}


def run(log, db, cfg=CFG_CONFIRMED):
    return classify(
        log,
        db,
        cfg,
        account_ids=log.account_ids | db.k6_account_ids,
        extra_account_ids=frozenset(a.id for a in db.accounts if not a.k6),
    )


def row(res, key):
    rows = [c for c in res.classified if c.key == key]
    assert len(rows) == 1, [c.key for c in res.classified]
    return rows[0]


def test_golden_covers_design_list():
    names = [c.name for c in CASES]
    assert len(names) == len(set(names))
    missing = {k: v for k, v in REQUIRED.items() if v not in names}
    assert not missing
    assert len(CASES) >= 22


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_golden(case):
    log, db = case.build()
    res = run(log, db, case.cfg)
    r = row(res, case.key)

    assert r.cls == case.cls
    assert r.k_state == case.k_state
    assert r.sub == case.sub
    assert r.idem == case.idem
    assert r.resend == case.resend
    assert tuple(r.detail.get("also", ())) == tuple(x.value for x in case.also)
    for k, v in case.detail.items():
        assert r.detail.get(k) == v, k

    for name in classes_of(r):
        assert res.counts[name] >= 1
        assert case.key in res.examples[name]

    expected = {m: 0 for m in CORE_METRICS}
    expected.update(inconclusive={}, warnings=())
    expected.update(case.summary)
    got = summarize(res, log_complete=case.log_complete)
    for k, v in expected.items():
        assert got[k] == v, k

    for k, v in res.marks.items():
        assert v == case.marks.get(k, 0), k


# ---------------------------------------------------------------- 규칙별 보강


def test_body_mismatch_fails_even_when_response_unconfirmed():
    """본문 비교는 응답 필드 확정과 무관하다(설계 5.3)."""
    it = make_intent("k1", ACCT_A)
    o = make_order(ACCT_A, "k1", qty="0.00020000")
    log = make_log([it], [make_result("k1")])
    res = run(log, make_db(orders=[o], fills=[make_fill(o)]), CFG_UNCONFIRMED)
    r = row(res, "k1")
    assert r.cls == OrderClass.MISMATCH
    assert r.detail["body_diff"][o.id]["qty"] == ["0.00010000", "0.00020000"]
    assert summarize(res)["mismatch"] == 1


def test_row_in_other_account_is_acct_mismatch_not_orphan():
    it = make_intent("k1", ACCT_A)
    o = make_order(ACCT_B, "k1")
    log = make_log([it], [make_result("k1")], accounts=[ACCT_A, ACCT_B])
    res = run(log, make_db(orders=[o], fills=[make_fill(o)]))
    assert [c.key for c in res.classified] == ["k1"]
    r = row(res, "k1")
    assert r.cls == OrderClass.MISMATCH
    assert r.detail["body_diff"][o.id]["acct"] == [ACCT_A, ACCT_B]
    assert summarize(res)["orphans"] == 0


def test_dup_order_keeps_matrix_class_in_also():
    it = make_intent("k1", ACCT_A)
    rs = make_result("k1", atts=[(500, None, 40)], lookup=404)
    a = make_order(ACCT_A, "k1")
    b = make_order(ACCT_A, "k1", n=1)
    res = run(make_log([it], [rs]), make_db(orders=[a, b], fills=[make_fill(a)]))
    r = row(res, "k1")
    assert r.cls == OrderClass.DUP_ORDER
    assert r.detail["also"] == ["FALSE_NOT_FOUND", "FILL_STATE"]
    s = summarize(res)
    assert s["duplicates"] == 2 and s["lost"] == 1


def test_db_anomaly_is_also_on_non_ok_cell():
    it = make_intent("k1", ACCT_A)
    rs = make_result("k1", atts=RETRY_FAIL, lookup=0)
    res = run(make_log([it], [rs]), make_db(orders=[make_order(ACCT_A, "k1")]))
    r = row(res, "k1")
    assert r.cls == OrderClass.UNRESOLVED_COMMITTED
    assert r.detail["also"] == ["FILL_STATE"]
    assert r.detail["fill"] == [f"{r.order_ids[0]}:filled_without_fill"]


def test_double_create_combines_with_resend_diff_and_409():
    it = make_intent("k1", ACCT_A)
    rs = make_result("k1", resend=[(409, "IDEMPOTENCY_KEY_REUSED", 3), (201, None, 4)])
    o = make_order(ACCT_A, "k1")
    res = run(make_log([it], [rs]), make_db(orders=[o], fills=[make_fill(o)]))
    r = row(res, "k1")
    assert [v.value for v in r.idem] == ["DOUBLE_CREATE", "409_SAME_BODY"]
    assert r.resend == ResendOutcome.DOUBLE_CREATE
    assert summarize(res)["duplicates"] == 1  # 행 하나는 IDEMPOTENCY_VIOLATION 1건


def test_fill_without_order_row():
    o = make_order(ACCT_A, "k1")
    ghost = make_order(ACCT_A, "gone")
    log = make_log([make_intent("k1", ACCT_A)], [make_result("k1")])
    db = make_db(orders=[o], fills=[make_fill(o), make_fill(ghost)])
    res = run(log, db)
    ghost_rows = [c for c in res.classified if c.key.startswith("(fill-order")]
    assert len(ghost_rows) == 1
    assert ghost_rows[0].cls == OrderClass.FILL_STATE
    assert ghost_rows[0].order_ids == (ghost.id,)
    assert summarize(res)["duplicates"] == 1


def test_summarize_account_set_mismatch_marks_lost_and_orphans():
    log, db = CASES[0].build()
    s = summarize(run(log, db), account_set_ok=False)
    assert s["inconclusive"] == {
        "lost": (reasons.ACCOUNT_SET_MISMATCH,),
        "orphans": (reasons.ACCOUNT_SET_MISMATCH,),
    }


def test_counts_subs_resend_have_stable_keys():
    log, db = CASES[0].build()
    res = run(log, db)
    assert set(res.counts) == {c.value for c in OrderClass}
    assert set(res.subs) == {s.value for s in OkSub}
    assert set(res.resend) == {r.value for r in ResendOutcome}
    assert list(res.counts) == sorted(res.counts)
    assert res.counts["OK"] == 1 and sum(res.counts.values()) == 1
    assert res.subs["clean"] == 1


def test_examples_capped_and_sorted():
    keys = [f"k{i:02d}" for i in range(25)]
    its = [make_intent(k, ACCT_A) for k in reversed(keys)]
    rss = [make_result(k) for k in keys]
    res = run(make_log(its, rss), make_db(accounts=[ACCT_A]))
    assert res.counts["LOST"] == 25
    assert res.examples["LOST"] == tuple(keys[:EXAMPLES_MAX])
    assert set(res.examples) == {"LOST"}


def _mixed():
    """여러 계좌·클래스가 섞인 기록과 DB(결정성·구간 집계용)."""
    its, rss, orders, fills = [], [], [], []
    fid = 1

    def add(key, acct, atts, *, t, lookup=None, row=True, fill=True, side="buy"):
        nonlocal fid
        its.append(make_intent(key, acct, t=t, side=side))
        rss.append(make_result(key, atts=atts, lookup=lookup, t_start=t))
        if row:
            o = make_order(acct, key, received_ms=t + 5, side=side)
            orders.append(o)
            if fill:
                fills.append(make_fill(o, fill_id=fid))
                fid += 1

    add("a-clean", ACCT_A, [(201,)], t=T0)
    add("a-commit", ACCT_A, [(0, "EOF"), (200,)], t=T0 + 10_000)
    add("a-lookup", ACCT_A, RETRY_FAIL, t=T0 + 20_000, lookup=200)
    add("b-lost", ACCT_B, [(201,)], t=T0 + 30_000, row=False)
    add("b-unknown", ACCT_B, RETRY_FAIL, t=T0 + 40_000, lookup=0, row=False)
    add("c-commit", ACCT_C, [(502,), (200,)], t=T0 + 50_000, side="sell")
    add("c-nofill", ACCT_C, [(201,)], t=T0 + 60_000, fill=False)
    orders.append(make_order(ACCT_B, "b-orphan", received_ms=T0 + 70_000))
    fills.append(make_fill(orders[-1], fill_id=fid))
    its.append(make_intent("c-cut", ACCT_C, t=T0 + 80_000))
    return its, rss, orders, fills


def test_determinism_under_shuffled_input():
    its, rss, orders, fills = _mixed()
    accts = [ACCT_A, ACCT_B, ACCT_C]
    a = run(make_log(its, rss, accts), make_db(accts, orders, fills))
    b = run(
        make_log(its[::-1], rss[::-1], accts),
        make_db(accts, orders[::-1], fills[::-1]),
    )
    assert canonical_json(a) == canonical_json(b)
    assert summarize(a) == summarize(b)
    keys = [c.key for c in a.classified]
    assert keys == sorted(keys)
    assert canonical_json(a) == canonical_json(
        run(make_log(its, rss, accts), make_db(accts, orders, fills))
    )


def test_window_and_account_counts():
    its, rss, orders, fills = _mixed()
    accts = [ACCT_A, ACCT_B, ACCT_C]
    res = run(make_log(its, rss, accts), make_db(accts, orders, fills))
    s = summarize(res)
    assert (s["ok"], s["lost"], s["unresolved"], s["orphans"]) == (4, 1, 1, 1)
    assert (s["duplicates"], s["interrupted"], s["recovered_after_commit"]) == (1, 1, 3)
    assert s["intents"] == 8 and s["user_impact"] == 2
    assert s["intent_unresolved_ratio"] == 0.25

    subs = ["after_commit", "lookup"]
    assert count_subs(res, subs) == 3
    assert count_subs(res, subs, accounts=[ACCT_A]) == 2  # fault_effect account: idx0
    assert count_subs(res, subs, start=T0 + 15_000, end=T0 + 50_000) == 1
    assert count_classes(res, ["LOST", "ORPHAN"]) == 2
    assert count_classes(res, ["LOST", "ORPHAN"], end=T0 + 40_000) == 1
    assert count_classes(res, ["FILL_STATE"], accounts=[ACCT_C]) == 1
    assert count_classes(res, ["INTERRUPTED"], start=T0 + 80_000) == 1

    # FILLED 주문 수(설계 5.5): a-clean, a-commit, a-lookup, c-commit(매도), c-nofill, b-orphan
    assert count_filled(res) == 6
    assert count_filled(res, side="sell") == 1
    assert count_filled(res, accounts=[ACCT_A]) == 3
    assert count_filled(res, start=T0 + 50_000, end=T0 + 60_000) == 1


def test_default_criteria_cfg_classifies_clean_order():
    cfg = integrity_cfg(
        load_criteria(REPO / "experiments" / "criteria" / "default.yaml")
    )
    log, db = CASES[0].build()
    r = row(run(log, db, cfg), "k1")
    assert r.cls == OrderClass.OK and r.sub == OkSub.CLEAN
    assert r.detail["response"] == "match"


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_design_signature_matches_explicit_scope(case):
    """설계 5.1 classify(log, db, cfg) 형태: 계좌 범위 기본값이 명시 호출과 같다."""
    log, db = case.build()
    assert classify(log, db, case.cfg) == run(log, db, case.cfg)


def test_default_cfg_still_compares_response():
    """IntegrityCfg() 기본 매핑(설계 6)으로도 응답 대 DB를 비교한다. 빈 매핑이면 비교가 사라진다."""
    by_name = {c.name: c for c in CASES}
    log, db = by_name["response_schema_unconfirmed"].build()
    r = row(classify(log, db, IntegrityCfg()), "k1")
    assert r.cls == OrderClass.RESPONSE_SCHEMA_UNCONFIRMED
    assert r.detail["response_diff"] == {"fee": ["999", "5"]}


def test_k_state_table():
    def rs(atts, lookup=None):
        return make_result("k", atts=atts, lookup=lookup)

    assert k_state(None) == KState.INTERRUPTED
    assert k_state(rs([(0,), (201,)])) == KState.K_ACK
    assert k_state(rs([(503,), (409,)])) == KState.K_4XX
    assert k_state(rs([(500,)], lookup=200)) == KState.K_FOUND
    assert k_state(rs([(500,)], lookup=404)) == KState.K_404
    assert k_state(rs([(500,)], lookup=0)) == KState.K_UNKNOWN
    assert k_state(rs(RETRY_FAIL)) == KState.K_UNKNOWN


@pytest.mark.parametrize(
    ("sts", "path"),
    [
        ([201], "clean"),
        ([200], "first_200"),
        ([0, 200], "after_commit"),
        ([502, 200], "after_commit"),
        ([504, 200], "after_commit"),
        ([0, 503, 200], "after_commit"),
        ([503, 200], "after_503"),
        ([503, 503, 200], "after_503"),
        ([0, 201], "not_committed"),
        ([503, 201], "not_committed"),
        ([0, 0, 0, 0], None),
    ],
)
def test_ack_path(sts, path):
    assert ack_path(make_attempts([(s,) for s in sts])) == path

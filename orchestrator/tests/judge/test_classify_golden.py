"""주문 분류기 골든 테스트(계약 명세 3–5, 10, 설계 5.3)."""

from __future__ import annotations

import ast
import json
import random
from dataclasses import replace
from pathlib import Path

import pytest

from orchestrator.judge import reasons
from orchestrator.judge.classify import (
    EXAMPLES_MAX,
    MARK_KEYS,
    ack_path,
    classes_of,
    classify,
    count_classes,
    count_filled,
    count_subs,
    k_state,
    run_order_count,
    summarize,
)
from orchestrator.judge.criteria import integrity_cfg, load_criteria
from orchestrator.judge.model import (
    DEFAULT_RESPONSE_FIELDS,
    IdemViolation,
    IntegrityCfg,
    KState,
    OkSub,
    OrderClass,
    Result,
    canonical_json,
    iso_us,
)

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_C,
    ACCT_EXTRA,
    FILLED_US,
    RUN_ID,
    T0,
    att,
    default_body,
    make_attempts,
    make_db,
    make_intent,
    make_log,
    make_order,
    make_result,
    response_body,
    run_key,
    vu_of,
)
from .golden_cases import (
    CASES,
    CFG,
    CORE_METRICS,
    FILLED_AT_MS,
    GW502,
    K1,
    K2,
    ORDER_TO_DICT_FIELDS,
    REJECTED_BODY_FIELDS,
    TIMEOUT,
    ack,
    iso_at,
    not_ready,
    rej,
    service_json,
)

REPO = Path(__file__).resolve().parents[3]
ORDER_APP = REPO / "workloads" / "order" / "app"

# 계약 명세 10 골든 목록 → 케이스 이름
REQUIRED = {
    "K_ACK×D1 일치": "ack_d1_match",
    "K_ACK×D1 불일치": "ack_d1_mismatch",
    "K_ACK×D0": "ack_d0_lost",
    "K_REJECT×D1 일치": "reject_d1_match",
    "K_REJECT×D1 불일치": "reject_d1_body_mismatch",
    "K_REJECT×D0": "reject_d0_rejected",
    "K_UNKNOWN×D1 일치": "unknown_d1_committed",
    "K_UNKNOWN×D1 불일치": "unknown_d1_body_mismatch",
    "K_UNKNOWN×D0": "unknown_d0_absent",
    "INTERRUPTED×D1 일치(커밋)": "interrupted_d1_committed",
    "INTERRUPTED×D1 불일치": "interrupted_d1_body_mismatch",
    "INTERRUPTED×D0(미커밋)": "interrupted_d0",
    "OK clean": "ack_d1_match",
    "OK after_commit": "after_commit",
    "OK not_committed": "not_committed",
    "OK after_503": "after_503_warning",
    "OK rejected": "reject_d0_rejected",
    "BODY_MISMATCH": "ack_body_mismatch",
    "DOUBLE_CREATE": "double_create",
    "FIRST_REPLAYED": "first_replayed",
    "RESEND_DIFF": "resend_diff",
    "409_SAME_BODY": "same_body_409",
    "DUP_ORDER": "dup_order",
    "FILL_STATE": "fill_state_null_amount",
    "ORPHAN": "orphan_complete_log",
    "PROTOCOL": "protocol_status_201",
    "503 stale_price 뒤 재시도 체결": "stale_price_retry_filled",
    "422 거절 D0": "reject_d0_rejected",
    "응답 원문 못 읽음": "response_unparsed_committed",
}
BY_NAME = {c.name: c for c in CASES}


def run(log, db, cfg=CFG, extra=()):
    return classify(
        log,
        db,
        cfg,
        account_ids=log.account_ids,
        extra_account_ids=frozenset(extra),
    )


def row(res, key):
    rows = [c for c in res.classified if c.key == key]
    assert len(rows) == 1, [c.key for c in res.classified]
    return rows[0]


def run_case(case):
    log, db = case.build()
    return run(log, db, case.cfg, case.extra)


# ---------------------------------------------------------------- 골든


def test_golden_covers_spec_list():
    names = [c.name for c in CASES]
    assert len(names) == len(set(names))
    missing = {k: v for k, v in REQUIRED.items() if v not in names}
    assert not missing
    assert len(CASES) >= 22


def test_golden_covers_every_enum_value():
    """모든 K·클래스·OK 세부·멱등 위반이 골든 어딘가에 나온다."""
    ks, classes, subs, idem = set(), set(), set(), set()
    for case in CASES:
        r = row(run_case(case), case.key)
        ks.add(r.k_state)
        classes.update(classes_of(r))
        subs.add(r.sub)
        idem.update(r.idem)
    assert ks >= set(KState)
    assert classes == {c.value for c in OrderClass}
    assert subs - {None} == set(OkSub)
    assert idem == set(IdemViolation)


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_golden(case):
    res = run_case(case)
    r = row(res, case.key)

    assert r.cls == case.cls
    assert r.k_state == case.k_state
    assert r.sub == case.sub
    assert r.idem == case.idem
    assert tuple(r.detail.get("also", ())) == tuple(x.value for x in case.also)
    for k, v in case.detail.items():
        assert r.detail.get(k) == v, k

    for name in classes_of(r):
        assert res.counts[name] >= 1
        assert case.key in res.examples[name]

    expected = {m: 0 for m in CORE_METRICS}
    expected.update(
        ok=1 if case.cls == OrderClass.OK else 0, inconclusive={}, warnings=()
    )
    expected.update(case.summary)
    got = summarize(res, log_complete=case.log_complete)
    for k, v in expected.items():
        assert got[k] == v, k

    for k, v in res.marks.items():
        assert v == case.marks.get(k, 0), k


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_result_is_json_and_deterministic(case):
    a, b = run_case(case), run_case(case)
    assert a == b
    assert canonical_json(a) == canonical_json(b)


@pytest.mark.parametrize(
    "case",
    [c for c in CASES if not c.extra],
    ids=[c.name for c in CASES if not c.extra],
)
def test_default_scope_matches_explicit_scope(case):
    """classify(log, db, cfg): 계좌 범위 기본값(intent 계좌, extra 없음)이 명시 호출과 같다."""
    log, db = case.build()
    assert classify(log, db, case.cfg) == run(log, db, case.cfg)


# ---------------------------------------------------------------- 입력 순서·JSON 키 순서


def _rekey_raws(log, order):
    """result 시도 원문의 JSON 키 순서를 order(keys → keys)로 바꾼다. 못 읽는 원문은 그대로 둔다."""

    def rekey(raw):
        try:
            obj = json.loads(raw)
        except (TypeError, ValueError):
            return raw
        if not isinstance(obj, dict):
            return raw
        keys = order(list(obj))
        return json.dumps({k: obj[k] for k in keys}, separators=(",", ":"))

    results = {
        k: replace(r, atts=tuple(replace(a, raw=rekey(a.raw)) for a in r.atts))
        for k, r in log.results.items()
    }
    return replace(log, results=results)


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_raw_key_order_does_not_change_result(case):
    log, db = case.build()
    base = canonical_json(run(log, db, case.cfg, case.extra))
    for order in (sorted, lambda ks: ks[::-1]):
        got = run(_rekey_raws(log, order), db, case.cfg, case.extra)
        assert canonical_json(got) == base


def _spec(tok, key, acct, body, filled_us):
    """혼합 시나리오 시도 토큰 → 시도 명세(응답은 그 키의 DB 행과 맞게 만든다)."""
    if tok in ("ack", "replay", "ack_null_amount"):
        kw = {"amount": None} if tok == "ack_null_amount" else {}
        return ack(
            tok == "replay", key=key, acct=acct, body=body, filled_us=filled_us, **kw
        )
    if tok == "not_ready":
        return not_ready(key)
    if tok == 422:
        return rej(422, "insufficient_balance", key=key)
    return {"timeout": TIMEOUT, 502: GW502}[tok]


def _mixed():
    """여러 계좌·클래스가 섞인 기록과 DB(결정성·구간 집계용). (intents, results, orders)."""
    its, rss, orders = [], [], []

    def add(acct, seq, toks, *, t, row=True, side="bid", **order_kw):
        key = run_key(seq, vu=vu_of(acct))
        body = default_body(key, acct, side=side)
        filled_us = (t + 5) * 1000 + 123
        its.append(make_intent(key, acct, t=t, body=body))
        specs = [_spec(x, key, acct, body, filled_us) for x in toks]
        rss.append(make_result(key, specs, acct=acct, t_start=t + 1))
        if row:
            orders.append(
                make_order(acct, key, body=body, filled_us=filled_us, **order_kw)
            )

    add(ACCT_A, 1, ["ack"], t=T0)
    add(ACCT_A, 2, ["timeout", "replay"], t=T0 + 10_000)
    add(ACCT_A, 3, ["not_ready", "ack"], t=T0 + 20_000)
    add(ACCT_B, 1, ["ack"], t=T0 + 30_000, row=False)
    add(ACCT_B, 2, ["timeout"] * 4, t=T0 + 40_000, row=False)
    add(ACCT_C, 1, [502, "replay"], t=T0 + 50_000, side="ask")
    add(ACCT_C, 2, ["ack_null_amount"], t=T0 + 60_000, amount=None)
    add(ACCT_A, 4, [422], t=T0 + 90_000, row=False)
    its.append(make_intent(run_key(3, vu=vu_of(ACCT_C)), ACCT_C, t=T0 + 80_000))
    orphan_us = (T0 + 70_000) * 1000
    orders.append(make_order(ACCT_B, run_key(9, vu=vu_of(ACCT_B)), filled_us=orphan_us))
    orders.append(make_order(ACCT_EXTRA, "presenter-1"))  # 정상 발표자 주문
    orders.append(make_order(ACCT_A, "manual-1", order_id=900))  # run 키가 아닌 주문
    return its, rss, orders


def _mixed_db(orders):
    # 분류는 positions를 보지 않는다(C가 사기 전에 판다)
    return make_db(orders=orders, positions=[], extra_accounts=[ACCT_EXTRA])


def _mixed_result(its=None, rss=None, orders=None, cfg=CFG):
    base = _mixed()
    its = base[0] if its is None else its
    rss = base[1] if rss is None else rss
    orders = base[2] if orders is None else orders
    db = replace(_mixed_db(base[2]), orders=tuple(orders))
    return run(make_log(its, rss), db, cfg, extra=(ACCT_EXTRA,))


@pytest.mark.parametrize("seed", range(6))
def test_shuffled_lines_and_raw_keys_do_not_change_result(seed):
    """JSONL 줄 순서, DB 행 순서, 응답 원문 키 순서를 섞어도 결과가 같다."""
    base = _mixed_result()
    rng = random.Random(seed)
    its, rss, orders = (list(x) for x in _mixed())
    rng.shuffle(its)
    rng.shuffle(rss)
    rng.shuffle(orders)
    log = make_log(its, rss)
    log = _rekey_raws(log, lambda ks: rng.sample(ks, len(ks)))
    db = replace(_mixed_db(orders), orders=tuple(orders))
    got = run(log, db, CFG, extra=(ACCT_EXTRA,))
    assert canonical_json(got) == canonical_json(base)
    assert summarize(got) == summarize(base)
    keys = [c.key for c in got.classified]
    assert keys == sorted(keys)


def test_window_and_account_counts():
    res = _mixed_result()
    s = summarize(res)
    assert [c.key for c in res.classified] == sorted(
        [run_key(i, vu=1) for i in (1, 2, 3, 4)]
        + [run_key(i, vu=2) for i in (1, 2, 9)]
        + [run_key(i, vu=3) for i in (1, 2, 3)]
    )  # 발표자·run 키 아닌 정상 주문은 분류하지 않는다
    assert (s["ok"], s["lost"], s["unresolved"], s["orphans"]) == (5, 1, 1, 1)
    assert (s["duplicates"], s["interrupted"], s["recovered_after_commit"]) == (1, 1, 2)
    assert s["rejected"] == 1
    assert s["intents"] == 9 and s["user_impact"] == 2
    assert s["intent_unresolved_ratio"] == round(2 / 9, 6)

    assert count_subs(res, ["after_commit"]) == 2
    assert count_subs(res, ["after_commit"], accounts=[ACCT_A]) == 1  # account: idx0
    assert count_subs(res, ["after_commit"], start=T0 + 15_000, end=T0 + 55_000) == 1
    assert count_subs(res, ["not_committed", "after_503"]) == 1
    assert count_classes(res, ["LOST", "ORPHAN"]) == 2
    assert count_classes(res, ["LOST", "ORPHAN"], end=T0 + 40_000) == 1
    assert count_classes(res, ["FILL_STATE"], accounts=[ACCT_C]) == 1
    assert count_classes(res, ["INTERRUPTED"], start=T0 + 80_000) == 1

    # filled 주문(설계 5.5): A1, A2, A3, C1(매도), C2(amount NULL), 고아
    assert count_filled(res) == 6
    assert count_filled(res, side="ask") == 1
    assert count_filled(res, side="bid") == 5
    assert count_filled(res, accounts=[ACCT_A]) == 3
    assert count_filled(res, start=T0 + 50_000, end=T0 + 60_000) == 1


def test_run_order_count_matches_snapshot():
    """분류기 D1 수 = 스냅샷 run 키 주문 수(계약 명세 7 judge_self_check)."""
    res = _mixed_result()
    db = _mixed_db(_mixed()[2])
    assert run_order_count(res, RUN_ID) == len(db.run_orders(RUN_ID)) == 6


def test_examples_capped_and_sorted():
    keys = [run_key(i) for i in range(1, 26)]
    its = [make_intent(k, ACCT_A) for k in reversed(keys)]
    rss = [make_result(k, [ack(key=k)]) for k in keys]
    res = run(make_log(its, rss), make_db(accounts=[ACCT_A]))
    assert res.counts["LOST"] == 25
    assert res.examples["LOST"] == tuple(sorted(keys)[:EXAMPLES_MAX])
    assert set(res.examples) == {"LOST"}


def test_counts_subs_idem_marks_have_stable_keys():
    res = run_case(BY_NAME["ack_d1_match"])
    assert list(res.counts) == sorted(c.value for c in OrderClass)
    assert list(res.subs) == sorted(s.value for s in OkSub)
    assert list(res.idem) == sorted(v.value for v in IdemViolation)
    assert list(res.marks) == sorted(MARK_KEYS)
    assert res.counts["OK"] == 1 and sum(res.counts.values()) == 1
    assert res.subs["clean"] == 1 and res.intents == 1


def test_summarize_account_set_mismatch_marks_lost_and_orphans():
    s = summarize(run_case(BY_NAME["ack_d1_match"]), account_set_ok=False)
    assert s["inconclusive"] == {
        "lost": (reasons.ACCOUNT_SET_MISMATCH,),
        "orphans": (reasons.ACCOUNT_SET_MISMATCH,),
    }


def test_default_criteria_cfg_classifies_clean_order():
    cfg = integrity_cfg(
        load_criteria(REPO / "experiments" / "criteria" / "default.yaml")
    )
    assert cfg.response_fields == DEFAULT_RESPONSE_FIELDS
    log, db = BY_NAME["ack_d1_match"].build()
    r = row(run(log, db, cfg), K1)
    assert r.cls == OrderClass.OK and r.sub == OkSub.CLEAN
    assert r.detail["response"] == "match"


# ---------------------------------------------------------------- K와 경로(계약 명세 3, 4)


def test_k_state_table():
    def rs(*specs):
        return make_result("k", list(specs))

    assert k_state(None) == KState.INTERRUPTED
    assert k_state(rs(0, 200)) == KState.K_ACK
    assert k_state(rs(200, 0)) == KState.K_ACK  # ACK가 하나라도 있으면
    assert k_state(rs(0, (503, "stale_price"))) == KState.K_REJECT
    assert k_state(rs(400)) == KState.K_REJECT
    assert k_state(rs(404)) == KState.K_REJECT
    assert k_state(rs(409)) == KState.K_REJECT
    assert k_state(rs((503, "price_unavailable"))) == KState.K_REJECT
    assert k_state(rs(0, 0)) == KState.K_UNKNOWN
    assert k_state(rs((503, "not_ready"))) == KState.K_UNKNOWN
    assert k_state(rs(503)) == KState.K_UNKNOWN  # reason 없는 503
    assert k_state(rs(500)) == KState.K_UNKNOWN
    assert k_state(rs(422, 0)) == KState.K_UNKNOWN  # 마지막 시도가 결과 불명
    assert k_state(rs(201)) == KState.K_UNKNOWN  # 마지막 시도가 PROTOCOL
    assert k_state(rs(att(200, raw="not json"))) == KState.K_UNKNOWN
    assert k_state(Result("k", ())) == KState.K_UNKNOWN


@pytest.mark.parametrize(
    ("specs", "path"),
    [
        ([att(200, rp=False)], "clean"),
        ([att(200, rp=True)], "first_replayed"),
        ([0, att(200, rp=True)], "after_commit"),
        ([502, att(200, rp=True)], "after_commit"),
        ([504, att(200, rp=True)], "after_commit"),
        (
            [503, att(200, rp=True)],
            "after_commit",
        ),  # reason 없는 503은 not_ready가 아니다
        ([(503, "not_ready"), att(200, rp=True)], "after_503"),
        ([(503, "not_ready"), (503, "not_ready"), att(200, rp=True)], "after_503"),
        ([(503, "not_ready"), 0, att(200, rp=True)], "after_commit"),
        ([0, att(200, rp=False)], "not_committed"),
        ([(503, "not_ready"), att(200, rp=False)], "not_committed"),
        ([(503, "stale_price"), att(200, rp=False)], "not_committed"),
        ([(503, "stale_price"), att(200, rp=True)], "first_replayed"),
        ([422, 0, att(200, rp=True)], "after_commit"),
        ([201, att(200, rp=True)], "after_commit"),  # PROTOCOL 시도도 커밋됐을 수 있다
        ([0, 0, 0, 0], None),
        ([att(200, rp=None)], None),  # 원문 replayed도 null
    ],
)
def test_ack_path(specs, path):
    assert ack_path(make_attempts(specs, "k")) == path


def _overlap(specs, key="k"):
    """같은 키 동시 전송 흉내: 모든 시도가 같은 t0에 시작하고 n 순서로 끝난다."""
    atts = make_attempts(specs, key)
    t0 = atts[0].t0
    return tuple(replace(a, t0=t0, t1=t0 + 10 * a.n) for a in atts)


@pytest.mark.parametrize(
    ("atts", "path"),
    [
        (_overlap([att(200, rp=True), att(200, rp=False)]), "after_commit"),
        (_overlap([att(200, rp=True), 0]), "after_commit"),
        (_overlap([att(200, rp=True), att(200, rp=True)]), "first_replayed"),
        (
            _overlap([att(200, rp=True), (422, "insufficient_balance")]),
            "first_replayed",
        ),
        (_overlap([att(200, rp=False), att(200, rp=True)]), "clean"),
        # 겹치지 않으면(순차, k6) 뒤 시도는 보지 않는다
        (make_attempts([att(200, rp=True), att(200, rp=False)], "k"), "first_replayed"),
    ],
)
def test_ack_path_overlapping_attempts(atts, path):
    """동시 전송(서비스 ④)에서 t0 순서 n=1이 진 쪽이면, 겹친 시도가 만든 것이라 FIRST_REPLAYED가 아니다."""
    assert ack_path(atts) == path


def test_concurrent_same_key_loser_first_is_ok():
    """실서비스 e2e에서 본 모양: 진 시도(replayed=true)가 n=1, 이긴 시도가 n=2. 위반 아님."""
    atts = _overlap([att(200, rp=True), att(200, rp=False), att(200, rp=True)], K1)
    r = row(_single(atts, [make_order(ACCT_A, K1)]), K1)
    assert (r.cls, r.sub, r.idem) == (OrderClass.OK, OkSub.AFTER_COMMIT, ())


# ---------------------------------------------------------------- 규칙별 보강


def _single(atts, orders, *, intent=None, cfg=CFG):
    it = intent if intent is not None else make_intent(K1, ACCT_A)
    log = make_log([it], [make_result(K1, atts)])
    return run(log, make_db(orders=orders), cfg)


def test_dup_order_with_double_create():
    a = make_order(ACCT_A, K1)
    b = make_order(ACCT_A, K1, n=1)
    res = _single([ack(), ack(order_id=b.id)], [a, b])
    r = row(res, K1)
    assert r.cls == OrderClass.DUP_ORDER
    assert r.idem == (IdemViolation.DOUBLE_CREATE, IdemViolation.RESEND_DIFF)
    assert classes_of(r) == ("DUP_ORDER", "IDEMPOTENCY_VIOLATION")
    assert r.order_ids == (a.id, b.id)
    assert r.detail["response"] == "match"  # 첫 ACK order_id = a
    assert summarize(res)["duplicates"] == 2  # 한 행이 두 클래스를 센다


def test_response_compared_with_row_named_by_order_id():
    a = make_order(ACCT_A, K1)
    b = make_order(ACCT_A, K1, n=1, fill_price="100000001")
    r = row(_single([ack(order_id=b.id, fill_price="100000001")], [a, b]), K1)
    assert r.cls == OrderClass.DUP_ORDER and "also" not in r.detail
    assert r.detail["response"] == "match"


def test_reject_after_ack_is_resend_diff():
    """이미 있는 키의 재요청은 replay여야 한다. 확정 거절은 원 응답과 다른 결과다."""
    r = row(
        _single([ack(), rej(422, "insufficient_balance")], [make_order(ACCT_A, K1)]), K1
    )
    assert r.k_state == KState.K_ACK
    assert r.cls == OrderClass.IDEMPOTENCY_VIOLATION
    assert r.idem == (IdemViolation.RESEND_DIFF,)
    assert r.detail["resend_diff"] == {"2": {"st": [200, 422]}}


def test_resend_after_ack_with_unknown_attempts_is_ok():
    """재요청 검사(설계 4.4): ACK 뒤 같은 키로 다시 보내 결과 불명 → replay. 위반 아님."""
    res = _single([ack(), TIMEOUT, ack(True)], [make_order(ACCT_A, K1)])
    r = row(res, K1)
    assert (r.cls, r.sub, r.idem) == (OrderClass.OK, OkSub.CLEAN, ())


def test_intent_key_mismatch_matches_db_by_body_key():
    """intent.key ≠ body.client_order_id. 서비스는 본문 키로 저장하므로 본문 키로 DB를 찾는다.

    k6 기록 결함은 PROTOCOL(INCONCLUSIVE 쪽)로만 남고, 정상 체결이 LOST·ORPHAN으로
    세어져 FAIL(INTEGRITY_VIOLATION)이 되지 않는다(검증된 지적 #2).
    """
    k3 = run_key(3)
    it = make_intent(K1, ACCT_A, body=default_body(k3, ACCT_A))
    res = _single([ack(key=k3)], [make_order(ACCT_A, k3)], intent=it)
    r = row(res, K1)
    assert r.cls == OrderClass.PROTOCOL
    assert "also" not in r.detail and r.detail["protocol"] == ["intent_key"]
    assert r.detail["db_key"] == k3 and r.detail["db_rows"] == 1
    assert [c.key for c in res.classified] == [
        K1
    ]  # k3 행이 ORPHAN으로 따로 생기지 않는다
    assert res.counts[OrderClass.LOST] == 0 and res.counts[OrderClass.ORPHAN] == 0


def test_rp_conflict_is_protocol_and_raw_wins():
    raw = service_json(response_body(K1, replayed=True), ORDER_TO_DICT_FIELDS)
    r = row(_single([att(200, rp=False, raw=raw)], [make_order(ACCT_A, K1)]), K1)
    assert r.cls == OrderClass.PROTOCOL
    assert r.detail["protocol"] == ["att1:rp_conflict"]
    assert r.idem == (IdemViolation.FIRST_REPLAYED,)


def test_result_without_attempts_is_protocol():
    log = make_log([make_intent(K1, ACCT_A)], [Result(K1, ())])
    r = row(run(log, make_db(orders=[make_order(ACCT_A, K1)])), K1)
    assert (r.k_state, r.cls) == (KState.K_UNKNOWN, OrderClass.PROTOCOL)
    assert r.detail["also"] == ["UNRESOLVED_COMMITTED"]
    assert r.detail["protocol"] == ["atts_empty"]


def test_attempt_numbers_out_of_order_is_protocol():
    atts = make_attempts([0, 200], K1)
    bad = (replace(atts[0], n=2), replace(atts[1], n=1))
    log = make_log([make_intent(K1, ACCT_A)], [Result(K1, bad)])
    r = row(run(log, make_db(orders=[make_order(ACCT_A, K1)])), K1)
    assert r.cls == OrderClass.PROTOCOL and "att_order" in r.detail["protocol"]


def test_400_reject_has_null_client_order_id():
    """400 invalid_request 본문은 client_order_id가 null이다(main.invalid_request). 확정 거절이다."""
    spec = rej(400, "invalid_request")
    assert json.loads(spec["raw"])["client_order_id"] is None
    r = row(_single([spec], []), K1)
    assert (r.k_state, r.cls, r.sub) == (KState.K_REJECT, OrderClass.OK, OkSub.REJECTED)


def test_body_mismatch_ignores_volume_notation():
    """보낸 volume "0.0001"과 DB "0.00010000"은 같다(Decimal 비교)."""
    body = default_body(K1, ACCT_A, volume="0.0001")
    res = _single([ack()], [make_order(ACCT_A, K1)], intent=make_intent(K1, body=body))
    assert row(res, K1).cls == OrderClass.OK


def test_body_mismatch_fails_even_when_response_unconfirmed():
    """본문 비교는 응답 필드 확정과 무관하다(계약 명세 4)."""
    body = default_body(K1, ACCT_A, volume="0.00020000")
    res = _single(
        [ack(body=body)],
        [make_order(ACCT_A, K1, body=body)],
        cfg=IntegrityCfg(response_fields_confirmed=False),
    )
    r = row(res, K1)
    assert r.cls == OrderClass.BODY_MISMATCH
    assert summarize(res)["mismatch"] == 1


def test_response_missing_field_is_mismatch():
    r = row(_single([ack(drop=("fill_price",))], [make_order(ACCT_A, K1)]), K1)
    assert r.cls == OrderClass.MISMATCH
    assert r.detail["response_diff"] == {"fill_price": [None, "100000000.00000000"]}


@pytest.mark.parametrize(
    "filled_at, response",
    [
        (iso_at(FILLED_US), "match"),  # 서비스 isoformat(µs, +00:00)
        (FILLED_AT_MS, "match"),  # 통일본(ms, Z). DB 값은 ms까지 내림
        (iso_at(FILLED_US, hours=9, timespec="milliseconds"), "match"),  # 다른 시간대
        # 초까지(isoformat은 µs가 0이면 소수부를 뺀다)
        (iso_at(FILLED_US, timespec="seconds"), "match"),
        (iso_at(FILLED_US).replace("+00:00", "000Z"), "match"),  # 9자리는 µs까지
        (iso_at(FILLED_US + 1), "mismatch"),  # µs 응답은 µs까지 본다
        (iso_at(FILLED_US + 1000, timespec="milliseconds"), "mismatch"),
        (iso_at(FILLED_US - 1_000_000, timespec="seconds"), "mismatch"),
        (None, "mismatch"),  # null은 값이 없는 것이다
        ("", "mismatch"),
        ("06/11/2026 09:00", "unparsed"),
        (iso_at(FILLED_US)[:-6], "unparsed"),  # 시간대 없음
        # 초가 없어 자릿수를 모른다
        (iso_at(FILLED_US, timespec="minutes"), "unparsed"),
        (1.5, "unparsed"),  # 문자열이 아니다
    ],
)
def test_response_filled_at_compared_to_response_precision(filled_at, response):
    """계약 명세 11: filled_at은 두 형식을 모두 읽고 응답 자릿수까지만 DB 값을 내림해 비교한다.

    못 읽으면 불일치가 아니라 response_unparsed(PASS만 막는 불확실성)다.
    """
    b = response_body(K1)
    b["filled_at"] = filled_at
    res = _single([att(200, rp=False, raw=service_json(b))], [make_order(ACCT_A, K1)])
    r = row(res, K1)
    unparsed = response == "unparsed"
    assert r.detail["response"] == response
    if response == "mismatch":
        assert r.cls == OrderClass.MISMATCH
        assert r.detail["response_diff"]["filled_at"][1] == iso_us(FILLED_US)
    else:
        assert (r.cls, r.sub) == (OrderClass.OK, OkSub.CLEAN)
        assert "response_diff" not in r.detail
    assert r.detail.get("response_unparsed") == (["filled_at"] if unparsed else None)
    assert res.marks["response_unparsed"] == int(unparsed)
    inconclusive = summarize(res)["inconclusive"]
    assert inconclusive == ({"mismatch": (reasons.NOT_EVALUATED,)} if unparsed else {})


def test_unreadable_filled_at_does_not_hide_other_mismatch():
    """시각을 못 읽어도 다른 필드가 다르면 MISMATCH다(못 읽은 필드는 함께 남긴다)."""
    b = response_body(K1, amount="1.0000")
    b["filled_at"] = "06/11/2026 09:00"
    res = _single([att(200, rp=False, raw=service_json(b))], [make_order(ACCT_A, K1)])
    r = row(res, K1)
    assert r.cls == OrderClass.MISMATCH
    assert r.detail["response"] == "mismatch"
    assert set(r.detail["response_diff"]) == {"amount"}
    assert r.detail["response_unparsed"] == ["filled_at"]


def test_custom_response_field_mapping():
    """서비스가 필드 이름을 바꾸면 매핑으로 따라간다. RESEND_DIFF도 같은 이름을 쓴다."""
    fields = {k: v for k, v in DEFAULT_RESPONSE_FIELDS.items() if k != "fill_price"}
    cfg = IntegrityCfg(response_fields={**fields, "price": "fill_price"})

    def renamed(rp, price):
        b = response_body(K1, replayed=rp)
        b.pop("fill_price")
        b["price"] = price
        return att(200, rp=rp, raw=service_json(b))

    first = renamed(False, "100000000.00000000")
    r = row(_single([first], [make_order(ACCT_A, K1)], cfg=cfg), K1)
    assert (r.cls, r.detail["response"]) == (OrderClass.OK, "match")
    later = renamed(True, "1")
    r = row(_single([first, later], [make_order(ACCT_A, K1)], cfg=cfg), K1)
    assert r.idem == (IdemViolation.RESEND_DIFF,)
    assert r.detail["resend_diff"] == {"2": {"price": ["100000000.00000000", "1"]}}


def test_rows_outside_run_scope_are_not_classified():
    other_run = run_key(1, run_id=RUN_ID + "-2")  # k6-r-test-2-1-1: 다른 run의 키
    orders = [
        make_order(ACCT_A, K1),
        make_order(ACCT_A, "manual-1", order_id=500),
        make_order(ACCT_A, other_run, order_id=501),
        make_order(ACCT_B, "someone-1", order_id=502, status="pending"),  # 범위 밖
    ]
    res = _single([ack()], orders)
    assert [c.key for c in res.classified] == [K1]


def test_anomalies_in_run_and_extra_accounts_outside_run_keys():
    orders = [
        make_order(ACCT_A, K1),
        make_order(ACCT_A, "manual-2", order_id=500, status="pending"),
        make_order(ACCT_EXTRA, "presenter-2", order_id=501, amount=None),
    ]
    log = make_log([make_intent(K1, ACCT_A)], [make_result(K1, [ack()])])
    res = run(log, make_db(orders=orders), extra=(ACCT_EXTRA,))
    a = row(res, "manual-2")
    assert (a.cls, a.k_state, a.detail["scope"]) == (
        OrderClass.FILL_STATE,
        None,
        "account",
    )
    assert a.detail["fill"] == ["500:status_pending"]
    e = row(res, "presenter-2")
    assert (e.cls, e.detail["scope"]) == (OrderClass.FILL_STATE, "extra")
    assert summarize(res)["duplicates"] == 2
    assert count_filled(res) == 1  # 범위 밖 계좌 행은 이번 run 주문이 아니다


def test_orphan_in_other_user_is_flagged_foreign():
    log = make_log([make_intent(K1, ACCT_A)], [make_result(K1, [ack()])])
    orphan = make_order(ACCT_C, run_key(1, vu=vu_of(ACCT_C)))
    res = run(log, make_db(orders=[make_order(ACCT_A, K1), orphan]))
    r = row(res, orphan.client_order_id)
    assert r.cls == OrderClass.ORPHAN and r.detail["foreign_account"] is True


def test_orphan_needs_full_run_key_format():
    """접두어만 같은 다른 run(r-test-2)의 키는 고아가 아니다(계약 명세 4, model.run_key_re)."""
    log = make_log([], [], run_id="r-test")
    key = run_key(1, run_id="r-test-2")
    res = run(log, make_db(orders=[make_order(ACCT_A, key)]))
    assert res.classified == ()


def test_double_create_needs_different_order_ids():
    r = row(_single([ack(), ack()], [make_order(ACCT_A, K1)]), K1)
    assert r.idem == ()  # rp=false 두 번이지만 같은 order_id


def test_409_after_ack_counts_both():
    r = row(
        _single([ack(), rej(409, "idempotency_conflict")], [make_order(ACCT_A, K1)]), K1
    )
    assert r.idem == (IdemViolation.RESEND_DIFF, IdemViolation.SAME_BODY_409)


# ---------------------------------------------------------------- 서비스 계약 모양


def _returned_dict_keys(path: Path, func: str) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func:
            for sub in ast.walk(node):
                if isinstance(sub, ast.Return) and isinstance(sub.value, ast.Dict):
                    return [k.value for k in sub.value.keys]  # type: ignore[union-attr]
    raise AssertionError(f"{path.name}:{func} 반환 dict를 찾지 못했다")


def test_golden_raw_shape_matches_service_source():
    """골든 원문 모양 = 서비스 소스(order_to_dict, rejected_body). 서비스가 바뀌면 여기서 드러난다."""
    schemas, main = ORDER_APP / "schemas.py", ORDER_APP / "main.py"
    if not schemas.exists() or not main.exists():
        pytest.skip("workloads/order가 없다")
    keys = _returned_dict_keys(schemas, "order_to_dict")
    assert tuple(keys) == ORDER_TO_DICT_FIELDS
    assert set(keys) - {"replayed"} == set(DEFAULT_RESPONSE_FIELDS)
    assert tuple(_returned_dict_keys(main, "rejected_body")) == REJECTED_BODY_FIELDS


def test_golden_ack_raw_is_service_bytes():
    raw = ack()["raw"]
    assert raw.startswith('{"order_id":')
    assert list(json.loads(raw)) == list(ORDER_TO_DICT_FIELDS)
    body = json.loads(raw)
    assert body["filled_at"].endswith("+00:00") and body["volume"] == "0.00010000"
    assert body["replayed"] is False and isinstance(body["order_id"], int)
    assert FILLED_US % 1000 == 123  # µs 자리가 있다(마이크로초 비교 확인용)


def test_keys_are_run_keys():
    assert K1 == f"k6-{RUN_ID}-1-1" and K2 == f"k6-{RUN_ID}-1-2"

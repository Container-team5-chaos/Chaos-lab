"""k6 기록 기반 구간 SLO(설계 5.5): ack_p95_ms, sys_err_ratio, evaluate_client_check와 파이프라인 연결."""

from __future__ import annotations

from decimal import Decimal

import pytest

import orchestrator.judge.client as client_mod
from orchestrator.judge import reasons as R
from orchestrator.judge.client import (
    ack_p95_ms,
    client_checks,
    evaluate_client_check,
    min_samples,
    sys_err_ratio,
)
from orchestrator.judge.context import load_run
from orchestrator.judge.model import Outcome

from .factories import T0, make_intent, make_log, make_result, run_key
from .test_pipeline_offline import CRIT, FIXTURE, by_id, run_eval

W = (T0, T0 + 1_000_000)  # 모든 기록을 담는 창
P95 = {"id": "p95", "kind": "client", "stat": "ack_p95_ms", "op": "<", "value": 300}
ERR = {"id": "err", "kind": "client", "stat": "sys_err_ratio", "op": "<", "value": 0.01}


def log_of(first: list[tuple], *, t_step: int = 10):
    """첫 시도 명세 목록 → K6Log. i번째 키의 첫 시도 t0 = T0 + 1 + i·t_step."""
    its, rs = [], []
    for i, atts in enumerate(first):
        k = run_key(i + 1)
        its.append(make_intent(k, t=T0 + i * t_step))
        rs.append(make_result(k, atts, t_start=T0 + 1 + i * t_step))
    return make_log(its, rs)


def test_min_samples():
    assert min_samples() == 400  # 20/(1−0.95)
    assert min_samples("0.99") == 2000


def test_ack_p95_nearest_rank_and_low_samples():
    log = log_of([[(200, None, ms)] for ms in range(1, 401)])
    assert ack_p95_ms(log, W) == (Decimal(380), 400)  # ceil(0.95·400) = 380번째
    assert ack_p95_ms(log, (W[0], T0 + 1 + 399 * 10)) == (None, 399)  # LOW_SAMPLES


def test_ack_p95_uses_first_ack_bucketed_by_t0():
    # 첫 시도 503(결과 불명) 뒤 200: ms는 200 시도의 것, 구간은 그 시도의 t0로 정한다
    log = log_of([[(503, None, 5), (200, None, 77)]])
    (r,) = log.results.values()
    ack = r.first_ack
    assert ack_p95_ms(log, (ack.t0, ack.t0 + 1), q=Decimal("0.5")) == (None, 1)
    assert client_mod._ack_ms(log, (ack.t0, ack.t0 + 1)) == [Decimal(77)]
    assert client_mod._ack_ms(log, (r.atts[0].t0, ack.t0)) == []


def test_sys_err_ratio_excludes_4xx_and_counts_0_5xx():
    log = log_of(
        [[200], [422], [409], [(0, "1050", 800), 200], [(503, None, 8), 200], [504]]
    )
    # 분모: 200, 0, 503, 504 = 4(4xx 2건 제외). 분자: 0, 503, 504 = 3. 재전송은 보지 않는다
    assert sys_err_ratio(log, W) == Decimal("0.75")
    assert sys_err_ratio(log_of([[422]]), W) is None
    assert sys_err_ratio(log_of([[200], [200], [0]]), W) == Decimal("0.333334")  # 올림


def test_evaluate_pass_fail_low_samples():
    ok = log_of([[(200, None, 20)]] * 400)
    r = evaluate_client_check(P95, ok, {"tail": W})
    assert (r.outcome, r.value, r.reasons) == (Outcome.PASS, {"tail": Decimal(20)}, ())
    assert r.detail["n"] == {"tail": 400}
    slow = log_of([[(200, None, 20)]] * 19 + [[(200, None, 900)]] * 381)
    assert evaluate_client_check(P95, slow, {"tail": W}).outcome is Outcome.FAIL
    few = evaluate_client_check(P95, log_of([[200]] * 10), {"tail": W})
    assert (few.outcome, few.reasons) == (Outcome.INCONCLUSIVE, (R.LOW_SAMPLES,))
    # 4xx만 많고 5xx가 없으면 오류율 0이지만 4xx는 표본(분모)에 넣지 않는다
    rej = log_of([[200]] * 400 + [[422]] * 100)
    e = evaluate_client_check(ERR, rej, {"tail": W})
    assert (e.outcome, e.value) == (Outcome.PASS, {"tail": Decimal(0)})
    assert e.detail["n"] == {"tail": 400}
    bad = log_of([[200]] * 395 + [[500]] * 5)
    assert evaluate_client_check(ERR, bad, {"tail": W}).outcome is Outcome.FAIL


def test_evaluate_phase_bucketing_fail_wins_over_low_samples():
    # 앞 400건은 빠르고 뒤 10건은 느리다. 뒤 구간은 표본 부족이지만 앞 구간 결과와 합친다
    log = log_of([[(200, None, 10)]] * 400 + [[(200, None, 999)]] * 10)
    split = T0 + 1 + 400 * 10
    r = evaluate_client_check(
        P95, log, {"baseline": (W[0], split), "tail": (split, W[1])}
    )
    assert r.outcome is Outcome.INCONCLUSIVE and r.reasons == (R.LOW_SAMPLES,)
    assert r.value == {"baseline": Decimal(10), "tail": None}
    fail_first = log_of([[(200, None, 999)]] * 400 + [[200]] * 10)
    r2 = evaluate_client_check(
        P95, fail_first, {"baseline": (W[0], split), "tail": (split, W[1])}
    )
    assert r2.outcome is Outcome.FAIL and r2.reasons == ()


def test_client_checks_from_criteria():
    ctx = load_run(FIXTURE / "run.json")
    log = log_of([[200]] * 5)
    out = by_id(client_checks(ctx, CRIT, log))
    assert set(out) == {"order_p95_k6", "order_err_k6"}
    # 장애 없는 run: baseline 없음, tail·steady는 표본 부족
    assert set(out["order_p95_k6"].value) == {"steady", "tail"}
    assert out["order_p95_k6"].reasons == (R.LOW_SAMPLES,)


def test_pipeline_low_samples_on_small_fixture():
    checks = by_id(run_eval(FIXTURE).checks)
    for cid in ("order_p95_k6", "order_err_k6"):
        assert checks[cid].outcome is Outcome.INCONCLUSIVE, cid
        assert checks[cid].reasons == (R.LOW_SAMPLES,), cid


@pytest.fixture
def one_sample(monkeypatch):
    monkeypatch.setattr(client_mod, "min_samples", lambda q=None: 1)


def test_pipeline_evaluates_client_checks(one_sample):
    v = run_eval(FIXTURE)
    checks = by_id(v.checks)
    p95, err = checks["order_p95_k6"], checks["order_err_k6"]
    # 첫 ACK ms: 11.5 × 7, 9.75(전송 오류 뒤), 12(503 뒤) → nearest-rank 9번째 = 12
    assert p95.outcome is Outcome.PASS
    assert p95.value == {"steady": Decimal(12), "tail": Decimal(12)}
    # 첫 시도: 0·503 = 2건, 422는 분모에서 뺀다 → tail 2/9, steady(warmup 2s 이후) 2/7
    assert err.outcome is Outcome.FAIL
    assert err.value == {"steady": Decimal("0.285715"), "tail": Decimal("0.222223")}
    assert v.client["phases"]["tail"]["ack_p95_ms"] == Decimal(12)
    assert v.verdict is Outcome.PASS  # 픽스처 기준은 required: false

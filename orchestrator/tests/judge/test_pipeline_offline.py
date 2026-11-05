"""오프라인 e2e: 증거 폴더 → evaluate → verdict(설계 5.1, 5.2 6단계, 5.6, 5.7, 8).

픽스처 fixtures/run_ok/는 장애 없는 작은 run이다(계좌 2개, 의도 10건, 모두 FILLED, 매도 3건).
criteria.yaml은 실제 default.yaml을 상속하고 표본 기준만 낮춘다. 변형은 tmp 복사본을 고쳐 만든다.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

import orchestrator.judge.__main__ as cli
from orchestrator.judge import reasons as R
from orchestrator.judge.client import phase_windows, phases_from_marks, run_window
from orchestrator.judge.context import load_run
from orchestrator.judge.criteria import load_criteria, write_resolved
from orchestrator.judge.db import parse_bundle_output, render_snapshot
from orchestrator.judge.errors import EXIT_PASS, InputError
from orchestrator.judge.k6log import parse_k6log
from orchestrator.judge.model import DbAccount, Outcome
from orchestrator.judge.pipeline import (
    CLASSIFIED_COLUMNS,
    CLASSIFIED_NAME,
    REPORT_NAME,
    VERDICT_NAME,
    assess,
    evaluate,
    judge,
)
from orchestrator.judge.report import summary_line

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "fixtures" / "run_ok"
CRITERIA = FIXTURE / "criteria.yaml"
CRIT = load_criteria(CRITERIA)
SNAPSHOT = Path("raw/db/snapshot.txt")
ORDERS = Path("k6/orders.jsonl")
LOST_KEY = "6a1f0c2e-5b7d-4c11-9e2a-000000000005"  # 계좌 A의 BTC 매수(FILLED)


# ---------------------------------------------------------------- 도우미


def copy_run(tmp_path: Path) -> Path:
    """픽스처 복사본. criteria.yaml의 extends는 저장소 상대 경로라 resolved 기준을 함께 둔다."""
    dst = tmp_path / "run_ok"
    shutil.copytree(FIXTURE, dst)
    write_resolved(CRIT, dst)
    return dst


def run_eval(run_dir: Path, crit=CRIT, **kw):
    return evaluate(load_run(run_dir / "run.json"), crit, run_dir, **kw)


def edit_snapshot(run_dir: Path, fn) -> None:
    p = run_dir / SNAPSHOT
    db = parse_bundle_output(p.read_text(encoding="utf-8"))
    p.write_text(render_snapshot(fn(db)), encoding="utf-8")


def edit_log(run_dir: Path, fn) -> None:
    """orders.jsonl을 고치고 sha256sum.txt도 맞춘다(fetch가 받은 원격 해시와 같게)."""
    p = run_dir / ORDERS
    rows = [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x]
    rows = fn(rows)
    data = "".join(
        json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in rows
    ).encode("utf-8")
    p.write_bytes(data)
    s = run_dir / "k6" / "sha256sum.txt"
    keep = [
        x for x in s.read_text(encoding="utf-8").splitlines() if "orders.jsonl" not in x
    ]
    head = f"{hashlib.sha256(data).hexdigest()}  /out/orders.jsonl"
    s.write_text("\n".join([head, *keep]) + "\n", encoding="utf-8")


def edit_json(path: Path, fn) -> None:
    doc = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(
        json.dumps(fn(doc), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def by_id(results):
    return {r.id: r for r in results}


# ---------------------------------------------------------------- PASS·FAIL·INCONCLUSIVE


def test_run_ok_passes():
    v = run_eval(FIXTURE)
    assert v.verdict is Outcome.PASS
    assert v.reasons == ()
    assert v.warnings == ()
    checks, guards = by_id(v.checks), by_id(v.guards)
    for cid in (
        "lost",
        "mismatch",
        "duplicates",
        "orphans",
        "invariants",
        "i7_stale_fill",
    ):
        assert checks[cid].outcome is Outcome.PASS, cid
        assert checks[cid].value == 0
    assert checks["protocol"].outcome is Outcome.PASS
    assert checks["k6_exit"].outcome is Outcome.PASS
    # 1차 미구현 구간 SLO는 required: false라 보고만 한다
    assert checks["order_p95_k6"].outcome is Outcome.INCONCLUSIVE
    assert checks["order_p95_k6"].required is False
    assert checks["judge_self_check"].outcome is Outcome.PASS
    for gid in ("safety", "account_set", "fill_coverage", "sell_coverage"):
        assert guards[gid].outcome is Outcome.PASS, gid
    assert guards["load_coverage"].outcome is Outcome.PASS
    assert guards["db_quiescent"].outcome is Outcome.PASS
    assert guards["fault_in_load"].outcome is Outcome.NA  # 장애 없는 run
    assert guards["fault_effect"].outcome is Outcome.NA
    assert guards["log_complete"].value is True
    assert guards["fill_coverage"].value == {"steady": 8, "tail": 10}
    assert guards["sell_coverage"].value == 3

    integ = v.integrity
    assert integ["intents"] == {"total": 10, "ack": 10, "results": 10}
    assert integ["log_complete"] is True
    assert integ["classes"]["OK"] == 10
    assert sum(integ["classes"].values()) == 10
    assert integ["subs"]["clean"] == 10
    assert integ["summary"]["resend_ok"] == 1
    assert set(integ["invariants"]) >= {"i2", "i4", "i5_cash", "i7"}
    assert all(n == 0 for n in integ["invariants"].values())
    assert v.inputs["criteria_sha256"] == CRIT.sha256
    assert v.inputs["run_json_sha256"] == load_run(FIXTURE / "run.json").run_json_sha256
    assert v.inputs["evidence_manifest"]["raw/db/snapshot.txt"] is not None
    assert set(v.client["phases"]) == {"steady", "tail", "warmup"}
    assert v.load["k6_exit_code"] == 0


def test_same_evidence_same_digest():
    v1 = run_eval(FIXTURE, generated_at=1_762_400_000_000)
    v2 = run_eval(FIXTURE, generated_at=1_762_400_999_999)
    assert v1.verdict_digest == v2.verdict_digest
    assert v1.verdict_digest == v1.compute_digest()
    d1, d2 = v1.to_dict(), v2.to_dict()
    d1.pop("generated_at")
    d2.pop("generated_at")
    assert d1 == d2


def test_lost_order_fails_with_integrity_violation(tmp_path):
    run = copy_run(tmp_path)

    def drop(db):
        gone = {o.id for o in db.orders if o.idem_key == LOST_KEY}
        return replace(
            db,
            orders=tuple(o for o in db.orders if o.id not in gone),
            fills=tuple(f for f in db.fills if f.order_id not in gone),
        )

    edit_snapshot(run, drop)
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    assert R.INTEGRITY_VIOLATION in v.reasons
    lost = by_id(v.checks)["lost"]
    assert lost.outcome is Outcome.FAIL
    assert lost.value == 1
    assert v.integrity["examples"]["LOST"] == [LOST_KEY]
    assert v.integrity["classes"]["LOST"] == 1


def test_no_fills_is_low_fill_coverage(tmp_path):
    run = copy_run(tmp_path)
    reason = "LIMIT_NOT_MARKETABLE"
    rejected = json.dumps(
        {"fee": None, "price": None, "reason": reason, "status": "REJECTED"},
        sort_keys=True,
        separators=(",", ":"),
    )

    def reject_db(db):
        orders = tuple(
            replace(o, status="REJECTED", reject_reason=reason) for o in db.orders
        )
        accounts = tuple(replace(a, cash=a.initial_cash) for a in db.accounts)
        return replace(db, orders=orders, fills=(), positions=(), accounts=accounts)

    def reject_log(rows):
        for r in rows:
            if r["kind"] != "result":
                continue
            r["ack"]["raw"] = rejected
            if r["resend"] is not None:
                r["resend"]["ack"]["raw"] = rejected
        return rows

    edit_snapshot(run, reject_db)
    edit_log(run, reject_log)
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.LOW_FILL_COVERAGE,)
    guards = by_id(v.guards)
    assert guards["fill_coverage"].outcome is Outcome.FAIL
    assert guards["fill_coverage"].value == {"steady": 0, "tail": 0}
    assert guards["sell_coverage"].value == 0
    assert all(c.outcome is Outcome.PASS for c in v.checks if c.required)


# ---------------------------------------------------------------- CLI·API


@pytest.mark.parametrize("argv", [["evaluate"], ["run", "--offline"]])
def test_cli_writes_outputs(tmp_path, argv, capsys):
    run = copy_run(tmp_path)
    assert cli.main([*argv, "--run", str(run / "run.json")]) == EXIT_PASS
    out = json.loads(capsys.readouterr().out)
    written = json.loads((run / VERDICT_NAME).read_text(encoding="utf-8"))
    assert written == out
    assert written["verdict"] == "PASS"
    assert isinstance(written["generated_at"], int)
    assert written["verdict_digest"] == run_eval(run).verdict_digest
    report = (run / REPORT_NAME).read_text(encoding="utf-8")
    assert report.splitlines()[0].endswith("→ PASS")
    rows = list(
        csv.reader(io.StringIO((run / CLASSIFIED_NAME).read_text(encoding="utf-8")))
    )
    assert tuple(rows[0]) == CLASSIFIED_COLUMNS
    assert len(rows) == 11
    assert {r[3] for r in rows[1:]} == {"OK"}


def test_judge_offline_api():
    v = judge(FIXTURE / "run.json", offline=True)
    assert v.verdict is Outcome.PASS
    assert v.verdict_digest == run_eval(FIXTURE).verdict_digest


def test_collect_is_stub():
    with pytest.raises(NotImplementedError, match="설계 5.2"):
        judge(FIXTURE / "run.json")  # collect가 바로 멈춰 아무것도 쓰지 않는다


def test_assess_returns_classified_rows():
    a = assess(load_run(FIXTURE / "run.json"), CRIT, FIXTURE)
    assert len(a.classified) == 10
    assert [c.key for c in a.classified] == sorted(c.key for c in a.classified)


# ---------------------------------------------------------------- 가드·입력 변형


def test_db_not_quiescent(tmp_path):
    run = copy_run(tmp_path)
    edit_snapshot(run, lambda db: replace(db, fingerprint2=(11, 10, 10)))
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.DB_NOT_QUIESCENT,)


def test_missing_snapshot_is_db_error(tmp_path):
    run = copy_run(tmp_path)
    (run / SNAPSHOT).unlink()
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert R.DB_ERROR in v.reasons
    checks = by_id(v.checks)
    assert checks["lost"].outcome is Outcome.INCONCLUSIVE
    assert checks["lost"].reasons == (R.DB_ERROR,)
    assert checks["invariants"].reasons == (R.DB_ERROR,)
    assert v.integrity["classes"] is None
    assert v.inputs["evidence_manifest"]["raw/db/snapshot.txt"] is None


def test_unverified_sha_makes_orphans_inconclusive(tmp_path):
    run = copy_run(tmp_path)
    (run / "k6" / "sha256sum.txt").unlink()
    v = run_eval(run)
    assert v.integrity["log_complete"] is False
    checks = by_id(v.checks)
    assert checks["orphans"].outcome is Outcome.INCONCLUSIVE
    assert checks["orphans"].reasons == (R.SENT_LOG_INCOMPLETE,)
    assert checks["lost"].outcome is Outcome.PASS  # 있는 줄이 근거라 계속 판정한다(5.3)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.SENT_LOG_INCOMPLETE,)


def test_k6_oom_exit_is_load_aborted(tmp_path):
    run = copy_run(tmp_path)

    def oom(doc):
        doc["containers"]["k6"] = {"exitCode": 137, "reason": "OOMKilled"}
        return doc

    edit_json(run / "k6" / "pod-status.json", oom)
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert R.LOAD_ABORTED in v.reasons
    assert by_id(v.checks)["k6_exit"].reasons == (R.LOAD_ABORTED,)
    assert v.load["k6_exit_reason"] == "OOMKilled"


def test_token_key_is_safety_violation(tmp_path):
    run = copy_run(tmp_path)

    def leak(doc):
        doc["setup_data"] = {"accounts": [{"token": "x"}]}
        return doc

    edit_json(run / "k6" / "summary.json", leak)
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.SAFETY_VIOLATION,)
    assert R.SAFETY_VIOLATION in v.warnings
    assert by_id(v.guards)["safety"].value["token_key"] is True


def test_aux_account_is_account_set_mismatch(tmp_path):
    run = copy_run(tmp_path)
    aux = DbAccount(
        id="00000000-0000-4000-8000-0000000000a7",
        nickname="k6-r-offline-ok-7",
        mode="replay",
        initial_cash=10_000_000,
        cash=10_000_000,
        version=0,
        k6=True,
    )
    other = replace(
        aux, id="00000000-0000-4000-8000-0000000000a8", nickname="k6-r-offline-ok-2-0"
    )
    edit_snapshot(run, lambda db: replace(db, accounts=(*db.accounts, aux, other)))
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.ACCOUNT_SET_MISMATCH,)
    guard = by_id(v.guards)["account_set"]
    assert guard.value["aux"] == 1  # 다른 run(…-2-0)은 정규식에 맞지 않는다
    checks = by_id(v.checks)
    assert checks["lost"].reasons == (R.ACCOUNT_SET_MISMATCH,)
    assert checks["orphans"].outcome is Outcome.INCONCLUSIVE
    assert checks["mismatch"].outcome is Outcome.PASS


def test_response_schema_unconfirmed_blocks_pass(tmp_path):
    run = copy_run(tmp_path)

    def wrong_fee(rows):
        for r in rows:
            if r["kind"] == "result" and r["key"].endswith("000000000002"):
                body = json.loads(r["ack"]["raw"])
                body["fee"] = 6
                r["ack"]["raw"] = json.dumps(
                    body, sort_keys=True, separators=(",", ":")
                )
        return rows

    edit_log(run, wrong_fee)
    v = run_eval(run)
    assert v.integrity["classes"]["RESPONSE_SCHEMA_UNCONFIRMED"] == 1
    mismatch = by_id(v.checks)["mismatch"]
    assert mismatch.outcome is Outcome.INCONCLUSIVE
    assert mismatch.reasons == (R.RESPONSE_SCHEMA_UNCONFIRMED,)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.RESPONSE_SCHEMA_UNCONFIRMED,)


def test_first_200_with_connection_reuse_is_transport_replay(tmp_path):
    run = copy_run(tmp_path)

    def reuse(rows):
        for r in rows:
            if r["kind"] == "run_start":
                r["conn_reuse"] = True
            if r["kind"] == "result" and r["key"].endswith("000000000003"):
                r["atts"][0]["st"] = 200
                r["ack"]["st"] = 200
        return rows

    edit_log(run, reuse)
    v = run_eval(run)
    dup = by_id(v.checks)["duplicates"]
    assert dup.outcome is Outcome.INCONCLUSIVE
    assert dup.reasons == (R.TRANSPORT_REPLAY_POSSIBLE,)
    assert v.reasons == (R.TRANSPORT_REPLAY_POSSIBLE,)


def test_run_id_mismatch_is_input_error(tmp_path):
    run = copy_run(tmp_path)
    edit_json(run / "run.json", lambda d: {**d, "run_id": "r-other"})
    with pytest.raises(InputError, match="run_id"):
        run_eval(run)


def test_bad_pod_status_is_input_error(tmp_path):
    run = copy_run(tmp_path)
    (run / "k6" / "pod-status.json").write_text("[]\n", encoding="utf-8")
    with pytest.raises(InputError, match="pod-status"):
        run_eval(run)


# ---------------------------------------------------------------- 장애 run


def fault_run(tmp_path, *, start, end, obs=None):
    run = copy_run(tmp_path)
    marks = {"inject_start": start, "inject_end": end}
    edit_json(
        run / "run.json",
        lambda d: {**d, "marks": marks, **({"obs": obs} if obs else {})},
    )
    return run


T0 = 1_762_300_000_000


def test_fault_in_load(tmp_path):
    inside = fault_run(tmp_path / "a", start=T0 + 4000, end=T0 + 5000)
    g = by_id(run_eval(inside).guards)["fault_in_load"]
    assert g.outcome is Outcome.PASS
    outside = fault_run(tmp_path / "b", start=T0 + 4000, end=T0 + 60_000)
    g = by_id(run_eval(outside).guards)["fault_in_load"]
    assert g.outcome is Outcome.FAIL
    assert g.reasons == (R.FAULT_OUTSIDE_LOAD,)
    assert g.detail["outside"] == ["marks.inject_end"]


def test_fault_effect_observe_uses_obs(tmp_path):
    crit_path = tmp_path / "crash.yaml"
    crit_path.write_text(
        f"extends: {CRITERIA}\n"
        "fault_effect: [{id: crash_restart, observe: crash_restart, target: order-api, min: 1}]\n",
        encoding="utf-8",
    )
    crit = load_criteria(crit_path)
    missing = fault_run(tmp_path / "a", start=T0 + 4000, end=T0 + 5000)
    item = by_id(run_eval(missing, crit).guards)["crash_restart"]
    assert item.kind == "fault_effect"
    assert item.outcome is Outcome.INCONCLUSIVE
    assert item.reasons == (R.NOT_EVALUATED,)
    seen = fault_run(
        tmp_path / "b", start=T0 + 4000, end=T0 + 5000, obs={"crash_restart": 1}
    )
    item = by_id(run_eval(seen, crit).guards)["crash_restart"]
    assert item.outcome is Outcome.PASS
    assert item.value == 1


def test_clock_skew_blocks_mark_phases(tmp_path):
    run = fault_run(tmp_path, start=T0 + 4000, end=T0 + 5000)
    edit_json(run / "k6" / "derived.json", lambda d: {**d, "offset_ms": 2500})
    v = run_eval(run)
    guards = by_id(v.guards)
    assert guards["fault_in_load"].reasons == (R.CLOCK_SKEW,)
    assert guards["fill_coverage"].reasons == (
        R.CLOCK_SKEW,
    )  # baseline은 표지 기준 구간


def test_phase_windows_fault_run_with_clock_offset():
    run = load_run(FIXTURE / "run.json")
    ctx = replace(run, marks={"inject_start": T0 + 4000, "inject_end": T0 + 5000})
    log = parse_k6log(FIXTURE / ORDERS)
    w = phase_windows(ctx, CRIT, log, clock_offset_ms=100)
    end = max(r.t for r in log.results.values()) + 1
    assert w["baseline"] == (T0 + 4100 - 150_000, T0 + 4100 - 2000)
    assert w["fault"] == (T0 + 4100, T0 + 5100 + 60_000)
    assert w["recovery"] == (T0 + 65_100, T0 + 65_100)  # 끝 뒤라 빈 창
    assert w["tail"] == (end - 120_000, end)
    assert w["warmup"] == (T0, T0 + 2000)
    assert "steady" not in w
    steady = phase_windows(run, CRIT, log)
    assert steady["steady"] == (T0 + 2000, end)
    assert "baseline" not in steady


# ---------------------------------------------------------------- 검토 지적 반영


def _result_row(rows, suffix):
    return next(r for r in rows if r["kind"] == "result" and r["key"].endswith(suffix))


@pytest.mark.parametrize("confirmed", [False, True])
def test_unparsed_response_blocks_pass(tmp_path, confirmed):
    """잘린 응답 원문(설계 4.6 raw ≤2KB)은 비교 못 함이다. 조용히 OK로 두지 않는다(5.7 규칙 4)."""
    run = copy_run(tmp_path)

    def truncate(rows):
        r = _result_row(rows, "000000000002")
        r["ack"]["raw"] = r["ack"]["raw"][:20]
        return rows

    edit_log(run, truncate)
    crit = CRIT
    if confirmed:
        p = tmp_path / "confirmed.yaml"
        p.write_text(
            f"extends: {CRITERIA}\nintegrity: {{response_fields_confirmed: true}}\n",
            encoding="utf-8",
        )
        crit = load_criteria(p)
    v = run_eval(run, crit)
    mismatch = by_id(v.checks)["mismatch"]
    assert mismatch.outcome is Outcome.INCONCLUSIVE
    assert mismatch.reasons == (R.NOT_EVALUATED,)
    assert mismatch.detail["uncertain_rows"] == {"response_unparsed": 1}
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.NOT_EVALUATED,)
    summary = v.integrity["summary"]
    assert summary["response_unparsed"] == 1
    assert summary["inconclusive"]["mismatch"] == (R.NOT_EVALUATED,)


def test_unparsed_resend_blocks_duplicates_not_resend_unresolved(tmp_path):
    """재송신 200을 비교하지 못하면 RESEND_UNRESOLVED(끝까지 2xx 없음)가 아니다(설계 4.4)."""
    run = copy_run(tmp_path)

    def truncate(rows):
        r = _result_row(rows, "000000000001")
        r["resend"]["ack"]["raw"] = r["resend"]["ack"]["raw"][:20]
        return rows

    edit_log(run, truncate)
    v = run_eval(run)
    dup = by_id(v.checks)["duplicates"]
    assert dup.outcome is Outcome.INCONCLUSIVE
    assert dup.reasons == (R.NOT_EVALUATED,)
    assert dup.detail["uncertain_rows"] == {"resend_uncompared": 1}
    summary = v.integrity["summary"]
    assert summary["resend_unresolved"] == 0
    assert summary["resend_uncompared"] == 1
    assert by_id(v.checks)["mismatch"].outcome is Outcome.PASS


def test_fault_effect_disable_same_verdict_from_resolved(tmp_path):
    """disable은 resolved에 남지 않는다. 원본과 resolved 기준의 판정이 같아야 한다(설계 0, 5.2-6, 6)."""
    p = tmp_path / "x.yaml"
    p.write_text(
        f"extends: {CRITERIA}\n"
        "disable: [fault_effect]\n"
        "fault_effect: [{id: crash_restart, observe: crash_restart, target: order-api, min: 1}]\n",
        encoding="utf-8",
    )
    source = load_criteria(p)
    resolved = load_criteria(write_resolved(source, tmp_path / "resolved.yaml"))
    assert source.sha256 == resolved.sha256
    run = fault_run(tmp_path / "run", start=T0 + 4000, end=T0 + 5000)
    v1, v2 = run_eval(run, source), run_eval(run, resolved)
    assert v1.verdict_digest == v2.verdict_digest
    item = by_id(v1.guards)["crash_restart"]
    assert item.required is False  # 가드가 없으면 보고만 한다
    assert by_id(v2.guards)["crash_restart"].required is False
    assert R.NOT_EVALUATED not in v1.reasons


def test_fault_effect_guard_present_keeps_items_required(tmp_path):
    p = tmp_path / "x.yaml"
    p.write_text(
        f"extends: {CRITERIA}\n"
        "fault_effect: [{id: crash_restart, observe: crash_restart, target: order-api, min: 1}]\n",
        encoding="utf-8",
    )
    run = fault_run(tmp_path / "run", start=T0 + 4000, end=T0 + 5000)
    item = by_id(run_eval(run, load_criteria(p)).guards)["crash_restart"]
    assert item.required is True
    assert item.reasons == (R.NOT_EVALUATED,)


def test_account_set_mismatch_still_fails_false_not_found(tmp_path):
    """ACCOUNT_SET_MISMATCH면 LOST·ORPHAN만 평가하지 않는다. FALSE_NOT_FOUND는 FAIL이다(설계 5.3, 5.7 규칙 1)."""
    run = copy_run(tmp_path)

    def not_found(rows):
        r = _result_row(rows, "000000000002")
        t0 = r["atts"][0]["t0"]
        r["atts"] = [{"ec": None, "ms": 40, "n": 1, "st": 500, "t0": t0, "t1": t0 + 40}]
        r["ack"] = None
        r["resend"] = None
        look = {
            "ec": None,
            "ms": 3,
            "n": 1,
            "st": 404,
            "t0": t0 + 1041,
            "t1": t0 + 1044,
        }
        r["lookup"] = {"atts": [look], "st": 404, "raw": '{"error":"NOT_FOUND"}'}
        return rows

    aux = DbAccount(
        id="00000000-0000-4000-8000-0000000000a7",
        nickname="k6-r-offline-ok-7",
        mode="replay",
        initial_cash=10_000_000,
        cash=10_000_000,
        version=0,
        k6=True,
    )
    edit_log(run, not_found)
    edit_snapshot(run, lambda db: replace(db, accounts=(*db.accounts, aux)))
    v = run_eval(run)
    assert v.integrity["classes"]["FALSE_NOT_FOUND"] == 1
    assert v.integrity["not_evaluated"] == {
        "LOST": R.ACCOUNT_SET_MISMATCH,
        "ORPHAN": R.ACCOUNT_SET_MISMATCH,
    }
    lost = by_id(v.checks)["lost"]
    assert lost.outcome is Outcome.FAIL
    assert lost.detail["decided_by_partial"] is True
    assert v.verdict is Outcome.FAIL
    assert v.reasons == (R.INTEGRITY_VIOLATION,)
    assert by_id(v.guards)["account_set"].reasons == (R.ACCOUNT_SET_MISMATCH,)
    assert summary_line(v).startswith("유실 ≥1(ACCOUNT_SET_MISMATCH)")


def test_missing_derived_blocks_mark_phases(tmp_path):
    """시계 차이를 모르면 표지 기준 구간과 fault_in_load를 판정하지 않는다(설계 4.8, 5.5)."""
    run = fault_run(tmp_path / "fault", start=T0 + 4000, end=T0 + 5000)
    (run / "k6" / "derived.json").unlink()
    v = run_eval(run)
    guards = by_id(v.guards)
    for gid in ("fault_in_load", "fill_coverage", "load_coverage"):
        assert guards[gid].outcome is Outcome.INCONCLUSIVE, gid
        assert guards[gid].reasons == (R.NOT_EVALUATED,), gid
        assert guards[gid].detail["clock_offset_ms"] is None
    assert v.load["clock_offset_ms"] is None
    assert v.verdict is Outcome.INCONCLUSIVE
    # 표지가 없는 run(k6 시계만 쓰는 구간)은 영향이 없다
    plain = copy_run(tmp_path / "plain")
    (plain / "k6" / "derived.json").unlink()
    assert run_eval(plain).verdict is Outcome.PASS


@pytest.mark.parametrize(
    "vu,key",
    [(1, "vu_multi_account"), (3, "vu_out_of_range")],
)
def test_safety_one_vu_one_account(tmp_path, vu, key):
    """1 VU = 1 계좌(설계 4.1, 4.2)를 judge가 다시 본다."""
    run = copy_run(tmp_path)
    acct_b = "00000000-0000-4000-8000-00000000000b"

    def remap(rows):
        for r in rows:
            if r["kind"] == "intent" and r["acct"] == acct_b:
                r["vu"] = vu
        return rows

    edit_log(run, remap)
    v = run_eval(run)
    safety = by_id(v.guards)["safety"]
    assert safety.outcome is Outcome.FAIL
    assert safety.reasons == (R.SAFETY_VIOLATION,)
    assert safety.value[key] == 1
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.SAFETY_VIOLATION,)


def test_k6_99_full_run_with_decimal_duration(tmp_path):
    """summary는 Decimal로 읽는다. 끝까지 돈 99의 SLO threshold는 경고만 낸다(설계 4.7, 5.7)."""
    run = copy_run(tmp_path)

    def exit99(doc):
        doc["containers"]["k6"] = {"exitCode": 99, "reason": "Error"}
        return doc

    def slo_fail(doc):
        doc["metrics"]["order_ack_ms"] = {
            "type": "trend",
            "contains": "time",
            "values": {"p(95)": 350.5},
            "thresholds": {"p(95)<300": {"ok": False}},
        }
        return doc

    edit_json(run / "k6" / "pod-status.json", exit99)
    edit_json(run / "k6" / "summary.json", slo_fail)
    v = run_eval(run)
    checks = by_id(v.checks)
    assert checks["k6_exit"].outcome is Outcome.PASS
    assert checks["k6_exit_slo"].required is False
    assert checks["k6_exit_slo"].reasons == (R.K6_THRESHOLD_DISAGREES,)
    assert v.verdict is Outcome.PASS
    assert v.warnings == (R.K6_THRESHOLD_DISAGREES,)


def test_truncated_summary_is_log_incomplete_not_input_error(tmp_path):
    """잘린 summary.json은 기록 불완전이다. DB만으로 되는 돈 FAIL은 그대로 낸다(설계 5.7 하단, 규칙 1)."""
    run = copy_run(tmp_path)
    p = run / "k6" / "summary.json"
    p.write_bytes(p.read_bytes()[:200])

    def oom(doc):
        doc["containers"]["k6"] = {"exitCode": 137, "reason": "OOMKilled"}
        return doc

    def drop(db):
        gone = {o.id for o in db.orders if o.idem_key == LOST_KEY}
        return replace(
            db,
            orders=tuple(o for o in db.orders if o.id not in gone),
            fills=tuple(f for f in db.fills if f.order_id not in gone),
        )

    edit_json(run / "k6" / "pod-status.json", oom)
    edit_snapshot(run, drop)
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    assert v.reasons == (R.INTEGRITY_VIOLATION,)
    assert by_id(v.checks)["lost"].value == 1
    lc = by_id(v.guards)["log_complete"]
    assert lc.value is False
    assert "summary_error" in lc.detail
    assert "summary.json" in lc.detail["why"]["counters"]
    assert v.load["summary_error"] is not None
    assert v.inputs["evidence_manifest"]["k6/summary.json"] is not None
    assert by_id(v.checks)["orphans"].reasons == (R.SENT_LOG_INCOMPLETE,)


def test_truncated_summary_token_key_is_safety_violation(tmp_path):
    run = copy_run(tmp_path)
    (run / "k6" / "summary.json").write_text(
        '{"setup_data":{"accounts":[{"token":"x"', encoding="utf-8"
    )
    v = run_eval(run)
    safety = by_id(v.guards)["safety"]
    assert safety.value["token_key"] is True
    assert safety.detail["summary_token_scan"] == "text"
    assert v.reasons == (R.SAFETY_VIOLATION,)


def test_phase_windows_use_run_json_k6_marks(tmp_path):
    """run.json의 k6_start·k6_end는 모든 구간과 fault_in_load가 같은 값으로 쓴다(설계 5.5)."""
    run = load_run(FIXTURE / "run.json")
    ctx = replace(run, marks={"k6_start": T0 + 3000, "k6_end": T0 + 8000})
    log = parse_k6log(FIXTURE / ORDERS)
    w = phase_windows(ctx, CRIT, log, clock_offset_ms=0)
    assert w["warmup"] == (T0 + 3000, T0 + 5000)
    assert w["steady"] == (T0 + 5000, T0 + 8000)
    assert w["tail"] == (T0 + 8000 - 120_000, T0 + 8000)
    assert run_window(ctx, log) == (T0 + 3000, T0 + 8000)
    assert phases_from_marks(ctx, CRIT) >= {"warmup", "steady", "tail"}

    fault = fault_run(tmp_path, start=T0 + 4000, end=T0 + 9000)
    edit_json(
        fault / "run.json",
        lambda d: {**d, "marks": {**d["marks"], "k6_end": T0 + 8000}},
    )
    g = by_id(run_eval(fault).guards)["fault_in_load"]
    assert g.detail["load_window"] == [T0, T0 + 8000]
    assert g.detail["outside"] == ["marks.inject_end"]


def test_shuffled_lines_same_verdict_body(tmp_path):
    """줄 순서를 섞어도 판정 본문이 같다(설계 8). 입력 해시만 바뀐다(digest 정의는 설계 결정 대기)."""
    run = copy_run(tmp_path)

    def shuffle(rows):
        head, rest = rows[:4], rows[4:]
        return head + rest[1::2][::-1] + rest[0::2]

    edit_log(run, shuffle)
    a, b = run_eval(FIXTURE).to_dict(), run_eval(run).to_dict()
    for d in (a, b):
        for k in ("verdict_digest", "generated_at", "run_info"):
            d.pop(k)
        for k in ("evidence_manifest", "evidence_manifest_sha256"):
            d["inputs"].pop(k)
        for k in ("sha256", "sha256_remote"):
            d["load"].pop(k)
    assert a == b
    assert b["verdict"] == "PASS"

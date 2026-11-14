"""오프라인 e2e: 증거 폴더 → evaluate → verdict(설계 5.1, 5.2 6단계, 5.6, 5.7, 8, 계약 명세 2–8).

픽스처 fixtures/run_ok/는 장애 없는 작은 run이다(JSONL v2, 새 스냅샷 형식).
- 계좌 2개(user-0001 = vu 1, user-0002 = vu 2), intent 10건, 종목 KRW-BTC.
- 체결 9건(매수 6, 매도 3), 422 insufficient_volume 거절 1건(DB 행 없음, OK rejected).
- 재전송 2건: 전송 오류 뒤 200 replayed=true(after_commit), 503 not_ready 뒤 200 replayed=false
  (not_committed). 나머지는 clean.
- 잔고·포지션은 서비스 공식으로 계산했다. 불변식 7개가 모두 0이다. ticks는 체결가마다 하나다.
criteria.yaml은 실제 default.yaml을 상속하고 표본 기준만 낮춘다. 변형은 tmp 복사본을 고쳐 만든다.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

import orchestrator.judge.__main__ as cli
import orchestrator.judge.pipeline as pipeline_mod
from orchestrator.judge import gather
from orchestrator.judge import reasons as R
from orchestrator.judge.client import phase_windows, phases_from_marks, run_window
from orchestrator.judge.context import load_run
from orchestrator.judge.criteria import (
    RESOLVED_NAME,
    load_criteria,
    select_criteria,
    write_resolved,
)
from orchestrator.judge.db import parse_bundle_output, render_snapshot
from orchestrator.judge.errors import EXIT_PASS, InputError
from orchestrator.judge.k6log import parse_k6log
from orchestrator.judge.model import (
    DbAccount,
    DbPosition,
    Outcome,
    compute_fingerprint,
)
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

from .factories import INITIAL_KRW, replay_krw, replay_positions

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "fixtures" / "run_ok"
CRITERIA = FIXTURE / "criteria.yaml"
CRIT = load_criteria(CRITERIA)
SNAPSHOT = Path("raw/db/snapshot.txt")
ORDERS = Path("k6/orders.jsonl")
RUN_ID = "r-offline-ok"
T0 = 1_762_300_000_000  # run_start.t
USER_A, USER_B = "user-0001", "user-0002"


def key(vu: int, seq: int) -> str:
    return f"k6-{RUN_ID}-{vu}-{seq}"


LOST_KEY = key(1, 2)  # user-0001 매수(clean, orders.id 3)
SELL_KEY = key(1, 3)  # user-0001 매도(clean, orders.id 5)
AFTER_COMMIT_KEY = key(1, 4)  # 전송 오류 뒤 200 replayed=true
NOT_COMMITTED_KEY = key(2, 4)  # 503 not_ready 뒤 200 replayed=false
REJECTED_KEY = key(2, 5)  # 422 insufficient_volume, DB 행 없음


# ---------------------------------------------------------------- 도우미


def copy_run(tmp_path: Path) -> Path:
    """픽스처 복사본. criteria.yaml의 extends는 저장소 상대 경로라 resolved 기준을 함께 둔다."""
    dst = tmp_path / "run_ok"
    shutil.copytree(FIXTURE, dst)
    write_resolved(CRIT, dst)
    return dst


def run_eval(run_dir: Path, crit=CRIT, **kw):
    return evaluate(load_run(run_dir / "run.json"), crit, run_dir, **kw)


def edit_snapshot(run_dir: Path, fn, *, refingerprint: bool = True) -> None:
    """snapshot.txt를 고친다. 기본으로 F1·F2를 고친 행에 맞춰 다시 잰다(정지 상태 그대로)."""
    p = run_dir / SNAPSHOT
    db = fn(parse_bundle_output(p.read_text(encoding="utf-8")))
    if refingerprint:
        fp = compute_fingerprint(db.accounts, db.positions, db.orders)
        db = replace(db, fingerprint1=fp, fingerprint2=fp)
    p.write_text(render_snapshot(db), encoding="utf-8")


def rebalance(db):
    """accounts.krw와 positions를 주문에서 서비스 공식대로 다시 계산한다(불변식이 맞는 DB)."""
    accounts = tuple(
        DbAccount(a.user_id, replay_krw(db.orders, a.user_id, INITIAL_KRW))
        for a in db.accounts
    )
    positions = tuple(
        DbPosition(u, m, v, p)
        for (u, m), (v, p) in sorted(replay_positions(db.orders).items())
    )
    return replace(db, accounts=accounts, positions=positions)


def drop_orders(*keys: str, rebalanced: bool = True):
    def fn(db):
        db = replace(
            db, orders=tuple(o for o in db.orders if o.client_order_id not in keys)
        )
        return rebalance(db) if rebalanced else db

    return fn


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


def write_criteria(tmp_path: Path, body: str) -> object:
    p = tmp_path / "crit.yaml"
    p.write_text(f"extends: {CRITERIA}\n{body}", encoding="utf-8")
    return load_criteria(p)


def by_id(results):
    return {r.id: r for r in results}


def result_row(rows, k):
    return next(r for r in rows if r["type"] == "result" and r["key"] == k)


def service_json(obj) -> str:
    """서비스 응답 바이트 모양(starlette JSONResponse: 키 순서 유지, 공백 없음)."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def rejected_raw(reason: str, k: str | None) -> str:
    return service_json({"status": "rejected", "reason": reason, "client_order_id": k})


def reject_only(st: int, ec: str):
    """키의 시도를 확정 거절 하나로 바꾼다(계약 명세 3 REJECT)."""

    def fn(r):
        t0 = r["atts"][0]["t0"]
        r["atts"] = [
            {
                "n": 1,
                "t0": t0,
                "t1": t0 + 7,
                "st": st,
                "ec": ec,
                "ms": 7.5,
                "rp": None,
                "raw": rejected_raw(ec, r["key"]),
            }
        ]

    return fn


def edit_ack(k: str, **fields):
    """키의 첫 200 응답 원문 필드를 바꾼다(rp도 replayed에 맞춘다)."""

    def fn(rows):
        r = result_row(rows, k)
        a = next(a for a in r["atts"] if a["st"] == 200)
        body = json.loads(a["raw"])
        body.update(fields)
        a["raw"] = service_json(body)
        a["rp"] = body["replayed"]
        return rows

    return fn


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
        "stale_fill",
        "protocol",
    ):
        assert checks[cid].outcome is Outcome.PASS, cid
        assert checks[cid].value == 0, cid
    assert checks["k6_exit"].outcome is Outcome.PASS
    # 구간 SLO는 표본 부족(LOW_SAMPLES)이고 required: false라 보고만 한다
    assert checks["order_p95_k6"].outcome is Outcome.INCONCLUSIVE
    assert checks["order_p95_k6"].required is False
    self_check = checks["judge_self_check"]
    assert self_check.outcome is Outcome.PASS and self_check.required is False
    assert self_check.detail["snapshot"] == {"orders": 9, "keys": 9, "filled": 9}
    assert self_check.detail["classifier"] == {"orders": 9, "keys": 9}
    for gid in (
        "safety",
        "account_set",
        "fill_coverage",
        "sell_coverage",
        "load_coverage",
        "db_quiescent",
    ):
        assert guards[gid].outcome is Outcome.PASS, gid
    assert guards["fault_in_load"].outcome is Outcome.NA  # 장애 없는 run
    assert guards["fault_effect"].outcome is Outcome.NA
    assert guards["log_complete"].value is True
    assert guards["fill_coverage"].value == {"steady": 7, "tail": 9}
    assert guards["sell_coverage"].value == 3  # side='ask' filled(계약 명세 8)
    assert guards["safety"].detail["key_prefix"] == f"k6-{RUN_ID}-"
    assert guards["safety"].detail["markets"] == ["KRW-BTC"]
    assert guards["account_set"].value == {
        "jsonl": 2,
        "missing": 0,
        "extra": 0,
        "extra_missing": 0,
    }
    assert guards["db_quiescent"].value["f1"]["orders"] == 9

    integ = v.integrity
    assert integ["intents"] == {"total": 10, "ack": 9, "results": 10}
    assert integ["log_complete"] is True
    assert integ["classes"]["OK"] == 10
    assert sum(integ["classes"].values()) == 10
    assert integ["subs"] == {
        "after_503": 0,
        "after_commit": 1,
        "clean": 7,
        "not_committed": 1,
        "rejected": 1,
    }
    assert integ["summary"]["recovered_after_commit"] == 1
    assert integ["summary"]["rejected"] == 1
    assert integ["invariants"] == {
        "arith": 0,
        "avg": 0,
        "cash": 0,
        "constraints": 0,
        "fresh": 0,
        "nonneg": 0,
        "qty": 0,
    }
    assert "db_errors" not in integ
    # criteria_sha256 = 기준 파일 원본 바이트(runner가 대조), resolved 해시는 따로
    assert (
        v.inputs["criteria_sha256"] == hashlib.sha256(CRITERIA.read_bytes()).hexdigest()
    )
    assert v.inputs["criteria_resolved_sha256"] == CRIT.sha256
    assert v.inputs["run_json_sha256"] == load_run(FIXTURE / "run.json").run_json_sha256
    assert v.inputs["evidence_manifest"]["raw/db/snapshot.txt"] is not None
    assert v.inputs["response_fields_confirmed"] is True
    assert set(v.client["phases"]) == {"steady", "tail", "warmup"}
    assert v.client["phases"]["steady"]["sell_filled"] == 3
    assert v.load["k6_exit_code"] == 0
    assert v.load["vus"] == 2
    assert "setup_ms" not in v.load and "conn_reuse" not in v.load
    assert summary_line(v) == (
        "유실 0 · 불일치 0 · 중복 0 · 고아 0 · 잔고 불일치 0 · 신선도 위반 체결 0 → PASS"
    )


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
    """ACK 받은 주문의 행이 DB에 없다(K_ACK × D0 = LOST). 잔고는 맞춰 둬 LOST만 보인다."""
    run = copy_run(tmp_path)
    edit_snapshot(run, drop_orders(LOST_KEY))
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    assert v.reasons == (R.INTEGRITY_VIOLATION,)
    lost = by_id(v.checks)["lost"]
    assert lost.outcome is Outcome.FAIL
    assert lost.value == 1
    assert v.integrity["examples"]["LOST"] == [LOST_KEY]
    assert v.integrity["classes"]["LOST"] == 1
    assert by_id(v.checks)["invariants"].outcome is Outcome.PASS
    assert by_id(v.checks)["judge_self_check"].outcome is Outcome.PASS
    assert summary_line(v).startswith("유실 1 · ")


def test_deleted_row_also_breaks_balance_invariants(tmp_path):
    """행만 지우면(잔고는 그대로) 유실과 함께 cash·qty·avg 불변식이 잡는다(계약 명세 7)."""
    run = copy_run(tmp_path)
    edit_snapshot(run, drop_orders(LOST_KEY, rebalanced=False))
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    inv = v.integrity["invariants"]
    assert (inv["cash"], inv["qty"], inv["avg"]) == (1, 1, 1)
    assert by_id(v.checks)["invariants"].outcome is Outcome.FAIL
    assert by_id(v.checks)["lost"].value == 1
    assert summary_line(v).startswith(
        "유실 1 · 불일치 0 · 중복 0 · 고아 0 · 잔고 불일치 3"
    )


def test_no_fills_is_low_fill_coverage(tmp_path):
    """모든 주문이 503 stale_price(확정 거절)면 DB에 행이 없다. 체결 0은 가드 미충족이다."""
    run = copy_run(tmp_path)

    def reject_log(rows):
        for r in rows:
            if r["type"] == "result":
                reject_only(503, "stale_price")(r)
        return rows

    edit_log(run, reject_log)
    edit_snapshot(run, lambda db: rebalance(replace(db, orders=())))
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.LOW_FILL_COVERAGE,)
    guards = by_id(v.guards)
    assert guards["fill_coverage"].outcome is Outcome.FAIL
    assert guards["fill_coverage"].value == {"steady": 0, "tail": 0}
    assert guards["sell_coverage"].value == 0
    assert all(c.outcome is Outcome.PASS for c in v.checks if c.required)
    assert v.integrity["subs"]["rejected"] == 10
    assert v.integrity["invariants"]["cash"] == 0


@pytest.mark.parametrize(
    "tamper,inv",
    [
        (
            lambda db: replace(
                db,
                accounts=tuple(
                    replace(a, krw=a.krw + 1) if a.user_id == USER_A else a
                    for a in db.accounts
                ),
            ),
            "cash",
        ),
        (
            lambda db: replace(
                db,
                positions=tuple(
                    replace(p, volume=p.volume + Decimal("0.00000001"))
                    if p.user_id == USER_B
                    else p
                    for p in db.positions
                ),
            ),
            "qty",
        ),
        (
            lambda db: replace(
                db,
                positions=tuple(
                    replace(p, avg_price=p.avg_price + Decimal("0.00000001"))
                    if p.user_id == USER_A
                    else p
                    for p in db.positions
                ),
            ),
            "avg",
        ),
        (
            lambda db: replace(
                db,
                orders=tuple(
                    replace(o, amount=o.amount + Decimal("0.0001"))
                    if o.client_order_id == SELL_KEY
                    else o
                    for o in db.orders
                ),
            ),
            "arith",
        ),
    ],
    ids=["cash", "qty", "avg", "arith"],
)
def test_balance_tamper_fails(tmp_path, tamper, inv):
    """DB를 조작하면 해당 불변식이 잡고 FAIL(INTEGRITY_VIOLATION)이다(계약 명세 7)."""
    run = copy_run(tmp_path)
    edit_snapshot(run, tamper)
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    assert v.reasons == (R.INTEGRITY_VIOLATION,)
    assert v.integrity["invariants"][inv] >= 1
    item = by_id(v.checks)["invariants"]
    assert item.outcome is Outcome.FAIL
    assert item.detail["parts"][inv]["violations"] >= 1


def test_reject_committed_fails(tmp_path):
    """확정 거절(422)을 받았는데 DB에 행이 있다(K_REJECT × D1, 계약 명세 4) → 유실 기준 FAIL."""
    run = copy_run(tmp_path)

    def reject(rows):
        reject_only(422, "insufficient_volume")(result_row(rows, SELL_KEY))
        return rows

    edit_log(run, reject)
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    assert v.reasons == (R.INTEGRITY_VIOLATION,)
    lost = by_id(v.checks)["lost"]
    assert lost.outcome is Outcome.FAIL and lost.value == 1
    assert v.integrity["classes"]["REJECT_COMMITTED"] == 1
    assert v.integrity["examples"]["REJECT_COMMITTED"] == [SELL_KEY]
    assert v.integrity["intents"]["ack"] == 8


def test_orphan_fails_and_foreign_orders_are_ignored(tmp_path):
    """run 키인데 intent가 없는 행은 ORPHAN이다. run 키가 아닌 다른 사용자 주문은 보지 않는다."""
    run = copy_run(tmp_path)

    def add(db):
        base = next(o for o in db.orders if o.client_order_id == LOST_KEY)
        orphan = replace(base, id=10, client_order_id=key(1, 99))
        foreign = replace(base, id=11, client_order_id="manual-1", user_id="user-0500")
        accounts = (*db.accounts, DbAccount("user-0500", Decimal(INITIAL_KRW)))
        return rebalance(
            replace(db, accounts=accounts, orders=(*db.orders, orphan, foreign))
        )

    edit_snapshot(run, add)
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    orphans = by_id(v.checks)["orphans"]
    assert orphans.outcome is Outcome.FAIL and orphans.value == 1
    assert v.integrity["examples"]["ORPHAN"] == [key(1, 99)]
    assert by_id(v.checks)["invariants"].outcome is Outcome.PASS
    assert by_id(v.checks)["judge_self_check"].detail["snapshot"]["orders"] == 10


def test_dup_order_fails(tmp_path):
    """같은 client_order_id 행이 2개(UNIQUE라 정상이면 0) → DUP_ORDER, 중복 기준 FAIL."""
    run = copy_run(tmp_path)

    def dup(db):
        base = next(o for o in db.orders if o.client_order_id == LOST_KEY)
        return rebalance(replace(db, orders=(*db.orders, replace(base, id=10))))

    edit_snapshot(run, dup)
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    dup_check = by_id(v.checks)["duplicates"]
    assert dup_check.outcome is Outcome.FAIL and dup_check.value == 1
    assert v.integrity["classes"]["DUP_ORDER"] == 1
    sc = by_id(v.checks)["judge_self_check"]
    assert sc.outcome is Outcome.PASS
    assert sc.detail["snapshot"] == {"orders": 10, "keys": 9, "filled": 10}


def test_first_replayed_is_idempotency_violation(tmp_path):
    """처음 보내는 키(n=1)인데 replayed=true(계약 명세 4 FIRST_REPLAYED) → 중복 기준 FAIL."""
    run = copy_run(tmp_path)
    edit_log(run, edit_ack(LOST_KEY, replayed=True))
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    assert by_id(v.checks)["duplicates"].outcome is Outcome.FAIL
    assert v.integrity["classes"]["IDEMPOTENCY_VIOLATION"] == 1
    assert v.integrity["summary"]["idem_first_replayed"] == 1


def test_response_mismatch_fails(tmp_path):
    """원 응답(첫 ACK)의 fill_price가 DB 행과 다르다 → MISMATCH FAIL(계약 명세 5)."""
    run = copy_run(tmp_path)
    edit_log(run, edit_ack(LOST_KEY, fill_price="101000001.00000000"))
    v = run_eval(run)
    assert v.verdict is Outcome.FAIL
    mismatch = by_id(v.checks)["mismatch"]
    assert mismatch.outcome is Outcome.FAIL and mismatch.value == 1
    assert v.integrity["examples"]["MISMATCH"] == [LOST_KEY]


def test_response_schema_unconfirmed_blocks_pass(tmp_path):
    """매핑 미확정이면 응답 대 DB 불일치는 MISMATCH가 아니라 PASS만 막는다(설계 5.7 규칙 4)."""
    run = copy_run(tmp_path)
    edit_log(run, edit_ack(LOST_KEY, fill_price="101000001.00000000"))
    crit = write_criteria(tmp_path, "integrity: {response_fields_confirmed: false}\n")
    v = run_eval(run, crit)
    assert v.integrity["classes"]["RESPONSE_SCHEMA_UNCONFIRMED"] == 1
    mismatch = by_id(v.checks)["mismatch"]
    assert mismatch.outcome is Outcome.INCONCLUSIVE
    assert mismatch.reasons == (R.RESPONSE_SCHEMA_UNCONFIRMED,)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.RESPONSE_SCHEMA_UNCONFIRMED,)
    assert v.inputs["response_fields_confirmed"] is False


def test_unparsed_response_blocks_pass(tmp_path):
    """잘린 200 원문(raw ≤ 2KB)은 ACK로 볼 수 없다(계약 명세 3 PROTOCOL). 조용히 OK로 두지 않는다."""
    run = copy_run(tmp_path)

    def truncate(rows):
        a = result_row(rows, LOST_KEY)["atts"][0]
        a["raw"] = a["raw"][:20]
        return rows

    edit_log(run, truncate)
    v = run_eval(run)
    checks = by_id(v.checks)
    assert v.integrity["classes"]["PROTOCOL"] == 1
    assert checks["protocol"].outcome is Outcome.FAIL  # on_violation inconclusive
    mismatch = checks["mismatch"]
    assert mismatch.outcome is Outcome.INCONCLUSIVE
    assert mismatch.reasons == (R.NOT_EVALUATED,)
    assert mismatch.detail["uncertain_rows"] == {"response_unparsed": 1}
    assert checks["duplicates"].detail["uncertain_rows"] == {"resend_uncompared": 1}
    assert checks["lost"].outcome is Outcome.PASS
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.CHECK_INCONCLUSIVE, R.NOT_EVALUATED)
    assert v.integrity["summary"]["inconclusive"]["mismatch"] == (R.NOT_EVALUATED,)


def test_unparsed_response_without_row_blocks_lost(tmp_path):
    """200 원문을 못 읽었고 DB 행도 없으면 LOST인지 모른다(lost_unknown). 유실 기준 PASS를 막는다."""
    run = copy_run(tmp_path)

    def truncate(rows):
        a = result_row(rows, LOST_KEY)["atts"][0]
        a["raw"] = a["raw"][:20]
        return rows

    edit_log(run, truncate)
    edit_snapshot(run, drop_orders(LOST_KEY))
    v = run_eval(run)
    lost = by_id(v.checks)["lost"]
    assert lost.outcome is Outcome.INCONCLUSIVE
    assert lost.reasons == (R.NOT_EVALUATED,)
    assert lost.detail["uncertain_rows"] == {"lost_unknown": 1}
    assert v.verdict is Outcome.INCONCLUSIVE


def test_after_503_is_warning_not_failure(tmp_path):
    """503 not_ready 뒤 replayed=true(커밋 뒤 연결 오류) → OK(after_503) + 경고(계약 명세 4)."""
    run = copy_run(tmp_path)
    edit_log(run, edit_ack(NOT_COMMITTED_KEY, replayed=True))
    v = run_eval(run)
    assert v.verdict is Outcome.PASS
    assert v.warnings == (R.ROLLBACK_REPLY_COMMITTED,)
    assert v.integrity["subs"]["after_503"] == 1
    assert v.integrity["subs"]["not_committed"] == 0


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
    col = {name: i for i, name in enumerate(rows[0])}
    assert {r[col["cls"]] for r in rows[1:]} == {"OK"}
    assert {r[col["k_state"]] for r in rows[1:]} == {"K_ACK", "K_REJECT"}
    by_key = {r[col["key"]]: r for r in rows[1:]}
    assert by_key[AFTER_COMMIT_KEY][col["sub"]] == "after_commit"
    assert by_key[AFTER_COMMIT_KEY][col["resend"]] == "1"  # 재전송 수(시도 수 − 1)
    assert by_key[LOST_KEY][col["resend"]] == "0"
    assert by_key[REJECTED_KEY][col["order_ids"]] == ""


def test_judge_offline_api():
    v = judge(FIXTURE / "run.json", offline=True)
    assert v.verdict is Outcome.PASS
    assert v.verdict_digest == run_eval(FIXTURE).verdict_digest


def test_judge_online_collects_then_evaluates(monkeypatch):
    seen = []

    def fake_collect(ctx, crit, tr):  # 클러스터 대신 픽스처 폴더를 증거로 돌려준다
        seen.append(ctx.run_id)
        return FIXTURE

    monkeypatch.setattr(gather, "collect", fake_collect)
    v = judge(FIXTURE / "run.json")
    assert seen == [RUN_ID]
    assert v.verdict_digest == run_eval(FIXTURE).verdict_digest


def test_assess_returns_classified_rows():
    a = assess(load_run(FIXTURE / "run.json"), CRIT, FIXTURE)
    assert len(a.classified) == 10
    assert [c.key for c in a.classified] == sorted(c.key for c in a.classified)


# ---------------------------------------------------------------- 가드·입력 변형


def test_db_not_quiescent(tmp_path):
    run = copy_run(tmp_path)
    edit_snapshot(
        run,
        lambda db: replace(db, fingerprint2=replace(db.fingerprint1, orders=10)),
        refingerprint=False,
    )
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.DB_NOT_QUIESCENT,)
    g = by_id(v.guards)["db_quiescent"]
    assert g.value["f2"]["orders"] == 10


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
    assert "judge_self_check" not in checks


def test_missing_orders_section_is_db_error(tmp_path):
    """psql이 orders 구획에서 멈추면(끝 표지 없음) 분류와 행 불변식이 DB_ERROR다."""
    run = copy_run(tmp_path)
    p = run / SNAPSHOT
    text = p.read_text(encoding="utf-8")
    p.write_text(text.split("--@end orders")[0], encoding="utf-8")
    v = run_eval(run)
    assert v.verdict is Outcome.INCONCLUSIVE
    assert by_id(v.checks)["lost"].reasons == (R.DB_ERROR,)
    assert "orders" in v.integrity["db_errors"][0]


def test_schema_drift_is_inconclusive(tmp_path):
    """서비스 스키마의 UNIQUE(client_order_id)가 없다 → constraints 평가 불가(SCHEMA_DRIFT, 계약 명세 7)."""
    run = copy_run(tmp_path)

    def drift(db):
        schema = tuple(s for s in db.schema if s.detail != "UNIQUE (client_order_id)")
        return replace(db, schema=schema)

    edit_snapshot(run, drift)
    v = run_eval(run)
    inv = by_id(v.checks)["invariants"]
    assert inv.outcome is Outcome.INCONCLUSIVE
    assert inv.reasons == (R.SCHEMA_DRIFT,)
    assert v.integrity["invariants"]["constraints"] is None
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.SCHEMA_DRIFT,)


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


@pytest.mark.parametrize("flag", [True, False])
def test_sigint_exit_is_normal_only_with_stop_flag(tmp_path, flag):
    """SIGINT로 멈춘 k6(105)는 fetch가 본 정지 표시가 있을 때만 정상이다(설계 4.8)."""
    run = copy_run(tmp_path)

    def sigint(doc):
        doc["containers"]["k6"] = {"exitCode": 105, "reason": "Error"}
        doc["stop_requested"] = flag
        return doc

    edit_json(run / "k6" / "pod-status.json", sigint)
    v = run_eval(run)
    assert (v.load["k6_stop_requested"], v.load["stopped_by_judge"]) == (flag, flag)
    if flag:
        assert v.verdict is Outcome.PASS
    else:
        assert v.verdict is Outcome.INCONCLUSIVE
        assert by_id(v.checks)["k6_exit"].reasons == (R.LOAD_ABORTED,)
        assert by_id(v.checks)["orphans"].reasons == (R.SENT_LOG_INCOMPLETE,)


def test_token_key_in_summary_is_safety_violation(tmp_path):
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


def test_authorization_key_in_log_is_safety_violation(tmp_path):
    run = copy_run(tmp_path)

    def leak(rows):
        rows[0]["headers"] = {"Authorization": "x"}
        return rows

    edit_log(run, leak)
    v = run_eval(run)
    assert v.reasons == (R.SAFETY_VIOLATION,)
    assert by_id(v.guards)["safety"].value["token_key"] is True


@pytest.mark.parametrize(
    "body,field,n",
    [
        ("safety: {markets: [KRW-ETH]}\n", "bad_market", 10),
        ("safety: {user_pattern: '^user-000[2-9]$'}\n", "bad_user", 5),
        ("safety: {user_max: 1}\n", "bad_user", 5),  # user-0002가 범위 밖(계약 명세 11)
    ],
)
def test_safety_markets_and_user_pattern(tmp_path, body, field, n):
    """계약 명세 8, 11: 모든 intent.body의 market ∈ safety.markets, user_id가 user_pattern에 전체
    일치하고 끝 번호가 1..safety.user_max 안이다."""
    crit = write_criteria(tmp_path, body)
    v = run_eval(FIXTURE, crit)
    safety = by_id(v.guards)["safety"]
    assert safety.outcome is Outcome.FAIL
    assert safety.reasons == (R.SAFETY_VIOLATION,)
    assert safety.value[field] == n
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.SAFETY_VIOLATION,)


def test_safety_key_outside_run_prefix(tmp_path):
    """client_order_id가 이 run 키가 아니다(k6-<run_id>2-…). 접두어가 비슷해도 다른 run이다."""
    run = copy_run(tmp_path)
    foreign = f"k6-{RUN_ID}2-1-2"

    def rename_log(rows):
        for r in rows:
            if r.get("key") != LOST_KEY:
                continue
            r["key"] = foreign
            if r["type"] == "intent":
                r["body"]["client_order_id"] = foreign
            else:
                a = r["atts"][0]
                body = json.loads(a["raw"])
                body["client_order_id"] = foreign
                a["raw"] = service_json(body)
        return rows

    def rename_db(db):
        orders = tuple(
            replace(o, client_order_id=foreign) if o.client_order_id == LOST_KEY else o
            for o in db.orders
        )
        return replace(db, orders=orders)

    edit_log(run, rename_log)
    edit_snapshot(run, rename_db)
    v = run_eval(run)
    safety = by_id(v.guards)["safety"]
    assert safety.value["bad_key"] == 1
    assert safety.detail["examples"]["bad_key"] == [foreign]
    assert v.reasons == (R.SAFETY_VIOLATION,)


def _rename_key(run: Path, old: str, new: str) -> None:
    """기록(intent·result 원문)과 스냅샷에서 키 하나를 바꾼다."""

    def rename_log(rows):
        for r in rows:
            if r.get("key") != old:
                continue
            r["key"] = new
            if r["type"] == "intent":
                r["body"]["client_order_id"] = new
            else:
                for a in r["atts"]:
                    if a["st"] != 200:
                        continue
                    body = json.loads(a["raw"])
                    body["client_order_id"] = new
                    a["raw"] = service_json(body)
        return rows

    def rename_db(db):
        orders = tuple(
            replace(o, client_order_id=new) if o.client_order_id == old else o
            for o in db.orders
        )
        return replace(db, orders=orders)

    edit_log(run, rename_log)
    edit_snapshot(run, rename_db)


@pytest.mark.parametrize("length, bad", [(64, 0), (65, 1)])
def test_safety_key_longer_than_service_limit(tmp_path, length, bad):
    """계약 명세 11: client_order_id는 64자 이하다(서비스 schemas.py). 전체 run 키 형식이어도 넘으면 위반."""
    run = copy_run(tmp_path)
    head = f"k6-{RUN_ID}-1-"
    # 숫자 묶음만 늘린 같은 run 키(seq 2)
    long_key = head + "0" * (length - len(head) - 1) + "2"
    assert len(long_key) == length
    _rename_key(run, LOST_KEY, long_key)
    v = run_eval(run)
    safety = by_id(v.guards)["safety"]
    assert safety.detail["key_max"] == 64
    assert safety.value["bad_key"] == bad
    if bad:
        assert safety.detail["examples"]["bad_key"] == [long_key]
        assert v.reasons == (R.SAFETY_VIOLATION,)
    else:
        assert safety.outcome is Outcome.PASS


@pytest.mark.parametrize(
    "remap,field",
    [
        (lambda r, i: 1, "vu_multi_account"),  # vu 1이 user-0001·user-0002를 쓴다
        (lambda r, i: 2 + i % 2, "account_multi_vu"),  # user-0002를 vu 2·3이 쓴다
    ],
)
def test_safety_one_vu_one_user(tmp_path, remap, field):
    """1 VU = 1 user(계약 명세 8)를 judge가 다시 본다."""
    run = copy_run(tmp_path)

    def fn(rows):
        b = [r for r in rows if r["type"] == "intent" and r["acct"] == USER_B]
        for i, r in enumerate(b):
            r["vu"] = remap(r, i)
        return rows

    edit_log(run, fn)
    v = run_eval(run)
    safety = by_id(v.guards)["safety"]
    assert safety.outcome is Outcome.FAIL
    assert safety.reasons == (R.SAFETY_VIOLATION,)
    assert safety.value[field] == 1
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.SAFETY_VIOLATION,)


# user-0002 주문을 지우면 체결이 줄어 표본 기준만 낮춘다(가드 사유를 하나로 본다)
LOW_COVERAGE = "coverage: {min_filled: {tail: 1, steady: 1}, min_sell_filled: 1}\n"


def drop_user_b(db):
    """user-0002의 계좌·주문·포지션을 지운다(다른 DB를 본 것과 같다)."""
    return replace(
        db,
        accounts=tuple(a for a in db.accounts if a.user_id != USER_B),
        orders=tuple(o for o in db.orders if o.user_id != USER_B),
        positions=tuple(p for p in db.positions if p.user_id != USER_B),
    )


def test_account_set_mismatch(tmp_path):
    """JSONL의 user_id가 DB accounts에 없다 → ACCOUNT_SET_MISMATCH, LOST·ORPHAN 보류(계약 명세 8)."""
    run = copy_run(tmp_path)
    edit_snapshot(run, drop_user_b)
    v = run_eval(run, write_criteria(tmp_path, LOW_COVERAGE))
    assert v.verdict is Outcome.INCONCLUSIVE
    assert v.reasons == (R.ACCOUNT_SET_MISMATCH,)
    guard = by_id(v.guards)["account_set"]
    assert guard.value["missing"] == 1
    assert guard.detail["examples"]["missing"] == [USER_B]
    checks = by_id(v.checks)
    assert checks["lost"].reasons == (R.ACCOUNT_SET_MISMATCH,)
    assert checks["orphans"].outcome is Outcome.INCONCLUSIVE
    assert checks["mismatch"].outcome is Outcome.PASS
    assert v.integrity["not_evaluated"] == {
        "LOST": R.ACCOUNT_SET_MISMATCH,
        "ORPHAN": R.ACCOUNT_SET_MISMATCH,
    }


def test_account_set_mismatch_still_fails_reject_committed(tmp_path):
    """ACCOUNT_SET_MISMATCH면 LOST·ORPHAN만 평가하지 않는다. REJECT_COMMITTED는 FAIL이다(설계 5.7 규칙 1)."""
    run = copy_run(tmp_path)

    def reject(rows):
        reject_only(422, "insufficient_volume")(result_row(rows, SELL_KEY))
        return rows

    edit_log(run, reject)
    edit_snapshot(run, drop_user_b)
    v = run_eval(run)
    assert v.integrity["classes"]["REJECT_COMMITTED"] == 1
    lost = by_id(v.checks)["lost"]
    assert lost.outcome is Outcome.FAIL
    assert lost.detail["decided_by_partial"] is True
    assert v.verdict is Outcome.FAIL
    assert v.reasons == (R.INTEGRITY_VIOLATION,)
    assert by_id(v.guards)["account_set"].reasons == (R.ACCOUNT_SET_MISMATCH,)
    assert summary_line(v).startswith("유실 ≥1(ACCOUNT_SET_MISMATCH)")


def test_extra_account_missing_then_present(tmp_path):
    """발표자 user_id(run.json extra_accounts)가 DB에 없으면 EXTRA_ACCOUNTS_MISSING이다."""
    run = copy_run(tmp_path)
    edit_json(run / "run.json", lambda d: {**d, "extra_accounts": ["user-0999"]})
    v = run_eval(run)
    assert v.reasons == (R.EXTRA_ACCOUNTS_MISSING,)
    assert by_id(v.guards)["account_set"].value["extra_missing"] == 1

    def add(db):
        extra = DbAccount("user-0999", Decimal(INITIAL_KRW))
        return replace(db, accounts=(*db.accounts, extra))

    edit_snapshot(run, add)
    v = run_eval(run)
    assert v.verdict is Outcome.PASS
    assert by_id(v.guards)["account_set"].value["extra"] == 1


def test_snapshot_large_is_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline_mod, "ORDERS_WARN_ROWS", 5)
    v = run_eval(FIXTURE)
    assert v.verdict is Outcome.PASS
    assert v.warnings == (R.SNAPSHOT_LARGE,)


def test_judge_self_check_reports_classifier_gap(monkeypatch):
    """분류기 D1 수가 스냅샷 run 키 주문 수와 다르면 JUDGE_SELF_CHECK(보고만, 계약 명세 7)."""
    monkeypatch.setattr(pipeline_mod, "run_order_count", lambda result, run_id: 8)
    v = run_eval(FIXTURE)
    sc = by_id(v.checks)["judge_self_check"]
    assert sc.outcome is Outcome.INCONCLUSIVE
    assert sc.reasons == (R.JUDGE_SELF_CHECK,)
    assert sc.detail["differs"] == ["orders"]
    assert v.verdict is Outcome.PASS


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
    crit = write_criteria(
        tmp_path,
        "fault_effect: [{id: crash_restart, observe: crash_restart, target: order-api, min: 1}]\n",
    )
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


def test_fault_effect_observe_falls_back_to_collect_obs(tmp_path):
    crit = write_criteria(
        tmp_path,
        "fault_effect: [{id: crash_restart, observe: crash_restart, target: order-api, min: 1}]\n",
    )
    run = fault_run(tmp_path / "a", start=T0 + 4000, end=T0 + 5000)
    (run / "raw/k8s").mkdir()
    (run / "raw/k8s/obs.json").write_text('{"crash_restart": 1}', encoding="utf-8")
    item = by_id(run_eval(run, crit).guards)["crash_restart"]
    assert item.outcome is Outcome.PASS
    assert item.detail["source"] == "collect"
    # run.json obs(inject 관측)가 있으면 그것이 먼저다
    edit_json(run / "run.json", lambda d: {**d, "obs": {"crash_restart": 0}})
    item = by_id(run_eval(run, crit).guards)["crash_restart"]
    assert item.outcome is Outcome.FAIL
    assert item.detail["source"] == "obs"


def test_fault_effect_class_sub_counts_retry_paths(tmp_path):
    """pod-kill 기대 효과(class_sub not_committed·after_503)는 분류 결과 OkSub로 센다."""
    crit = write_criteria(
        tmp_path,
        "fault_effect: [{id: shutdown_seen_by_k6, class_sub: [not_committed, after_503], "
        "account: any, op: '>=', value: 1}]\n",
    )
    run = fault_run(tmp_path / "run", start=T0 + 4000, end=T0 + 5000)
    item = by_id(run_eval(run, crit).guards)["shutdown_seen_by_k6"]
    assert item.outcome is Outcome.PASS
    assert item.value == 1
    crit_idx = write_criteria(
        tmp_path,
        "fault_effect: [{id: crash_seen_by_k6, class_sub: [after_commit], "
        "account: idx0, op: '>=', value: 1}]\n",
    )
    item = by_id(run_eval(run, crit_idx).guards)["crash_seen_by_k6"]
    assert item.outcome is Outcome.PASS  # idx0 = 가장 작은 VU의 user(user-0001)
    assert item.detail["account"] == "idx0"


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
    # k6_end = 마지막 시도 끝(atts[].t1) + 1. v2 result 줄에는 시각이 없다
    end = max(a.t1 for r in log.results.values() for a in r.atts) + 1
    assert w["baseline"] == (T0 + 4100 - 150_000, T0 + 4100 - 2000)
    assert w["fault"] == (T0 + 4100, T0 + 5100 + 60_000)
    assert w["recovery"] == (T0 + 65_100, T0 + 65_100)  # 끝 뒤라 빈 창
    assert w["tail"] == (end - 120_000, end)
    assert w["warmup"] == (T0, T0 + 2000)  # k6_start = run_start.t
    assert "steady" not in w
    steady = phase_windows(run, CRIT, log)
    assert steady["steady"] == (T0 + 2000, end)
    assert "baseline" not in steady


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
    run = fault_run(tmp_path / "run", start=T0 + 4000, end=T0 + 5000)
    write_resolved(source, run)
    # 증거 판정은 resolved를 읽되 inputs.criteria_sha256은 run.json이 가리키는 원본 파일 값이다
    resolved = select_criteria(p, run, prefer_resolved=True)
    assert resolved.source == run / RESOLVED_NAME
    assert source.sha256 == resolved.sha256
    assert source.file_sha256 == resolved.file_sha256
    v1, v2 = run_eval(run, source), run_eval(run, resolved)
    assert v1.verdict_digest == v2.verdict_digest
    item = by_id(v1.guards)["crash_restart"]
    assert item.required is False  # 가드가 없으면 보고만 한다
    assert by_id(v2.guards)["crash_restart"].required is False
    assert R.NOT_EVALUATED not in v1.reasons


def test_fault_effect_guard_present_keeps_items_required(tmp_path):
    crit = write_criteria(
        tmp_path,
        "fault_effect: [{id: crash_restart, observe: crash_restart, target: order-api, min: 1}]\n",
    )
    run = fault_run(tmp_path / "run", start=T0 + 4000, end=T0 + 5000)
    item = by_id(run_eval(run, crit).guards)["crash_restart"]
    assert item.required is True
    assert item.reasons == (R.NOT_EVALUATED,)


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

    edit_json(run / "k6" / "pod-status.json", oom)
    edit_snapshot(run, drop_orders(LOST_KEY))
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
    """줄 순서와 응답 원문 JSON 키 순서를 바꿔도 판정 본문이 같다(설계 8). 입력 해시만 바뀐다."""
    run = copy_run(tmp_path)

    def shuffle(rows):
        head, rest = rows[:1], rows[1:]
        for r in rest:
            for a in r.get("atts", ()):
                if a["raw"] is not None:
                    a["raw"] = json.dumps(json.loads(a["raw"]), sort_keys=True)
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


@pytest.mark.parametrize("cmd", ["evaluate", "run"])
def test_verdict_passes_runner_acceptance(tmp_path, cmd):
    """A의 수용 조건(orchestrator.judging.accept, PR #5)을 judge 출력이 그대로 통과한다.

    runner는 `python -m orchestrator.judge run --run … [--offline]`을 부르고 run.json criteria
    파일 원본 바이트의 sha256을 verdict inputs.criteria_sha256과 대조한다(설계 1.4).
    resolved 기준으로 판정해도 그 값은 run.json이 가리키는 원본 파일 값이어야 한다.
    """
    from orchestrator.judging import accept

    run = copy_run(tmp_path)  # criteria.resolved.yaml이 함께 있다
    run_json = run / "run.json"
    rc = cli.main([cmd, "--run", str(run_json), "--offline"])
    verdict = json.loads((run / "verdict.json").read_text(encoding="utf-8"))
    got = accept(rc, verdict, run_json, load_run(run_json).run_id)
    assert got.passed, got.details
    raw = hashlib.sha256((run / "criteria.yaml").read_bytes()).hexdigest()
    assert verdict["inputs"]["criteria_sha256"] == raw
    assert verdict["inputs"]["criteria_resolved_sha256"] == CRIT.sha256


def test_runner_acceptance_fails_when_criteria_file_changes(tmp_path):
    """판정 뒤 run.json의 기준 파일이 바뀌면 runner 대조가 실패한다(바뀐 기준의 판정이 아니다)."""
    from orchestrator.judging import accept

    run = copy_run(tmp_path)
    run_json = run / "run.json"
    rc = cli.main(["evaluate", "--run", str(run_json), "--offline"])
    verdict = json.loads((run / "verdict.json").read_text(encoding="utf-8"))
    crit_file = run / "criteria.yaml"
    crit_file.write_text(
        crit_file.read_text(encoding="utf-8") + "# 바뀜\n", encoding="utf-8"
    )
    got = accept(rc, verdict, run_json, load_run(run_json).run_id)
    assert not got.passed
    assert got.details["checks"]["criteria_sha256"] is False

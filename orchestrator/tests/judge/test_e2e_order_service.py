"""-m e2e: 실제 주문 서비스(workloads/order)를 띄워 judge를 끝까지 돌린다(계약 명세 10 마지막 항목).

- docker로 postgres:16(e2e-judge-pg)과 redis:7(e2e-judge-redis)을 띄운다. 호스트 포트는 docker가
  127.0.0.1의 빈 포트를 고른다. 스키마·seed는 workloads/order/db/schema.sql·seed.sql을 그대로 넣는다.
- 앱은 별도 venv의 파이썬(환경변수 ORDER_SERVICE_PYTHON)으로 `python -m app`(cwd=workloads/order)을 띄운다.
  서비스 설정은 DATABASE_URL·REDIS_URL·PORT만 주고, 셸에서는 PATH·HOME·LANG 같은 실행 값만
  넘긴다(e2e_client.app_env). 셸의 STALE_MS·DB_POOL_SIZE·PG*는 넘기지 않는다(서비스 기본값을 쓴다).
  DB 비밀번호는 테스트가 그때 만든다(코드에 없다).
- 시세는 수집기 대역(e2e_client.Ticker)이 1초마다 ticks 행과 Redis price:KRW-BTC를 함께 쓴다.
- 파이썬 클라이언트(e2e_client.Recorder)가 k6와 같은 JSONL v2(계약 명세 2)를 쓰며 계획 도착률대로 주문을 보낸다.
  정상 매수·매도, 같은 키 재요청(replayed), 422 insufficient_volume, 503 stale_price 뒤 시세 갱신 후 같은 키
  재시도 체결, 같은 키 동시 전송, 전량 매도(volume 0, avg 유지).
- judge db 번들은 `docker exec -i e2e-judge-pg psql -U exchange -d exchange`(Transports.for_local_pg)로 돈다.
  evaluate는 PASS다. krw를 1원 바꾸고 다시 찍으면 FAIL(INTEGRITY_VIOLATION, cash 1건)이다.
- `-m e2e`를 명시하고 ORDER_SERVICE_PYTHON이 실행 파일일 때만 돈다. docker를 못 쓰면 skip한다.
  끝나면 앱 프로세스를 멈추고 두 컨테이너를 지운다. 기본 실행(`-m "not … and not e2e"`)에서는 돌지 않는다.

준비(예): python3.11 -m venv <venv> && <venv>/bin/pip install -r workloads/order/requirements.txt
실행: ORDER_SERVICE_PYTHON=<venv>/bin/python ../.venv/bin/pytest -q -m e2e tests/judge/test_e2e_order_service.py
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest

from orchestrator.judge import reasons as R
from orchestrator.judge.context import load_run
from orchestrator.judge.criteria import load_criteria
from orchestrator.judge.model import KState, OkSub, OrderClass, Outcome
from orchestrator.judge.pipeline import assess, render_outputs
from orchestrator.judge.report import summary_line

from .e2e_client import (
    PG_DB,
    PG_USER,
    OrderService,
    Recorder,
    Ticker,
    app_env,
    docker,
    docker_available,
    env_with,
    free_port,
    host_port,
    price_series,
    psql,
    run_key,
    take_snapshot,
    wait_app_ready,
    wait_pg_ready,
    wait_redis_ready,
    wait_until,
    write_evidence,
)

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[3]
APP_DIR = ROOT / "workloads" / "order"
SERVICE_DB = APP_DIR / "db"
DEFAULT_YAML = ROOT / "experiments" / "criteria" / "default.yaml"
PYTHON_ENV = "ORDER_SERVICE_PYTHON"
PG = "e2e-judge-pg"
REDIS = "e2e-judge-redis"
PG_IMAGE = "postgres:16"  # 서비스 docker-compose.dev.yml과 같다
REDIS_IMAGE = "redis:7"

RUN_ID = "r-e2e-order"
RATE = 2  # 초당 intent(계획 도착률). WARMUP_S × RATE가 정수라 steady 달성률이 정확히 맞는다
WARMUP_S = 2
VUS = 5  # vu n = user-000n(1 VU = 1 user)
BURST = 4  # 같은 키 동시 전송 수
STALE_AGE_MS = 10_000  # 서비스 STALE_MS(기본 5000)보다 오래된 시세

# (vu, side, volume, 경로). 한 줄 = intent 하나, RATE로 차례로 보낸다.
# once: 한 번. replay: 200 뒤 같은 키 재요청(replayed=true). reject: 422.
# stale: 503 stale_price 뒤 시세 갱신, 같은 키 재전송(200 replayed=false). burst: 같은 키 BURST건 동시.
STEPS = (
    (1, "bid", "0.0010", "once"),
    (2, "bid", "0.0020", "once"),
    (3, "bid", "0.0015", "once"),
    (4, "bid", "0.0005", "once"),
    (5, "bid", "0.00123456", "once"),
    (1, "bid", "0.0007", "once"),
    (2, "ask", "0.0005", "once"),
    (3, "bid", "0.0001", "replay"),
    (4, "ask", "0.0100", "reject"),  # 보유 0.0005 → 422 insufficient_volume
    (1, "ask", "0.0004", "once"),
    (5, "bid", "0.0001", "stale"),
    (2, "bid", "0.0003", "burst"),
    (3, "ask", "0.0006", "once"),
    (4, "bid", "0.0003", "once"),
    (5, "ask", "0.0002", "once"),
    (1, "bid", "0.00000001", "once"),
    (2, "bid", "0.0002", "burst"),
    (3, "bid", "0.0002", "once"),
    (4, "ask", "0.0008", "once"),  # 전량 매도. positions.volume 0, avg_price 유지
    (5, "bid", "0.0004", "once"),
)
PLAN = {
    "executor": "ramping-arrival-rate",
    "time_unit_s": 1,
    "start_rate": RATE,
    "stages": [{"duration_s": len(STEPS) // RATE, "target": RATE}],
}
N_FILLED = sum(1 for s in STEPS if s[3] != "reject")
N_SELL = sum(1 for s in STEPS if s[3] != "reject" and s[1] == "ask")

# 테스트용 기준: 실제 default.yaml을 상속하고 작은 run에 맞게 표본 기준만 낮춘다.
# 구간 p95·오류율(client)은 1차 미구현이라 required: false로 보고만 한다.
CRITERIA_TEXT = f"""# test_e2e_order_service.py가 쓴 기준(실서비스 e2e)
extends: {json.dumps(str(DEFAULT_YAML))}
phases: {{warmup: {WARMUP_S}s}}
coverage: {{min_filled: {{tail: 15, steady: 12}}, min_sell_filled: 4}}
checks: [{{id: order_p95_k6, required: false}}, {{id: order_err_k6, required: false}}]
"""


def key(vu: int, seq: int) -> str:
    return run_key(RUN_ID, vu, seq)  # Recorder와 같은 k6 형식(계약 명세 11)


def _keys_by_path() -> dict[str, list[str]]:
    """경로 → 키(STEPS 순서). 키의 seq는 vu별 차례다."""
    seq: dict[int, int] = {}
    out: dict[str, list[str]] = {}
    for vu, _side, _vol, path in STEPS:
        seq[vu] = seq.get(vu, 0) + 1
        out.setdefault(path, []).append(key(vu, seq[vu]))
    return out


KEYS = _keys_by_path()


# ---------------------------------------------------------------- 환경


@dataclass(frozen=True)
class Stack:
    app_port: int
    app: OrderService
    work: Path


def _remove_containers() -> None:
    docker("rm", "-f", PG, REDIS, timeout=60)


@pytest.fixture(scope="module")
def stack(request, tmp_path_factory):
    # 명시적으로 -m e2e를 줬을 때만 돈다(그냥 pytest -q로 컨테이너·앱을 띄우지 않는다)
    if "e2e" not in (request.config.getoption("markexpr") or ""):
        pytest.skip("-m e2e로 실행할 때만 돈다")
    python = os.environ.get(PYTHON_ENV)
    if not python:
        pytest.skip(f"{PYTHON_ENV}(주문 서비스 venv의 python 경로)가 없다")
    if not (Path(python).is_file() and os.access(python, os.X_OK)):
        pytest.skip(f"{PYTHON_ENV}={python!r}는 실행 파일이 아니다")
    if shutil.which("docker") is None or not docker_available():
        pytest.skip("docker를 쓸 수 없다")

    work = tmp_path_factory.mktemp("e2e-stack")
    password = secrets.token_urlsafe(18)  # 이 컨테이너 전용. 저장하지 않는다
    app: OrderService | None = None
    _remove_containers()
    try:
        cp = docker(
            "run",
            "-d",
            "--name",
            PG,
            "-e",
            f"POSTGRES_USER={PG_USER}",
            "-e",
            f"POSTGRES_DB={PG_DB}",
            "-e",
            "POSTGRES_PASSWORD",  # 값은 docker CLI 환경변수로 넘긴다(argv에 남기지 않는다)
            "-p",
            "127.0.0.1::5432",
            PG_IMAGE,
            env=env_with(POSTGRES_PASSWORD=password),
            timeout=600,
        )
        if cp.returncode != 0:
            pytest.skip(f"{PG_IMAGE}를 띄우지 못했다: {cp.stderr.decode()[-300:]}")
        cp = docker(
            "run",
            "-d",
            "--name",
            REDIS,
            "-p",
            "127.0.0.1::6379",
            REDIS_IMAGE,
            timeout=600,
        )
        if cp.returncode != 0:
            pytest.skip(f"{REDIS_IMAGE}를 띄우지 못했다: {cp.stderr.decode()[-300:]}")
        wait_pg_ready(PG)
        wait_redis_ready(REDIS)
        psql(PG, (SERVICE_DB / "schema.sql").read_text(encoding="utf-8"))
        psql(PG, (SERVICE_DB / "seed.sql").read_text(encoding="utf-8"))

        pg_port, redis_port = host_port(PG, "5432/tcp"), host_port(REDIS, "6379/tcp")
        app_port = free_port()
        app = OrderService(
            python=python,
            app_dir=APP_DIR,
            env=app_env(
                DATABASE_URL=f"postgresql://{PG_USER}:{password}@127.0.0.1:{pg_port}/{PG_DB}",
                REDIS_URL=f"redis://127.0.0.1:{redis_port}/0",
                PORT=str(app_port),
            ),
            log_path=work / "order-service.log",
            port=app_port,
        )
        app.start()
        wait_app_ready(app_port, app.alive, app.log_tail)
        yield Stack(app_port=app_port, app=app, work=work)
    finally:
        if app is not None:
            app.stop()
        _remove_containers()


def _run_step(rec: Recorder, ticker: Ticker, step: tuple[int, str, str, str]) -> None:
    vu, side, volume, path = step
    k, body = rec.intent(vu, side, volume)
    if path == "stale":
        with ticker.paused():
            ticker.make_stale(STALE_AGE_MS)
            atts = [rec.send(body)]
        ticker.tick()  # 시세 갱신 뒤 같은 키·같은 본문으로 다시 보낸다
        atts.append(rec.send(body))
    elif path == "burst":
        atts = rec.send_concurrently(body, BURST)
    else:
        atts = [rec.send(body)]
        if path == "replay":
            atts.append(rec.send(body))
    rec.result(k, atts)


@pytest.fixture(scope="module")
def run_dir(stack, tmp_path_factory) -> Path:
    """시나리오를 한 번 돌려 증거 폴더(runs/<run_id>/ 모양)를 만든다."""
    out = tmp_path_factory.mktemp("e2e-run") / RUN_ID
    out.mkdir()
    rec = Recorder(port=stack.app_port, run_id=RUN_ID, plan=PLAN, vus=VUS)
    ticker = Ticker(PG, REDIS, prices=price_series(), period_s=1.0)
    ticker.start()
    try:
        origin = rec.run_start()
        for i, step in enumerate(STEPS):
            wait_until(origin + i * 1000 // RATE)
            _run_step(rec, ticker, step)
        end = rec.run_end()
    finally:
        ticker.stop()
    assert not ticker.errors, ticker.errors
    assert stack.app.alive(), stack.app.log_tail()
    write_evidence(out, rec, criteria_text=CRITERIA_TEXT, duration_ms=end - origin)
    ctx = load_run(out / "run.json")
    take_snapshot(out, ctx, load_criteria(ctx.criteria), PG)
    return out


def _judge(run: Path):
    """evaluate와 같다(assess). 판정 결과 파일도 증거 폴더에 남긴다(실패 때 보기 쉽게)."""
    ctx = load_run(run / "run.json")
    a = assess(ctx, load_criteria(ctx.criteria), run)
    for name, text in render_outputs(a).items():
        (run / name).write_text(text, encoding="utf-8")
    return a


def _results(run: Path) -> dict[str, list[dict]]:
    rows = [
        json.loads(x) for x in (run / "k6" / "orders.jsonl").read_text().splitlines()
    ]
    return {r["key"]: r["atts"] for r in rows if r["type"] == "result"}


def by_id(items):
    return {x.id: x for x in items}


# ---------------------------------------------------------------- 시나리오가 서비스 경로를 지났는지


def test_scenario_hits_service_paths(run_dir):
    """PASS가 빈말이 아니도록, 기록이 의도한 서비스 경로를 지났는지 먼저 본다(계약 명세 1)."""
    res = _results(run_dir)
    assert len(res) == len(STEPS)
    for k in KEYS["once"]:
        assert [(a["st"], a["rp"]) for a in res[k]] == [(200, False)], k
    for k in KEYS["replay"]:
        atts = res[k]
        assert [(a["st"], a["rp"]) for a in atts] == [(200, False), (200, True)]
        ids = {json.loads(a["raw"])["order_id"] for a in atts}
        assert len(ids) == 1
    for k in KEYS["reject"]:
        assert [(a["st"], a["ec"]) for a in res[k]] == [(422, "insufficient_volume")]
    for k in KEYS["stale"]:
        atts = res[k]
        assert [(a["st"], a["ec"], a["rp"]) for a in atts] == [
            (503, "stale_price", None),
            (200, None, False),
        ]
    for k in KEYS["burst"]:
        atts = res[k]
        assert len(atts) == BURST and all(a["st"] == 200 for a in atts), atts
        # 서비스: 하나만 새로 체결하고 나머지는 같은 행을 replayed=true로 돌려준다(계약 명세 1 ④)
        assert sorted(a["rp"] for a in atts) == [False] + [True] * (BURST - 1)
        assert len({json.loads(a["raw"])["order_id"] for a in atts}) == 1
    # 전량 매도한 계좌는 positions.volume 0, avg_price 유지(행이 남는다)
    out = psql(
        PG,
        "COPY (SELECT volume, avg_price FROM positions WHERE user_id = 'user-0004') "
        "TO STDOUT WITH CSV;\n",
    )
    volume, avg = out.strip().split(",")
    assert float(volume) == 0 and float(avg) > 0


# ---------------------------------------------------------------- PASS


def test_real_service_run_passes(run_dir):
    a = _judge(run_dir)
    v = a.verdict
    assert v.verdict is Outcome.PASS, (v.reasons, run_dir)
    assert v.reasons == ()
    assert v.warnings == ()
    checks, guards = by_id(v.checks), by_id(v.guards)
    for cid in ("lost", "mismatch", "duplicates", "orphans", "invariants", "protocol"):
        assert checks[cid].outcome is Outcome.PASS, (cid, checks[cid])
        assert checks[cid].value == 0, cid
    # 수집기 대역이 ticks를 썼으므로 신선도도 평가된다(required: false)
    assert checks["stale_fill"].outcome is Outcome.PASS, checks["stale_fill"]
    assert checks["k6_exit"].outcome is Outcome.PASS
    self_check = checks["judge_self_check"]
    assert self_check.outcome is Outcome.PASS, self_check.detail
    assert self_check.detail["snapshot"]["orders"] == N_FILLED
    for gid in (
        "safety",
        "account_set",
        "fill_coverage",
        "sell_coverage",
        "load_coverage",
        "db_quiescent",
    ):
        assert guards[gid].outcome is Outcome.PASS, (gid, guards[gid])
    assert guards["log_complete"].value is True, guards["log_complete"].detail
    assert guards["sell_coverage"].value == N_SELL

    integ = v.integrity
    assert integ["intents"] == {
        "total": len(STEPS),
        "ack": N_FILLED,
        "results": len(STEPS),
    }
    assert integ["classes"][OrderClass.OK] == len(STEPS)
    assert sum(integ["classes"].values()) == len(STEPS)
    assert set(integ["invariants"].values()) == {0}, integ["invariants"]
    assert "fresh" in integ["invariants"]
    subs = integ["subs"]
    assert subs[OkSub.REJECTED] == 1
    assert subs[OkSub.NOT_COMMITTED] == len(KEYS["stale"])
    assert subs[OkSub.AFTER_503] == 0
    # 동시 전송 키는 이긴 시도가 n=1이면 clean, 아니면 after_commit(겹친 시도가 먼저 커밋)이다
    assert subs[OkSub.CLEAN] + subs[OkSub.AFTER_COMMIT] == N_FILLED - len(KEYS["stale"])
    assert subs[OkSub.AFTER_COMMIT] <= len(KEYS["burst"])
    assert summary_line(v).endswith("→ PASS")

    rows = {c.key: c for c in a.classified}
    for k in KEYS["stale"]:
        assert rows[k].k_state is KState.K_ACK and rows[k].sub is OkSub.NOT_COMMITTED
    for k in KEYS["reject"]:
        assert rows[k].k_state is KState.K_REJECT and rows[k].sub is OkSub.REJECTED
        assert rows[k].order_ids == ()
    for k in KEYS["replay"]:
        assert rows[k].sub is OkSub.CLEAN and rows[k].idem == ()
    for k in KEYS["burst"]:
        assert rows[k].cls is OrderClass.OK and rows[k].idem == (), rows[k].detail
        assert len(rows[k].order_ids) == 1


# ---------------------------------------------------------------- 변형: 잔고 조작


def test_krw_tamper_fails_cash_invariant(run_dir, tmp_path):
    """DB에서 한 계좌의 krw를 1원 바꾸고 다시 찍으면 FAIL(INTEGRITY_VIOLATION), cash 1건(계약 명세 7)."""
    run = tmp_path / RUN_ID
    shutil.copytree(run_dir, run)
    user = "user-0002"
    psql(PG, f"UPDATE accounts SET krw = krw + 1 WHERE user_id = '{user}';\n")
    try:
        ctx = load_run(run / "run.json")
        take_snapshot(run, ctx, load_criteria(ctx.criteria), PG)
    finally:
        psql(PG, f"UPDATE accounts SET krw = krw - 1 WHERE user_id = '{user}';\n")
    v = _judge(run).verdict
    assert v.verdict is Outcome.FAIL
    assert v.reasons == (R.INTEGRITY_VIOLATION,)
    inv = v.integrity["invariants"]
    assert inv["cash"] == 1, inv
    assert all(n == 0 for k, n in inv.items() if k != "cash"), inv
    checks = by_id(v.checks)
    assert checks["invariants"].outcome is Outcome.FAIL
    assert checks["invariants"].value == 1
    for cid in ("lost", "mismatch", "duplicates", "orphans"):
        assert checks[cid].outcome is Outcome.PASS, cid
    assert by_id(v.guards)["db_quiescent"].outcome is Outcome.PASS
    assert summary_line(v).startswith(
        "유실 0 · 불일치 0 · 중복 0 · 고아 0 · 잔고 불일치 1"
    )

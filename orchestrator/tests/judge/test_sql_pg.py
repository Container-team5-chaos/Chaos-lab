"""-m pg: docker postgres에 명세 DDL을 넣고 bundle.sql 전체를 psql로 돌린다(설계 8).

- 이미지 postgres:18(없으면 postgres:17), 컨테이너 pgtest-judge. 끝나면 docker rm -f로 지운다.
- 접속은 운영과 같은 경로다: 컨테이너 안 Unix 소켓 trust, `-U pt_ro`, `psql -f -`.
  Transports.for_local_pg()가 kubectl 접두어를 docker exec로 바꾼다.
- 깨끗한 데이터에서는 모든 불변식 구획이 비어야 한다. 위반을 하나씩 심어 해당 구획만 행을 내는지 본다.
- `-m pg`를 명시했을 때만 돈다(같은 컨테이너 이름을 쓰는 실행이 겹치지 않게). docker가 없거나
  데몬이 꺼져 있으면 skip한다.
"""

from __future__ import annotations

import re
import secrets
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from orchestrator.judge import db as D
from orchestrator.judge import reasons as R
from orchestrator.judge.criteria import load_criteria
from orchestrator.judge.invariants import check_invariants
from orchestrator.judge.model import (
    INVARIANT_SQL,
    SNAPSHOT_SECTIONS,
    Outcome,
    SpeedPoint,
)
from orchestrator.judge.transport import Transports

pytestmark = pytest.mark.pg

CONTAINER = "pgtest-judge"
IMAGES = ("postgres:18", "postgres:17")
HERE = Path(__file__).resolve().parent
SCHEMA = HERE / "fixtures" / "schema.sql"
DEFAULT_YAML = HERE.parents[2] / "experiments" / "criteria" / "default.yaml"
RUN = "r1117-crash"

# 계좌
A = "00000000-0000-4000-8000-0000000000a1"  # k6 idx0(JSONL)
B = "00000000-0000-4000-8000-0000000000b1"  # k6 idx1(JSONL)
S = "00000000-0000-4000-8000-0000000000c1"  # 서버에만 생긴 k6 계좌(정규식으로만 찾는다)
E = "00000000-0000-4000-8000-0000000000e1"  # extra(발표자)
F = "00000000-0000-4000-8000-0000000000f1"  # 다른 run(r1117-crash-2). 위반이 있어도 안 보여야 한다
L = "00000000-0000-4000-8000-0000000000d1"  # 무관한 live 계좌. 위반이 있어도 안 보여야 한다
G = "00000000-0000-4000-8000-0000000000a9"  # i2 위반 심기용


def _oid(n: int) -> str:
    return f"00000000-0000-4000-9000-{n:012d}"


def _key(n: int) -> str:
    return f"00000000-0000-4000-a000-{n:012d}"


EPOCH0 = datetime(2025, 11, 5, tzinfo=timezone.utc)
R0 = int(EPOCH0.timestamp()) * 1000


def _ts(off_ms: int) -> str:
    return (EPOCH0 + timedelta(milliseconds=off_ms)).isoformat()


BTC = "REPLAY:UPBIT:KRW-BTC"
ETH = "REPLAY:UPBIT:KRW-ETH"

# (n, account, symbol, side, qty, limit, status, reason, ref_price, ref_age, received offset)
ORDERS = [
    (
        1,
        A,
        BTC,
        "buy",
        "0.00010000",
        "105000000",
        "FILLED",
        None,
        "100000000",
        500,
        1123,
    ),
    (
        2,
        A,
        BTC,
        "sell",
        "0.00005000",
        "104500000",
        "FILLED",
        None,
        "110000000",
        500,
        2000,
    ),
    (
        3,
        A,
        BTC,
        "buy",
        "0.00010000",
        "105000000",
        "REJECTED",
        "STALE_PRICE",
        "100000000",
        70000,
        3000,
    ),
    (4, B, ETH, "buy", "0.00020000", "5250000", "FILLED", None, "5000000", 500, 4000),
    (5, B, ETH, "sell", "0.00020000", "5225000", "FILLED", None, "5500000", 500, 5000),
    (
        6,
        E,
        BTC,
        "buy",
        "0.00010000",
        "105000000",
        "FILLED",
        None,
        "100000000",
        500,
        6000,
    ),
    (
        7,
        F,
        BTC,
        "buy",
        "0.00010000",
        "105000000",
        "FILLED",
        None,
        "100000000",
        500,
        7000,
    ),
    (
        8,
        L,
        BTC,
        "buy",
        "0.00010000",
        "105000000",
        "FILLED",
        None,
        "100000000",
        500,
        8000,
    ),
]
# (order n, account, symbol, side, qty, price, amount, fee, realized_pnl)
FILLS = [
    (1, A, BTC, "buy", "0.00010000", "100000000", 10000, 5, -5),
    (2, A, BTC, "sell", "0.00005000", "110000000", 5500, 3, 497),
    (4, B, ETH, "buy", "0.00020000", "5000000", 1000, 1, -1),
    (5, B, ETH, "sell", "0.00020000", "5500000", 1100, 1, 99),
    (6, E, BTC, "buy", "0.00010000", "100000000", 10000, 5, -5),
    (7, F, BTC, "buy", "0.00010000", "100000000", 10000, 5, -5),
    (8, L, BTC, "buy", "0.00010000", "100000000", 10000, 5, -5),
]
# (id, nickname, mode, initial_cash, cash, version). F·L은 현금이 틀렸고 L은 live인데 REPLAY 체결이다.
ACCOUNTS = [
    (A, "k6-r1117-crash-0", "replay", 10_000_000, 9_995_492, 3),
    (B, "k6-r1117-crash-1", "replay", 10_000_000, 10_000_098, 4),
    (S, "k6-r1117-crash-5", "replay", 10_000_000, 10_000_000, 0),
    (E, "presenter", "replay", 10_000_000, 9_989_995, 1),
    (F, "k6-r1117-crash-2-0", "replay", 10_000_000, 1, 0),
    (L, "someone", "live", 10_000_000, 2, 0),
]
POSITIONS = [
    (A, BTC, "0.00005000", 5000),
    (B, ETH, "0.00000000", 0),
    (E, BTC, "0.00010000", 10000),
    (F, BTC, "0.00010000", 10000),
    (L, BTC, "0.00010000", 10000),
]


def _q(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _clean_data_sql() -> str:
    out = []
    for aid, nick, mode, init, cash, ver in ACCOUNTS:
        out.append(
            "INSERT INTO accounts (id, nickname, mode, initial_cash, cash, version, created_at) "
            f"VALUES ({_q(aid)}, {_q(nick)}, {_q(mode)}, {init}, {cash}, {ver}, {_q(_ts(0))});"
        )
    for n, acct, sym, side, qty, lim, st, rsn, ref, age, off in ORDERS:
        recv = R0 + off
        ev = recv - age
        out.append(
            "INSERT INTO orders (id, account_id, idem_key, symbol, side, qty, limit_price, status, "
            "reject_reason, ref_source, ref_seq, ref_event_ts, ref_recv_ts, ref_price, ref_age_ms, "
            "received_at) VALUES ("
            + ", ".join(
                [
                    _q(_oid(n)),
                    _q(acct),
                    _q(_key(n)),
                    _q(sym),
                    _q(side),
                    qty,
                    lim,
                    _q(st),
                    _q(rsn),
                    _q("replay"),
                    _q(str(1000 + n)),
                    str(ev),
                    str(ev + 10),
                    ref,
                    str(age),
                    _q(_ts(off)),
                ]
            )
            + ");"
        )
    for n, acct, sym, side, qty, price, amount, fee, pnl in FILLS:
        out.append(
            "INSERT INTO fills (order_id, account_id, symbol, side, qty, price, amount, fee, "
            "fee_rate, realized_pnl, created_at) VALUES ("
            f"{_q(_oid(n))}, {_q(acct)}, {_q(sym)}, {_q(side)}, {qty}, {price}, {amount}, {fee}, "
            f"0.000500, {pnl}, {_q(_ts(n * 1000 + 10))});"
        )
    for acct, sym, qty, cb in POSITIONS:
        out.append(
            "INSERT INTO positions (account_id, symbol, qty, cost_basis) "
            f"VALUES ({_q(acct)}, {_q(sym)}, {qty}, {cb});"
        )
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- docker


def _docker(*args: str, input: bytes | None = None, timeout: float = 120):
    return subprocess.run(
        ["docker", *args], input=input, capture_output=True, timeout=timeout
    )


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return _docker("info", timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _psql_super(sql: str, dbname: str = "papertrade") -> None:
    cp = _docker(
        "exec",
        "-i",
        CONTAINER,
        "psql",
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "postgres",
        "-d",
        dbname,
        "-f",
        "-",
        input=sql.encode("utf-8"),
    )
    assert cp.returncode == 0, cp.stderr.decode("utf-8", "replace")


def _wait_ready(deadline_s: float = 90) -> None:
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        # TCP로 확인한다. 초기화 중 임시 서버는 Unix 소켓만 연다.
        cp = _docker(
            "exec", CONTAINER, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"
        )
        if cp.returncode == 0:
            return
        time.sleep(1)
    logs = _docker("logs", "--tail", "30", CONTAINER).stderr.decode("utf-8", "replace")
    raise RuntimeError(f"postgres가 {deadline_s}s 안에 준비되지 않았다\n{logs}")


@pytest.fixture(scope="module")
def pg_container(request):
    # 명시적으로 -m pg(또는 pg를 포함한 식)를 줬을 때만 돈다. 그냥 pytest -q에서는 컨테이너를 띄우지 않는다
    if "pg" not in (request.config.getoption("markexpr") or ""):
        pytest.skip("-m pg로 실행할 때만 돈다")
    if not _docker_available():
        pytest.skip("docker를 쓸 수 없다")
    _docker("rm", "-f", CONTAINER)
    started = None
    errors = []
    try:
        for image in IMAGES:
            cp = _docker(
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-e",
                # 초기화에만 쓰는 일회용 값. 접속은 Unix 소켓 trust라 쓰지 않는다
                f"POSTGRES_PASSWORD={secrets.token_hex(16)}",
                "-e",
                "POSTGRES_DB=papertrade",
                image,
                timeout=600,
            )
            if cp.returncode == 0:
                started = image
                break
            errors.append(cp.stderr.decode("utf-8", "replace").strip())
            _docker("rm", "-f", CONTAINER)
        if started is None:
            pytest.skip(f"postgres 컨테이너를 띄우지 못했다: {errors}")
        _wait_ready()
        yield started
    finally:
        _docker("rm", "-f", CONTAINER)


@pytest.fixture
def fresh_db(pg_container):
    """매 테스트마다 papertrade DB를 새로 만들고 명세 DDL과 깨끗한 데이터를 넣는다."""
    _psql_super(
        "DROP DATABASE IF EXISTS papertrade WITH (FORCE);\nCREATE DATABASE papertrade;\n",
        dbname="postgres",
    )
    _psql_super(SCHEMA.read_text(encoding="utf-8"))
    _psql_super(_clean_data_sql())
    return pg_container


def _snapshot():
    bundle = D.build_bundle(None, [A, B], [E], RUN)
    run = D.exec_bundle(Transports.for_local_pg(CONTAINER), bundle, timeout_s=60)
    return run, D.parse_bundle_output(run.stdout)


ALL_SECTIONS = frozenset(SNAPSHOT_SECTIONS) | {f"inv_{n}" for n in INVARIANT_SQL}
SPEED1 = (SpeedPoint(from_ms=R0 - 3_600_000, speed=Decimal(1)),)


def _with_rows(snap) -> set[str]:
    return {n for n in INVARIANT_SQL if snap.invariant_violations(n)}


# ---------------------------------------------------------------- 깨끗한 데이터


def test_clean_bundle_has_all_sections_and_no_violations(fresh_db):
    run, snap = _snapshot()
    assert run.returncode == 0, run.stderr
    assert run.stderr == ""
    assert snap.sections == ALL_SECTIONS
    assert snap.invariant_rows == {}, snap.invariant_rows

    # 대상 계좌: JSONL(A, B) + 정규식(S) + extra(E). 다른 run(F)·live(L)는 빠진다
    assert [a.id for a in snap.accounts] == [A, B, S, E]
    assert {a.id: a.k6 for a in snap.accounts} == {A: True, B: True, S: True, E: False}
    assert len(snap.orders) == 6 and len(snap.fills) == 5 and len(snap.positions) == 3
    assert snap.fingerprint1 == snap.fingerprint2 == (6, 5, 8)
    assert snap.quiescent

    o1 = next(o for o in snap.orders if o.id == _oid(1))
    assert o1.received_ms == R0 + 1123
    assert o1.qty == Decimal("0.00010000") and str(o1.qty) == "0.00010000"
    assert o1.ref_event_ts == R0 + 1123 - 500 and o1.ref_age_ms == 500
    assert o1.received_at == "2025-11-05 00:00:01.123+00"
    f1 = next(f for f in snap.fills if f.order_id == _oid(1))
    assert f1.fee_rate == Decimal("0.000500") and f1.amount == 10000
    assert f1.created_ms == R0 + 1010
    b = next(p for p in snap.positions if p.account_id == B)
    assert b.qty == Decimal("0E-8") and b.qty == 0 and b.cost_basis == 0


def test_section_headers_match_declared_columns(fresh_db):
    run, _ = _snapshot()
    headers = dict(re.findall(r"^--@ (\S+)\n([^\n]*)\n", run.stdout, re.M))
    for name in INVARIANT_SQL:
        assert headers[f"inv_{name}"] == ",".join(D.INVARIANT_COLUMNS[name]), name
    assert headers["accounts"] == ",".join(D.ACCOUNT_COLUMNS)
    assert headers["orders"] == ",".join(D.ORDER_COLUMNS)
    assert headers["fills"] == ",".join(D.FILL_COLUMNS)
    assert headers["positions"] == ",".join(D.POSITION_COLUMNS)
    assert headers["fingerprint1"] == ",".join(D.FINGERPRINT_COLUMNS)


def test_clean_bundle_passes_check_invariants_with_default_criteria(fresh_db):
    _, snap = _snapshot()
    res = check_invariants(
        snap, None, load_criteria(DEFAULT_YAML), speed_timeline=SPEED1
    )
    assert all(r.outcome is Outcome.PASS for r in res), [
        (r.id, r.outcome, r.reasons) for r in res
    ]


# ---------------------------------------------------------------- 위반 하나씩

VIOLATIONS = [
    pytest.param(
        # CHECK 때문에 음수 현금을 넣으려면 제약을 내리고 NOT VALID로 다시 건다.
        # 검증되지 않은 CHECK는 기존 행을 보장하지 않으므로 constraints도 행을 내는 것이 맞다.
        f"""
        ALTER TABLE accounts DROP CONSTRAINT accounts_cash_check;
        INSERT INTO accounts (id, nickname, mode, initial_cash, cash, version)
            VALUES ('{G}', 'k6-r1117-crash-9', 'replay', -100, -100, 0);
        ALTER TABLE accounts ADD CONSTRAINT accounts_cash_check CHECK (cash >= 0) NOT VALID;
        """,
        {"i2", "constraints"},
        id="i2_negative_cash",
    ),
    pytest.param(
        f"UPDATE orders SET limit_price = NULL WHERE id = '{_oid(3)}';",
        {"nulls"},
        id="nulls_limit_price",
    ),
    pytest.param(
        f"UPDATE orders SET side = 'BUY' WHERE id = '{_oid(3)}';",
        {"nulls"},
        id="nulls_side_invalid",
    ),
    pytest.param(
        "ALTER TABLE fills DROP CONSTRAINT fills_order_id_key;",
        {"constraints"},
        id="constraints_unique_dropped",
    ),
    pytest.param(
        "ALTER TABLE positions DROP CONSTRAINT positions_qty_check;",
        {"constraints"},
        id="constraints_check_dropped",
    ),
    pytest.param(
        "ALTER TABLE orders DROP CONSTRAINT orders_account_id_idem_key_key;"
        " CREATE UNIQUE INDEX orders_key_partial ON orders (account_id, idem_key)"
        " WHERE status = 'FILLED';",
        {"constraints"},
        id="constraints_partial_unique_not_enough",
    ),
    pytest.param(
        f"UPDATE orders SET status = 'REJECTED', reject_reason = 'INSUFFICIENT_CASH'"
        f" WHERE id = '{_oid(4)}';",
        {"i4"},
        id="i4_rejected_with_fill",
    ),
    pytest.param(
        f"UPDATE accounts SET cash = cash + 1 WHERE id = '{A}';",
        {"i5_cash"},
        id="i5_cash",
    ),
    pytest.param(
        f"UPDATE positions SET qty = qty + 0.00000001 WHERE account_id = '{A}';",
        {"i5_qty"},
        id="i5_qty",
    ),
    pytest.param(
        # 보존식을 맞추려고 실현손익도 1 올린다(매도라 arith는 보지 않는다)
        f"UPDATE positions SET cost_basis = 1 WHERE account_id = '{B}';"
        f" UPDATE fills SET realized_pnl = realized_pnl + 1 WHERE order_id = '{_oid(5)}';",
        {"i5_flat"},
        id="i5_flat",
    ),
    pytest.param(
        f"UPDATE orders SET ref_price = ref_price + 1 WHERE id = '{_oid(1)}';",
        {"i6_db"},
        id="i6_price_ne_ref",
    ),
    pytest.param(
        f"UPDATE orders SET symbol = 'UPBIT:KRW-ETH' WHERE account_id = '{B}';"
        f" UPDATE fills SET symbol = 'UPBIT:KRW-ETH' WHERE account_id = '{B}';"
        f" UPDATE positions SET symbol = 'UPBIT:KRW-ETH' WHERE account_id = '{B}';",
        {"i8"},
        id="i8_replay_account_live_symbol",
    ),
    pytest.param(
        f"UPDATE fills SET fee_rate = 0.002 WHERE order_id = '{_oid(4)}';",
        {"arith"},
        id="arith_fee",
    ),
    pytest.param(
        "GRANT UPDATE ON orders TO pt_app;",
        {"privileges"},
        id="privileges_update",
    ),
    pytest.param(
        f"UPDATE accounts SET cash = cash + 1 WHERE id = '{E}';",
        {"i5_cash"},
        id="extra_account_in_scope",
    ),
    pytest.param(
        f"UPDATE accounts SET cash = cash + 1 WHERE id = '{S}';",
        {"i5_cash"},
        id="regex_account_in_scope",
    ),
]


@pytest.mark.parametrize(("plant", "expected"), VIOLATIONS)
def test_planted_violation_shows_only_in_its_section(fresh_db, plant, expected):
    _psql_super(plant)
    run, snap = _snapshot()
    assert run.returncode == 0, run.stderr
    assert snap.sections == ALL_SECTIONS
    assert _with_rows(snap) == expected, snap.invariant_rows


def test_constraint_violation_rows_name_the_constraint(fresh_db):
    _psql_super("ALTER TABLE fills DROP CONSTRAINT fills_order_id_key;")
    _, snap = _snapshot()
    assert snap.invariant_violations("constraints") == (
        {"kind": "unique", "table_name": "fills", "columns": "order_id"},
    )
    r = {x.id: x for x in check_invariants(snap, None, None, speed_timeline=SPEED1)}
    assert r["inv_constraints"].reasons == (R.CONSTRAINT_MISSING,)


def test_i6_ref_source_row_is_reported_but_not_counted(fresh_db):
    _psql_super(f"UPDATE orders SET ref_source = 'upbit' WHERE id = '{_oid(1)}';")
    _, snap = _snapshot()
    assert _with_rows(snap) == {"i6_db"}
    assert snap.invariant_violations("i6_db")[0]["problem"] == "ref_source"
    r = {x.id: x for x in check_invariants(snap, None, None, speed_timeline=SPEED1)}
    assert r["inv_i6_db"].outcome is Outcome.PASS
    assert r["inv_i6_db"].detail["advisory_rows"] == 1


def test_null_ref_age_on_filled_order_fails_nulls_and_i7(fresh_db):
    _psql_super(f"UPDATE orders SET ref_age_ms = NULL WHERE id = '{_oid(1)}';")
    _, snap = _snapshot()
    assert _with_rows(snap) == {"nulls"}
    r = {x.id: x for x in check_invariants(snap, None, None, speed_timeline=SPEED1)}
    assert r["i7"].outcome is Outcome.FAIL and r["i7"].value == 1


def test_stale_fill_is_i7_violation_from_db_values(fresh_db):
    _psql_super(f"UPDATE orders SET ref_age_ms = 6001 WHERE id = '{_oid(6)}';")
    _, snap = _snapshot()
    ten = (SpeedPoint(from_ms=R0 - 3_600_000, speed=Decimal(10)),)
    r = {x.id: x for x in check_invariants(snap, None, None, speed_timeline=ten)}
    assert r["i7"].outcome is Outcome.FAIL
    assert r["i7"].detail["examples"] == [_oid(6)]


# ---------------------------------------------------------------- psql 실패


def test_required_failure_stops_and_keeps_earlier_sections(fresh_db):
    _psql_super("REVOKE SELECT ON positions FROM pt_ro;")
    run, snap = _snapshot()
    assert run.returncode == 3  # ON_ERROR_STOP
    assert "permission denied" in run.stderr
    assert snap.sections == frozenset({"fingerprint1", "accounts", "orders", "fills"})
    assert snap.fingerprint2 is None and not snap.quiescent
    res = {x.id: x for x in check_invariants(snap, None, None, speed_timeline=SPEED1)}
    for n in INVARIANT_SQL:
        assert res[f"inv_{n}"].outcome is Outcome.INCONCLUSIVE
        assert res[f"inv_{n}"].reasons == (R.DB_ERROR,)
    assert res["i7"].outcome is Outcome.PASS  # orders 구획은 받았다


def test_end_markers_drop_partial_and_failed_sections(fresh_db):
    """선택 검사 실패는 끝 표지가 없어 버리고 다음 검사는 계속한다. 중간에 깨진 COPY도 버린다."""
    partial = "SELECT CASE WHEN g < 3 THEN g ELSE 1 / (g - g) END AS v FROM generate_series(1, 5) AS g"
    script = "\n".join(
        [
            "\\set ON_ERROR_STOP off",
            *D._section("inv_bad", "SELECT 1 / 0 AS v", optional=True),
            *D._section("inv_good", "SELECT 1 AS v", optional=True),
            "\\set ON_ERROR_STOP on",
            *D._section("inv_partial", partial),
            *D._section("inv_never", "SELECT 1 AS v"),
        ]
    )
    run = D.exec_bundle(Transports.for_local_pg(CONTAINER), script + "\n", timeout_s=60)
    assert run.returncode == 3
    assert "division by zero" in run.stderr
    # psql은 오류 전 행을 이미 냈다. 끝 표지가 없어서 이 구획을 버린다
    assert "--@ inv_partial\nv\n1\n2\n" in run.stdout
    snap = D.parse_bundle_output(run.stdout)
    assert snap.sections == frozenset({"inv_good"})

"""db.py: bundle.sql 조립, psql 명령, 출력 파싱(계약 정렬 명세 6). 네트워크·docker 없이 돈다.

실제 psql 출력과 같은지는 test_sql_pg.py(-m pg)가 postgres:16으로 확인한다.
"""

from __future__ import annotations

import dataclasses
import re
import subprocess
from decimal import Decimal
from types import SimpleNamespace

import pytest

from orchestrator.judge import db as D
from orchestrator.judge.errors import InputError, JudgeError
from orchestrator.judge.model import (
    ACCOUNT_COLUMNS,
    INVARIANTS,
    ORDER_COLUMNS,
    POSITION_COLUMNS,
    SNAPSHOT_SECTIONS,
    DbTarget,
    Fingerprint,
    is_run_key,
)
from orchestrator.judge.transport import Transports

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_EXTRA,
    FILLED_US,
    default_schema,
    make_db,
    make_order,
    make_tick,
    run_key,
)

RUN = "r1117-crash"


def _bundle(**kw):
    args = {"run_id": RUN, "extra_ids": [ACCT_EXTRA]}
    args.update(kw)
    return D.build_bundle(None, **args)


def _index(lines, needle):
    for i, ln in enumerate(lines):
        if ln == needle:
            return i
    raise AssertionError(f"{needle!r} 줄이 없다")


def _marks(text):
    return [
        ln[len("\\qecho '--@ ") : -1]
        for ln in text.splitlines()
        if ln.startswith("\\qecho '--@ ")
    ]


# ---------------------------------------------------------------- build_bundle


def test_bundle_sections_follow_spec_6():
    assert _marks(_bundle()) == list(D.BUNDLE_SECTIONS)
    assert set(D.BUNDLE_SECTIONS) == set(SNAPSHOT_SECTIONS)


def test_fingerprints_are_outside_the_snapshot_transaction():
    """REPEATABLE READ 안에서 재면 F1 == F2가 늘 참이라 db_quiescent가 아무것도 못 본다."""
    lines = _bundle().splitlines()
    f1 = _index(lines, "\\qecho '--@ fingerprint1'")
    begin = _index(lines, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
    schema = _index(lines, "\\qecho '--@ schema'")
    orders_end = _index(lines, "\\qecho '--@end orders'")
    commit = _index(lines, "COMMIT;")
    f2 = _index(lines, "\\qecho '--@ fingerprint2'")
    off = _index(lines, "\\set ON_ERROR_STOP off")
    ticks = _index(lines, "\\qecho '--@ ticks'")
    assert f1 < begin < schema < orders_end < commit < f2 < off < ticks


def test_bundle_session_settings():
    lines = _bundle().splitlines()
    for need in (
        "SET TIME ZONE 'UTC';",
        "SET DateStyle = 'ISO, YMD';",
        "SET default_transaction_read_only = on;",
        "SET client_encoding = 'UTF8';",
    ):
        assert _index(lines, need) < _index(lines, "\\qecho '--@ fingerprint1'")
    assert all(
        ln.endswith("TO STDOUT WITH (FORMAT csv, HEADER true, FORCE_QUOTE *);")
        for ln in lines
        if ln.startswith(") TO STDOUT")
    )
    assert sum(1 for ln in lines if ln.startswith("COPY (")) == len(D.BUNDLE_SECTIONS)


def test_bundle_variables():
    lines = _bundle(stale_ms=7000).splitlines()
    assert "\\set run_key_re '^k6-r1117\\\\-crash-[0-9]+-[0-9]+$'" in lines
    assert "\\set extra '{\"user-0999\"}'" in lines
    assert '\\set schema_tables \'{"accounts","positions","orders","ticks"}\'' in lines
    assert "\\set stale_ms 7000" in lines
    assert f"\\set tick_slack_ms {D.TICK_SLACK_MS}" in lines
    assert "\\set extra '{}'" in _bundle(extra_ids=[]).splitlines()
    assert "\\set stale_ms 5000" in _bundle().splitlines()  # 기본 DEFAULT_STALE_MS


def test_bundle_is_deterministic_and_reads_ctx():
    a = _bundle(extra_ids=[ACCT_B, ACCT_A, ACCT_B])
    assert a == _bundle(extra_ids=[ACCT_A, ACCT_B])
    assert '\\set extra \'{"user-0001","user-0002"}\'' in a.splitlines()
    ctx = SimpleNamespace(run_id=RUN, extra_accounts=(ACCT_EXTRA,))
    assert D.build_bundle(ctx) == _bundle()
    # 'NULL'이라는 user_id도 문자열 원소로 남는다(따옴표로 감싼다)
    assert "\\set extra '{\"NULL\"}'" in _bundle(extra_ids=["NULL"]).splitlines()


@pytest.mark.parametrize(
    "kw",
    [
        {"run_id": "R1117"},
        {"run_id": "ab"},
        {"run_id": "a" * 41},
        {"run_id": "r1;drop"},
        {"run_id": "r1117\n"},
        {"run_id": None},
        {"extra_ids": ["-x"]},
        {"extra_ids": ["a b"]},
        {"extra_ids": ["it's"]},
        {"extra_ids": ['a"b']},
        {"extra_ids": "user-0001"},
        {"stale_ms": -1},
        {"stale_ms": True},
        {"stale_ms": 1.5},
    ],
)
def test_bundle_rejects_unsafe_inputs(kw):
    with pytest.raises(InputError):
        _bundle(**kw)


def test_section_selects_follow_model_columns():
    assert D._ACCOUNTS_SQL.startswith(f"SELECT {', '.join(ACCOUNT_COLUMNS)}\n")
    assert D._POSITIONS_SQL.startswith(f"SELECT {', '.join(POSITION_COLUMNS)}\n")
    assert D._ORDERS_SQL.startswith(f"SELECT {', '.join(ORDER_COLUMNS)}\n")
    assert D._ORDERS_SQL.endswith("ORDER BY id")


def test_sql_files_are_single_statements():
    for name in D.SQL_FILES:
        body = D._embed(D.load_sql(name), name)
        assert body.startswith("WITH ")
    with pytest.raises(JudgeError):
        D.load_sql("inv_i4")
    with pytest.raises(JudgeError):
        D._embed("SELECT 1; SELECT 2;", "x")
    with pytest.raises(JudgeError):
        D._embed("-- 주석만\n", "x")
    assert D._embed("-- c\nSELECT 1;\n", "x") == "SELECT 1"


@pytest.mark.parametrize(
    ("key", "ok"),
    [
        ("k6-r1117-crash-1-1", True),
        ("k6-r1117-crash-12-345", True),
        ("k6-r1117-crash-2-5-1", False),  # r1117-crash-2의 키
        ("k6-r1117-crash-1", False),
        ("k6-r1117-crash-1-x", False),
        ("k6-r1117-crash-1-1\n", False),
        ("xk6-r1117-crash-1-1", False),
        ("k6-r1117xcrash-1-1", False),
    ],
)
def test_run_key_regex_agrees_with_model(key, ok):
    # PostgreSQL ARE와 Python re는 `\-`를 똑같이 `-`로 읽는다
    assert (re.fullmatch(D.run_key_regex(RUN), key) is not None) is ok
    assert is_run_key(key, RUN) is ok


# ---------------------------------------------------------------- exec_bundle


class _FakeRunner:
    def __init__(self, stdout=b"", rc=0, stderr=b""):
        self.calls = []
        self.stdout, self.rc, self.stderr = stdout, rc, stderr

    def __call__(self, argv, *, input=None, timeout):
        self.calls.append((tuple(argv), input, timeout))
        return subprocess.CompletedProcess(argv, self.rc, self.stdout, self.stderr)


PSQL_TAIL = ("psql", "-X", "-q", "-v", "ON_ERROR_STOP=1")


def test_exec_bundle_default_target():
    fake = _FakeRunner(stdout=b"--@ x\n")
    out = D.run_snapshot(Transports(runner=fake), "SELECT 1;\n", timeout_s=7)
    assert out == "--@ x\n"
    argv, stdin, timeout = fake.calls[0]
    assert argv == (
        "kubectl",
        "-n",
        "papertrade",
        "exec",
        "-i",
        "postgres-0",
        "--",
        *PSQL_TAIL,
        "-U",
        "exchange",
        "-d",
        "exchange",
        "-f",
        "-",
    )
    assert stdin == b"SELECT 1;\n" and timeout == 7


def test_exec_bundle_uses_run_json_db_target():
    fake = _FakeRunner()
    target = DbTarget(pod="pg-1", container="postgres", user="judge", database="ex")
    res = D.exec_bundle(Transports(runner=fake), "x", target=target)
    assert res.argv == (
        "kubectl",
        "-n",
        "papertrade",
        "exec",
        "-i",
        "pg-1",
        "-c",
        "postgres",
        "--",
        *PSQL_TAIL,
        "-U",
        "judge",
        "-d",
        "ex",
        "-f",
        "-",
    )
    assert not any("password" in a.lower() for a in res.argv)


def test_exec_bundle_local_pg_prefix_and_nonzero_exit_is_not_an_error():
    fake = _FakeRunner(stdout=b"partial", rc=3, stderr=b"ERROR: boom")
    tr = Transports(psql_prefix=("docker", "exec", "-i", "pgtest-judge"), runner=fake)
    res = D.exec_bundle(tr, "x", target=DbTarget(user="u", database="d"))
    assert res.argv == (
        "docker",
        "exec",
        "-i",
        "pgtest-judge",
        *PSQL_TAIL,
        "-U",
        "u",
        "-d",
        "d",
        "-f",
        "-",
    )
    assert (res.returncode, res.stdout, res.stderr) == (3, "partial", "ERROR: boom")


def test_non_utf8_output_is_a_judge_error():
    fake = _FakeRunner(stdout=b"\xff\xfe")
    with pytest.raises(JudgeError):
        D.run_snapshot(Transports(runner=fake), "x")


# ---------------------------------------------------------------- parse_bundle_output


def _db():
    k1, k2 = run_key(1), run_key(2)
    orders = [
        make_order(ACCT_A, k1),
        make_order(
            ACCT_A, k2, side="ask", volume="0.00005000", fill_price="100000001.5"
        ),
        make_order(
            ACCT_B,
            "manual-1",
            status="pending",
            fill_price=None,
            filled_us=None,
            created_us=FILLED_US - 7,
        ),
    ]
    ticks = [
        make_tick(),
        make_tick(price="99999999", received_us=FILLED_US - 2_000_001),
    ]
    return make_db(
        orders=orders,
        ticks=ticks,
        extra_accounts=[ACCT_EXTRA],
        sections=SNAPSHOT_SECTIONS,
    )


def test_roundtrip_render_then_parse_is_identity():
    db = _db()
    assert D.parse_bundle_output(D.render_snapshot(db)) == db


def test_roundtrip_keeps_inv_sections():
    db = make_db(invariant_rows={"cash": [{"user_id": "user-0001", "krw": None}]})
    back = D.parse_bundle_output(D.render_snapshot(db))
    assert back == db
    assert back.invariant_violations("cash") == ({"user_id": "user-0001", "krw": None},)
    assert back.invariant_violations("qty") == ()
    assert {f"inv_{i}" for i in INVARIANTS} <= back.sections


def test_render_looks_like_psql_force_quote_output():
    text = D.render_snapshot(_db())
    assert (
        "id,client_order_id,user_id,market,side,volume,status,fill_price,amount,created_at,filled_at\n"
        in text
    )
    pending = next(ln for ln in text.splitlines() if '"manual-1"' in ln)
    # NULL은 따옴표 없는 빈 값, 시각은 psql 모양(UTC, +00, 끝 0 없음)
    assert pending.endswith(',"pending",,,"' + D._pg_ts(FILLED_US - 7) + '",')
    assert D._pg_ts(FILLED_US) == "2025-11-04 23:46:40.005123+00"
    assert D._pg_ts(1_762_300_000_000_000) == "2025-11-04 23:46:40+00"
    assert D._pg_ts(1_762_300_000_500_000) == "2025-11-04 23:46:40.5+00"


PSQL_SAMPLE = """--@ fingerprint1
orders,max_order_id,krw_sum,positions,volume_sum
"0",,"10000000000.0000","0",
--@end fingerprint1
--@ schema
kind,table_name,name,detail
"column","accounts","krw","numeric(24,4) not null"
"constraint","orders","orders_client_order_id_key","UNIQUE (client_order_id)"
"table","accounts","accounts",
--@end schema
--@ accounts
user_id,krw
"user-0002","9989999.9999"
"user-0001","10000000.0000"
--@end accounts
--@ orders
id,client_order_id,user_id,market,side,volume,status,fill_price,amount,created_at,filled_at
"7","k6-r1117-crash-1-1","user-0001","KRW-BTC","bid","0.00010000","filled","100000000.00000000","10000.0000","2026-11-06 05:46:16.550091+00","2026-11-06 05:46:16+00"
--@end orders
"""


def test_parse_real_psql_shapes():
    snap = D.parse_bundle_output(PSQL_SAMPLE)
    assert snap.sections == {"fingerprint1", "schema", "accounts", "orders"}
    assert snap.fingerprint1 == Fingerprint(
        0, None, Decimal("10000000000.0000"), 0, None
    )
    assert str(snap.fingerprint1.krw_sum) == "10000000000.0000"
    assert [a.user_id for a in snap.accounts] == ["user-0001", "user-0002"]
    assert str(snap.accounts[1].krw) == "9989999.9999"
    [o] = snap.orders
    assert (o.id, o.volume, o.created_us % 1_000_000, o.filled_us % 1_000_000) == (
        7,
        Decimal("0.0001"),
        550091,
        0,
    )
    assert [s.kind for s in snap.schema] == ["column", "constraint", "table"]
    assert snap.schema[2].detail is None
    assert snap.errors == ()


def test_quoted_field_starting_with_marker_is_data():
    text = '--@ accounts\nuser_id,krw\n"--@ evil","1"\n"a\nb","2"\n--@end accounts\n'
    snap = D.parse_bundle_output(text)
    assert [a.user_id for a in snap.accounts] == ["--@ evil", "a\nb"]


def test_null_vs_empty_string():
    text = D.format_section(
        "orders",
        ORDER_COLUMNS,
        [(1, "", None, "", None, None, None, None, None, None, None)],
    )
    [o] = D.parse_bundle_output(text).orders
    assert (o.client_order_id, o.user_id, o.market, o.side) == ("", "", "", None)


def test_crlf_output():
    text = D.render_snapshot(_db()).replace("\n", "\r\n")
    assert D.parse_bundle_output(text) == _db()


def test_unclosed_section_is_dropped_and_reported():
    text = D.render_snapshot(_db())
    cut = text[: text.index("--@end orders")]  # psql이 orders 도중 멈춤
    snap = D.parse_bundle_output(cut)
    assert snap.sections == {"fingerprint1", "schema", "accounts", "positions"}
    assert snap.orders == () and snap.fingerprint2 is None
    assert any("orders" in e and "끝 표지" in e for e in snap.errors)


def test_broken_quote_drops_section():
    head = D.format_section("fingerprint1", D.FINGERPRINT_COLUMNS, [(1, 1, 0, 0, None)])
    partial = '--@ accounts\nuser_id,krw\n"user-0001","10\n'
    snap = D.parse_bundle_output(head + partial)
    assert snap.sections == {"fingerprint1"}
    assert any("accounts" in e for e in snap.errors)


def test_without_any_end_marker_sections_with_header_are_kept():
    text = '--@ accounts\nuser_id,krw\n"user-0001","1"\n--@ positions\n'
    snap = D.parse_bundle_output(text)
    assert snap.sections == {"accounts"}
    assert any("positions" in e for e in snap.errors)


def test_empty_section_is_present_with_no_rows():
    snap = D.parse_bundle_output("--@ ticks\nmarket,price,received_at\n--@end ticks\n")
    assert snap.sections == {"ticks"} and snap.ticks == ()


def test_ticks_and_schema_parse():
    db = make_db(
        ticks=[make_tick(), make_tick(market="KRW-ETH", price=None)],
        schema=default_schema(),
    )
    back = D.parse_bundle_output(D.render_snapshot(db))
    assert back.ticks == db.ticks and back.schema == db.schema


def test_text_before_first_marker_is_ignored():
    db = _db()
    assert D.parse_bundle_output("SET\nBEGIN\n" + D.render_snapshot(db)) == db


@pytest.mark.parametrize(
    "text",
    [
        "--@ a\nx,y\n1,2\n--@end a\n--@ a\nx,y\n--@end a\n",  # 중복 구획
        "--@ a\nx,y\n1\n--@end a\n",  # 열 수 불일치
        "--@ a\nx,x\n--@end a\n",  # 중복 열 이름
        "--@ a\nx,,y\n--@end a\n",  # 빈 열 이름
        "--@ a\nx,y\n--@end b\n",  # 끝 표지 불일치
        "--@weird\n",  # 모르는 표지 줄
        "--@ \nx\n",  # 빈 구획 이름
        "--@ fingerprint1\norders,max_order_id,krw_sum,positions,volume_sum\n1,x,0,0,\n--@end fingerprint1\n",
        "--@ fingerprint1\norders,max_order_id,krw_sum,positions,volume_sum\n--@end fingerprint1\n",
        "--@ fingerprint1\norders,max_order_id,krw_sum,positions,volume_sum\n,,,0,\n--@end fingerprint1\n",
        "--@ accounts\nuser_id\n--@end accounts\n",  # 필수 열 없음
        "--@ accounts\nuser_id,krw\n,1\n--@end accounts\n",  # user_id NULL
        "--@ accounts\nuser_id,krw\nu,abc\n--@end accounts\n",  # 숫자 아님
        "--@ accounts\nuser_id,krw\nu,NaN\n--@end accounts\n",  # 유한하지 않음
        "--@ orders\n"
        + ",".join(ORDER_COLUMNS)
        + "\n"
        + "," * 10
        + "\n--@end orders\n",  # id NULL
        "--@ orders\n"
        + ",".join(ORDER_COLUMNS)
        + '\n"1","k","u","m","bid","1","filled","1","1","2026-11-06 01:02:03",\n--@end orders\n',  # 시간대 없음
        "--@ schema\nkind,table_name,name,detail\nindex,orders,x,\n--@end schema\n",  # 모르는 kind
        "--@ ticks\nmarket,price,received_at\n,1,\n--@end ticks\n",  # market NULL
    ],
)
def test_malformed_output_is_input_error(text):
    with pytest.raises(InputError):
        D.parse_bundle_output(text)


def test_parse_is_order_independent():
    db = _db()
    text = D.render_snapshot(dataclasses.replace(db, orders=tuple(reversed(db.orders))))
    assert D.parse_bundle_output(text).orders == db.orders

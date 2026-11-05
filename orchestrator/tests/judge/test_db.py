"""db.py: bundle.sql 조립, psql 명령, 출력 파싱(설계 5.4). 네트워크·docker 없이 돈다."""

from __future__ import annotations

import re
import subprocess
from decimal import Decimal
from types import SimpleNamespace

import pytest

from orchestrator.judge import db as D
from orchestrator.judge.errors import InputError, JudgeError
from orchestrator.judge.model import INVARIANT_SQL, OPTIONAL_INVARIANT_SQL
from orchestrator.judge.transport import Transports

from .factories import (
    ACCT_A,
    ACCT_B,
    ACCT_EXTRA,
    T0,
    make_db,
    make_fill,
    make_order,
)

RUN = "r1117-crash"


def _bundle(**kw):
    args = {"account_ids": [ACCT_B, ACCT_A], "extra_ids": [ACCT_EXTRA], "run_id": RUN}
    args.update(kw)
    return D.build_bundle(None, **args)


def _index(lines, needle):
    for i, ln in enumerate(lines):
        if ln == needle:
            return i
    raise AssertionError(f"{needle!r} 줄이 없다")


# ---------------------------------------------------------------- build_bundle


def test_bundle_order_follows_design_5_4():
    lines = _bundle().splitlines()
    marks = [ln for ln in lines if ln.startswith("\\qecho '--@ ")]
    names = [m[len("\\qecho '--@ ") : -1] for m in marks]
    required = [f"inv_{n}" for n in INVARIANT_SQL if n not in OPTIONAL_INVARIANT_SQL]
    optional = [f"inv_{n}" for n in INVARIANT_SQL if n in OPTIONAL_INVARIANT_SQL]
    assert names == [
        "fingerprint1",
        "accounts",
        "orders",
        "fills",
        "positions",
        *required,
        "fingerprint2",
        *optional,
    ]
    i_ids = _index(lines, f"\\set ids '{{{ACCT_A},{ACCT_B}}}'")
    i_extra = _index(lines, f"\\set extra '{{{ACCT_EXTRA}}}'")
    i_nick = next(i for i, ln in enumerate(lines) if ln.startswith("\\set nick_re "))
    i_f1 = _index(lines, "\\qecho '--@ fingerprint1'")
    i_begin = _index(lines, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
    i_acc = _index(lines, "\\qecho '--@ accounts'")
    i_last_req = _index(lines, f"\\qecho '--@end {required[-1]}'")
    i_commit = _index(lines, "COMMIT;")
    i_f2 = _index(lines, "\\qecho '--@ fingerprint2'")
    i_off = _index(lines, "\\set ON_ERROR_STOP off")
    i_opt = _index(lines, f"\\qecho '--@ {optional[0]}'")
    assert i_ids < i_extra < i_nick < i_f1 < i_begin < i_acc < i_last_req < i_commit
    assert i_commit < i_f2 < i_off < i_opt


def test_every_section_has_end_marker_and_copy_csv_header():
    text = _bundle()
    starts = re.findall(r"^\\qecho '--@ (\S+)'$", text, re.M)
    ends = re.findall(r"^\\qecho '--@end (\S+)'$", text, re.M)
    assert starts == ends
    assert text.count("TO STDOUT WITH (FORMAT csv, HEADER true);") == len(starts)


def test_optional_sections_emit_end_marker_only_on_success():
    text = _bundle()
    for name in sorted(OPTIONAL_INVARIANT_SQL):
        block = f"\\if :ERROR\n\\else\n\\qecho '--@end inv_{name}'\n\\endif"
        assert block in text


def test_snapshot_copies_carry_k6_column_and_session_is_read_only():
    text = _bundle()
    assert "SET default_transaction_read_only = on;" in text
    assert "SET TIME ZONE 'UTC';" in text
    for name in ("accounts", "orders", "fills", "positions"):
        seg = text.split(f"\\qecho '--@ {name}'")[1].split("\\qecho '--@end")[0]
        assert "AS k6" in seg or "ra.k6" in seg
    assert ":'ids'::uuid[]" in text and ":'extra'::uuid[]" in text
    assert ":'nick_re'" in text


def test_bundle_is_deterministic_and_dedupes_ids():
    a = _bundle(account_ids=[ACCT_B, ACCT_A, ACCT_A.upper()])
    b = _bundle(account_ids=[ACCT_A, ACCT_B])
    assert a == b


def test_empty_id_lists_become_empty_arrays():
    lines = _bundle(account_ids=[], extra_ids=[]).splitlines()
    assert "\\set ids '{}'" in lines
    assert "\\set extra '{}'" in lines


def test_ctx_like_supplies_run_id_and_extra_accounts():
    ctx = SimpleNamespace(run_id=RUN, extra_accounts=(ACCT_EXTRA,))
    assert D.build_bundle(ctx, [ACCT_A, ACCT_B]) == _bundle()


def test_nick_re_escapes_run_id_for_psql_and_regex():
    lines = _bundle().splitlines()
    # psql 작은따옴표 안에서 \\는 역슬래시 하나다 → 값은 ^k6-r1117\-crash-[0-9]+$
    assert "\\set nick_re '^k6-r1117\\\\-crash-[0-9]+$'" in lines
    rx = D.nick_regex(RUN)
    assert rx == "^k6-r1117\\-crash-[0-9]+$"
    # 역슬래시 + 비영숫자는 Python re와 PostgreSQL ARE 모두 그 글자 자체다
    assert re.fullmatch(rx, "k6-r1117-crash-12")
    assert not re.fullmatch(rx, "k6-r1117-crash-2-0")  # 다른 run(r1117-crash-2)
    assert not re.fullmatch(rx, "k6-r1117xcrash-1")
    assert not re.fullmatch(rx, "k6-r1117-crash-")


@pytest.mark.parametrize(
    "bad",
    [
        "x'; DROP TABLE accounts; --",
        "00000000000040008000000000000000",  # 하이픈 없음
        "{00000000-0000-4000-8000-00000000000a}",
        "00000000-0000-4000-8000-00000000000a\n",
        "00000000-0000-4000-8000-00000000000g",
        "",
        None,
        12,
    ],
)
def test_malformed_uuid_is_rejected(bad):
    with pytest.raises(InputError):
        _bundle(account_ids=[ACCT_A, bad])
    with pytest.raises(InputError):
        _bundle(extra_ids=[bad])


def test_uuid_list_must_not_be_a_bare_string():
    with pytest.raises(InputError):
        _bundle(account_ids=ACCT_A)


@pytest.mark.parametrize(
    "bad", ["R1117", "ab", "a'b", "abc\n", "abc\\", "a" * 41, "r1117_crash", None]
)
def test_malformed_run_id_is_rejected(bad):
    with pytest.raises(InputError):
        _bundle(run_id=bad)


def test_sql_files_are_single_selects_with_ra_variables():
    for name in INVARIANT_SQL:
        raw = D.load_sql(name)
        body = D._embed(raw, name)
        assert ";" not in body and "\\" not in body
        assert body.startswith(("WITH", "SELECT"))
        if name not in ("constraints", "privileges"):
            assert ":'ids'::uuid[]" in body
            assert ":'extra'::uuid[]" in body
            assert ":'nick_re'" in body
    with pytest.raises(JudgeError):
        D.load_sql("nope")


def test_embed_rejects_multiple_statements_and_meta_commands():
    with pytest.raises(JudgeError):
        D._embed("SELECT 1; SELECT 2", "x")
    with pytest.raises(JudgeError):
        D._embed("SELECT 1\n\\! echo hi", "x")
    assert D._embed("-- c\nSELECT 1;\n", "x") == "SELECT 1"


# ---------------------------------------------------------------- run_snapshot


class _FakeRunner:
    def __init__(self, stdout=b"", rc=0, stderr=b""):
        self.calls = []
        self.stdout, self.rc, self.stderr = stdout, rc, stderr

    def __call__(self, argv, *, input=None, timeout):
        self.calls.append((tuple(argv), input, timeout))
        return subprocess.CompletedProcess(argv, self.rc, self.stdout, self.stderr)


def test_run_snapshot_uses_kubectl_exec_psql_by_default():
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
        "-c",
        "postgres",
        "--",
        "psql",
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "pt_ro",
        "-d",
        "papertrade",
        "-f",
        "-",
    )
    assert stdin == b"SELECT 1;\n" and timeout == 7


def test_run_snapshot_local_pg_prefix_and_nonzero_exit_is_not_an_error():
    fake = _FakeRunner(stdout=b"partial", rc=3, stderr=b"ERROR: boom")
    tr = Transports(psql_prefix=("docker", "exec", "-i", "pgtest-judge"), runner=fake)
    res = D.exec_bundle(tr, "x")
    assert res.argv[:5] == ("docker", "exec", "-i", "pgtest-judge", "psql")
    assert (res.returncode, res.stdout, res.stderr) == (3, "partial", "ERROR: boom")


def test_non_utf8_output_is_a_judge_error():
    fake = _FakeRunner(stdout=b"\xff\xfe")
    with pytest.raises(JudgeError):
        D.run_snapshot(Transports(runner=fake), "x")


# ---------------------------------------------------------------- parse_bundle_output


def _full_text(db=None):
    if db is None:
        o = make_order(ACCT_A, "k1")
        db = make_db(orders=[o], fills=[make_fill(o)])
    return D.render_snapshot(db), db


def test_roundtrip_render_then_parse_is_identity():
    text, db = _full_text()
    assert D.parse_bundle_output(text) == db


def test_null_vs_empty_string_and_types():
    text = D.format_section(
        "accounts",
        D.ACCOUNT_COLUMNS,
        [
            (ACCT_A, "", "replay", 10, 9, None, None, T0, True),
            (ACCT_B, None, None, None, None, 3, None, None, False),
        ],
    )
    assert f'{ACCT_A},"",replay,10,9,,,{T0},t' in text
    snap = D.parse_bundle_output(text)
    a, b = snap.accounts
    assert a.nickname == "" and a.version is None and a.k6 is True
    assert a.created_ms == T0 and isinstance(a.cash, int)
    assert b.nickname is None and b.mode is None and b.cash is None and b.k6 is False


def test_numeric_becomes_exact_decimal_and_quoted_fields_parse():
    o = make_order(ACCT_A, "k1", qty="0.00010000", reject_reason='a,"b"\nc')
    snap = D.parse_bundle_output(D.render_snapshot(make_db(orders=[o])))
    got = snap.orders[0]
    assert got.qty == Decimal("0.00010000") and str(got.qty) == "0.00010000"
    assert got.limit_price == Decimal("105000000")
    assert got.reject_reason == 'a,"b"\nc'


def test_quoted_line_starting_with_marker_is_data_not_a_section():
    text = (
        "--@ inv_nulls\n"
        "table_name,row_key,column_name,problem\n"
        'orders,"x\n--@ accounts\ny",qty,null\n'
        "--@end inv_nulls\n"
    )
    snap = D.parse_bundle_output(text)
    assert snap.sections == frozenset({"inv_nulls"})
    assert snap.invariant_rows["nulls"][0]["row_key"] == "x\n--@ accounts\ny"


def test_missing_section_is_absent_and_others_still_parse():
    text, db = _full_text()
    cut = text.split("--@ positions\n")[0]  # positions부터 끝까지 없음
    snap = D.parse_bundle_output(cut)
    assert "positions" not in snap.sections and snap.positions == ()
    assert "inv_i2" not in snap.sections
    assert snap.invariant_violations("i2") is None
    assert snap.fingerprint2 is None and not snap.quiescent
    assert snap.orders == db.orders and snap.fills == db.fills


def test_section_without_end_marker_is_dropped_when_end_markers_are_used():
    head = D.format_section("fingerprint1", D.FINGERPRINT_COLUMNS, [(1, 1, 0)])
    partial = "--@ inv_i2\naccount_id,field,symbol,value\n"  # psql이 여기서 멈춤
    snap = D.parse_bundle_output(head + partial)
    assert snap.sections == frozenset({"fingerprint1"})
    assert snap.fingerprint1 == (1, 1, 0)


def test_unterminated_quote_drops_the_open_section():
    text = (
        D.format_section("fingerprint1", D.FINGERPRINT_COLUMNS, [(0, 0, 0)])
        + '--@ inv_nulls\ntable_name,row_key,column_name,problem\norders,"abc'
    )
    snap = D.parse_bundle_output(text)
    assert snap.sections == frozenset({"fingerprint1"})


def test_plain_output_without_end_markers_is_accepted():
    text = "--@ fingerprint1\norders,fills,version_sum\n2,1,5\n--@ inv_i2\naccount_id,field,symbol,value\n"
    snap = D.parse_bundle_output(text)
    assert snap.sections == frozenset({"fingerprint1", "inv_i2"})
    assert snap.invariant_violations("i2") == ()


def test_header_only_section_is_present_with_zero_rows():
    text = D.format_section("inv_i5_cash", D.INVARIANT_COLUMNS["i5_cash"], [])
    snap = D.parse_bundle_output(text)
    assert snap.has_invariant("i5_cash")
    assert snap.invariant_violations("i5_cash") == ()


def test_violation_rows_keep_text_and_null():
    text = D.format_section(
        "inv_i2",
        D.INVARIANT_COLUMNS["i2"],
        [(ACCT_A, "cash", None, "-1")],
    )
    rows = D.parse_bundle_output(text).invariant_violations("i2")
    assert rows == (
        {"account_id": ACCT_A, "field": "cash", "symbol": None, "value": "-1"},
    )


def test_crlf_line_endings_parse():
    text = D.format_section("fingerprint1", D.FINGERPRINT_COLUMNS, [(3, 2, 1)])
    snap = D.parse_bundle_output(text.replace("\n", "\r\n"))
    assert snap.fingerprint1 == (3, 2, 1)


def test_received_ms_falls_back_to_received_at_when_column_absent():
    cols = [c for c in D.ORDER_COLUMNS if c not in ("received_ms", "k6")]
    o = make_order(ACCT_A, "k1")
    row = [getattr(o, c) for c in cols]
    row[cols.index("received_at")] = "2025-11-05 00:00:00.123+00"
    snap = D.parse_bundle_output(D.format_section("orders", cols, [row]))
    assert snap.orders[0].received_ms == 1_762_300_800_123


def test_null_idem_key_and_fill_order_id_become_empty_string():
    o = make_order(ACCT_A, "k1")
    f = make_fill(o)
    text = D.render_snapshot(make_db(orders=[o], fills=[f]))
    text = text.replace(f"{o.id},{ACCT_A},k1,", f"{o.id},{ACCT_A},,", 1)
    text = text.replace(f"\n1,{o.id},{ACCT_A},", f"\n1,,{ACCT_A},", 1)
    snap = D.parse_bundle_output(text)
    assert snap.orders[0].idem_key == ""
    assert snap.fills[0].order_id == ""


def test_rows_are_sorted_for_determinism():
    o1 = make_order(ACCT_B, "k2")
    o2 = make_order(ACCT_A, "k1")
    text = D.format_section(
        "orders",
        D.ORDER_COLUMNS,
        [
            [getattr(o, c, None) for c in D.ORDER_COLUMNS[:-1]] + [True]
            for o in (o1, o2)
        ],
    )
    snap = D.parse_bundle_output(text)
    assert [o.account_id for o in snap.orders] == [ACCT_A, ACCT_B]


@pytest.mark.parametrize(
    "text",
    [
        "--@ a\nx,y\n1,2\n--@end a\n--@ a\nx,y\n--@end a\n",  # 중복 구획
        "--@ a\nx,y\n1\n--@end a\n",  # 열 수 불일치
        "--@ a\nx,x\n--@end a\n",  # 중복 열 이름
        "--@ a\nx,y\n--@end b\n",  # 끝 표지 불일치
        "--@weird\n",  # 모르는 표지 줄
        "--@ fingerprint1\norders,fills,version_sum\n1,x,0\n--@end fingerprint1\n",
        "--@ fingerprint1\norders,fills,version_sum\n--@end fingerprint1\n",
        "--@ accounts\nid,nickname\n--@end accounts\n",  # 필수 열 없음
        "--@ fills\n" + ",".join(D.FILL_COLUMNS) + "\n" + "," * 13 + "\n--@end fills\n",
    ],
)
def test_malformed_output_is_input_error(text):
    with pytest.raises(InputError):
        D.parse_bundle_output(text)


def test_text_before_first_marker_is_ignored():
    text, db = _full_text()
    assert D.parse_bundle_output("SET\nBEGIN\n" + text) == db

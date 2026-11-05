"""k6 기록 파서와 기록 완전성(설계 4.6, 5.3, 8)."""

import dataclasses
import gzip
import json
import random
from decimal import Decimal
from pathlib import Path

import pytest

from orchestrator.judge import reasons
from orchestrator.judge.errors import InputError
from orchestrator.judge.k6log import (
    COND_COUNTERS,
    COND_EXIT,
    COND_LINES,
    COND_RECORDS,
    COND_RUN,
    COND_RUN_START,
    COND_SHA,
    LOG_COMPLETE_CONDITIONS,
    MAX_PROBLEMS_PER_CODE,
    find_token_keys,
    load_summary,
    log_complete,
    parse_k6log,
    problem_code,
    problem_counts,
    sha_matches,
    summary_count,
)
from orchestrator.judge.model import Outcome, sha256_bytes

from .factories import (
    ACCT_A,
    ACCT_B,
    RUN_ID,
    T0,
    make_intent,
    make_log,
    make_result,
)

FIX = Path(__file__).parent / "fixtures" / "k6"
OK = FIX / "orders_ok.jsonl"
BAD = FIX / "orders_bad.jsonl"
SUMMARY = FIX / "summary_ok.json"

K1 = "6a1f0c2e-5b7d-4c11-9e2a-000000000001"
K2 = "6a1f0c2e-5b7d-4c11-9e2a-000000000002"
K3 = "6a1f0c2e-5b7d-4c11-9e2a-000000000003"
FAKE = "not-a-real-token-0000"  # 유출 검사용 가짜 값


def ok_objs():
    return [json.loads(x) for x in OK.read_text("utf-8").splitlines()]


def dump(objs):
    return [json.dumps(o, separators=(",", ":")) for o in objs]


def summary(intents=3, results=3):
    return {
        "metrics": {
            "order_intents": {"type": "counter", "values": {"count": intents}},
            "order_results": {"type": "counter", "values": {"count": results}},
        },
        "state": {"testRunDurationMs": 10000},
    }


def codes(log):
    return [problem_code(p) for p in log.problems]


def complete(log, summ=None, exit_code=0, sha_ok=True):
    return log_complete(log, summ if summ is not None else summary(), exit_code, sha_ok)


# ---------------------------------------------------------------- 정상


def test_ok_fixture_parses_cleanly():
    log = parse_k6log(OK, summary=load_summary(SUMMARY))
    assert log.problems == ()
    assert log.run_id == RUN_ID
    assert log.line_count == 10
    assert log.kind_counts == {
        "account": 2,
        "intent": 3,
        "result": 3,
        "run_start": 1,
        "setup_done": 1,
    }
    assert log.sha256 == sha256_bytes(OK.read_bytes())
    assert log.has_token_key is False
    assert list(log.intents) == [K1, K2, K3]  # 파일 순서
    assert list(log.results) == [K1, K2, K3]
    assert [a.acct for a in log.accounts] == [ACCT_A, ACCT_B]
    assert log.account_by_idx(1) == ACCT_B
    assert log.setup_done.accounts == 2


def test_run_start_fields():
    rs = parse_k6log(OK).run_start
    assert rs.v == 1 and rs.run == RUN_ID and rs.seed == 42
    assert rs.profile == "smoke" and rs.script_ver == "0.1.0"
    assert rs.conn_reuse is False
    assert rs.initial_cash == 10_000_000
    assert rs.symbols == ("REPLAY:UPBIT:KRW-BTC", "REPLAY:UPBIT:KRW-ETH")
    assert rs.plan["executor"] == "ramping-arrival-rate"
    assert rs.plan["stages"][1] == {"duration_s": 10, "target": 180}
    assert rs.extra == {"ws_reconnect": False}  # 모르는 키는 extra
    assert rs.t == T0 - 60_000


def test_parsed_records_match_factories():
    """파서 출력 모양 = 골든 테스트가 쓰는 빌더 모양(classify 입력 계약)."""
    log = parse_k6log(OK)
    assert log.intents[K1] == make_intent(K1, ACCT_A)
    assert log.results[K1] == make_result(
        K1, atts=[(201, None, 12)], resend=[(200, None, 5)]
    )


def test_attempt_lookup_and_ec_parsing():
    log = parse_k6log(OK)
    r2 = log.results[K2]
    assert [a.st for a in r2.atts] == [0, 200]
    assert r2.atts[0].ec == "1050"  # 숫자 error_code는 문자열로
    assert r2.atts[0].ms == 5000.123 and isinstance(r2.atts[0].ms, float)
    assert r2.ack.st == 200 and r2.ack.json()["fee"] == 3
    r3 = log.results[K3]
    assert r3.ack is None and len(r3.atts) == 4
    assert {a.ec for a in r3.atts} == {"LEDGER_BUSY"}
    assert r3.lookup.st == 404 and r3.lookup.json() == {"code": "NOT_FOUND"}
    it2 = log.intents[K2]
    assert it2.body["side"] == "sell" and it2.body_raw.startswith("{")


def test_input_forms_are_equivalent():
    by_path = parse_k6log(OK)
    by_str = parse_k6log(str(OK))
    by_bytes = parse_k6log(OK.read_bytes())
    by_lines = parse_k6log(OK.read_text("utf-8").splitlines())
    assert by_path == by_str == by_bytes == by_lines
    assert by_lines.sha256 == sha256_bytes(OK.read_bytes())


def test_line_shuffle_is_deterministic():
    """줄 순서가 달라도 같은 내용이면 같은 K6Log다(sha256만 다르다, 설계 8)."""
    lines = OK.read_text("utf-8").splitlines()
    shuffled = list(lines)
    random.Random(7).shuffle(shuffled)
    assert shuffled != lines
    a = parse_k6log(lines)
    b = parse_k6log(shuffled)
    assert b.problems == ()
    assert dataclasses.replace(a, sha256="") == dataclasses.replace(b, sha256="")


def test_ws_lines_are_counted_and_ignored():
    lines = dump(ok_objs() + [{"kind": "ws_open", "vu": 1}, {"kind": "wsagg", "n": 3}])
    log = parse_k6log(lines)
    assert log.problems == ()
    assert log.kind_counts["ws_open"] == 1 and log.kind_counts["wsagg"] == 1
    assert complete(log).outcome == Outcome.PASS


# ---------------------------------------------------------------- 깨진 줄


def test_bad_fixture_reports_each_line():
    log = parse_k6log(BAD)
    located = [p.split(":", 1)[0] for p in log.problems]
    assert located == [
        "bad_json L6",
        "unknown_kind L7",
        "missing_field L8",
        "not_object L10",
        "dup_key L11",
        "bad_json L12",
        "bad_field L13",
        "bad_json L14",
    ]
    assert log.line_count == 13  # 빈 줄 4는 세지 않는다
    assert list(log.intents) == [K1]  # 성한 줄은 남는다
    assert log.results == {}
    assert log.kind_counts == {
        "account": 1,
        "intent": 2,
        "mystery": 1,
        "result": 1,
        "run_start": 1,
        "setup_done": 1,
        "ws_open": 1,
    }
    assert "resend" in log.problems[2]
    assert "epoch ms" in log.problems[6]
    res = complete(log, summary(1, 0))
    assert res.outcome == Outcome.INCONCLUSIVE
    assert res.detail["failed"] == [COND_LINES]


def test_missing_trailing_newline_still_parses():
    data = OK.read_bytes().rstrip(b"\n")
    log = parse_k6log(data)
    assert log.problems == ()
    assert len(log.results) == 3


def test_problem_values_are_not_echoed():
    objs = ok_objs()
    objs[2]["idx"] = FAKE  # 문자열 값이 틀린 필드(account B)
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field"]
    assert FAKE not in log.problems[0]
    assert "길이" in log.problems[0]
    assert [a.acct for a in log.accounts] == [ACCT_A]


def test_problems_are_capped_per_code():
    lines = dump(ok_objs()) + ["{"] * (MAX_PROBLEMS_PER_CODE + 50)
    log = parse_k6log(lines)
    assert codes(log).count("bad_json") == MAX_PROBLEMS_PER_CODE + 1
    assert log.problems[-1] == "bad_json: 그 밖 50건 생략"
    assert problem_counts(log.problems) == {"bad_json": MAX_PROBLEMS_PER_CODE + 50}


@pytest.mark.parametrize(
    "mutate, needle",
    [
        (lambda a: a.update(st=700), "atts[0].st"),
        (lambda a: a.update(t0=T0 // 1000), "atts[0].t0"),
        (lambda a: a.pop("ec"), "atts[0].ec"),
        (lambda a: a.update(ms="12"), "atts[0].ms"),
    ],
)
def test_bad_attempt_drops_result(mutate, needle):
    objs = ok_objs()
    mutate(objs[5]["atts"][0])  # result K1
    log = parse_k6log(dump(objs))
    assert len(log.problems) == 1 and needle in log.problems[0]
    assert log.problems[0].split(":", 1)[0].endswith(" L6")
    assert K1 not in log.results  # intent만 남아 INTERRUPTED가 된다


def test_soft_problems_keep_record():
    objs = ok_objs()
    objs[5]["ack"] = None  # 201 시도가 있는데 ack가 null
    objs[4]["body"] = "not json"
    objs[0]["conn_reuse"] = "no"
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field", "bad_field", "bad_field"]
    assert log.results[K1].ack is None
    assert log.intents[K1].body == {} and log.intents[K1].body_raw == "not json"
    assert log.run_start.conn_reuse is None  # 모르면 재사용 가능으로 본다


def test_unsupported_version_drops_run_start():
    objs = ok_objs()
    objs[0]["v"] = 2
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_version"]
    assert log.run_start is None
    assert log.run_id == RUN_ID  # 나머지 줄의 run 값
    assert complete(log).detail["failed"] == [COND_LINES]


def test_empty_input():
    log = parse_k6log([])
    assert log.run_id is None and log.run_start is None
    assert codes(log) == ["run_start_count", "setup_done_count"]
    res = complete(log, summary(0, 0))
    assert res.detail["failed"] == [COND_RUN_START, COND_RECORDS]


def test_missing_file_is_input_error(tmp_path):
    with pytest.raises(InputError):
        parse_k6log(tmp_path / "nope.jsonl")


# ---------------------------------------------------------------- run·run_start·키


def test_two_run_starts():
    objs = ok_objs()
    lines = dump([objs[0]] + objs)
    log = parse_k6log(lines)
    assert codes(log) == ["run_start_count"]
    assert "2개" in log.problems[0]
    res = complete(log)
    assert res.outcome == Outcome.INCONCLUSIVE
    assert res.reasons == (reasons.SENT_LOG_INCOMPLETE,)
    assert res.detail["failed"] == [COND_RUN_START]


def test_mixed_run_values():
    objs = ok_objs()
    objs[6]["run"] = "r-other"  # intent K2
    log = parse_k6log(dump(objs))
    assert codes(log) == ["run_mixed"]
    assert "'r-other'(L7)" in log.problems[0] and "'r-test'(L1)" in log.problems[0]
    assert log.run_id == RUN_ID  # run_start 값
    assert complete(log).detail["failed"] == [COND_RUN]


def test_result_without_intent():
    objs = ok_objs()
    del objs[6]  # intent K2
    log = parse_k6log(dump(objs))
    assert codes(log) == ["orphan_result"]
    assert log.problems[0].startswith("orphan_result L8:")
    assert K2 in log.results and K2 not in log.intents
    res = complete(log, summary(2, 3))
    assert res.detail["failed"] == [COND_RECORDS]


def test_duplicate_intent_and_result_keep_first():
    objs = ok_objs()
    dup_intent = dict(objs[4], vu=9)
    log = parse_k6log(dump(objs + [dup_intent, objs[5]]))
    assert codes(log) == ["dup_intent", "dup_result"]
    assert "다른 내용" in log.problems[0]
    assert log.intents[K1].vu == 1
    assert complete(log, summary(4, 4)).detail["failed"] == [COND_RECORDS]


def test_account_problems():
    objs = ok_objs()
    upper = dict(objs[1], idx=5, acct=ACCT_A.upper())
    log = parse_k6log(dump(objs[:3] + [objs[1], upper] + objs[3:]))
    assert codes(log) == ["dup_account", "bad_field", "account_count"]
    assert "UUID" in log.problems[1]
    assert "account 줄 4개" in log.problems[2]
    assert len(log.accounts) == 2


# ---------------------------------------------------------------- token 키


def test_token_key_nested_in_line():
    objs = ok_objs()
    objs[1]["meta"] = {"auth": [{"token": FAKE}]}  # account 줄 중첩
    log = parse_k6log(dump(objs))
    assert log.has_token_key is True
    assert codes(log) == ["token_key"]
    assert "$.meta.auth[0].token" in log.problems[0]
    assert FAKE not in log.problems[0]
    # token은 safety 가드 몫이라 기록 완전성은 그대로다
    assert complete(log).outcome == Outcome.PASS


def test_token_key_inside_raw_text_and_case():
    objs = ok_objs()
    objs[5]["ack"]["raw"] = '{"status":"FILLED","accessToken":"' + FAKE[:5]  # 잘린 원문
    log = parse_k6log(dump(objs))
    assert log.has_token_key is True
    assert "$.ack.raw(텍스트)" in log.problems[0]


def test_token_key_in_summary():
    summ = dict(summary(), setup_data={"accts": [{"id": ACCT_A, "token": FAKE}]})
    log = parse_k6log(OK, summary=summ)
    assert log.has_token_key is True
    assert codes(log) == ["token_key"]
    assert log.problems[0].startswith("token_key: summary")


def test_find_token_keys_ignores_values():
    assert find_token_keys({"ec": "INVALID_TOKEN", "raw": '{"code":"TOKEN"}'}) == ()
    assert find_token_keys({"a": {"Token": 1}}) == ("$.a.Token",)


# ---------------------------------------------------------------- Counter·sha·gzip


def test_fewer_lines_than_counter():
    objs = ok_objs()
    log = parse_k6log(dump(objs[:-1]), summary=summary())  # 마지막 result 빠짐
    assert codes(log) == ["counter_short"]
    assert "result 줄 2개 < order_results 3" in log.problems[0]
    assert K3 in log.intents and K3 not in log.results
    res = complete(log)
    assert res.detail["failed"] == [COND_COUNTERS]
    assert res.detail["order_results"] == 3 and res.detail["result_lines"] == 2


def test_more_lines_than_counter_is_fine():
    log = parse_k6log(OK)
    assert complete(log, summary(2, 2)).outcome == Outcome.PASS


@pytest.mark.parametrize(
    "summ, msg",
    [
        ({}, "metrics가 없다"),
        ({"metrics": {"order_results": {"values": {"count": 3}}}}, "order_intents"),
        (
            {"metrics": {**summary()["metrics"], "order_intents": {"values": {}}}},
            "형식",
        ),
    ],
)
def test_bad_summary_fails_counters(summ, msg):
    res = complete(parse_k6log(OK), summ)
    assert res.detail["failed"] == [COND_COUNTERS]
    assert msg in res.detail["why"][COND_COUNTERS]


def test_missing_summary_fails_counters():
    res = log_complete(parse_k6log(OK), None, 0, True)
    assert res.detail["failed"] == [COND_COUNTERS]
    assert "summary가 없다" in res.detail["why"][COND_COUNTERS]


def test_summary_count_shapes():
    assert summary_count(load_summary(SUMMARY), "order_intents") == 3
    assert summary_count({"metrics": {"x": {"count": 4}}}, "x") == 4  # summary-export
    assert (
        summary_count({"data": {"metrics": {"x": {"values": {"count": 5}}}}}, "x") == 5
    )
    assert summary_count({"metrics": {"x": {"values": {"count": 6.0}}}}, "x") == 6
    assert (
        summary_count({"metrics": {"x": {"values": {"count": Decimal("7")}}}}, "x") == 7
    )
    assert summary_count({"metrics": {"x": {"values": {"count": 1.5}}}}, "x") is None
    assert summary_count({"metrics": {"x": {"values": {"count": True}}}}, "x") is None
    assert summary_count({"metrics": {}}, "x") is None
    assert summary_count(None, "x") is None


def test_counter_absent_with_no_lines_counts_as_zero():
    objs = [o for o in ok_objs() if o["kind"] not in ("intent", "result")]
    log = parse_k6log(dump(objs))
    res = complete(log, {"metrics": {"vus": {"values": {"value": 0}}}})
    assert res.outcome == Outcome.PASS
    assert res.detail["order_intents"] == 0


def test_load_summary(tmp_path):
    assert load_summary(tmp_path / "missing.json") is None
    (tmp_path / "broken.json").write_text("{", encoding="utf-8")
    with pytest.raises(InputError):
        load_summary(tmp_path / "broken.json")
    (tmp_path / "list.json").write_text("[]", encoding="utf-8")
    with pytest.raises(InputError):
        load_summary(tmp_path / "list.json")
    s = load_summary(SUMMARY)
    assert s["state"]["testRunDurationMs"] == Decimal("10234.567")


def test_expected_sha256():
    sha = sha256_bytes(OK.read_bytes())
    log = parse_k6log(OK, expected_sha256=f"{sha.upper()}  /out/orders.jsonl")
    assert log.problems == ()
    assert sha_matches(log, sha) is True
    assert sha_matches(log, None) is False
    bad = parse_k6log(OK, expected_sha256="0" * 64)
    assert codes(bad) == ["sha_mismatch"]
    assert complete(bad).detail["failed"] == [COND_SHA]
    assert codes(parse_k6log(OK, expected_sha256="xyz")) == ["sha_mismatch"]


def test_gzip_input(tmp_path):
    gz = tmp_path / "orders.jsonl.gz"
    gz.write_bytes(gzip.compress(OK.read_bytes()))
    sha = sha256_bytes(OK.read_bytes())
    log = parse_k6log(gz, expected_sha256=sha)
    assert log == parse_k6log(OK)
    assert log.sha256 == sha  # 압축을 푼 원본 바이트 기준
    assert parse_k6log(gz.read_bytes()) == log


def test_gzip_multi_member(tmp_path):
    data = OK.read_bytes()
    half = data.index(b"\n", len(data) // 2) + 1
    gz = gzip.compress(data[:half]) + gzip.compress(data[half:])
    log = parse_k6log(gz)
    assert log.problems == ()
    assert log.sha256 == sha256_bytes(data)


def test_gzip_truncated_salvages_lines():
    gz = gzip.compress(OK.read_bytes())
    log = parse_k6log(gz[:-12])
    assert "gzip_error" in codes(log)
    assert log.run_start is not None and K1 in log.intents
    assert complete(log).outcome == Outcome.INCONCLUSIVE


# ---------------------------------------------------------------- log_complete


def test_log_complete_pass():
    log = parse_k6log(OK)
    res = log_complete(log, load_summary(SUMMARY), 0, sha_matches(log, log.sha256))
    assert res.outcome == Outcome.PASS
    assert res.value is True and res.reasons == ()
    assert res.id == "log_complete" and res.required is False
    assert res.detail["failed"] == [] and res.detail["problems"] == {}
    json.dumps(res.detail)  # verdict에 넣을 수 있다


@pytest.mark.parametrize(
    "exit_code, ok",
    [(0, True), (99, True), (107, False), (108, False), (137, False), (None, False)],
)
def test_log_complete_exit_codes(exit_code, ok):
    res = complete(parse_k6log(OK), exit_code=exit_code)
    assert res.value is ok
    assert res.detail["failed"] == ([] if ok else [COND_EXIT])


@pytest.mark.parametrize("sha_ok, why", [(False, "불일치"), (None, "미검증")])
def test_log_complete_sha_flag(sha_ok, why):
    res = complete(parse_k6log(OK), sha_ok=sha_ok)
    assert res.detail["failed"] == [COND_SHA]
    assert why in res.detail["why"][COND_SHA]


def test_log_complete_reports_all_conditions_in_order():
    objs = ok_objs()
    objs[6]["run"] = "r-other"
    lines = dump([objs[0]] + objs[:-1]) + ["{"]
    log = parse_k6log(lines)
    res = log_complete(log, summary(), 1, None)
    assert res.detail["failed"] == [
        COND_LINES,
        COND_RUN,
        COND_RUN_START,
        COND_COUNTERS,
        COND_SHA,
        COND_EXIT,
    ]
    assert set(res.detail["failed"]) <= set(LOG_COMPLETE_CONDITIONS)


def test_log_complete_accepts_factory_logs():
    it = make_intent("k1", ACCT_A)
    rs = make_result("k1")
    assert complete(make_log([it], [rs]), summary(1, 1)).outcome == Outcome.PASS
    noisy = make_log([it], [rs], problems=["broken line"])
    assert complete(noisy, summary(1, 1)).detail["failed"] == [COND_LINES]


def test_problem_code():
    assert problem_code("bad_json L3: x") == "bad_json"
    assert problem_code("run_mixed: x") == "run_mixed"
    assert problem_code("broken line") == ""
    assert problem_counts(["broken line", "bad_json L1: x"]) == {
        "bad_json": 1,
        "other": 1,
    }

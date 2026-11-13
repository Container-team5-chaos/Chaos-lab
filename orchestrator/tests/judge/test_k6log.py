"""k6 기록 파서(JSONL v2)와 기록 완전성(설계 4.6, 5.3, 8, 계약 명세 2)."""

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
    RAW_MAX,
    find_token_keys,
    intent_issues,
    load_summary,
    log_complete,
    parse_k6log,
    problem_code,
    problem_counts,
    sha_matches,
    summary_count,
)
from orchestrator.judge.model import (
    JSONL_VERSION,
    AttemptKind,
    Outcome,
    canonical_json,
    sha256_bytes,
)

from .factories import (
    ACCT_A,
    ACCT_B,
    RUN_ID,
    T0,
    att,
    make_intent,
    make_log,
    make_result,
    run_key,
)

FIX = Path(__file__).parent / "fixtures" / "k6"
OK = FIX / "orders_ok.jsonl"
BAD = FIX / "orders_bad.jsonl"
SUMMARY = FIX / "summary_ok.json"

K1 = run_key(1, vu=1)  # ACCT_A, 첫 시도 200(clean)
K2 = run_key(1, vu=2)  # ACCT_B, 0 뒤 200 rp=true
K3 = run_key(2, vu=1)  # ACCT_A, 503 not_ready 뒤 503 stale_price
FAKE = "not-a-real-token-0000"  # 유출 검사용 가짜 값

# orders_ok.jsonl 줄 순서(0부터)
RS, I1, R1, I2, I3, R2, R3, END = range(8)


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


def rekey(objs, key):
    """intent K1과 result K1의 키를 함께 바꾼다(body.client_order_id 포함)."""
    objs[I1]["key"] = key
    objs[I1]["body"]["client_order_id"] = key
    objs[R1]["key"] = key


# ---------------------------------------------------------------- 정상


def test_ok_fixture_parses_cleanly():
    log = parse_k6log(OK, summary=load_summary(SUMMARY))
    assert log.problems == ()
    assert log.run_id == RUN_ID
    assert log.line_count == 8
    assert log.kind_counts == {"intent": 3, "result": 3, "run_end": 1, "run_start": 1}
    assert log.sha256 == sha256_bytes(OK.read_bytes())
    assert log.has_token_key is False
    assert list(log.intents) == [K1, K2, K3]  # 파일 순서
    assert list(log.results) == [K1, K2, K3]


def test_accounts_come_from_intents():
    """v2에는 account 줄이 없다. 계좌 목록은 intent에서 만든다(VU 오름차순)."""
    log = parse_k6log(OK)
    assert [(a.idx, a.acct, a.vus) for a in log.accounts] == [
        (0, ACCT_A, (1,)),
        (1, ACCT_B, (2,)),
    ]
    assert log.accounts_by_vu == {1: (ACCT_A,), 2: (ACCT_B,)}
    assert log.account_by_idx(1) == ACCT_B
    assert log.account_ids == {ACCT_A, ACCT_B}


def test_vu_user_mapping_is_left_to_pipeline():
    """한 VU가 두 user를 써도 파서는 문제로 적지 않는다(safety 가드 몫, 계약 명세 8)."""
    objs = ok_objs()
    objs[I3]["acct"] = objs[I3]["body"]["user_id"] = ACCT_B  # VU 1이 B도 쓴다
    log = parse_k6log(dump(objs))
    assert log.problems == ()
    assert log.accounts_by_vu == {1: (ACCT_A, ACCT_B), 2: (ACCT_B,)}


def test_run_start_fields():
    rs = parse_k6log(OK).run_start
    assert rs.v == JSONL_VERSION == 2
    assert rs.run == RUN_ID and rs.seed == 42 and rs.vus == 2
    assert rs.profile == "smoke" and rs.script_ver == "0.2.0"
    assert rs.conn_reuse is None  # 없어도 된다(판정에 쓰지 않는다)
    assert rs.initial_cash == 10_000_000
    assert rs.symbols == ("KRW-BTC",)
    assert rs.plan["executor"] == "ramping-arrival-rate"
    assert rs.plan["stages"][1] == {"duration_s": 10, "target": 180}
    assert rs.extra == {"ws_reconnect": False}  # 모르는 키는 extra
    assert rs.t == T0 - 60_000


def test_conn_reuse_is_optional_but_kept():
    objs = ok_objs()
    objs[RS]["conn_reuse"] = False
    rs = parse_k6log(dump(objs)).run_start
    assert rs.conn_reuse is False and "conn_reuse" not in rs.extra


def test_parsed_records_match_factories():
    """파서 출력 모양 = 골든 테스트가 쓰는 빌더 모양(classify 입력 계약)."""
    log = parse_k6log(OK)
    # body가 객체면 body_raw는 None이다(빌더는 canonical_json 원문을 둔다)
    want_i1 = dataclasses.replace(make_intent(K1, ACCT_A, t=T0), body_raw=None)
    assert log.intents[K1] == want_i1
    assert log.results[K1] == make_result(K1)
    assert log.results[K2] == make_result(
        K2,
        [att(0, "1050", 5000.123), att(200, rp=True, ms=30.5)],
        acct=ACCT_B,
        t_start=T0 + 501,
    )
    assert log.results[K3] == make_result(
        K3,
        [att(503, "not_ready", 8.75), att(503, "stale_price", 8.75)],
        t_start=T0 + 2001,
    )


def test_attempt_fields_and_kinds():
    log = parse_k6log(OK)
    a1 = log.results[K1].atts[0]
    assert a1.kind is AttemptKind.ACK and a1.rp is False
    assert a1.json()["order_id"] == 100001 and a1.json()["replayed"] is False
    r2 = log.results[K2]
    assert [a.st for a in r2.atts] == [0, 200]
    assert r2.atts[0].ec == "1050"  # 숫자 error_code는 문자열로
    assert r2.atts[0].ms == 5000.123 and isinstance(r2.atts[0].ms, float)
    assert r2.atts[0].rp is None and r2.atts[0].raw is None
    assert [a.kind for a in r2.atts] == [AttemptKind.UNKNOWN, AttemptKind.ACK]
    assert r2.first_ack is r2.atts[1] and r2.first_ack.rp is True
    r3 = log.results[K3]
    assert [a.kind for a in r3.atts] == [AttemptKind.UNKNOWN, AttemptKind.REJECT]
    assert r3.atts[0].is_not_ready and r3.first_ack is None
    assert r3.atts[1].json() == {
        "client_order_id": K3,
        "reason": "stale_price",
        "status": "rejected",
    }
    assert log.intents[K2].body["user_id"] == ACCT_B


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
    assert a.accounts == b.accounts


def test_ws_lines_are_counted_and_ignored():
    lines = dump(ok_objs() + [{"type": "ws_open", "vu": 1}, {"type": "wsagg", "n": 3}])
    log = parse_k6log(lines)
    assert log.problems == ()
    assert log.kind_counts["ws_open"] == 1 and log.kind_counts["wsagg"] == 1
    assert complete(log).outcome == Outcome.PASS


def test_run_end_is_optional():
    objs = ok_objs()
    without = parse_k6log(dump(objs[:END]))
    assert without.problems == ()
    assert complete(without).outcome == Outcome.PASS
    objs[END]["t"] = T0 // 1000  # 있으면 형식을 본다
    assert codes(parse_k6log(dump(objs))) == ["bad_field"]


def test_run_on_records_is_optional_but_checked():
    objs = ok_objs()
    for i in (I1, R1, I2, I3, R2, R3):
        objs[i]["run"] = RUN_ID
    log = parse_k6log(dump(objs))
    assert log.problems == ()
    assert dataclasses.replace(log, sha256="") == dataclasses.replace(
        parse_k6log(OK), sha256=""
    )
    objs[I2]["run"] = 7
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field", "orphan_result"]  # intent K2를 버린다
    assert K2 not in log.intents


# ---------------------------------------------------------------- 깨진 줄


def test_bad_fixture_reports_each_line():
    log = parse_k6log(BAD)
    located = [p.split(":", 1)[0] for p in log.problems]
    assert located == [
        "bad_json L4",
        "unknown_type L5",
        "missing_field L6",
        "not_object L8",
        "dup_key L9",
        "bad_json L10",
        "bad_field L11",
        "bad_version L12",
        "bad_version L13",
        "bad_json L14",
    ]
    assert log.line_count == 13  # 빈 줄 3은 세지 않는다
    assert list(log.intents) == [K1]  # 성한 줄은 남는다
    assert log.results == {}
    assert log.kind_counts == {
        "account": 1,
        "intent": 2,
        "mystery": 1,
        "result": 1,
        "run_start": 1,
        "ws_open": 1,
    }
    assert "atts[0].rp" in log.problems[2]
    assert "epoch ms" in log.problems[6]
    assert "kind" in log.problems[7]
    assert "'account'" in log.problems[8]
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
    objs[I2]["vu"] = FAKE  # 문자열 값이 틀린 필드
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field", "orphan_result"]
    assert FAKE not in log.problems[0]
    assert "길이" in log.problems[0]
    assert K2 not in log.intents


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
        (lambda a: a.update(n=0), "atts[0].n"),
        (lambda a: a.pop("rp"), "atts[0].rp"),
        (lambda a: a.update(rp="false"), "atts[0].rp"),
        (lambda a: a.pop("raw"), "atts[0].raw"),
        (lambda a: a.update(raw={"order_id": 1}), "atts[0].raw"),
    ],
)
def test_bad_attempt_drops_result(mutate, needle):
    objs = ok_objs()
    mutate(objs[R1]["atts"][0])
    log = parse_k6log(dump(objs))
    assert len(log.problems) == 1 and needle in log.problems[0]
    assert log.problems[0].split(":", 1)[0].endswith(" L3")
    assert K1 not in log.results  # intent만 남아 INTERRUPTED가 된다


@pytest.mark.parametrize(
    "atts, code", [([], "bad_field"), (None, "missing_field"), ("[]", "bad_field")]
)
def test_empty_or_missing_atts_drops_result(atts, code):
    objs = ok_objs()
    if atts is None:
        del objs[R1]["atts"]
    else:
        objs[R1]["atts"] = atts
    log = parse_k6log(dump(objs))
    assert codes(log) == [code]
    assert K1 not in log.results


def test_dropped_line_does_not_add_soft_problems():
    objs = ok_objs()
    objs[R2]["atts"][0]["rp"] = True  # soft(200이 아닌데 rp)
    objs[R2]["atts"][1]["st"] = "200"  # hard
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field"]
    assert "atts[1].st" in log.problems[0]


@pytest.mark.parametrize(
    "where, mutate, needle",
    [
        (R2, lambda r: r["atts"][1].update(n=3), "n이 1부터 차례가 아니다"),
        (R3, lambda r: r["atts"][0].update(rp=False), "rp는 200 응답에만"),
        (R1, lambda r: r["atts"][0].update(rp=True), "raw.replayed와 다르다"),
        (R1, lambda r: r["atts"][0].update(rp=None), "raw.replayed와 다르다"),
        (R3, lambda r: r["atts"][1].update(raw="x" * (RAW_MAX + 1)), "2048자"),
    ],
)
def test_soft_attempt_problems_keep_record(where, mutate, needle):
    objs = ok_objs()
    mutate(objs[where])
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field"]
    assert needle in log.problems[0]
    assert objs[where]["key"] in log.results
    assert complete(log).detail["failed"] == [COND_LINES]


def test_soft_run_start_problems_keep_record():
    objs = ok_objs()
    objs[RS].update(conn_reuse="no", vus=0, symbols="KRW-BTC", initial_cash=-1)
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field"] * 4
    rs = log.run_start
    assert rs is not None
    assert rs.conn_reuse is None and rs.vus is None
    assert rs.symbols == () and rs.initial_cash is None


def test_run_start_requires_vus():
    objs = ok_objs()
    del objs[RS]["vus"]
    log = parse_k6log(dump(objs))
    assert codes(log) == ["missing_field"]
    assert "vus" in log.problems[0] and log.run_start is None


def test_missing_file_is_input_error(tmp_path):
    with pytest.raises(InputError):
        parse_k6log(tmp_path / "nope.jsonl")


def test_empty_input():
    log = parse_k6log([])
    assert log.run_id is None and log.run_start is None
    assert codes(log) == ["run_start_count"]
    res = complete(log, summary(0, 0))
    assert res.detail["failed"] == [COND_RUN_START]


# ---------------------------------------------------------------- v1 거부


_V1_ACCOUNT = {"run": RUN_ID, "idx": 0, "acct": ACCT_A, "nick": "k6-r-test-0"}


@pytest.mark.parametrize(
    "extra, needle",
    [
        ({"kind": "account", **_V1_ACCOUNT}, "kind"),
        ({"type": "account", **_V1_ACCOUNT}, "'account'"),
        ({"type": "setup_done", "run": RUN_ID, "accounts": 2, "t": T0}, "'setup_done'"),
        ({"type": "run_end", "kind": "run_end", "run": RUN_ID}, "kind"),
    ],
)
def test_v1_line_types_are_rejected(extra, needle):
    log = parse_k6log(dump(ok_objs() + [extra]))
    assert codes(log) == ["bad_version"]
    assert needle in log.problems[0]
    assert complete(log).detail["failed"] == [COND_LINES]


@pytest.mark.parametrize("field", ["lookup", "ack", "resend"])
def test_v1_result_fields_are_rejected(field):
    objs = ok_objs()
    objs[R1][field] = None
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_version"]
    assert field in log.problems[0]
    assert K1 not in log.results


@pytest.mark.parametrize("v", [1, 3])
def test_unsupported_version_drops_run_start(v):
    objs = ok_objs()
    objs[RS]["v"] = v
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_version"]
    assert log.run_start is None
    assert log.run_id == RUN_ID  # 버린 run_start의 run 값도 센다
    assert complete(log).detail["failed"] == [COND_LINES]


def test_v1_file_is_rejected():
    """v1 기록(kind 필드)은 줄마다 bad_version이고 읽은 기록이 없다."""
    v1 = [
        {"kind": "run_start", "v": 1, "run": RUN_ID, "t": T0},
        {"kind": "account", **_V1_ACCOUNT},
        {"kind": "intent", "run": RUN_ID, "key": K1, "acct": ACCT_A, "vu": 1},
    ]
    log = parse_k6log(dump(v1))
    assert codes(log) == ["bad_version"] * 3 + ["run_start_count"]
    assert log.run_start is None and log.intents == {} and log.kind_counts == {}
    assert log.run_id is None


# ---------------------------------------------------------------- run·run_start·키


def test_two_run_starts():
    objs = ok_objs()
    lines = dump([objs[RS]] + objs)
    log = parse_k6log(lines)
    assert codes(log) == ["run_start_count"]
    assert "2개" in log.problems[0]
    res = complete(log)
    assert res.outcome == Outcome.INCONCLUSIVE
    assert res.reasons == (reasons.SENT_LOG_INCOMPLETE,)
    assert res.detail["failed"] == [COND_RUN_START]


def test_mixed_run_values():
    objs = ok_objs()
    objs[I2]["run"] = "r-other"  # intent K2
    log = parse_k6log(dump(objs))
    assert codes(log) == ["run_mixed"]
    assert "'r-other'(L4)" in log.problems[0] and "'r-test'(L1)" in log.problems[0]
    assert log.run_id == RUN_ID  # run_start 값
    assert complete(log).detail["failed"] == [COND_RUN]


def test_result_without_intent():
    objs = ok_objs()
    del objs[I2]
    log = parse_k6log(dump(objs))
    assert codes(log) == ["orphan_result"]
    assert log.problems[0].startswith("orphan_result L5:")
    assert K2 in log.results and K2 not in log.intents
    res = complete(log, summary(2, 3))
    assert res.detail["failed"] == [COND_RECORDS]


def test_duplicate_intent_and_result_keep_first():
    objs = ok_objs()
    dup_intent = dict(objs[I1], vu=9)
    log = parse_k6log(dump(objs + [dup_intent, objs[R1]]))
    assert codes(log) == ["dup_intent", "dup_result"]
    assert "다른 내용" in log.problems[0]
    assert log.intents[K1].vu == 1
    assert complete(log, summary(4, 4)).detail["failed"] == [COND_RECORDS]


# ---------------------------------------------------------------- intent 형식(표시만)


def _set_body(field, value):
    def f(objs):
        objs[I1]["body"][field] = value

    return f


def _pop_body(field):
    return lambda objs: objs[I1]["body"].pop(field)


@pytest.mark.parametrize(
    "mutate, needle",
    [
        (_set_body("client_order_id", run_key(9)), "key ≠ body.client_order_id"),
        (_set_body("user_id", ACCT_B), "acct ≠ body.user_id"),
        (_pop_body("market"), "body 필드 없음: market"),
        (_set_body("volume", 0.0001), "문자열이 아니다: volume"),
        (_set_body("side", ""), "문자열이 아니다: side"),
        (lambda objs: objs[I1].update(body="not json"), "JSON 객체가 아니다"),
        (lambda objs: objs[I1].update(body="[1]"), "JSON 객체가 아니다"),
        (lambda objs: objs[I1].update(vu=7), "key의 vu 1가 intent.vu 7와 다르다"),
        (lambda objs: rekey(objs, "order-1"), "형식이 아니다"),
        (lambda objs: rekey(objs, "k6-r-test-2-1-1"), "형식이 아니다"),  # run r-test-2
        (lambda objs: rekey(objs, run_key(int("1" * 60))), "64자를 넘는다"),
    ],
)
def test_intent_format_is_flagged_not_dropped(mutate, needle):
    """형식 위반은 표시만 한다. 기록은 남고 완전성과 무관하다(분류는 classify, 계약 명세 2)."""
    objs = ok_objs()
    mutate(objs)
    log = parse_k6log(dump(objs))
    assert codes(log) == ["intent_format"]
    assert log.problems[0].startswith("intent_format L2:")
    assert needle in log.problems[0]
    key = objs[I1]["key"]
    assert key in log.intents and key in log.results
    assert needle in "; ".join(intent_issues(log.intents[key], RUN_ID))
    assert complete(log).outcome == Outcome.PASS


def test_intent_format_values_are_not_echoed():
    objs = ok_objs()
    objs[I1]["body"]["user_id"] = FAKE
    log = parse_k6log(dump(objs))
    assert codes(log) == ["intent_format"]
    assert FAKE not in log.problems[0]


def test_intent_body_json_string_is_parsed():
    objs = ok_objs()
    body = objs[I1]["body"]
    objs[I1]["body"] = canonical_json(body)
    log = parse_k6log(dump(objs))
    assert log.problems == ()
    assert log.intents[K1] == make_intent(K1, ACCT_A, t=T0)  # body_raw = 원문
    assert dict(log.intents[K1].body) == body


def test_intent_body_wrong_type_drops_intent():
    objs = ok_objs()
    objs[I1]["body"] = 5
    log = parse_k6log(dump(objs))
    assert codes(log) == ["bad_field", "orphan_result"]
    assert K1 not in log.intents


def test_intent_issues_direct():
    it = make_intent(run_key(1))
    assert intent_issues(it) == () and intent_issues(it, RUN_ID) == ()
    assert it.consistent
    free = make_intent("free-key")
    assert intent_issues(free) == ()  # run을 모르면 키 형식은 보지 않는다
    assert intent_issues(free, RUN_ID) == (
        "key가 k6-<run_id>-<vu>-<seq> 형식이 아니다",
    )
    other_run = make_intent(run_key(1, run_id="r-test-2"))  # k6-r-test-2-1-1
    assert intent_issues(other_run, RUN_ID) != ()
    assert intent_issues(other_run, "r-test-2") == ()
    swapped = dataclasses.replace(it, acct=ACCT_B)
    assert not swapped.consistent
    assert intent_issues(swapped) == ("acct ≠ body.user_id",)


# ---------------------------------------------------------------- 비밀 키(token·authorization)


def test_token_key_nested_in_line():
    objs = ok_objs()
    objs[RS]["meta"] = {"auth": [{"token": FAKE}]}
    log = parse_k6log(dump(objs))
    assert log.has_token_key is True
    assert codes(log) == ["token_key"]
    assert "$.meta.auth[0].token" in log.problems[0]
    assert FAKE not in log.problems[0]
    # 비밀 키는 safety 가드 몫이라 기록 완전성은 그대로다
    assert complete(log).outcome == Outcome.PASS


def test_authorization_key_in_line():
    objs = ok_objs()
    objs[I1]["headers"] = {"Authorization": "Bearer " + FAKE}
    log = parse_k6log(dump(objs))
    assert log.has_token_key is True
    assert codes(log) == ["token_key"]
    assert "$.headers.Authorization" in log.problems[0]
    assert FAKE not in log.problems[0]
    assert K1 in log.intents  # 줄은 버리지 않는다


@pytest.mark.parametrize(
    "raw", ['{"status":"filled","accessToken":"', '{"x-authorization" : "']
)
def test_secret_key_inside_raw_text(raw):
    objs = ok_objs()
    objs[R1]["atts"][0]["raw"] = raw + FAKE[:5]  # 잘린 원문
    log = parse_k6log(dump(objs))
    assert log.has_token_key is True
    assert codes(log) == ["token_key"]
    assert "$.atts[0].raw(텍스트)" in log.problems[0]
    assert log.results[K1].atts[0].kind is AttemptKind.PROTOCOL  # 200인데 원문이 깨졌다


def test_token_key_in_summary():
    summ = dict(summary(), setup_data={"accts": [{"id": ACCT_A, "token": FAKE}]})
    log = parse_k6log(OK, summary=summ)
    assert log.has_token_key is True
    assert codes(log) == ["token_key"]
    assert log.problems[0].startswith("token_key: summary")


def test_find_token_keys():
    assert find_token_keys({"ec": "INVALID_TOKEN", "raw": '{"code":"TOKEN"}'}) == ()
    assert find_token_keys({"reason": "authorization failed"}) == ()  # 값은 보지 않는다
    assert find_token_keys({"a": {"Token": 1}}) == ("$.a.Token",)
    assert find_token_keys({"Proxy-Authorization": 1}) == ("$.Proxy-Authorization",)
    assert find_token_keys('{"authorization": "x"') == ("$(텍스트)",)  # 깨진 원문


# ---------------------------------------------------------------- Counter·sha·gzip


def test_fewer_lines_than_counter():
    objs = ok_objs()
    del objs[R3]
    log = parse_k6log(dump(objs), summary=summary())
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
    objs = [o for o in ok_objs() if o["type"] not in ("intent", "result")]
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


@pytest.mark.parametrize(
    "exit_code, stopped, ok",
    [(105, True, True), (105, False, False), (0, True, True), (137, True, False)],
)
def test_log_complete_accepts_105_only_when_we_stopped(exit_code, stopped, ok):
    res = log_complete(parse_k6log(OK), summary(), exit_code, True, stopped=stopped)
    assert res.value is ok and res.detail["stopped_by_judge"] is stopped
    if exit_code == 105 and not ok:
        assert "정지 표시가 없다" in res.detail["why"][COND_EXIT]


@pytest.mark.parametrize("sha_ok, why", [(False, "불일치"), (None, "미검증")])
def test_log_complete_sha_flag(sha_ok, why):
    res = complete(parse_k6log(OK), sha_ok=sha_ok)
    assert res.detail["failed"] == [COND_SHA]
    assert why in res.detail["why"][COND_SHA]


def test_log_complete_reports_all_conditions_in_order():
    objs = ok_objs()
    objs[I2]["run"] = "r-other"
    lines = dump([objs[RS]] + objs[:R3] + objs[R3 + 1 :]) + ["{"]
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
    k = run_key(1)
    it = make_intent(k, ACCT_A)
    rs = make_result(k)
    assert complete(make_log([it], [rs]), summary(1, 1)).outcome == Outcome.PASS
    noisy = make_log([it], [rs], problems=["broken line"])
    assert complete(noisy, summary(1, 1)).detail["failed"] == [COND_LINES]
    flagged = make_log([it], [rs], problems=["intent_format L2: x"])
    assert complete(flagged, summary(1, 1)).outcome == Outcome.PASS


def test_problem_code():
    assert problem_code("bad_json L3: x") == "bad_json"
    assert problem_code("run_mixed: x") == "run_mixed"
    assert problem_code("broken line") == ""
    assert problem_counts(["broken line", "bad_json L1: x"]) == {
        "bad_json": 1,
        "other": 1,
    }

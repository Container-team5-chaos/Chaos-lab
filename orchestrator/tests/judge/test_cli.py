"""chaos-judge CLI 종료 코드, mark, transport, client 순수 계산(설계 1.4, 5.1, 5.2, 5.5, 8)."""

import json
import sys
import types
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest

import orchestrator.judge.__main__ as cli
from orchestrator.judge import fetch as fetch_mod
from orchestrator.judge.client import achieved_ratio, nearest_rank, planned_arrivals
from orchestrator.judge.context import MARKS_LOCAL_NAME, load_run
from orchestrator.judge.criteria import load_criteria, write_resolved
from orchestrator.judge.errors import (
    EXIT_FAIL,
    EXIT_INCONCLUSIVE,
    EXIT_INPUT,
    EXIT_JUDGE_ERROR,
    EXIT_PASS,
    InputError,
)
from orchestrator.judge.model import DbTarget, Outcome, SpeedPoint, Verdict
from orchestrator.judge.transport import (
    CMD_PREFIX,
    PSQL_ARGS,
    TEST_PSQL_PREFIX,
    TransportError,
    Transports,
    psql_args,
)

from .factories import ACCT_A, ACCT_EXTRA, RUN_ID, T0, make_intent

REPO = Path(__file__).resolve().parents[3]
CRITERIA_DIR = REPO / "experiments" / "criteria"
PIPELINE = "orchestrator.judge.pipeline"
EXIT_USAGE = 2


# ---------------------------------------------------------------- 도우미


@pytest.fixture
def run_json(tmp_path):
    return write_run(tmp_path)


def write_run(tmp_path, **over):
    doc = {
        "schema": "chaos.run/v1",
        "run_id": RUN_ID,
        "experiment": "order-api-pod-kill",
        "criteria": str(CRITERIA_DIR / "order-api-pod-kill.yaml"),
        "marks": {"inject_start": None, "inject_end": None},
        "extra_accounts": [],
    }
    doc.update(over)
    p = tmp_path / "run.json"
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


def mark(run, *args):
    return cli.main(["mark", "--run", str(run), *args])


def local_doc(run):
    return json.loads((run.parent / MARKS_LOCAL_NAME).read_text(encoding="utf-8"))


def local_bytes(run):
    p = run.parent / MARKS_LOCAL_NAME
    return p.read_bytes() if p.exists() else None


def _forbidden_runner(argv, *, input=None, timeout):
    raise AssertionError(f"테스트에서 클러스터 명령을 실행하면 안 된다: {argv}")


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        cli, "_transports", lambda: Transports(runner=_forbidden_runner)
    )


@pytest.fixture
def no_pipeline(monkeypatch):
    monkeypatch.setitem(sys.modules, PIPELINE, None)


def fake_pipeline(monkeypatch, *, verdict=Outcome.PASS, evaluate=None, collect=None):
    """pipeline 대역. CLI는 assess(→ verdict)와 render_outputs(→ 파일 내용)를 부른다."""
    calls = {}

    def _evaluate(ctx, crit, evidence):
        calls["evaluate"] = (ctx, crit, evidence)
        return Verdict(
            run_id=ctx.run_id,
            experiment=ctx.experiment,
            verdict=verdict,
            reasons=("CHECK_FAILED",) if verdict == Outcome.FAIL else (),
        )

    def _collect(ctx, crit, tr):
        calls["collect"] = (ctx, crit, tr)
        return ctx.run_dir / "evidence"

    ev = evaluate or _evaluate

    def _assess(ctx, crit, evidence, *, generated_at=None):
        calls["generated_at"] = generated_at
        return types.SimpleNamespace(verdict=ev(ctx, crit, evidence), classified=())

    def _render_outputs(a):
        return {"verdict.json": json.dumps({"verdict": str(a.verdict.verdict)}) + "\n"}

    mod = types.ModuleType(PIPELINE)
    mod.evaluate = ev
    mod.assess = _assess
    mod.render_outputs = _render_outputs
    mod.collect = collect or _collect
    monkeypatch.setitem(sys.modules, PIPELINE, mod)
    return calls


# ---------------------------------------------------------------- 파서·종료 코드


def test_help_exits_zero(capsys):
    assert cli.main(["--help"]) == EXIT_PASS
    out = capsys.readouterr().out
    for cmd in ("plan", "accounts", "doctor", "fetch", "collect", "evaluate"):
        assert cmd in out
    for cmd in ("run", "mark", "stop", "precheck", "limit"):
        assert cmd in out
    assert "아직 구현하지 않은 명령" in out


def test_subcommand_help_exits_zero(capsys):
    assert cli.main(["mark", "--help"]) == EXIT_PASS
    assert "--force" in capsys.readouterr().out


def test_no_command_is_usage_error():
    assert cli.main([]) == EXIT_USAGE


def test_unknown_command_is_usage_error(capsys):
    assert cli.main(["bogus", "--run", "run.json"]) == EXIT_USAGE
    assert "invalid choice" in capsys.readouterr().err


def test_run_argument_required():
    assert cli.main(["mark", "inject_start"]) == EXIT_USAGE


def test_not_implemented_constant_is_distinct():
    codes = {EXIT_PASS, EXIT_FAIL, EXIT_INCONCLUSIVE, EXIT_INPUT, EXIT_JUDGE_ERROR}
    assert cli.EXIT_NOT_IMPLEMENTED not in codes | {EXIT_USAGE}


# 명령을 구현하면 그 명령을 이 목록에서 뺀다. 클러스터 호출은 no_cluster가 막는다.
@pytest.mark.parametrize(
    ("cmd", "section"),
    [
        ("plan", "설계 1.4"),
        ("accounts", "설계 3"),
        ("doctor", "설계 7.5"),
        ("collect", "설계 5.2"),
        ("run", "설계 5.2"),
        ("precheck", "설계 1.4"),
        ("limit", "설계 4.10"),
    ],
)
def test_not_implemented_exit_code(
    cmd, section, run_json, no_cluster, no_pipeline, capsys
):
    assert cli.main([cmd, "--run", str(run_json)]) == cli.EXIT_NOT_IMPLEMENTED
    err = capsys.readouterr().err
    assert f"not implemented yet ({section}" in err


def test_not_implemented_from_module_stub(run_json, no_cluster, monkeypatch, capsys):
    def stub(ctx, tr, run_dir):
        raise NotImplementedError("설계 4.8, 1차")

    monkeypatch.setattr(fetch_mod, "fetch", stub)
    assert cli.main(["fetch", "--run", str(run_json)]) == cli.EXIT_NOT_IMPLEMENTED
    assert "not implemented yet (설계 4.8, 1차)" in capsys.readouterr().err


def test_input_error_comes_before_not_implemented(tmp_path, no_cluster):
    missing = tmp_path / "run.json"
    assert cli.main(["doctor", "--run", str(missing)]) == EXIT_INPUT


def test_bad_criteria_is_input_error(run_json, tmp_path, no_pipeline):
    bad = tmp_path / "bad.yaml"
    bad.write_text("schema_version: 1\nunknown_key: 1\n", encoding="utf-8")
    argv = ["evaluate", "--run", str(run_json), "--criteria", str(bad)]
    assert cli.main(argv) == EXIT_INPUT


@pytest.mark.parametrize("cmd", ["fetch", "stop", "collect", "accounts", "doctor"])
def test_offline_rejected_for_cluster_commands(cmd, run_json, no_cluster):
    assert cli.main([cmd, "--run", str(run_json), "--offline"]) == EXIT_USAGE


# ---------------------------------------------------------------- evaluate·run 래퍼


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        (Outcome.PASS, EXIT_PASS),
        (Outcome.FAIL, EXIT_FAIL),
        (Outcome.INCONCLUSIVE, EXIT_INCONCLUSIVE),
        (Outcome.NA, EXIT_JUDGE_ERROR),
    ],
)
def test_evaluate_maps_verdict_to_exit_code(outcome, code, run_json, monkeypatch):
    fake_pipeline(monkeypatch, verdict=outcome)
    assert cli.main(["evaluate", "--run", str(run_json)]) == code
    # N/A는 verdict가 될 수 없다. 산출물을 쓰지 않고 judge 오류로 끝난다
    written = (run_json.parent / "verdict.json").exists()
    assert written is (outcome != Outcome.NA)


def test_evaluate_without_pipeline_module_is_not_implemented(
    run_json, no_pipeline, capsys
):
    assert cli.main(["evaluate", "--run", str(run_json)]) == cli.EXIT_NOT_IMPLEMENTED
    assert "not implemented yet (설계 5.2" in capsys.readouterr().err


def test_evaluate_passes_context_and_prints_verdict(run_json, monkeypatch, capsys):
    calls = fake_pipeline(monkeypatch, verdict=Outcome.FAIL)
    assert cli.main(["evaluate", "--run", str(run_json)]) == EXIT_FAIL
    ctx, crit, evidence = calls["evaluate"]
    assert ctx.run_id == RUN_ID
    assert evidence == run_json.parent
    assert isinstance(calls["generated_at"], int)  # 호출자(CLI)가 찍는다. digest 밖
    written = json.loads((run_json.parent / "verdict.json").read_text(encoding="utf-8"))
    assert written == {"verdict": "FAIL"}
    want = load_criteria(CRITERIA_DIR / "order-api-pod-kill.yaml")
    assert crit.sha256 == want.sha256
    out = json.loads(capsys.readouterr().out)
    assert out["schema"] == "papertrade.verdict/v1"
    assert out["verdict"] == "FAIL"
    assert out["reasons"] == ["CHECK_FAILED"]


def test_evaluate_prefers_resolved_then_flag(run_json, monkeypatch):
    calls = fake_pipeline(monkeypatch)
    resolved = load_criteria(CRITERIA_DIR / "order-api-crash-after-commit.yaml")
    write_resolved(resolved, run_json.parent)
    assert cli.main(["evaluate", "--run", str(run_json)]) == EXIT_PASS
    assert calls["evaluate"][1].sha256 == resolved.sha256

    demo = CRITERIA_DIR / "demo-min.yaml"
    argv = ["evaluate", "--run", str(run_json), "--criteria", str(demo)]
    assert cli.main(argv) == EXIT_PASS
    assert calls["evaluate"][1].sha256 == load_criteria(demo).sha256


def test_evaluate_errors_map_to_exit_codes(run_json, monkeypatch):
    def bad_input(ctx, crit, evidence):
        raise InputError("증거 형식 오류")

    fake_pipeline(monkeypatch, evaluate=bad_input)
    assert cli.main(["evaluate", "--run", str(run_json)]) == EXIT_INPUT

    def crash(ctx, crit, evidence):
        raise RuntimeError("버그")

    fake_pipeline(monkeypatch, evaluate=crash)
    assert cli.main(["evaluate", "--run", str(run_json)]) == EXIT_JUDGE_ERROR


def test_pipeline_inner_import_error_is_not_masked(run_json, monkeypatch):
    def broken(name):
        raise ModuleNotFoundError("No module named 'x'", name="orchestrator.judge.db")

    monkeypatch.setattr(cli.importlib, "import_module", broken)
    assert cli.main(["evaluate", "--run", str(run_json)]) == EXIT_JUDGE_ERROR


def test_run_offline_skips_collect(run_json, monkeypatch, no_cluster):
    def no_collect(ctx, crit, tr):
        raise AssertionError("--offline인데 collect를 불렀다")

    calls = fake_pipeline(monkeypatch, collect=no_collect)
    assert cli.main(["run", "--run", str(run_json), "--offline"]) == EXIT_PASS
    assert calls["evaluate"][2] == run_json.parent


def test_run_collects_then_evaluates(run_json, monkeypatch, no_cluster):
    calls = fake_pipeline(monkeypatch, verdict=Outcome.INCONCLUSIVE)
    assert cli.main(["run", "--run", str(run_json)]) == EXIT_INCONCLUSIVE
    assert isinstance(calls["collect"][2], Transports)
    assert calls["evaluate"][2] == run_json.parent / "evidence"


# ---------------------------------------------------------------- mark


def test_mark_writes_marks_local(run_json):
    at = T0 + 300_000
    assert mark(run_json, "inject_start", "--at", str(at)) == EXIT_PASS
    assert local_doc(run_json) == {"run_id": RUN_ID, "marks": {"inject_start": at}}
    ctx = load_run(run_json)
    assert ctx.inject_start == at
    assert "marks.inject_start" in ctx.filled_from_marks_local
    names = sorted(p.name for p in run_json.parent.iterdir())
    assert names == [MARKS_LOCAL_NAME, "run.json"]  # 임시 파일이 남지 않는다


def test_mark_without_at_uses_current_time(run_json, monkeypatch):
    monkeypatch.setattr(cli, "_now_ms", lambda: T0 + 1)
    assert mark(run_json, "inject_start") == EXIT_PASS
    assert load_run(run_json).inject_start == T0 + 1


def test_mark_refuses_overwrite_without_force(run_json, capsys):
    assert mark(run_json, "inject_start", "--at", str(T0)) == EXIT_PASS
    before = local_bytes(run_json)
    assert mark(run_json, "inject_start", "--at", str(T0 + 5)) == EXIT_INPUT
    assert "--force" in capsys.readouterr().err
    assert local_bytes(run_json) == before


def test_mark_same_value_is_noop(run_json, capsys):
    assert mark(run_json, "inject_start", "--at", str(T0)) == EXIT_PASS
    before = local_bytes(run_json)
    assert mark(run_json, "inject_start", "--at", str(T0)) == EXIT_PASS
    assert "변경 없음" in capsys.readouterr().out
    assert local_bytes(run_json) == before


def test_mark_force_overwrites(run_json):
    assert mark(run_json, "inject_start", "--at", str(T0)) == EXIT_PASS
    assert mark(run_json, "inject_start", "--at", str(T0 + 5), "--force") == EXIT_PASS
    assert load_run(run_json).inject_start == T0 + 5


def test_mark_refuses_value_shadowed_by_run_json(tmp_path):
    run = write_run(tmp_path, marks={"inject_start": T0})
    assert mark(run, "inject_start", "--at", str(T0 + 5), "--force") == EXIT_INPUT
    assert local_bytes(run) is None


def test_mark_checks_inject_order(run_json):
    assert mark(run_json, "inject_start", "--at", str(T0 + 10)) == EXIT_PASS
    before = local_bytes(run_json)
    assert mark(run_json, "inject_end", "--at", str(T0)) == EXIT_INPUT
    assert local_bytes(run_json) == before
    assert mark(run_json, "inject_end", "--at", str(T0 + 10)) == EXIT_PASS
    ctx = load_run(run_json)
    assert (ctx.inject_start, ctx.inject_end) == (T0 + 10, T0 + 10)


@pytest.mark.parametrize("at", ["1762300000", "abc", "1.5e12", "-1"])
def test_mark_rejects_non_epoch_ms(run_json, at):
    assert mark(run_json, "inject_start", "--at", at) == EXIT_USAGE
    assert local_bytes(run_json) is None


def test_mark_speed_extra_account_obs(run_json):
    args = ["inject_start", "--at", str(T0), "--speed", "10"]
    args += ["--extra-account", ACCT_EXTRA]
    args += ["--obs", "crash_restart=1", "--obs", "pod_replaced=true"]
    args += ["--obs", "note=manual"]
    assert mark(run_json, *args) == EXIT_PASS
    ctx = load_run(run_json)
    assert ctx.inject_start == T0
    assert ctx.replay_speed == (SpeedPoint(from_ms=T0, speed=Decimal(10)),)
    assert ctx.extra_accounts == (ACCT_EXTRA,)
    assert dict(ctx.obs) == {"crash_restart": 1, "note": "manual", "pod_replaced": True}
    # 같은 계좌를 다시 주면 바뀌지 않는다. user_id는 대소문자를 구분한다(TEXT PK)
    assert mark(run_json, "--extra-account", ACCT_EXTRA) == EXIT_PASS
    assert local_doc(run_json)["extra_accounts"] == [ACCT_EXTRA]
    assert mark(run_json, "--extra-account", ACCT_EXTRA.upper()) == EXIT_PASS
    assert local_doc(run_json)["extra_accounts"] == [ACCT_EXTRA, ACCT_EXTRA.upper()]


def test_mark_speed_points_sorted_and_guarded(run_json):
    assert mark(run_json, "--speed", "1", "--at", str(T0 + 100)) == EXIT_PASS
    assert mark(run_json, "--speed", "2.5", "--at", str(T0)) == EXIT_PASS
    ctx = load_run(run_json)
    assert ctx.replay_speed == (
        SpeedPoint(from_ms=T0, speed=Decimal("2.5")),
        SpeedPoint(from_ms=T0 + 100, speed=Decimal(1)),
    )
    assert (
        local_doc(run_json)["replay_speed"][0]["speed"] == "2.5"
    )  # float로 쓰지 않는다
    assert mark(run_json, "--speed", "10", "--at", str(T0)) == EXIT_INPUT
    assert mark(run_json, "--speed", "10", "--at", str(T0), "--force") == EXIT_PASS
    assert load_run(run_json).replay_speed[0].speed == Decimal(10)


def test_mark_obs_overwrite_needs_force(run_json):
    assert mark(run_json, "--obs", "crash_restart=1") == EXIT_PASS
    assert mark(run_json, "--obs", "crash_restart=1") == EXIT_PASS
    assert mark(run_json, "--obs", "crash_restart=true") == EXIT_INPUT
    assert mark(run_json, "--obs", "crash_restart=2", "--force") == EXIT_PASS
    assert load_run(run_json).obs["crash_restart"] == 2


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--extra-account", "user 0001"],
        ["--extra-account", "x" * 65],
        ["--speed", "0"],
        ["--speed", "nan"],
        ["--obs", "Bad=1"],
        ["--obs", "novalue"],
        ["--obs", "k="],
        ["--obs", "k=1", "--obs", "k=2"],
        ["inject_middle"],
    ],
)
def test_mark_usage_errors(run_json, args):
    assert mark(run_json, *args) == EXIT_USAGE
    assert local_bytes(run_json) is None


def test_mark_missing_run_json_is_input_error(tmp_path):
    assert mark(tmp_path / "run.json", "inject_start", "--at", str(T0)) == EXIT_INPUT


def test_mark_rejects_corrupt_marks_local(run_json):
    (run_json.parent / MARKS_LOCAL_NAME).write_text("{", encoding="utf-8")
    assert mark(run_json, "inject_start", "--at", str(T0)) == EXIT_INPUT


# ---------------------------------------------------------------- transport


def test_psql_argv_prefix_swap():
    tail = ("exec", "-i", "postgres-0", "--")  # DbTarget 기본값에 container가 없다
    assert Transports().psql_argv() == CMD_PREFIX + tail + PSQL_ARGS
    assert Transports.for_local_pg().psql_argv() == TEST_PSQL_PREFIX + PSQL_ARGS
    assert PSQL_ARGS[-6:] == ("-U", "exchange", "-d", "exchange", "-f", "-")
    # run.json db 대상(계약 명세 6). container가 있으면 -c를 붙인다. 비밀번호 인자는 없다
    t = DbTarget(pod="pg-1", container="postgres", user="u1", database="d1")
    argv = Transports().psql_argv(t)
    assert argv == CMD_PREFIX + (
        "exec",
        "-i",
        "pg-1",
        "-c",
        "postgres",
        "--",
    ) + psql_args(t)
    assert "-W" not in argv and not any("password" in a.lower() for a in argv)
    swapped = Transports().with_prefix(("kubectl", "--context", "x", "-n", "p"))
    assert swapped.kubectl_argv("get", "pods")[:3] == ("kubectl", "--context", "x")


def test_transport_uses_runner():
    seen = []

    def runner(argv, *, input=None, timeout):
        seen.append((argv, input, timeout))
        return types.SimpleNamespace(returncode=0, stdout=b"ok", stderr=b"")

    tr = Transports(runner=runner, timeout_s=7)
    tr.kubectl("get", "pods", input=b"x")
    tr.run(["echo"], timeout=3)
    assert seen == [
        (CMD_PREFIX + ("get", "pods"), b"x", 7),
        (("echo",), None, 3),
    ]
    assert tr.exec_argv("k6-pod", "holder", "date", "-u", "+%s") == CMD_PREFIX + (
        "exec",
        "k6-pod",
        "-c",
        "holder",
        "--",
        "date",
        "-u",
        "+%s",
    )


def test_transport_missing_executable():
    with pytest.raises(TransportError):
        Transports().run(["chaos-judge-no-such-binary-for-test"])


# ---------------------------------------------------------------- nearest_rank(설계 5.2 6단계)


@pytest.mark.parametrize("q", [0.95, "0.5", Decimal("0.01"), 1])
def test_nearest_rank_single_sample(q):
    assert nearest_rank([42.5], q) == 42.5


@pytest.mark.parametrize(
    ("n", "q", "want"),
    [
        (20, 0.95, 19),  # ceil(19) = 19
        (21, 0.95, 20),  # ceil(19.95) = 20
        (400, 0.95, 380),
        (100, 0.07, 7),  # float면 0.07 × 100 = 7.000000000000001 → 8
        (4, 0.5, 2),
        (10, 1, 10),
    ],
)
def test_nearest_rank_boundaries(n, q, want):
    values = list(range(n, 0, -1))  # 정렬되지 않은 입력
    assert nearest_rank(values, q) == want


def test_nearest_rank_returns_sample_value():
    xs = [Decimal("3.0"), Decimal("1.5"), Decimal("2.25")]
    assert nearest_rank(xs, Decimal("0.5")) == Decimal("2.25")


@pytest.mark.parametrize(
    ("values", "q"),
    [
        ([1, 2], 0),
        ([1, 2], 1.5),
        ([1, 2], -0.1),
        ([], 0.95),
        ([1.0, float("nan")], 0.5),
        ([Decimal("NaN")], 0.5),
        ([1, 2], float("inf")),
    ],
)
def test_nearest_rank_rejects(values, q):
    with pytest.raises(ValueError):
        nearest_rank(values, q)


def test_nearest_rank_rejects_bool_q():
    with pytest.raises(TypeError):
        nearest_rank([1], True)


# ---------------------------------------------------------------- achieved_ratio(설계 4.5, 5.5)


def steady_plan(rps=30, duration_s=600):
    target = rps * 60
    return {
        "executor": "ramping-arrival-rate",
        "time_unit_s": 60,
        "start_rate": 0,
        "stages": [
            {"duration_s": 0, "target": target},
            {"duration_s": duration_s, "target": target},
        ],
    }


def intents_at(times):
    return [make_intent(f"k{i}", ACCT_A, t=t) for i, t in enumerate(times)]


def test_planned_arrivals_steady():
    plan = steady_plan()
    assert planned_arrivals(plan, origin_ms=T0) == 18000
    w = (T0 + 60_000, T0 + 70_000)
    assert planned_arrivals(plan, origin_ms=T0, window=w) == 300
    before = (T0 - 10_000, T0 + 10_000)  # 계획 밖은 0
    assert planned_arrivals(plan, origin_ms=T0, window=before) == 300


def test_planned_arrivals_ramp_and_jump():
    ramp = {
        "executor": "ramping-arrival-rate",
        "time_unit_s": 60,
        "start_rate": 0,
        "stages": [{"duration_s": 60, "target": 60}],  # 0 → 1/s
    }
    assert planned_arrivals(ramp, origin_ms=T0) == 30
    half = (T0, T0 + 30_000)
    assert planned_arrivals(ramp, origin_ms=T0, window=half) == Fraction(15, 2)

    spike = {
        "executor": "ramping-arrival-rate",
        "time_unit_s": 60,
        "start_rate": 0,
        "stages": [
            {"duration_s": 0, "target": 1800},
            {"duration_s": 10, "target": 1800},
            {"duration_s": 0, "target": 18000},  # 0s 점프
            {"duration_s": 10, "target": 18000},
        ],
    }
    assert planned_arrivals(spike, origin_ms=T0) == 300 + 3000


def test_planned_arrivals_accepts_decimal_plan():
    plan = steady_plan()
    plan["time_unit_s"] = Decimal("60")
    plan["stages"] = [{"duration_s": Decimal("0"), "target": Decimal("1800")}] + [
        {"duration_s": Decimal("600"), "target": Decimal("1800")}
    ]
    assert planned_arrivals(plan, origin_ms=T0) == 18000


def test_achieved_ratio_window_and_floor():
    plan = steady_plan()
    w = (T0 + 60_000, T0 + 70_000)
    start = T0 + 60_000
    ok = intents_at([start + i * 30 for i in range(294)])
    r = achieved_ratio(ok, plan, origin_ms=T0, window=w)
    assert r == Decimal("0.98")
    assert str(r) == "0.980000"
    low = intents_at([start + i * 30 for i in range(293)])
    assert str(achieved_ratio(low, plan, origin_ms=T0, window=w)) == "0.976666"  # 내림


def test_achieved_ratio_half_open_window_and_mapping():
    plan = steady_plan()
    w = (T0 + 60_000, T0 + 70_000)
    its = intents_at([T0 + 59_999, T0 + 60_000, T0 + 69_999, T0 + 70_000])
    by_key = {it.key: it for it in its}
    r = achieved_ratio(by_key, plan, origin_ms=T0, window=w)
    assert str(r) == "0.006666"  # 2 ÷ 300. 앞 끝 포함, 뒤 끝 제외
    whole = achieved_ratio(its, plan, origin_ms=T0)
    assert str(whole) == "0.000222"  # 4 ÷ 18000


def test_achieved_ratio_none_without_plan_in_window():
    plan = steady_plan()
    late = (T0 + 600_000, T0 + 700_000)
    its = intents_at([T0 + 650_000])
    assert achieved_ratio(its, plan, origin_ms=T0, window=late) is None


def test_achieved_ratio_rejects_reversed_window():
    with pytest.raises(ValueError):
        achieved_ratio([], steady_plan(), origin_ms=T0, window=(T0 + 10, T0))


@pytest.mark.parametrize(
    "patch",
    [
        {"executor": "constant-arrival-rate"},
        {"time_unit_s": 0},
        {"start_rate": -1},
        {"stages": "x"},
        {"stages": [{"duration_s": 10}]},
        {"stages": [{"duration_s": -1, "target": 10}]},
        {"stages": [{"duration_s": True, "target": 10}]},
        {"time_unit_s": "abc"},
    ],
)
def test_achieved_ratio_rejects_bad_plan(patch):
    plan = {**steady_plan(), **patch}
    with pytest.raises(InputError):
        achieved_ratio([], plan, origin_ms=T0)


def test_achieved_ratio_rejects_missing_plan_key():
    plan = steady_plan()
    del plan["start_rate"]
    with pytest.raises(InputError):
        achieved_ratio([], plan, origin_ms=T0)

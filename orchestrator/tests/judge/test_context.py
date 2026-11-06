"""run.json 검증과 marks.local.json 병합(설계 1.4)."""

import hashlib
import json
from decimal import Decimal

import pytest

from orchestrator.judge.context import check_user_id, load_run
from orchestrator.judge.errors import EXIT_INPUT, InputError
from orchestrator.judge.model import DbTarget

T = 1_762_300_000_000


def base_run(**over):
    doc = {
        "schema": "chaos.run/v1",
        "run_id": "r1117-crash",
        "experiment": "order-api-crash-after-commit",
        "criteria": "../../experiments/criteria/order-api-crash-after-commit.yaml",
        "scenario": "scenario.yaml",
        "k6": {"job": "k6-r1117-crash"},
        "marks": {"inject_start": T + 300_000, "inject_end": None},
        "faults": [
            {
                "kind": "crash_after_commit",
                "target": "order-api",
                "start_ms": T + 300_000,
            }
        ],
        "replay_speed": [{"from_ms": T, "speed": 10}],
        "extra_accounts": [],
    }
    doc.update(over)
    return doc


def write(tmp_path, doc, name="run.json"):
    p = tmp_path / name
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


def test_load_valid(tmp_path):
    p = write(tmp_path, base_run())
    ctx = load_run(p)
    assert ctx.run_id == "r1117-crash"
    assert ctx.experiment == "order-api-crash-after-commit"
    assert (
        ctx.criteria
        == tmp_path / "../../experiments/criteria/order-api-crash-after-commit.yaml"
    )
    assert ctx.scenario == tmp_path / "scenario.yaml"
    assert ctx.k6_job == "k6-r1117-crash"
    assert ctx.inject_start == T + 300_000
    assert ctx.inject_end is None
    assert ctx.faults[0].kind == "crash_after_commit"
    assert ctx.faults[0].start_ms == T + 300_000
    assert ctx.replay_speed[0].speed == Decimal(10)
    assert ctx.run_json_sha256 == hashlib.sha256(p.read_bytes()).hexdigest()
    assert ctx.marks_local_sha256 is None
    assert ctx.filled_from_marks_local == ()


def test_optional_keys_default(tmp_path):
    doc = {k: base_run()[k] for k in ("schema", "run_id", "experiment", "criteria")}
    ctx = load_run(write(tmp_path, doc))
    assert ctx.scenario is None
    assert ctx.k6_job == "k6-r1117-crash"
    assert ctx.marks == {}
    assert ctx.faults == ()
    assert ctx.replay_speed == ()
    assert ctx.extra_accounts == ()
    assert ctx.db == DbTarget()
    assert ctx.key_prefix == "k6-r1117-crash-"


def test_absolute_paths_kept(tmp_path):
    crit = tmp_path / "abs.yaml"
    ctx = load_run(write(tmp_path, base_run(criteria=str(crit))))
    assert ctx.criteria == crit


@pytest.mark.parametrize(
    "run_id", ["ab", "R1117", "r_1117", "a" * 41, "r1117 crash", "abc\n", "", 123, None]
)
def test_bad_run_id(tmp_path, run_id):
    with pytest.raises(InputError) as e:
        load_run(write(tmp_path, base_run(run_id=run_id)))
    assert e.value.exit_code == EXIT_INPUT


@pytest.mark.parametrize("run_id", ["abc", "a" * 40, "r1117-crash-2", "0-0"])
def test_good_run_id(tmp_path, run_id):
    assert load_run(write(tmp_path, base_run(run_id=run_id))).run_id == run_id


@pytest.mark.parametrize(
    "over",
    [
        {"schema": "chaos.run/v2"},
        {"experiment": ""},
        {"criteria": None},
        {"surprise": 1},
        {"k6": {"job": "x", "image": "y"}},
        {"k6": "k6-x"},
        {"marks": {"inject_start": "soon"}},
        {"marks": {"inject_start": True}},
        {"marks": {"inject_start": 1_762_300_000}},  # 초 단위 실수
        {"marks": {"inject_start": T + 10, "inject_end": T}},
        {"marks": {"Bad-Name": T}},
        {"marks": {"inject_start": 1.7623e12}},
        {"faults": [{"kind": "pod_kill", "target": "order-api"}]},
        {"faults": {"kind": "pod_kill"}},
        {
            "faults": [
                {"kind": "pod_kill", "target": "x", "start_ms": T, "end_ms": T - 1}
            ]
        },
        {"replay_speed": [{"from_ms": T, "speed": 0}]},
        {"replay_speed": [{"from_ms": T, "speed": -1}]},
        {"replay_speed": [{"from_ms": T}]},
        {"replay_speed": [{"from_ms": T, "speed": 1}, {"from_ms": T, "speed": 10}]},
        {"extra_accounts": ["user 0001"]},
        {"extra_accounts": [""]},
        {"extra_accounts": [5]},
        {"extra_accounts": ["-user"]},
        {"extra_accounts": ["u" * 65]},
        {"extra_accounts": ["user-0001\n"]},
        {"extra_accounts": "user-0999"},
        {"db": "postgres-0"},
        {"db": {"host": "db.internal"}},
        {"db": {"password": "x"}},
        {"db": {"pod": "Postgres_0"}},
        {"db": {"pod": "-postgres"}},
        {"db": {"container": ""}},
        {"db": {"container": "pg.main"}},
        {"db": {"user": "-c"}},
        {"db": {"user": "ex change"}},
        {"db": {"database": 5}},
    ],
)
def test_invalid_fields(tmp_path, over):
    with pytest.raises(InputError):
        load_run(write(tmp_path, base_run(**over)))


@pytest.mark.parametrize("missing", ["schema", "run_id", "experiment", "criteria"])
def test_missing_required(tmp_path, missing):
    doc = base_run()
    del doc[missing]
    with pytest.raises(InputError, match=missing):
        load_run(write(tmp_path, doc))


def test_not_json_or_not_object(tmp_path):
    p = tmp_path / "run.json"
    p.write_text("{broken", encoding="utf-8")
    with pytest.raises(InputError):
        load_run(p)
    p.write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(InputError):
        load_run(p)
    with pytest.raises(InputError):
        load_run(tmp_path / "missing.json")


def test_fault_extra_keys_kept(tmp_path):
    f = {"kind": "pod_kill", "target": "order-api", "start_ms": T, "pod": "order-api-0"}
    ctx = load_run(write(tmp_path, base_run(faults=[f])))
    assert ctx.faults[0].extra == {"pod": "order-api-0"}
    assert ctx.faults[0].end_ms is None


def test_extra_accounts_are_user_ids(tmp_path):
    """발표자 계좌는 user_id다(계약 명세 8). 순서를 지키고 중복은 뺀다."""
    accts = ["user-0999", "user-0001", "user-0999"]
    ctx = load_run(write(tmp_path, base_run(extra_accounts=accts)))
    assert ctx.extra_accounts == ("user-0999", "user-0001")


def test_check_user_id():
    assert check_user_id("user-0001", "x") == "user-0001"
    assert check_user_id("a" * 64, "x") == "a" * 64
    with pytest.raises(InputError, match="--extra-account"):
        check_user_id("not ok", "--extra-account")


def test_db_target(tmp_path):
    """psql 대상(계약 명세 6). 빠진 키·null은 기본값이다."""
    db = {"pod": "postgres-1", "container": "postgres", "user": "app_ro"}
    ctx = load_run(write(tmp_path, base_run(db=db)))
    assert ctx.db == DbTarget(
        pod="postgres-1", container="postgres", user="app_ro", database="exchange"
    )
    ctx = load_run(write(tmp_path, base_run(db={"container": None})))
    assert ctx.db == DbTarget()
    ctx = load_run(write(tmp_path, base_run(db=None)))
    assert ctx.db == DbTarget()


def test_db_password_rejected_with_reason(tmp_path):
    with pytest.raises(InputError, match="비밀번호"):
        load_run(write(tmp_path, base_run(db={"password": "x"})))


# ---------------------------------------------------------------- marks.local.json


def test_marks_local_fills_only_blanks(tmp_path):
    p = write(tmp_path, base_run(replay_speed=[], obs={"speed_source": "pod"}))
    local = {
        "run_id": "r1117-crash",
        "marks": {
            "inject_start": T + 1,
            "inject_end": T + 400_000,
            "k6_end": T + 600_000,
        },
        "replay_speed": [{"from_ms": T + 5, "speed": 1}],
        "extra_accounts": ["user-0999"],
        "obs": {"speed_source": "mark", "note": "manual"},
    }
    write(tmp_path, local, "marks.local.json")
    ctx = load_run(p)
    assert ctx.inject_start == T + 300_000  # run.json 우선
    assert ctx.inject_end == T + 400_000  # null이라 채움
    assert ctx.marks["k6_end"] == T + 600_000  # 없던 키 채움
    assert ctx.replay_speed[0].from_ms == T + 5  # 빈 목록이라 채움
    assert ctx.extra_accounts == ("user-0999",)
    assert ctx.obs == {"note": "manual", "speed_source": "pod"}
    assert ctx.filled_from_marks_local == (
        "marks.inject_end",
        "marks.k6_end",
        "obs.note",
        "replay_speed",
        "extra_accounts",
    )
    assert ctx.marks_local_sha256 is not None
    # run.json 해시는 run.json 바이트만 본다
    assert ctx.run_json_sha256 == hashlib.sha256(p.read_bytes()).hexdigest()


def test_marks_local_does_not_override_lists(tmp_path):
    p = write(tmp_path, base_run())
    write(
        tmp_path,
        {"replay_speed": [{"from_ms": T + 5, "speed": 1}], "faults": []},
        "marks.local.json",
    )
    ctx = load_run(p)
    assert ctx.replay_speed[0].speed == Decimal(10)
    assert len(ctx.faults) == 1
    assert ctx.filled_from_marks_local == ()


def test_marks_local_run_id_mismatch(tmp_path):
    p = write(tmp_path, base_run())
    write(tmp_path, {"run_id": "other-run"}, "marks.local.json")
    with pytest.raises(InputError, match="run_id"):
        load_run(p)


@pytest.mark.parametrize("key", ["criteria", "db"])
def test_marks_local_unknown_key(tmp_path, key):
    p = write(tmp_path, base_run())
    write(tmp_path, {key: "x"}, "marks.local.json")
    with pytest.raises(InputError, match="모르는 키"):
        load_run(p)


def test_marks_local_values_validated(tmp_path):
    p = write(tmp_path, base_run())
    write(tmp_path, {"marks": {"inject_end": "later"}}, "marks.local.json")
    with pytest.raises(InputError):
        load_run(p)


def test_reads_run_json_written_by_runner(tmp_path):
    """A의 runner(orchestrator.runjson.RunFile, PR #5)가 쓴 run.json을 그대로 읽는다(설계 1.4)."""
    from orchestrator.runjson import RunFile

    rf = RunFile(tmp_path / "runs", "r-abc123")
    rf.create("r-abc123", {"name": "pod-kill"}, scenario_path="scenario.yaml")
    rf.mark("inject_start", 1762300000000)
    rf.add_fault("pod-kill", "order", 1762300000100)
    ctx = load_run(rf.path)
    assert (ctx.run_id, ctx.experiment) == ("r-abc123", "pod-kill")
    assert ctx.marks["inject_start"] == 1762300000000
    assert (
        ctx.criteria.name == "default.yaml"
    )  # runner 기본값 ../../experiments/criteria/default.yaml
    assert (ctx.db.pod, ctx.db.user, ctx.db.database) == (
        "postgres-0",
        "exchange",
        "exchange",
    )

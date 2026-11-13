"""judge fetch·stop(설계 4.8). 가짜 kubectl로 돈다(클러스터 없음).

원격 /out 내용은 오프라인 fixture(run_ok/k6)를 쓴다. fetch가 쓴 폴더를
pipeline.load_evidence로 다시 읽어 판정이 그대로 받아들이는지까지 본다.
"""

import gzip
import json
import subprocess
from pathlib import Path

import pytest

import orchestrator.judge.__main__ as cli
from orchestrator.judge import fetch as F
from orchestrator.judge.errors import EXIT_JUDGE_ERROR, EXIT_PASS, JudgeError
from orchestrator.judge.model import RunContext, sha256_bytes
from orchestrator.judge.pipeline import load_evidence
from orchestrator.judge.transport import Transports, TransportError

from .test_cli import write_run

K6FIX = Path(__file__).parent / "fixtures" / "run_ok" / "k6"
ORDERS = (K6FIX / "orders.jsonl").read_bytes()
SUMMARY = (K6FIX / "summary.json").read_bytes()
SHA_TEXT = (K6FIX / "sha256sum.txt").read_text(encoding="utf-8")
BAD_SHA_TEXT = SHA_TEXT.replace(sha256_bytes(ORDERS), "0" * 64)
JOB = "k6-r-offline-ok"
POD = "k6-r-offline-ok-x7k2p"
NODE_S = 1_762_300_100
RUNNING = {"running": {"startedAt": "2026-11-13T00:00:00Z"}}


def done(code=0, reason="Completed"):
    return {"terminated": {"exitCode": code, "reason": reason}}


def pod_doc(**states):
    statuses = [{"name": n, "state": s} for n, s in states.items()]
    return {"status": {"containerStatuses": statuses}}


def ok(out=b""):
    if isinstance(out, (dict, list)):
        out = json.dumps(out)
    if isinstance(out, str):
        out = out.encode("utf-8")
    return subprocess.CompletedProcess((), 0, out, b"")


def fail(err="error: unable to upgrade connection"):
    return subprocess.CompletedProcess((), 1, b"", err.encode("utf-8"))


def kind(argv):
    s = " ".join(argv)
    if " get pods " in s:
        return "pods"
    if " get pod " in s:
        return "pod"
    if "sha256sum" in s:
        return "sha"
    if "gzip -c" in s:
        return "gzip " + argv[-1].rsplit("/", 1)[-1]
    for k in ("date", "touch", "kill"):
        if k in argv:
            return k
    raise AssertionError(f"예상하지 못한 명령: {argv}")


class FakeKubectl:
    """명령 종류별 응답. 값이 list면 차례로 꺼내고, 마지막 것은 계속 쓴다."""

    def __init__(self, over=None):
        self.calls = []
        self.resp = {
            "pods": ok({"items": [{"metadata": {"name": POD}}]}),
            "pod": ok(pod_doc(k6=done(), holder=RUNNING)),
            "sha": ok(SHA_TEXT),
            "gzip orders.jsonl": ok(gzip.compress(ORDERS)),
            "gzip summary.json": ok(gzip.compress(SUMMARY)),
            "date": ok(f"{NODE_S}\n"),
            "touch": ok(),
            "kill": ok(),
            **(over or {}),
        }

    def __call__(self, argv, *, input=None, timeout):
        argv = tuple(argv)
        self.calls.append(argv)
        r = self.resp[kind(argv)]
        if isinstance(r, list):
            r = r.pop(0) if len(r) > 1 else r[0]
        return r

    def of(self, k):
        return [c for c in self.calls if kind(c) == k]


def tr(fake):
    return Transports(runner=fake)


def no_sleep(_s):
    pass


@pytest.fixture
def ctx():
    return RunContext(
        run_id="r-offline-ok",
        experiment="offline-ok",
        criteria=Path("criteria.yaml"),
        scenario=None,
        k6_job=JOB,
    )


def fetch(ctx, fake, run_dir):
    return F.fetch(ctx, tr(fake), run_dir, sleep=no_sleep)


# ---------------------------------------------------------------- 1단계: Pod 찾기·종료 대기


def test_find_pod_selects_by_job_label(ctx):
    fake = FakeKubectl()
    assert F.find_pod(ctx, tr(fake)) == POD
    sel = f"batch.kubernetes.io/job-name={JOB}"
    assert fake.calls == [
        ("kubectl", "-n", "exchange", "get", "pods", "-l", sel, "-o", "json")
    ]


@pytest.mark.parametrize(
    ("names", "msg"), [([], "Pod가 없다"), (["k6-a", "k6-b"], "재실행 흔적")]
)
def test_find_pod_needs_exactly_one(ctx, names, msg):
    items = [{"metadata": {"name": n}} for n in names]
    fake = FakeKubectl({"pods": ok({"items": items})})
    with pytest.raises(JudgeError, match=msg):
        F.find_pod(ctx, tr(fake))


def test_wait_terminated_polls_until_k6_ends(ctx):
    running = ok(pod_doc(k6=RUNNING, holder=RUNNING))
    oom = ok(pod_doc(k6=done(137, "OOMKilled"), holder=RUNNING))
    fake = FakeKubectl({"pod": [running, running, oom]})
    slept = []
    st = F.wait_terminated(ctx, tr(fake), POD, sleep=slept.append)
    assert st.containers == {"k6": {"exitCode": 137, "reason": "OOMKilled"}}
    assert slept == [F.POLL_S, F.POLL_S]


def test_wait_terminated_gives_up_after_timeout(ctx):
    fake = FakeKubectl({"pod": ok(pod_doc(k6=RUNNING, holder=RUNNING))})
    now = iter(range(0, 1000, 5))
    with pytest.raises(JudgeError, match="k6 컨테이너가 끝나지 않았다"):
        F.wait_terminated(
            ctx, tr(fake), POD, timeout_s=10, sleep=no_sleep, clock=lambda: next(now)
        )


# ---------------------------------------------------------------- 2–4단계 부품


def test_remote_sha256_lets_pod_shell_expand_glob():
    fake = FakeKubectl()
    text, sums = F.remote_sha256(tr(fake), POD)
    assert fake.calls[-1][-6:] == ("-c", "holder", "--", "sh", "-c", "sha256sum /out/*")
    assert text == SHA_TEXT
    assert sums["orders.jsonl"] == sha256_bytes(ORDERS)
    assert set(sums) == {"orders.jsonl", "summary.json"}


def test_copy_gz_rejects_truncated_stream():
    cut = gzip.compress(ORDERS)[:-10]
    fake = FakeKubectl({"gzip orders.jsonl": ok(cut)})
    with pytest.raises(TransportError, match="풀 수 없다"):
        F.copy_gz(tr(fake), POD, "orders.jsonl")


def test_measure_clock_centers_the_node_second():
    t = iter([NODE_S * 1000, NODE_S * 1000 + 40])
    c = F.measure_clock(tr(FakeKubectl()), POD, now_ms=lambda: next(t))
    # 노드 초 + 500ms − RTT 중앙(+20ms)
    assert c == F.ClockOffset(offset_ms=480, rtt_ms=40)


# ---------------------------------------------------------------- fetch 전체


def test_fetch_writes_evidence_that_evaluate_reads(ctx, tmp_path):
    fake = FakeKubectl()
    res = fetch(ctx, fake, tmp_path)
    assert (res.sha_ok, res.attempts, res.errors) == (True, 1, ())
    names = sorted(p.name for p in (tmp_path / "k6").iterdir())
    assert names == [
        "derived.json",
        "orders.jsonl.gz",
        "pod-status.json",
        "sha256sum.txt",
        "summary.json",
    ]
    ev = load_evidence(tmp_path)
    assert ev.log is not None
    assert not [p for p in ev.log.problems if p.startswith("sha_mismatch")]
    assert ev.remote_sha == sha256_bytes(ORDERS)
    assert (ev.k6_exit_code, ev.k6_exit_reason) == (0, "Completed")
    assert ev.summary is not None
    assert ev.clock_offset_ms == res.clock.offset_ms
    assert not fake.of("touch")  # RELEASE는 collect가 DB 수집 뒤에 한다
    pod_status = json.loads((tmp_path / "k6" / "pod-status.json").read_text())
    assert pod_status["stop_requested"] is False


def test_fetch_records_stop_flag_from_sha_list(ctx, tmp_path):
    flag = f"{sha256_bytes(b'')}  /out/STOP_REQUESTED\n"
    res = fetch(ctx, FakeKubectl({"sha": ok(SHA_TEXT + flag)}), tmp_path)
    assert res.sha_ok and res.stop_requested
    pod_status = json.loads((tmp_path / "k6" / "pod-status.json").read_text())
    assert pod_status["stop_requested"] is True
    assert load_evidence(tmp_path).k6_stop_requested  # 판정이 105를 인정하는 근거


def test_fetch_retries_until_sha_matches(ctx, tmp_path):
    fake = FakeKubectl({"sha": [ok(BAD_SHA_TEXT), ok(SHA_TEXT)]})
    res = fetch(ctx, fake, tmp_path)
    assert (res.sha_ok, res.attempts) == (True, 2)
    assert len(fake.of("gzip orders.jsonl")) == 2
    assert (tmp_path / "k6" / "sha256sum.txt").read_text(encoding="utf-8") == SHA_TEXT


def test_fetch_keeps_last_copy_when_sha_never_matches(ctx, tmp_path):
    res = fetch(ctx, FakeKubectl({"sha": ok(BAD_SHA_TEXT)}), tmp_path)
    assert (res.sha_ok, res.attempts) == (False, F.FETCH_ATTEMPTS)
    ev = load_evidence(tmp_path)  # 판정은 sha_mismatch로 기록 불완전을 낸다
    assert ev.log is not None
    assert any(p.startswith("sha_mismatch") for p in ev.log.problems)


def test_fetch_without_log_leaves_no_file(ctx, tmp_path):
    res = fetch(ctx, FakeKubectl({"gzip orders.jsonl": fail()}), tmp_path)
    assert (res.sha_ok, dict(res.files)) == (False, {})
    assert len(res.errors) == F.FETCH_ATTEMPTS
    assert not (tmp_path / "k6" / "orders.jsonl.gz").exists()
    assert not (tmp_path / "k6" / "sha256sum.txt").exists()
    assert load_evidence(tmp_path).log is None  # → SENT_LOG_MISSING


def test_fetch_without_clock_skips_derived(ctx, tmp_path):
    res = fetch(ctx, FakeKubectl({"date": fail()}), tmp_path)
    assert res.sha_ok and res.clock is None
    assert res.errors and res.errors[-1].startswith("시계:")
    assert not (tmp_path / "k6" / "derived.json").exists()
    assert load_evidence(tmp_path).clock_offset_ms is None


# ---------------------------------------------------------------- release·stop·CLI


def test_release_touches_flag_in_holder():
    fake = FakeKubectl()
    F.release(tr(fake), POD)
    argv = ("kubectl", "-n", "exchange", "exec", POD, "-c", "holder", "--")
    assert fake.calls == [(*argv, "touch", "/out/RELEASE")]


@pytest.mark.parametrize(
    ("ws", "containers"), [(False, ["k6"]), (True, ["k6", "k6-ws"])]
)
def test_stop_sends_sigint(ctx, ws, containers):
    fake = FakeKubectl()
    F.stop(ctx, tr(fake), ws=ws)
    execs = [c for c in fake.calls if "exec" in c]
    # 정지 표시가 먼저, SIGINT가 나중(k6load.K6JobLoad.stop과 같은 순서)
    assert execs[0][-4:] == ("holder", "--", "touch", "/out/STOP_REQUESTED")
    assert [c[c.index("-c") + 1] for c in execs[1:]] == containers
    assert all(c[-3:] == ("kill", "-INT", "1") for c in execs[1:])


@pytest.mark.parametrize(
    ("over", "code"),
    [({}, EXIT_PASS), ({"gzip orders.jsonl": fail()}, EXIT_JUDGE_ERROR)],
)
def test_cli_fetch_exit_code(tmp_path, monkeypatch, capsys, over, code):
    run = write_run(tmp_path)
    fake = FakeKubectl(over)
    monkeypatch.setattr(cli, "_transports", lambda: Transports(runner=fake))
    assert cli.main(["fetch", "--run", str(run)]) == code
    assert json.loads(capsys.readouterr().out)["sha_ok"] is (code == EXIT_PASS)

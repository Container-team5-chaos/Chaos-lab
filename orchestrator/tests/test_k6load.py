"""K6JobLoad 단위 테스트 (클러스터 없이 가짜 kubectl로 검증)."""

import pytest
import yaml

from orchestrator import main as main_mod
from orchestrator.k6load import (
    K6JobLoad,
    LoadStartError,
    PLACEHOLDER_RE,
    load_settings,
    render_job,
)
from orchestrator.observe import MockLoad

RUN_ID = "pod-kill-001"
POD = "k6-pod-kill-001-abc"


class FakeKubectl:
    """kubectl 흉내. jsonl은 holder exec로 읽히는 /out/orders.jsonl 내용(호출마다 다음 값)."""

    def __init__(self, jsonl=("",), job_exists=False, pod=POD):
        self.calls: list[list[str]] = []
        self.jsonl = list(jsonl)
        self.job_exists = job_exists
        self.pod = pod
        self.created: list[str] = []

    def __call__(self, args, stdin=None):
        self.calls.append(list(args))
        assert args[:2] == ["--namespace", "exchange"]
        a = args[2:]
        if a[:2] == ["get", "job"]:
            return f"job.batch/{a[2]}" if self.job_exists else ""
        if a[:2] == ["get", "pods"]:
            return self.pod or ""
        if a[:2] == ["create", "configmap"]:
            return "kind: ConfigMap"
        if a[:3] == ["create", "-f", "-"]:
            self.created.append(stdin)
            self.job_exists = True
            return ""
        if a[0] == "exec" and "holder" in a:
            return self.jsonl.pop(0) if len(self.jsonl) > 1 else self.jsonl[0]
        return ""

    def verbs(self):
        return [c[2] for c in self.calls]


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def make(k, clock=None):
    clock = clock or FakeClock()
    return K6JobLoad("exchange", kubectl=k, sleep=clock.sleep, clock=clock.now)


RUN_START = '{"type":"run_start","v":2}\n'
INTENT = RUN_START + '{"type":"intent","key":"k6-pod-kill-001-1-1"}\n'


def test_load_defaults():
    assert load_settings({}) == {
        "profile": "steady",
        "rate": 30,
        "duration_s": 600,
        "accounts": 300,
        "start_timeout_s": 90,
    }
    assert load_settings({"load": {"profile": "demo", "rate": 2.5}})["rate"] == 2.5


@pytest.mark.parametrize(
    "block",
    [
        {"profile": "step"},
        {"profile": "replay"},
        {"profile": "nope"},
        {"rate": 0},
        {"accounts": 1001},
        {"duration_s": "600"},
        {"start_timeout_s": True},
        {"unknown": 1},
    ],
)
def test_load_validation(block):
    with pytest.raises(ValueError):
        load_settings({"load": block})


def test_rendered_job_has_no_placeholders_and_name_labels():
    text = render_job(RUN_ID, {"load": {"profile": "demo", "duration_s": 900}})
    body = "".join(
        ln for ln in text.splitlines(True) if not ln.lstrip().startswith("#")
    )
    assert not PLACEHOLDER_RE.search(body)
    job = yaml.safe_load(text)
    assert job["metadata"]["name"] == f"k6-{RUN_ID}"
    assert job["metadata"]["labels"]["app.kubernetes.io/name"] == "k6"
    assert (
        job["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/name"] == "k6"
    )
    env = {
        e["name"]: e["value"]
        for e in job["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["PROFILE"] == "demo" and env["DURATION_S"] == "900"
    assert env["RUN_ID"] == RUN_ID and env["USER_BASE"] == ""
    assert job["spec"]["activeDeadlineSeconds"] == 180 + 900 + 60 + 600 + 120


def test_render_rejects_bad_run_id():
    with pytest.raises(ValueError):
        render_job("Bad_ID", {})


def test_start_waits_for_first_intent_not_run_start():
    k = FakeKubectl(jsonl=["", RUN_START, RUN_START, INTENT])
    clock = FakeClock()
    make(k, clock).start(RUN_ID, {})
    assert len(k.created) == 1
    holder_reads = [c for c in k.calls if c[2] == "exec"]
    assert len(holder_reads) == 4
    assert clock.t == pytest.approx(6.0)


def test_start_timeout_raises():
    k = FakeKubectl(jsonl=[RUN_START])
    clock = FakeClock()
    with pytest.raises(LoadStartError):
        make(k, clock).start(RUN_ID, {"load": {"start_timeout_s": 10}})
    assert 10 <= clock.t <= 12


def test_start_is_idempotent_for_same_run_id():
    k = FakeKubectl(jsonl=[INTENT])
    load = make(k)
    load.start(RUN_ID, {})
    load.start(RUN_ID, {})
    assert len(k.created) == 1
    k2 = FakeKubectl(jsonl=[INTENT], job_exists=True)
    make(k2).start(RUN_ID, {})
    assert k2.created == [] and "apply" not in k2.verbs()


def test_stop_sends_int_and_never_deletes():
    k = FakeKubectl(jsonl=[INTENT])
    load = make(k)
    load.start(RUN_ID, {})
    load.stop(RUN_ID)
    load.stop(RUN_ID)
    kills = [c for c in k.calls if c[-3:] == ["kill", "-INT", "1"]]
    assert kills == [
        ["--namespace", "exchange", "exec", POD, "-c", "k6", "--", "kill", "-INT", "1"]
    ]
    assert not any("delete" in c for c in k.calls)


def test_stop_before_start_is_noop():
    k = FakeKubectl(pod=None)
    make(k).stop(RUN_ID)
    assert not any(c[2] == "exec" for c in k.calls)

    def boom(args, stdin=None):
        raise RuntimeError("no cluster")

    make(boom).stop(RUN_ID)  # 예외 없이 끝난다


def test_main_build_real_uses_k6_and_mock_keeps_mockload(monkeypatch):
    import orchestrator.inject as inject

    monkeypatch.setattr(inject, "ChaosMeshInjector", lambda ns: object())
    _, load, _ = main_mod._build(False, "exchange")
    assert isinstance(load, K6JobLoad) and load.namespace == "exchange"
    _, load, _ = main_mod._build(True, "exchange")
    assert isinstance(load, MockLoad)

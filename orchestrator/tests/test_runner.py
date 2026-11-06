import pytest

from orchestrator.cleanup import recover
from orchestrator.inject import MockInjector, chaos_name
from orchestrator.judge import MockJudge
from orchestrator.observe import MockLoad
from orchestrator.runner import Runner
from orchestrator.store import StateStore

SCENARIO = {
    "name": "pod-kill",
    "durations": {"warmup": 1, "observe": 1, "recover": 1},
    "faults": [{"kind": "PodChaos", "spec": {"action": "pod-kill"}}],
}


class Crash(BaseException):
    """프로세스가 죽는 상황 흉내 (try/except Exception에 안 잡힘)."""


def make(tmp_path, injector=None, sleep=lambda s: None):
    store = StateStore(tmp_path / "state.json")
    inj = injector or MockInjector()
    return store, inj, Runner(store, inj, MockLoad(), MockJudge(), sleep=sleep)


def test_normal_run_reaches_done(tmp_path):
    store, inj, runner = make(tmp_path)
    state = runner.run(SCENARIO, exp_id="e1")
    assert state["phase"] == "DONE"
    assert store.load()["phase"] == "DONE"
    assert inj.resources == {}  # 끝나면 장애 리소스 정리
    phases = [h["phase"] for h in state["history"]]
    assert phases == [
        "PRECHECK",
        "WARMUP",
        "INJECTING",
        "OBSERVING",
        "RECOVERING",
        "JUDGING",
        "DONE",
    ]


def test_chaos_name_is_deterministic():
    assert chaos_name("e1", 0) == chaos_name("e1", 0)
    assert chaos_name("E_1", 0) == "e-1-s0"


def test_apply_twice_creates_one(tmp_path):
    inj = MockInjector()
    inj.apply("e1", 0, {})
    inj.apply("e1", 0, {})
    assert len(inj.resources) == 1


def test_restart_mid_observe_aborts_without_reinjecting(tmp_path):
    """완료 기준: 실행 중 Pod 삭제 → 재시작 후 ABORTED, PodChaos 중복 0."""
    inj = MockInjector()  # 클러스터는 Pod가 죽어도 남아 있으므로 같은 인스턴스 공유

    calls = []

    def die_on_observe(seconds):
        calls.append(seconds)
        if len(calls) == 2:  # warmup 다음 sleep = OBSERVING 중
            raise Crash()

    store, _, runner = make(tmp_path, inj, sleep=die_on_observe)
    # 프로세스 강제 종료를 흉내: Runner의 except가 돌기 전에 죽은 것처럼 _abort를 막는다
    runner._abort = lambda reason: None
    with pytest.raises(Crash):
        runner.run(SCENARIO, exp_id="e1")
    assert store.load()["phase"] == "OBSERVING"
    assert len(inj.resources) == 1
    creates_before = inj.create_calls

    # 새 Pod 기동
    state = recover(StateStore(tmp_path / "state.json"), inj)
    assert state["phase"] == "ABORTED"
    assert state["aborted_from"] == "OBSERVING"
    assert inj.resources == {}  # 남은 Chaos 정리
    assert inj.create_calls == creates_before  # 복구 중 새 주입 없음


def test_exception_in_process_aborts_and_cleans(tmp_path):
    calls = []

    def boom(seconds):
        calls.append(seconds)
        if len(calls) == 2:
            raise RuntimeError("k6 died")

    store, inj, runner = make(tmp_path, sleep=boom)
    with pytest.raises(RuntimeError):
        runner.run(SCENARIO, exp_id="e2")
    s = store.load()
    assert s["phase"] == "ABORTED" and s["aborted_from"] == "OBSERVING"
    assert inj.resources == {}


def test_recover_is_noop_after_done(tmp_path):
    store, inj, runner = make(tmp_path)
    runner.run(SCENARIO, exp_id="e3")
    assert recover(store, inj) is None


def test_next_run_cleans_previous_interrupted(tmp_path):
    store = StateStore(tmp_path / "state.json")
    store.save({"exp_id": "old", "phase": "INJECTING", "history": []})
    inj = MockInjector()
    inj.apply("old", 0, {})
    runner = Runner(store, inj, MockLoad(), MockJudge(), sleep=lambda s: None)
    runner.run(SCENARIO, exp_id="new")
    assert "old-s0" not in inj.resources

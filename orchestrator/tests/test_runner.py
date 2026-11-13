import hashlib
import json

import pytest

from orchestrator.cleanup import recover
from orchestrator.gates import (
    CompositeGate,
    no_leftover_chaos,
    previous_cleanup_ok,
    price_fresh,
)
from orchestrator.inject import MockInjector, chaos_name
from orchestrator.judging import FAIL, INCONCLUSIVE, MockJudge, accept
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


class Clock:
    """가짜 시계. sleep하면 시간이 흐른다."""

    def __init__(self):
        self.t = 1_000.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def make(tmp_path, injector=None, sleep=None, judge=None, **kw):
    store = StateStore(tmp_path / "state.json")
    inj = injector or MockInjector()
    clock = Clock()
    runner = Runner(
        store,
        inj,
        MockLoad(),
        judge or MockJudge(),
        sleep=sleep or clock.sleep,
        now=clock.now,
        runs_dir=tmp_path / "runs",
        **kw,
    )
    return store, inj, runner


# ---- 기본 흐름 ----------------------------------------------------------
def test_normal_run_passes(tmp_path):
    store, inj, runner = make(tmp_path)
    state = runner.run(SCENARIO, run_id="r-e1")
    assert state["phase"] == "DONE" and state["result"] == "PASSED"
    assert state["cleanup"] == "OK"
    assert store.load()["result"] == "PASSED"
    assert inj.resources == {}
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


@pytest.mark.parametrize(
    "outcome,result", [(FAIL, "FAILED"), (INCONCLUSIVE, "INCONCLUSIVE")]
)
def test_judge_outcome_maps_to_result(tmp_path, outcome, result):
    _, _, runner = make(tmp_path, judge=MockJudge(outcome))
    assert runner.run(SCENARIO, run_id="r-j1")["result"] == result


def test_chaos_name_is_deterministic():
    assert chaos_name("r-e1", 0) == chaos_name("r-e1", 0)
    assert chaos_name("R_e1", 0) == "r-e1-s0"


def test_apply_twice_creates_one():
    inj = MockInjector()
    inj.apply("r-e1", 0, {})
    inj.apply("r-e1", 0, {})
    assert len(inj.resources) == 1


# ---- 시나리오 5-b: 재시작 -------------------------------------------------
def test_restart_mid_observe_aborts_without_reinjecting(tmp_path):
    """완료 기준: 실행 중 Pod 삭제 → 재시작 후 ABORTED, PodChaos 중복 0."""
    inj = MockInjector()  # 클러스터는 Pod가 죽어도 남아 있으므로 같은 인스턴스 공유
    store, _, runner = make(tmp_path, inj)
    clock_sleep = runner.sleep

    def die_on_observe(seconds):
        if runner.state.get("phase") == "OBSERVING":
            raise Crash()
        clock_sleep(seconds)

    runner.sleep = die_on_observe
    runner._abort = lambda reason: None  # kill -9: 정리 코드가 돌 틈이 없음
    with pytest.raises(Crash):
        runner.run(SCENARIO, run_id="r-e1")
    assert store.load()["phase"] == "OBSERVING"
    assert len(inj.resources) == 1
    creates_before = inj.create_calls

    state = recover(StateStore(tmp_path / "state.json"), inj)  # 새 Pod 기동
    assert state["phase"] == "ABORTED" and state["result"] == "ABORTED"
    assert state["reason"] == "orchestrator_restart"
    assert state["aborted_from"] == "OBSERVING"
    assert state["cleanup"] == "OK"
    assert inj.resources == {}
    assert inj.create_calls == creates_before  # 복구 중 새 주입 없음


def test_exception_in_process_aborts_and_cleans(tmp_path):
    store, inj, runner = make(tmp_path)
    clock_sleep = runner.sleep

    def boom(seconds):
        if runner.state.get("phase") == "OBSERVING":
            raise RuntimeError("k6 died")
        clock_sleep(seconds)

    runner.sleep = boom
    with pytest.raises(RuntimeError):
        runner.run(SCENARIO, run_id="r-e2")
    s = store.load()
    assert s["result"] == "ABORTED" and s["aborted_from"] == "OBSERVING"
    assert inj.resources == {}


def test_recover_is_noop_after_done(tmp_path):
    store, inj, runner = make(tmp_path)
    runner.run(SCENARIO, run_id="r-e3")
    assert recover(store, inj) is None


def test_next_run_cleans_previous_interrupted(tmp_path):
    store, inj, runner = make(tmp_path)
    store.save({"run_id": "r-old", "phase": "INJECTING", "history": []})
    inj.apply("r-old", 0, {})
    runner.run(SCENARIO, run_id="r-new")
    assert "r-old-s0" not in inj.resources


# ---- 시나리오 5-a: 관측 소실 ---------------------------------------------
class FlakyWatch:
    """n번째 확인부터 계속 실패."""

    def __init__(self, fail_from):
        self.n, self.fail_from = 0, fail_from

    def healthy(self):
        self.n += 1
        return self.n < self.fail_from


def test_observability_lost_is_inconclusive_and_cleans(tmp_path):
    scenario = {**SCENARIO, "durations": {"warmup": 5, "observe": 120, "recover": 5}}
    _, inj, runner = make(tmp_path, watch=FlakyWatch(fail_from=4))
    state = runner.run(scenario, run_id="r-w1")
    assert state["result"] == "INCONCLUSIVE"
    assert state["reason"].startswith("observability_lost")
    assert state["interrupted_at"] == "OBSERVING"
    assert state["cleanup"] == "OK" and inj.resources == {}
    assert "verdict" not in state  # 판정하지 않는다
    assert [h["phase"] for h in state["history"]][-1] == "DONE"


def test_short_blip_is_tolerated(tmp_path):
    class Blip:
        def __init__(self):
            self.n = 0

        def healthy(self):
            self.n += 1
            return self.n != 3  # 한 번만 실패 (5초) < 15초

    scenario = {**SCENARIO, "durations": {"warmup": 5, "observe": 60, "recover": 5}}
    _, _, runner = make(tmp_path, watch=Blip())
    assert runner.run(scenario, run_id="r-w2")["result"] == "PASSED"


def test_lost_detected_within_15s(tmp_path):
    clock_log = []

    class Dead:
        def healthy(self):
            return False

    scenario = {**SCENARIO, "durations": {"warmup": 0, "observe": 300, "recover": 0}}
    _, _, runner = make(tmp_path, watch=Dead())
    start = runner.now()
    state = runner.run(scenario, run_id="r-w3")
    clock_log.append(state["ended_at"] - start)
    assert state["result"] == "INCONCLUSIVE"
    assert clock_log[0] <= 15


# ---- 정리 실패 → 다음 run 차단 --------------------------------------------
class BrokenCleanup(MockInjector):
    def cleanup(self, run_id):
        raise RuntimeError("api down")


def test_cleanup_failure_is_inconclusive_and_blocks_next_run(tmp_path):
    inj = BrokenCleanup()
    store, _, runner = make(tmp_path, inj)
    state = runner.run(SCENARIO, run_id="r-c1")
    assert state["cleanup"] == "INCOMPLETE"
    assert state["result"] == "INCONCLUSIVE"  # 장애가 남았을 수 있으면 PASS 아님

    runner.gate = CompositeGate(
        {
            "previous_cleanup": previous_cleanup_ok(store),
            "no_leftover_chaos": no_leftover_chaos(inj),
        }
    )
    # recover가 다시 정리를 시도하지만 여전히 실패 → 게이트가 막는다
    blocked = runner.run(SCENARIO, run_id="r-c2")
    assert blocked["phase"] == "GATE_FAILED"
    assert "previous_cleanup" in blocked["reason"]
    assert inj.create_calls == 1  # 두 번째 run은 주입하지 않았다
    assert store.load()["run_id"] == "r-c1"  # 이전 기록을 덮어쓰지 않는다


def test_recover_retries_incomplete_cleanup(tmp_path):
    store = StateStore(tmp_path / "state.json")
    inj = MockInjector()
    inj.apply("r-old", 0, {})
    store.save({"run_id": "r-old", "phase": "DONE", "cleanup": "INCOMPLETE"})
    state = recover(store, inj)
    assert state["cleanup"] == "OK" and inj.resources == {}
    assert state["phase"] == "DONE"  # 결과는 그대로 두고 정리만 다시 한다


# ---- 시작 게이트 ----------------------------------------------------------
def test_gate_blocks_on_leftover_chaos(tmp_path):
    inj = MockInjector()
    inj.apply("r-zombie", 0, {})
    _, _, runner = make(tmp_path, inj)
    runner.gate = CompositeGate({"no_leftover_chaos": no_leftover_chaos(inj)})
    state = runner.run(SCENARIO, run_id="r-g1")
    assert state["phase"] == "GATE_FAILED"
    assert inj.create_calls == 1


def test_price_fresh_check():
    def now():
        return 1_000.0

    fresh = price_fresh(lambda m: json.dumps({"price": "1", "ts": 999_000}), now=now)
    stale = price_fresh(lambda m: json.dumps({"price": "1", "ts": 990_000}), now=now)
    missing = price_fresh(lambda m: None, now=now)
    assert fresh({})[0] and not stale({})[0] and not missing({})[0]


def test_gate_error_closes(tmp_path):
    def boom(scenario):
        raise ConnectionError("redis down")

    result = CompositeGate({"price_fresh": boom}).check({})
    assert not result.ok and "redis down" in result.reason


# ---- run.json 계약 --------------------------------------------------------
def test_run_json_contract(tmp_path):
    _, _, runner = make(tmp_path)
    runner.run(SCENARIO, run_id="r-c1")
    run = json.loads((tmp_path / "runs" / "r-c1" / "run.json").read_text())
    assert run["schema"] == "chaos.run/v1"
    assert run["k6"]["job"] == "k6-r-c1"
    assert run["marks"]["inject_start"] <= run["marks"]["inject_end"]
    assert len(run["faults"]) == 1


def test_bad_run_id_rejected(tmp_path):
    _, _, runner = make(tmp_path)
    with pytest.raises(ValueError):
        runner.run(SCENARIO, run_id="Bad_ID")


# ---- B 판정 수용 ----------------------------------------------------------
def _sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.fixture
def run_dir(tmp_path):
    d = tmp_path / "runs" / "r-a1"
    d.mkdir(parents=True)
    (d / "crit.yaml").write_text("x: 1")
    rj = d / "run.json"
    rj.write_text(json.dumps({"run_id": "r-a1", "criteria": "crit.yaml"}))
    good_inputs = {
        "run_json_sha256": _sha(rj),
        "criteria_sha256": _sha(d / "crit.yaml"),
    }
    return rj, good_inputs


def test_accept_pass_needs_all_four(run_dir):
    rj, inputs = run_dir
    ok = {"verdict": "PASS", "run_id": "r-a1", "inputs": inputs}
    assert accept(0, ok, rj, "r-a1").outcome == "PASS"
    assert accept(0, {**ok, "run_id": "r-x"}, rj, "r-a1").outcome == "INCONCLUSIVE"
    assert accept(0, {**ok, "inputs": {}}, rj, "r-a1").outcome == "INCONCLUSIVE"


def test_accept_fail_and_inconclusive(run_dir):
    rj, inputs = run_dir
    fail = {"verdict": "FAIL", "run_id": "r-a1", "inputs": inputs}
    assert accept(10, fail, rj, "r-a1").outcome == "FAIL"
    # 다른 run의 FAIL은 믿지 않는다
    assert accept(10, {**fail, "run_id": "r-x"}, rj, "r-a1").outcome == "INCONCLUSIVE"
    inc = {"verdict": "INCONCLUSIVE", "run_id": "r-a1", "inputs": inputs}
    assert accept(11, inc, rj, "r-a1").outcome == "INCONCLUSIVE"


def test_job_restart_with_same_run_id_does_not_start_new_run(tmp_path):
    """5-b: Job이 같은 RUN_ID로 대체 Pod를 띄우면 정리만 하고 끝난다."""
    inj = MockInjector()
    store, _, runner = make(tmp_path, inj)
    clock_sleep = runner.sleep

    def die(seconds):
        if runner.state.get("phase") == "OBSERVING":
            raise Crash()
        clock_sleep(seconds)

    runner.sleep = die
    runner._abort = lambda reason: None
    with pytest.raises(Crash):
        runner.run(SCENARIO, run_id="r-job1")

    _, _, runner2 = make(tmp_path, inj)  # 대체 Pod
    state = runner2.run(SCENARIO, run_id="r-job1")
    assert state["result"] == "ABORTED" and state["reason"] == "orchestrator_restart"
    assert inj.create_calls == 1 and inj.resources == {}
    # 한 번 더 재시작돼도 그대로
    assert runner2.run(SCENARIO, run_id="r-job1")["result"] == "ABORTED"
    assert inj.create_calls == 1


def test_criteria_path_is_absolute_from_scenario(tmp_path):
    scen_dir = tmp_path / "experiments" / "scenarios"
    scen_dir.mkdir(parents=True)
    _, _, runner = make(tmp_path)
    runner.run(SCENARIO, run_id="r-p1", scenario_path=str(scen_dir / "pod-kill.yaml"))
    run = json.loads((tmp_path / "runs" / "r-p1" / "run.json").read_text())
    assert run["criteria"] == str(
        tmp_path / "experiments" / "criteria" / "default.yaml"
    )


# ---- 5-b: 재시작 시 k6도 멈춘다 -------------------------------------------
def test_restart_stops_k6_load(tmp_path):
    """재시작 후 정리 대상은 Chaos와 k6 둘 다다(잔여 PodChaos·k6 0)."""
    inj = MockInjector()
    load = MockLoad()  # 클러스터의 k6 Job처럼 Pod가 죽어도 남는다
    store = StateStore(tmp_path / "state.json")
    clock = Clock()

    def make_runner():
        return Runner(
            store,
            inj,
            load,
            MockJudge(),
            sleep=clock.sleep,
            now=clock.now,
            runs_dir=tmp_path / "runs",
        )

    first = make_runner()

    def die(seconds):
        if first.state.get("phase") == "OBSERVING":
            raise Crash()
        clock.sleep(seconds)

    first.sleep = die
    first._abort = lambda reason: None  # kill -9
    with pytest.raises(Crash):
        first.run(SCENARIO, run_id="r-k6")
    assert "r-k6" in load.running

    state = make_runner().run(SCENARIO, run_id="r-k6")  # Job 대체 Pod
    assert state["result"] == "ABORTED" and state["load_stopped"] is True
    assert load.running == set()
    assert inj.resources == {}


def test_recover_survives_load_stop_error(tmp_path):
    class BrokenStop(MockLoad):
        def stop(self, run_id):
            raise RuntimeError("kubectl down")

    store = StateStore(tmp_path / "state.json")
    store.save({"run_id": "r-x", "phase": "OBSERVING", "history": []})
    inj = MockInjector()
    inj.apply("r-x", 0, {})
    state = recover(store, inj, load=BrokenStop())
    assert state["result"] == "ABORTED"
    assert state["load_stopped"] is False
    assert state["cleanup"] == "OK" and inj.resources == {}  # Chaos 정리는 계속한다


# ---- 시나리오 6: 대기열이 쌓였다 빠지는가 ------------------------------------
class QueueMetric:
    """phase별로 대기열 값을 돌려주는 가짜 Prometheus. 회복 단계에서는 drain_after번째 조회부터 base."""

    def __init__(self, runner, base=2, fault=40, drain_after=3, unknown=False):
        self.runner, self.base, self.fault = runner, base, fault
        self.drain_after, self.unknown = drain_after, unknown
        self.recover_calls = 0

    def value(self, query):
        assert query == "sum(adapter_queue_depth)"
        phase = self.runner.state.get("phase")
        if self.unknown and phase != "WARMUP":
            return None
        if phase == "WARMUP":
            return self.base
        if phase in ("INJECTING", "OBSERVING"):
            return self.fault
        self.recover_calls += 1
        return self.base if self.recover_calls > self.drain_after else self.fault


DRAIN_SCENARIO = {
    **SCENARIO,
    "durations": {"warmup": 10, "observe": 20, "recover": 0},
    "drain": {"timeout_s": 60},
}


def _drain_runner(tmp_path, judge=None, **metric_kw):
    _, inj, runner = make(tmp_path, judge=judge)
    runner.metrics = QueueMetric(runner, **metric_kw)
    return inj, runner


def test_queue_builds_and_drains_passes(tmp_path):
    _, runner = _drain_runner(tmp_path)
    state = runner.run(DRAIN_SCENARIO, run_id="r-q1")
    d = state["drain"]
    assert state["result"] == "PASSED" and state["drain_outcome"] == "drained"
    assert d["baseline_peak"] == 2 and d["peak"] == 40
    assert d["drained"] is True and d["drain_s"] > 0
    run = json.loads((tmp_path / "runs" / "r-q1" / "run.json").read_text())
    assert run["marks"]["inject_end"] <= run["marks"]["queue_drained"]


def test_queue_not_drained_in_time_fails(tmp_path):
    inj, runner = _drain_runner(tmp_path, drain_after=10_000)
    state = runner.run(DRAIN_SCENARIO, run_id="r-q2")
    assert state["result"] == "FAILED"
    assert state["drain_outcome"] == "queue_not_drained"
    assert state["drain"]["drained"] is False
    assert inj.resources == {}  # 실패여도 장애는 정리됐다


def test_queue_never_built_is_inconclusive(tmp_path):
    _, runner = _drain_runner(tmp_path, fault=2)  # 장애가 효과 없음
    state = runner.run(DRAIN_SCENARIO, run_id="r-q3")
    assert state["result"] == "INCONCLUSIVE"
    assert state["drain_outcome"] == "queue_not_built"


def test_queue_unobservable_is_inconclusive(tmp_path):
    _, runner = _drain_runner(tmp_path, unknown=True)  # 지표가 비면 0이 아니라 모름
    state = runner.run(DRAIN_SCENARIO, run_id="r-q4")
    assert state["result"] == "INCONCLUSIVE"
    assert state["drain_outcome"] == "drain_unobserved"


def test_judge_fail_is_kept_even_if_drained(tmp_path):
    _, runner = _drain_runner(tmp_path, judge=MockJudge(FAIL))
    assert runner.run(DRAIN_SCENARIO, run_id="r-q5")["result"] == "FAILED"


def test_bad_drain_block_rejected_before_start(tmp_path):
    _, inj, runner = make(tmp_path)
    with pytest.raises(ValueError):
        runner.run({**SCENARIO, "drain": {"timeout": 60}}, run_id="r-q6")
    assert inj.create_calls == 0


def test_prom_query_parses_and_fails_closed(monkeypatch):
    import io

    from orchestrator import metrics

    def fake(body):
        def _open(url, timeout):
            return io.BytesIO(json.dumps(body).encode())

        return _open

    ok = {"status": "success", "data": {"result": [{"value": [0, "12"]}]}}
    monkeypatch.setattr(metrics.urllib.request, "urlopen", fake(ok))
    assert metrics.PromQuery("http://p").value("q") == 12.0
    empty = {"status": "success", "data": {"result": []}}
    monkeypatch.setattr(metrics.urllib.request, "urlopen", fake(empty))
    assert metrics.PromQuery("http://p").value("q") is None

    def boom(url, timeout):
        raise OSError("down")

    monkeypatch.setattr(metrics.urllib.request, "urlopen", boom)
    assert metrics.PromQuery("http://p").value("q") is None

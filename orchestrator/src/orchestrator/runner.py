"""실험 phase 상태 머신.

IDLE → PRECHECK → WARMUP → INJECTING → OBSERVING → RECOVERING → JUDGING → DONE
어디서든 예외/재시작 → ABORTED,  게이트 실패 → GATE_FAILED

원칙: phase를 state.json에 먼저 기록하고 나서 그 phase의 동작을 한다.
그래야 동작 도중 죽어도 재시작한 쪽이 "어디까지 했는지"를 안다.
"""

import contextlib
import time
import uuid
from enum import Enum

from .cleanup import recover
from .gates import AlwaysOpen, Gate
from .inject import Injector
from .judging import Judge
from .observe import LoadGenerator
from .runjson import RunFile, check_run_id
from .store import StateStore


class Phase(str, Enum):
    IDLE = "IDLE"
    PRECHECK = "PRECHECK"
    WARMUP = "WARMUP"
    INJECTING = "INJECTING"
    OBSERVING = "OBSERVING"
    RECOVERING = "RECOVERING"
    JUDGING = "JUDGING"
    DONE = "DONE"
    ABORTED = "ABORTED"
    GATE_FAILED = "GATE_FAILED"


class Runner:
    def __init__(
        self,
        store: StateStore,
        injector: Injector,
        load: LoadGenerator,
        judge: Judge,
        gate: Gate | None = None,
        runs_dir: str = "runs",
        sleep=time.sleep,
        now=time.time,
    ):
        self.store = store
        self.injector = injector
        self.load = load
        self.judge = judge
        self.gate = gate or AlwaysOpen()
        self.runs_dir = runs_dir
        self.sleep = sleep
        self.now = now
        self.state: dict = {}

    # --- 상태 기록 -------------------------------------------------------
    def _enter(self, phase: Phase, **extra) -> None:
        at = self.now()
        self.state.update(phase=phase.value, phase_entered_at=at, **extra)
        self.state.setdefault("history", []).append({"phase": phase.value, "at": at})
        self.store.save(self.state)

    # --- 실행 -----------------------------------------------------------
    def run(
        self,
        scenario: dict,
        run_id: str | None = None,
        scenario_path: str | None = None,
    ) -> dict:
        # 1) 이전 실험이 끊겼으면 주입 없이 정리만 하고 끝난 것으로 기록
        recover(self.store, self.injector, now=self.now)

        run_id = check_run_id(run_id or f"r-{uuid.uuid4().hex[:8]}")
        run_file = RunFile(self.runs_dir, run_id)
        run_file.create(run_id, scenario, scenario_path)
        durations = scenario.get("durations", {})
        self.state = {
            "run_id": run_id,
            "scenario_name": scenario.get("name", "unnamed"),
            "started_at": self.now(),
            "chaos_resources": [],
            "run_json": str(run_file.path),
        }
        try:
            self._enter(Phase.PRECHECK)
            gate = self.gate.check(scenario)
            if not gate.ok:
                self._enter(Phase.GATE_FAILED, reason=gate.reason, ended_at=self.now())
                return self.state

            self._enter(Phase.WARMUP)
            self.load.start(run_id, scenario)
            self.sleep(durations.get("warmup", 0))

            self._enter(Phase.INJECTING)
            run_file.mark("inject_start")
            for step, fault in enumerate(scenario.get("faults", [])):
                name = self.injector.apply(run_id, step, fault)
                run_file.add_fault(fault.get("kind", "?"), fault.get("target", name))
                if name not in self.state["chaos_resources"]:
                    self.state["chaos_resources"].append(name)
                    self.store.save(self.state)

            self._enter(Phase.OBSERVING)
            self.sleep(durations.get("observe", 0))

            self._enter(Phase.RECOVERING)
            self.injector.cleanup(run_id)
            run_file.mark("inject_end")
            self.sleep(durations.get("recover", 0))
            self.load.stop(run_id)

            self._enter(Phase.JUDGING)
            verdict = self.judge.evaluate(run_file.path, run_id)
            self._enter(
                Phase.DONE,
                verdict={"passed": verdict.passed, "details": verdict.details},
                ended_at=self.now(),
            )
            return self.state
        except BaseException as e:
            # 같은 프로세스 안에서 난 예외·SIGTERM: 바로 정리하고 ABORTED
            self._abort(repr(e))
            raise

    def _abort(self, reason: str) -> None:
        interrupted = self.state.get("phase")
        try:
            removed = self.injector.cleanup(self.state["run_id"])
        finally:
            with contextlib.suppress(Exception):
                self.load.stop(self.state["run_id"])
        self._enter(
            Phase.ABORTED,
            aborted_from=interrupted,
            reason=reason,
            cleaned_resources=removed,
            ended_at=self.now(),
        )

"""실험 phase 상태 머신.

PRECHECK → WARMUP → INJECTING → OBSERVING → RECOVERING → JUDGING → DONE
  게이트 실패 → GATE_FAILED
  예외·SIGTERM·재시작 → ABORTED

원칙
- phase를 state.json에 먼저 기록하고 나서 그 phase의 동작을 한다.
- 판단할 수 없으면 PASSED를 내지 않는다(fail-closed).

최종 결과(result): PASSED / FAILED / INCONCLUSIVE / ABORTED
정리 상태(cleanup): OK / INCOMPLETE  (INCOMPLETE면 다음 run의 시작 게이트가 막는다)
"""

import contextlib
import time
import uuid
from enum import Enum

from .cleanup import TERMINAL, recover, safe_cleanup
from .gates import AlwaysOpen, Gate
from .inject import Injector
from .judging import FAIL, PASS, Judge
from .observe import LoadGenerator
from .runjson import RunFile, check_run_id
from .store import StateStore
from .watch import AlwaysHealthy, ObservabilityLost, Watch

OUTCOME_TO_RESULT = {PASS: "PASSED", FAIL: "FAILED"}  # 그 밖은 INCONCLUSIVE


class Phase(str, Enum):
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
        watch: Watch | None = None,
        sleep=time.sleep,
        now=time.time,
        runs_dir: str = "runs",
        poll_s: float = 5.0,
        lost_after_s: float = 15.0,
    ):
        self.store = store
        self.injector = injector
        self.load = load
        self.judge = judge
        self.gate = gate or AlwaysOpen()
        self.watch = watch or AlwaysHealthy()
        self.sleep = sleep
        self.now = now
        self.runs_dir = runs_dir
        self.poll_s = poll_s
        self.lost_after_s = lost_after_s
        self.state: dict = {}

    # --- 상태 기록 -------------------------------------------------------
    def _enter(self, phase: Phase, **extra) -> None:
        at = self.now()
        self.state.update(phase=phase.value, phase_entered_at=at, **extra)
        self.state.setdefault("history", []).append({"phase": phase.value, "at": at})
        self.store.save(self.state)

    def _finish(self, phase: Phase, result: str, **extra) -> dict:
        self._enter(phase, result=result, ended_at=self.now(), **extra)
        return self.state

    # --- 관찰: 기다리는 동안 관측을 감시한다 -----------------------------
    def _observe(self, seconds: float) -> None:
        """seconds 동안 poll_s마다 watch를 확인한다. 정상 응답 없는 구간이 lost_after_s를 넘으면 중단."""
        end = self.now() + seconds
        last_ok = self.now()
        while True:
            if self.watch.healthy():
                last_ok = self.now()
            elif self.now() - last_ok >= self.lost_after_s:
                raise ObservabilityLost(f"관측 없음 {self.now() - last_ok:.0f}s")
            remaining = end - self.now()
            if remaining <= 0:
                return
            self.sleep(min(self.poll_s, remaining))

    def _stop_everything(self) -> tuple[str, list[str], str]:
        status, removed, err = safe_cleanup(self.injector, self.state["run_id"])
        with contextlib.suppress(Exception):
            self.load.stop(self.state["run_id"])
        return status, removed, err

    # --- 실행 -----------------------------------------------------------
    def run(
        self,
        scenario: dict,
        run_id: str | None = None,
        scenario_path: str | None = None,
    ) -> dict:
        run_id = check_run_id(run_id or f"r-{uuid.uuid4().hex[:8]}")

        # 1) 이전 실험이 끊겼으면 주입 없이 정리만 하고 끝난 것으로 기록
        recovered = recover(self.store, self.injector, now=self.now)
        # Job이 같은 RUN_ID로 대체 Pod를 띄운 경우: 정리만 하고 새 실험은 시작하지 않는다
        last = recovered or self.store.load()
        if last and last.get("run_id") == run_id and last.get("phase") in TERMINAL:
            return last
        durations = scenario.get("durations", {})

        # 2) 시작 게이트: 이전 run 기록을 덮어쓰기 전에 확인한다
        gate = self.gate.check(scenario)
        if not gate.ok:
            return {
                "run_id": run_id,
                "phase": Phase.GATE_FAILED.value,
                "result": "NOT_STARTED",
                "reason": gate.reason,
                "gate": gate.checks,
            }

        run_file = RunFile(self.runs_dir, run_id)
        run_file.create(run_id, scenario, scenario_path)
        self.state = {
            "run_id": run_id,
            "scenario_name": scenario.get("name", "unnamed"),
            "started_at": self.now(),
            "chaos_resources": [],
            "run_json": str(run_file.path),
            "gate": gate.checks,
        }
        try:
            self._enter(Phase.PRECHECK)

            self._enter(Phase.WARMUP)
            self.load.start(run_id, scenario)
            self._observe(durations.get("warmup", 0))

            self._enter(Phase.INJECTING)
            run_file.mark("inject_start")
            for step, fault in enumerate(scenario.get("faults", [])):
                name = self.injector.apply(run_id, step, fault)
                run_file.add_fault(fault.get("kind", "?"), fault.get("target", name))
                if name not in self.state["chaos_resources"]:
                    self.state["chaos_resources"].append(name)
                    self.store.save(self.state)

            self._enter(Phase.OBSERVING)
            self._observe(durations.get("observe", 0))

            self._enter(Phase.RECOVERING)
            status, _, err = safe_cleanup(self.injector, run_id)
            run_file.mark("inject_end")
            self.state["cleanup"] = status
            if err:
                self.state["cleanup_error"] = err
            self._observe(durations.get("recover", 0))
            self.load.stop(run_id)

            self._enter(Phase.JUDGING)
            verdict = self.judge.evaluate(run_file.path, run_id)
            result = OUTCOME_TO_RESULT.get(verdict.outcome, "INCONCLUSIVE")
            if self.state["cleanup"] != "OK":
                result = "INCONCLUSIVE"  # 장애가 남아 있을 수 있으면 통과로 보지 않는다
            return self._finish(
                Phase.DONE,
                result,
                verdict={"outcome": verdict.outcome, "details": verdict.details},
            )

        except ObservabilityLost as e:
            # 5-a: 부하·장애를 멈추고 판정하지 않는다
            status, removed, err = self._stop_everything()
            with contextlib.suppress(Exception):
                run_file.mark("inject_end")
            return self._finish(
                Phase.DONE,
                "INCONCLUSIVE",
                reason=f"observability_lost: {e}",
                interrupted_at=self.state.get("phase"),
                cleanup=status,
                cleaned_resources=removed,
                **({"cleanup_error": err} if err else {}),
            )
        except BaseException as e:
            # 같은 프로세스 안에서 난 예외·SIGTERM: 바로 정리하고 ABORTED
            self._abort(repr(e))
            raise

    def _abort(self, reason: str) -> None:
        interrupted = self.state.get("phase")
        status, removed, err = self._stop_everything()
        self._finish(
            Phase.ABORTED,
            "ABORTED",
            aborted_from=interrupted,
            reason=reason,
            cleanup=status,
            cleaned_resources=removed,
            **({"cleanup_error": err} if err else {}),
        )

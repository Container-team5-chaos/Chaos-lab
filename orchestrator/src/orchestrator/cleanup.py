"""기동 시 복구: 이전 실험이 중간에 끊겼으면 주입 없이 정리하고 ABORTED로 남긴다.

정리 대상은 Chaos 리소스와 k6 부하 둘 다다(시나리오 5-b: 잔여 PodChaos·k6 0).
"""

import time

from .inject import Injector
from .store import StateStore

TERMINAL = {"DONE", "ABORTED", "GATE_FAILED"}


def safe_cleanup(injector: Injector, run_id: str) -> tuple[str, list[str], str]:
    """정리하고 (cleanup 상태, 지운 이름, 오류)를 돌려준다. 실패해도 예외를 올리지 않는다."""
    try:
        return "OK", injector.cleanup(run_id), ""
    except Exception as e:  # noqa: BLE001 - 정리 실패는 기록하고 다음 게이트가 막는다
        return "INCOMPLETE", [], repr(e)


def stop_load(load, run_id: str) -> bool:
    """k6에 멈춤 신호를 보낸다. 실패해도 예외를 올리지 않고 성공 여부만 돌려준다."""
    if load is None:
        return False
    try:
        load.stop(run_id)
        return True
    except Exception:  # noqa: BLE001 - 정리 경로라 삼키고 기록만 한다
        return False


def recover(
    store: StateStore, injector: Injector, now=time.time, load=None
) -> dict | None:
    """끊긴 실험이 있으면 정리한 상태를 돌려준다. 없으면 None.

    순서: k6 멈춤 → Chaos 정리 → ABORTED 기록. 이 함수는 절대 새 주입·새 부하를 만들지 않는다.
    이전에 정리가 INCOMPLETE였던 run도 다시 정리를 시도한다.
    """
    state = store.load()
    if state is None:
        return None
    retry = state.get("phase") in TERMINAL and state.get("cleanup") == "INCOMPLETE"
    if state.get("phase") in TERMINAL and not retry:
        return None
    if not retry:
        state["load_stopped"] = stop_load(load, state["run_id"])
    status, removed, err = safe_cleanup(injector, state["run_id"])
    at = now()
    if not retry:
        state.update(
            phase="ABORTED",
            result="ABORTED",
            reason="orchestrator_restart",
            aborted_from=state["phase"],
            ended_at=at,
        )
        state.setdefault("history", []).append({"phase": "ABORTED", "at": at})
    state.update(cleanup=status, cleaned_resources=removed)
    if err:
        state["cleanup_error"] = err
    store.save(state)
    return state


def cleanup(store: StateStore, injector: Injector, load=None) -> dict | None:
    return recover(store, injector, load=load)

"""기동 시 복구: 이전 실험이 중간에 끊겼으면 주입 없이 정리하고 ABORTED로 남긴다."""

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


def recover(store: StateStore, injector: Injector, now=time.time) -> dict | None:
    """끊긴 실험이 있으면 정리한 상태를 돌려준다. 없으면 None.

    순서: 정리 → ABORTED 기록. 이 함수는 절대 새 주입을 하지 않는다.
    이전에 정리가 INCOMPLETE였던 run도 다시 정리를 시도한다.
    """
    state = store.load()
    if state is None:
        return None
    retry = state.get("phase") in TERMINAL and state.get("cleanup") == "INCOMPLETE"
    if state.get("phase") in TERMINAL and not retry:
        return None
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


def cleanup(store: StateStore, injector: Injector) -> dict | None:
    return recover(store, injector)

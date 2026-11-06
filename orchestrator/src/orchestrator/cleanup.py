"""기동 시 복구: 이전 실험이 중간에 끊겼으면 주입 없이 정리하고 ABORTED로 남긴다."""

import time

from .inject import Injector
from .store import StateStore

TERMINAL = {"DONE", "ABORTED", "GATE_FAILED"}


def recover(store: StateStore, injector: Injector, now=time.time) -> dict | None:
    """끊긴 실험이 있으면 정리한 상태를 돌려준다. 없으면 None.

    순서가 중요하다: 정리 → ABORTED 기록. 이 함수는 절대 새 주입을 하지 않는다.
    """
    state = store.load()
    if state is None or state.get("phase") in TERMINAL:
        return None
    interrupted_at = state["phase"]
    removed = injector.cleanup(state["run_id"])
    state.update(
        phase="ABORTED",
        aborted_from=interrupted_at,
        cleaned_resources=removed,
        ended_at=now(),
    )
    state.setdefault("history", []).append(
        {"phase": "ABORTED", "at": state["ended_at"]}
    )
    store.save(state)
    return state


def cleanup(store: StateStore, injector: Injector) -> dict | None:
    return recover(store, injector)

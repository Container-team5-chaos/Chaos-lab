"""실험 상태(state.json) 저장소.

Pod가 재시작돼도 남아 있어야 하므로 경로는 PVC 마운트 위치를 가리켜야 한다.
쓰기는 임시 파일 + os.replace로 원자적으로 처리해서, 쓰는 도중 죽어도
반쯤 쓰인 파일이 남지 않게 한다.
"""

import json
import os
import tempfile
from pathlib import Path

DEFAULT_PATH = os.environ.get("ORCH_STATE_PATH", "/var/lib/orchestrator/state.json")


class StateStore:
    def __init__(self, path: str | Path = DEFAULT_PATH):
        self.path = Path(path)

    def load(self) -> dict | None:
        if not self.path.exists():
            return None
        with self.path.open(encoding="utf-8") as f:
            return json.load(f)

    def save(self, state: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".state-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise


def save(state: dict, path: str | Path = DEFAULT_PATH) -> None:
    StateStore(path).save(state)


def load(path: str | Path = DEFAULT_PATH) -> dict | None:
    return StateStore(path).load()

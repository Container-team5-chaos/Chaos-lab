"""run.json (chaos.run/v1) 작성. B 판정 패키지와의 계약(docs/design/b-measure-judge.md 1.4).

runner가 쓰고 judge는 읽기만 한다. 시각은 epoch ms(UTC).
"""

import json
import re
import time
from pathlib import Path

RUN_ID_RE = re.compile(r"^[a-z0-9-]{3,40}$")


def now_ms() -> int:
    return int(time.time() * 1000)


def check_run_id(run_id: str) -> str:
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"run_id 형식 오류: {run_id!r} (^[a-z0-9-]{{3,40}}$)")
    return run_id


class RunFile:
    def __init__(self, runs_dir: str | Path, run_id: str):
        self.path = Path(runs_dir) / run_id / "run.json"

    def create(
        self, run_id: str, scenario: dict, scenario_path: str | None = None
    ) -> dict:
        data = {
            "schema": "chaos.run/v1",
            "run_id": run_id,
            "experiment": scenario.get("name", "unnamed"),
            "criteria": scenario.get(
                "criteria", "../../experiments/criteria/default.yaml"
            ),
            "k6": {"job": f"k6-{run_id}"},
            "marks": {},
            "faults": [],
        }
        if scenario_path:
            data["scenario"] = scenario_path
        self.write(data)
        return data

    def read(self) -> dict:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def write(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def mark(self, key: str, at_ms: int | None = None) -> None:
        data = self.read()
        data.setdefault("marks", {})[key] = at_ms if at_ms is not None else now_ms()
        self.write(data)

    def add_fault(self, kind: str, target: str, start_ms: int | None = None) -> None:
        data = self.read()
        data.setdefault("faults", []).append(
            {"kind": kind, "target": target, "start_ms": start_ms or now_ms()}
        )
        self.write(data)

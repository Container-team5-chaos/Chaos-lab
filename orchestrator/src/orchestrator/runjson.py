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
            "criteria": self._criteria(scenario, scenario_path),
            "k6": {"job": f"k6-{run_id}"},
            "marks": {},
            "faults": [],
        }
        if scenario_path:
            data["scenario"] = str(Path(scenario_path).resolve())
        self.write(data)
        return data

    @staticmethod
    def _criteria(scenario: dict, scenario_path: str | None) -> str:
        """기준 파일 경로. 시나리오 파일 기준 상대 경로를 절대 경로로 바꿔 적는다.

        run.json은 PVC(runs/)에, 시나리오는 이미지 안에 있어 위치가 다르기 때문이다.
        """
        base = Path(scenario_path).resolve().parent if scenario_path else Path.cwd()
        crit = scenario.get("criteria", "../criteria/default.yaml")
        return str((base / crit).resolve())

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

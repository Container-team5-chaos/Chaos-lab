"""판정 연결부. 판정 자체는 B의 orchestrator.judge 패키지가 한다.

runner는 넷을 모두 만족할 때만 PASS로 받는다 (B 설계 1.4):
종료 코드 0, verdict == "PASS", run_id 일치, criteria·run.json sha256 일치.
"""

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


@dataclass
class Verdict:
    passed: bool
    details: dict = field(default_factory=dict)


class Judge(Protocol):
    def evaluate(self, run_json: Path, run_id: str) -> Verdict: ...


class MockJudge:
    def __init__(self, passed: bool = True):
        self.passed = passed

    def evaluate(self, run_json: Path, run_id: str) -> Verdict:
        return Verdict(self.passed, {"mock": True})


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def accept(exit_code: int, verdict: dict, run_json: Path, run_id: str) -> Verdict:
    """B가 정한 수용 조건 4개를 확인한다."""
    run = json.loads(run_json.read_text(encoding="utf-8"))
    criteria = (run_json.parent / run["criteria"]).resolve()
    inputs = verdict.get("inputs", {})
    checks = {
        "exit_code_0": exit_code == 0,
        "verdict_pass": verdict.get("verdict") == "PASS",
        "run_id_match": verdict.get("run_id") == run_id,
        "run_json_sha256": inputs.get("run_json_sha256") == _sha256(run_json),
        "criteria_sha256": criteria.exists()
        and inputs.get("criteria_sha256") == _sha256(criteria),
    }
    return Verdict(
        all(checks.values()),
        {"checks": checks, "verdict": verdict.get("verdict"), "exit_code": exit_code},
    )


class ChaosJudge:
    """`python -m orchestrator.judge run --run runs/<id>/run.json` 실행 후 수용 조건 확인."""

    def __init__(self, offline: bool = False):
        self.offline = offline

    def evaluate(self, run_json: Path, run_id: str) -> Verdict:
        cmd = [
            sys.executable,
            "-m",
            "orchestrator.judge",
            "run",
            "--run",
            str(run_json),
        ]
        if self.offline:
            cmd.append("--offline")
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        out = run_json.parent / "verdict.json"
        if not out.exists():
            return Verdict(
                False, {"exit_code": proc.returncode, "stderr": proc.stderr[-2000:]}
            )
        return accept(
            proc.returncode,
            json.loads(out.read_text(encoding="utf-8")),
            run_json,
            run_id,
        )

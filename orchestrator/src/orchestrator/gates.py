"""시작 게이트: 실험을 시작해도 되는 상태인지 확인한다.

무엇을 확인할지(Pod Ready, 수집기 정상 등)는 팀 합의 후 채운다.
"""

from dataclasses import dataclass
from typing import Protocol


@dataclass
class GateResult:
    ok: bool
    reason: str = ""


class Gate(Protocol):
    def check(self, scenario: dict) -> GateResult: ...


class AlwaysOpen:
    def check(self, scenario: dict) -> GateResult:
        return GateResult(True)


def check(gate: Gate, scenario: dict) -> GateResult:
    return gate.check(scenario)

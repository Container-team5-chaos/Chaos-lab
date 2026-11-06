"""판정 인터페이스. PromQL·정합성 검사 구현은 B 파트(judge/)와 맞춘다."""

from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class Verdict:
    passed: bool
    details: dict = field(default_factory=dict)


class Judge(Protocol):
    def evaluate(self, exp_id: str, started_at: float, ended_at: float) -> Verdict: ...


class MockJudge:
    def __init__(self, passed: bool = True):
        self.passed = passed

    def evaluate(self, exp_id: str, started_at: float, ended_at: float) -> Verdict:
        return Verdict(self.passed, {"mock": True})


def judge(j: Judge, exp_id: str, started_at: float, ended_at: float) -> Verdict:
    return j.evaluate(exp_id, started_at, ended_at)

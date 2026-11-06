"""부하 발생(k6) 인터페이스. 실제 구현은 B 파트(load/k6)와 맞춘다."""

from typing import Protocol


class LoadGenerator(Protocol):
    def start(self, exp_id: str, scenario: dict) -> None: ...

    def stop(self, exp_id: str) -> None: ...


class MockLoad:
    def __init__(self):
        self.running: set[str] = set()

    def start(self, exp_id: str, scenario: dict) -> None:
        self.running.add(exp_id)

    def stop(self, exp_id: str) -> None:
        self.running.discard(exp_id)


def collect(load: LoadGenerator, exp_id: str, scenario: dict) -> None:
    load.start(exp_id, scenario)

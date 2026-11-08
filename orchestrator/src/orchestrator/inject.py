"""장애 주입 인터페이스 (구현은 E 파트 chaos/injector와 맞춘다).

규칙
- Chaos 리소스 이름은 실험 ID와 단계로 결정적으로 만든다 → 재시도해도 중복 생성되지 않는다.
- 모든 리소스에 LABEL_KEY=<run_id> 라벨을 붙인다 → 재시작 후 라벨로 찾아서 지운다.
- 보호 Pod(DB·Redis·시세 stub)와 k6 Pod는 시나리오와 상관없이 항상 대상에서 뺀다.
- 정리는 리소스가 실제로 사라질 때까지 확인한다. 기한 안에 안 사라지면 CleanupIncomplete.
"""

import copy
import re
import time
from typing import Protocol

LABEL_KEY = "chaos/run-id"
CHAOS_GROUP = "chaos-mesh.org"
CHAOS_VERSION = "v1alpha1"
KIND_PLURALS = {"PodChaos": "podchaos", "NetworkChaos": "networkchaos"}

# 주입 대상에서 항상 제외할 Pod (infra/k8s의 protected 라벨, B의 k6 Job 라벨)
PROTECTED_LABEL = "chaos.exchange/protected"
SAFETY_EXPRESSIONS = (
    {"key": PROTECTED_LABEL, "operator": "DoesNotExist"},
    {"key": "app.kubernetes.io/name", "operator": "NotIn", "values": ["k6"]},
)


def with_safety_selector(spec: dict) -> dict:
    """spec.selector에 제외 조건을 더한 복사본. 원본은 그대로 두고, 여러 번 불러도 중복되지 않는다."""
    spec = copy.deepcopy(spec)
    exprs = spec.setdefault("selector", {}).setdefault("expressionSelectors", [])
    for expr in SAFETY_EXPRESSIONS:
        if expr not in exprs:
            exprs.append(copy.deepcopy(expr))
    return spec


class CleanupIncomplete(RuntimeError):
    """정리 기한 안에 Chaos 리소스가 사라지지 않음 (설계의 cleanup = INCOMPLETE).

    설계: 다음 run은 이전 run 정리 상태가 INCOMPLETE면 시작을 거부한다.
    지금 runner에서는 예외가 올라가 실험이 DONE/ABORTED로 기록되지 않고,
    다음 실행의 recover()가 다시 정리를 시도한다 (정리될 때까지 새 주입 없음).
    """

    def __init__(self, run_id: str, remaining: list[str]):
        self.run_id = run_id
        self.remaining = remaining
        super().__init__(f"cleanup incomplete for {run_id}: {remaining}")


def chaos_name(run_id: str, step: int) -> str:
    """같은 (run_id, step)이면 항상 같은 이름. k8s 이름 규칙(소문자, 63자)에 맞춘다."""
    base = re.sub(r"[^a-z0-9-]", "-", f"{run_id}-s{step}".lower()).strip("-")
    return base[:63].rstrip("-")


class Injector(Protocol):
    def apply(self, run_id: str, step: int, fault: dict) -> str:
        """장애 리소스를 만들고 이름을 돌려준다. 이미 있으면 새로 만들지 않는다."""

    def cleanup(self, run_id: str) -> list[str]:
        """run_id 라벨이 붙은 Chaos 리소스를 모두 지우고 지운 이름 목록을 돌려준다."""

    def leftovers(self) -> list[str]:
        """우리 라벨이 붙은 Chaos 리소스 중 아직 남아 있는 것(run 무관)."""


class MockInjector:
    """클러스터 없이 로직을 검증하기 위한 가짜 구현."""

    def __init__(self):
        self.resources: dict[str, dict] = {}
        self.create_calls = 0

    def apply(self, run_id: str, step: int, fault: dict) -> str:
        name = chaos_name(run_id, step)
        self.create_calls += 1
        if name not in self.resources:  # 실제 API의 409 AlreadyExists와 같은 동작
            self.resources[name] = {"run_id": run_id, "fault": fault}
        return name

    def cleanup(self, run_id: str) -> list[str]:
        names = [n for n, r in self.resources.items() if r["run_id"] == run_id]
        for n in names:
            del self.resources[n]
        return names

    def leftovers(self) -> list[str]:
        return list(self.resources)


class ChaosMeshInjector:
    """Chaos Mesh CR을 직접 만드는 구현. `pip install .[k8s]` 필요."""

    def __init__(
        self,
        namespace: str = "exchange",
        *,
        api=None,
        api_exception: type[Exception] | None = None,
        cleanup_timeout: float = 60.0,
        poll_interval: float = 1.0,
        sleep=time.sleep,
        clock=time.monotonic,
    ):
        """api/api_exception을 넘기면 클러스터 없이 테스트할 수 있다."""
        if api is None:
            from kubernetes import client, config

            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
            api = client.CustomObjectsApi()
            api_exception = client.exceptions.ApiException
        self.api = api
        self.ApiException = api_exception
        self.namespace = namespace
        self.cleanup_timeout = (
            cleanup_timeout  # 정리 대기 상한(초). 넘으면 CleanupIncomplete
        )
        self.poll_interval = poll_interval
        self.sleep = sleep
        self.clock = clock

    def apply(self, run_id: str, step: int, fault: dict) -> str:
        kind = fault["kind"]
        name = chaos_name(run_id, step)
        body = {
            "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
            "kind": kind,
            "metadata": {"name": name, "labels": {LABEL_KEY: run_id}},
            "spec": with_safety_selector(fault["spec"]),
        }
        try:
            self.api.create_namespaced_custom_object(
                CHAOS_GROUP, CHAOS_VERSION, self.namespace, KIND_PLURALS[kind], body
            )
        except self.ApiException as e:
            if e.status != 409:
                raise
        return name

    def _list(self, run_id: str) -> list[tuple[str, str]]:
        """run_id 라벨이 붙은 (plural, name) 목록. 삭제 중(Terminating)인 것도 포함된다."""
        found = []
        for plural in KIND_PLURALS.values():
            items = self.api.list_namespaced_custom_object(
                CHAOS_GROUP,
                CHAOS_VERSION,
                self.namespace,
                plural,
                label_selector=f"{LABEL_KEY}={run_id}",
            )["items"]
            found += [(plural, item["metadata"]["name"]) for item in items]
        return found

    def cleanup(self, run_id: str) -> list[str]:
        targets = self._list(run_id)
        for plural, name in targets:
            try:
                self.api.delete_namespaced_custom_object(
                    CHAOS_GROUP, CHAOS_VERSION, self.namespace, plural, name
                )
            except self.ApiException as e:
                if e.status != 404:
                    raise

        # DELETE 성공 ≠ 정리 완료. finalizer가 풀려 목록에서 사라질 때까지 기다린다.
        deadline = self.clock() + self.cleanup_timeout
        while remaining := self._list(run_id):
            if self.clock() >= deadline:
                raise CleanupIncomplete(run_id, [name for _, name in remaining])
            self.sleep(self.poll_interval)
        return [name for _, name in targets]

    def leftovers(self) -> list[str]:
        names = []
        for plural in KIND_PLURALS.values():
            items = self.api.list_namespaced_custom_object(
                CHAOS_GROUP,
                CHAOS_VERSION,
                self.namespace,
                plural,
                label_selector=LABEL_KEY,
            )["items"]
            names += [i["metadata"]["name"] for i in items]
        return names


def inject(injector: Injector, run_id: str, step: int, fault: dict) -> str:
    return injector.apply(run_id, step, fault)

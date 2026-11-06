"""장애 주입 인터페이스 (구현은 E 파트 chaos/injector와 맞춘다).

규칙
- Chaos 리소스 이름은 실험 ID와 단계로 결정적으로 만든다 → 재시도해도 중복 생성되지 않는다.
- 모든 리소스에 LABEL_KEY=<run_id> 라벨을 붙인다 → 재시작 후 라벨로 찾아서 지운다.
"""

import re
from typing import Protocol

LABEL_KEY = "chaos/run-id"
CHAOS_GROUP = "chaos-mesh.org"
CHAOS_VERSION = "v1alpha1"
KIND_PLURALS = {"PodChaos": "podchaos", "NetworkChaos": "networkchaos"}


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

    def __init__(self, namespace: str = "exchange"):
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.api = client.CustomObjectsApi()
        self.ApiException = client.exceptions.ApiException
        self.namespace = namespace

    def apply(self, run_id: str, step: int, fault: dict) -> str:
        kind = fault["kind"]
        name = chaos_name(run_id, step)
        body = {
            "apiVersion": f"{CHAOS_GROUP}/{CHAOS_VERSION}",
            "kind": kind,
            "metadata": {"name": name, "labels": {LABEL_KEY: run_id}},
            "spec": fault["spec"],
        }
        try:
            self.api.create_namespaced_custom_object(
                CHAOS_GROUP, CHAOS_VERSION, self.namespace, KIND_PLURALS[kind], body
            )
        except self.ApiException as e:
            if e.status != 409:
                raise
        return name

    def cleanup(self, run_id: str) -> list[str]:
        deleted = []
        for plural in KIND_PLURALS.values():
            items = self.api.list_namespaced_custom_object(
                CHAOS_GROUP,
                CHAOS_VERSION,
                self.namespace,
                plural,
                label_selector=f"{LABEL_KEY}={run_id}",
            )["items"]
            for item in items:
                name = item["metadata"]["name"]
                try:
                    self.api.delete_namespaced_custom_object(
                        CHAOS_GROUP, CHAOS_VERSION, self.namespace, plural, name
                    )
                except self.ApiException as e:
                    if e.status != 404:
                        raise
                deleted.append(name)
        return deleted

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

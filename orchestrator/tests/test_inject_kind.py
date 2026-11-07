"""ChaosMeshInjector kind 통합 테스트 — 실제 Chaos Mesh에서 apply·cleanup을 확인한다.

기본 pytest 실행에서는 건너뛴다. 실행하려면 (Git Bash, 레포 루트):

    source orchestrator/.venv/Scripts/activate   # orchestrator 작업 전 항상
    cd orchestrator
    KIND_TESTS=1 python -m pytest tests/test_inject_kind.py -v

필요한 것: kind 클러스터(kind-chaos-lab) + Chaos Mesh. infra/local/README.md 참고.
테스트마다 새 Namespace를 만들고 끝나면 지운다. Chaos 리소스도 그 Namespace 안에만 만든다.
"""

import os
import time

import pytest

if os.environ.get("KIND_TESTS") != "1":
    pytest.skip("KIND_TESTS=1 일 때만 실행", allow_module_level=True)

k8s = pytest.importorskip("kubernetes", reason="pip install -e '.[k8s]' 필요")
from kubernetes.client.rest import ApiException  # noqa: E402

from orchestrator.inject import (  # noqa: E402
    LABEL_KEY,
    PROTECTED_LABEL,
    ChaosMeshInjector,
)

CONTEXT = os.environ.get("KIND_CONTEXT", "kind-chaos-lab")
IMAGE = "registry.k8s.io/pause:3.10"
READY_TIMEOUT = 180  # 이미지 pull 포함
INJECT_TIMEOUT = 60  # workbook: 주입 확인 ≤ 60초
CLEANUP_LIMIT = 60  # workbook: 정리 소요 ≤ 60초


def wait_until(predicate, timeout: float, interval: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture(scope="module")
def apis():
    if not CONTEXT.startswith("kind-"):
        pytest.fail(f"kind 클러스터에서만 실행한다 (KIND_CONTEXT={CONTEXT})")
    k8s.config.load_kube_config(context=CONTEXT)
    return k8s.client.CoreV1Api(), k8s.client.AppsV1Api(), k8s.client.CustomObjectsApi()


@pytest.fixture
def env(apis):
    """테스트 전용 Namespace에 일반 Pod 3개(app=target) + protected Pod 1개를 띄운다."""
    core, apps, custom = apis
    ns = f"injtest-{int(time.time() * 1000) % 10**9}"
    core.create_namespace({"metadata": {"name": ns}})
    try:
        apps.create_namespaced_deployment(
            ns,
            {
                "metadata": {"name": "target"},
                "spec": {
                    "replicas": 3,
                    "selector": {"matchLabels": {"app": "target"}},
                    "template": {
                        "metadata": {"labels": {"app": "target"}},
                        "spec": {
                            "terminationGracePeriodSeconds": 0,
                            "containers": [{"name": "pause", "image": IMAGE}],
                        },
                    },
                },
            },
        )
        core.create_namespaced_pod(
            ns,
            {
                "metadata": {
                    "name": "protected",
                    "labels": {"app": "target", PROTECTED_LABEL: "true"},
                },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{"name": "pause", "image": IMAGE}],
                },
            },
        )

        def all_ready():
            pods = core.list_namespaced_pod(ns, label_selector="app=target").items
            return len(pods) == 4 and all(
                p.status.container_statuses
                and all(c.ready for c in p.status.container_statuses)
                for p in pods
            )

        assert wait_until(all_ready, READY_TIMEOUT), "테스트 대상 Pod가 준비되지 않음"
        injector = ChaosMeshInjector(ns, api=custom, api_exception=ApiException)
        yield ns, core, custom, injector
    finally:
        # 남은 Chaos 리소스를 먼저 지우고 Namespace 삭제 (finalizer로 Namespace가 막히지 않게)
        for plural in ("podchaos", "networkchaos"):
            for item in custom.list_namespaced_custom_object(
                "chaos-mesh.org", "v1alpha1", ns, plural
            )["items"]:
                try:
                    custom.delete_namespaced_custom_object(
                        "chaos-mesh.org",
                        "v1alpha1",
                        ns,
                        plural,
                        item["metadata"]["name"],
                    )
                except ApiException:
                    pass
        core.delete_namespace(ns)


def uids(core, ns: str, selector: str) -> set[str]:
    return {
        p.metadata.uid
        for p in core.list_namespaced_pod(ns, label_selector=selector).items
    }


def pod_kill_all(ns: str) -> dict:
    # mode: all → 선택된 Pod는 모두 종료. 보호 조건이 없으면 protected Pod도 종료된다.
    return {
        "kind": "PodChaos",
        "spec": {
            "action": "pod-kill",
            "mode": "all",
            "gracePeriod": 0,
            "selector": {"namespaces": [ns], "labelSelectors": {"app": "target"}},
        },
    }


def test_apply_on_kind_excludes_protected_pod(env):
    """실제 Chaos Mesh에서 apply가 중복 없이 한 번만 만들고, protected Pod는 종료되지 않는다."""
    ns, core, custom, injector = env
    run_id = f"r-{ns}"
    targets_before = uids(core, ns, f"app=target,!{PROTECTED_LABEL}")
    protected_before = uids(core, ns, f"{PROTECTED_LABEL}=true")

    name = injector.apply(run_id, 0, pod_kill_all(ns))
    assert injector.apply(run_id, 0, pod_kill_all(ns)) == name  # 409 무시

    created = custom.list_namespaced_custom_object(
        "chaos-mesh.org",
        "v1alpha1",
        ns,
        "podchaos",
        label_selector=f"{LABEL_KEY}={run_id}",
    )["items"]
    assert [c["metadata"]["name"] for c in created] == [name]

    killed = wait_until(
        lambda: targets_before.isdisjoint(
            uids(core, ns, f"app=target,!{PROTECTED_LABEL}")
        ),
        INJECT_TIMEOUT,
    )
    assert killed, "일반 Pod가 종료되지 않음 (주입이 적용되지 않음)"
    assert (
        uids(core, ns, f"{PROTECTED_LABEL}=true") == protected_before
    ), "protected Pod가 종료됨"

    injector.cleanup(run_id)


def test_cleanup_on_kind_waits_and_is_idempotent(env):
    """실제 Chaos Mesh에서 cleanup이 리소스가 사라질 때까지 기다리고, 두 번 불러도 오류가 없다."""
    ns, core, custom, injector = env
    run_id = f"r-{ns}"
    name = injector.apply(run_id, 0, pod_kill_all(ns))
    time.sleep(5)  # 주입이 진행되도록 잠시 대기

    started = time.monotonic()
    assert injector.cleanup(run_id) == [name]
    elapsed = time.monotonic() - started
    print(f"cleanup 소요 {elapsed:.1f}s")
    assert elapsed < CLEANUP_LIMIT

    left = custom.list_namespaced_custom_object(
        "chaos-mesh.org",
        "v1alpha1",
        ns,
        "podchaos",
        label_selector=f"{LABEL_KEY}={run_id}",
    )["items"]
    assert left == []  # Terminating 상태로도 남아 있지 않음
    assert injector.cleanup(run_id) == []  # 멱등

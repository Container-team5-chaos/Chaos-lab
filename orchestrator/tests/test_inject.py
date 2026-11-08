"""ChaosMeshInjector 단위 테스트 (클러스터 없이 가짜 Kubernetes API로 검증)."""

import pytest

from orchestrator.cleanup import recover, safe_cleanup
from orchestrator.inject import (
    LABEL_KEY,
    PROTECTED_LABEL,
    ChaosMeshInjector,
    CleanupIncomplete,
    with_safety_selector,
)
from orchestrator.store import StateStore

FAULT = {
    "kind": "PodChaos",
    "spec": {
        "action": "pod-kill",
        "mode": "one",
        "selector": {"namespaces": ["exchange"], "labelSelectors": {"app": "order"}},
    },
}


class FakeApiException(Exception):
    def __init__(self, status: int):
        super().__init__(status)
        self.status = status


class FakeApi:
    """CustomObjectsApi 흉내. 삭제 요청 후 finalizer_polls번 조회될 때까지 Terminating으로 남는다."""

    def __init__(self, finalizer_polls: int = 0):
        self.objects: dict[tuple[str, str], dict] = {}
        self.finalizer_polls = finalizer_polls
        self.create_calls = 0

    def create_namespaced_custom_object(self, group, version, namespace, plural, body):
        self.create_calls += 1
        key = (plural, body["metadata"]["name"])
        if key in self.objects:
            raise FakeApiException(409)
        self.objects[key] = {"body": body, "deleting": None}

    def list_namespaced_custom_object(
        self, group, version, namespace, plural, label_selector
    ):
        # "key=value" 또는 "key"(라벨 존재) 둘 다 지원
        k, _, v = label_selector.partition("=")
        items = []
        for key, obj in list(self.objects.items()):
            if key[0] != plural:
                continue
            if obj["deleting"] is not None:
                if obj["deleting"] <= 0:
                    del self.objects[key]  # finalizer가 풀려 실제로 사라짐
                    continue
                obj["deleting"] -= 1
            labels = obj["body"]["metadata"]["labels"]
            if k in labels and (not v or labels[k] == v):
                items.append(obj["body"])
        return {"items": items}

    def delete_namespaced_custom_object(self, group, version, namespace, plural, name):
        obj = self.objects.get((plural, name))
        if obj is None:
            raise FakeApiException(404)
        if obj["deleting"] is None:
            obj["deleting"] = self.finalizer_polls


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


def make(api: FakeApi, timeout: float = 60.0):
    clock = FakeClock()
    injector = ChaosMeshInjector(
        api=api,
        api_exception=FakeApiException,
        cleanup_timeout=timeout,
        poll_interval=1.0,
        sleep=clock.sleep,
        clock=clock.now,
    )
    return injector, clock


# --- with_safety_selector ---------------------------------------------------


def expr_keys(spec):
    return [e["key"] for e in spec["selector"]["expressionSelectors"]]


def test_safety_selector_excludes_protected_and_k6():
    """보호 Pod(DB·Redis·stub)와 k6 Pod는 항상 제외된다."""
    spec = with_safety_selector(FAULT["spec"])
    assert PROTECTED_LABEL in expr_keys(spec)
    assert "app.kubernetes.io/name" in expr_keys(spec)


def test_safety_selector_keeps_selector_and_original():
    """기존 선택자는 유지하고, 원본 시나리오는 바꾸지 않는다."""
    spec = with_safety_selector(FAULT["spec"])
    assert spec["selector"]["labelSelectors"] == {"app": "order"}
    assert spec["mode"] == "one"
    assert "expressionSelectors" not in FAULT["spec"]["selector"]


def test_safety_selector_is_idempotent():
    """여러 번 적용해도 조건이 중복되지 않는다."""
    once = with_safety_selector(FAULT["spec"])
    assert with_safety_selector(once) == once


def test_safety_selector_keeps_custom_exprs_and_creates_selector():
    """시나리오의 기존 조건은 유지하고, selector가 없으면 만든다."""
    custom = {"key": "tier", "operator": "In", "values": ["api"]}
    spec = with_safety_selector({"selector": {"expressionSelectors": [custom]}})
    assert custom in spec["selector"]["expressionSelectors"]
    assert len(spec["selector"]["expressionSelectors"]) == 3
    assert len(with_safety_selector({})["selector"]["expressionSelectors"]) == 2


# --- apply -----------------------------------------------------------------


def test_apply_sends_label_and_safety_selector():
    """apply는 run-id 라벨과 제외 조건을 붙여 보낸다."""
    api = FakeApi()
    injector, _ = make(api)
    name = injector.apply("r-1", 0, FAULT)
    body = api.objects[("podchaos", name)]["body"]
    assert body["metadata"]["labels"] == {LABEL_KEY: "r-1"}
    assert PROTECTED_LABEL in expr_keys(body["spec"])


def test_apply_twice_ignores_409_and_keeps_one():
    """같은 이름으로 두 번 만들면 409를 무시하고 하나만 남는다."""
    api = FakeApi()
    injector, _ = make(api)
    assert injector.apply("r-1", 0, FAULT) == injector.apply("r-1", 0, FAULT)
    assert api.create_calls == 2
    assert len(api.objects) == 1


def test_apply_raises_non_409_errors():
    """409가 아닌 오류는 그대로 올린다."""

    class Broken(FakeApi):
        def create_namespaced_custom_object(self, *a, **kw):
            raise FakeApiException(500)

    injector, _ = make(Broken())
    with pytest.raises(FakeApiException):
        injector.apply("r-1", 0, FAULT)


# --- cleanup ---------------------------------------------------------------


def test_cleanup_waits_until_finalizer_released():
    """DELETE 후 finalizer가 풀려 실제로 사라질 때까지 기다린다."""
    api = FakeApi(finalizer_polls=3)
    injector, clock = make(api)
    injector.apply("r-1", 0, FAULT)
    assert injector.cleanup("r-1") == ["r-1-s0"]
    assert api.objects == {}
    assert clock.t > 0  # 바로 돌아오지 않고 기다렸다


def test_cleanup_twice_returns_empty_without_error():
    """두 번 불러도 오류 없이 빈 목록을 돌려준다 (멱등)."""
    api = FakeApi(finalizer_polls=2)
    injector, _ = make(api)
    injector.apply("r-1", 0, FAULT)
    injector.cleanup("r-1")
    assert injector.cleanup("r-1") == []


def test_cleanup_leaves_other_runs_untouched():
    """다른 run_id의 리소스는 건드리지 않는다."""
    api = FakeApi()
    injector, _ = make(api)
    injector.apply("r-1", 0, FAULT)
    injector.apply("r-2", 0, FAULT)
    assert injector.cleanup("r-1") == ["r-1-s0"]
    assert ("podchaos", "r-2-s0") in api.objects


def test_cleanup_ignores_404_race():
    """조회와 삭제 사이에 이미 지워진 리소스는 404를 무시한다."""

    class Racing(FakeApi):
        def delete_namespaced_custom_object(self, group, version, ns, plural, name):
            self.objects.pop((plural, name), None)  # 조회와 삭제 사이에 다른 쪽이 지움
            raise FakeApiException(404)

    api = Racing()
    injector, _ = make(api)
    injector.apply("r-1", 0, FAULT)
    assert injector.cleanup("r-1") == ["r-1-s0"]


def test_cleanup_raises_incomplete_after_timeout():
    """기한 안에 안 사라지면 CleanupIncomplete를 올리고 무기한 기다리지 않는다."""
    api = FakeApi(finalizer_polls=10_000)  # finalizer가 끝내 안 풀림
    injector, clock = make(api, timeout=5)
    injector.apply("r-1", 0, FAULT)
    with pytest.raises(CleanupIncomplete) as e:
        injector.cleanup("r-1")
    assert e.value.remaining == ["r-1-s0"]
    assert clock.t <= 6  # 무기한 기다리지 않는다


def test_leftovers_includes_terminating_until_cleanup_finishes():
    """삭제 중(Terminating)인 리소스도 leftovers에 보이고, cleanup이 끝나면 비어 있다."""
    api = FakeApi(finalizer_polls=3)
    injector, _ = make(api)
    injector.apply("r-1", 0, FAULT)
    api.delete_namespaced_custom_object(
        "chaos-mesh.org", "v1alpha1", "exchange", "podchaos", "r-1-s0"
    )
    assert injector.leftovers() == ["r-1-s0"]
    injector.cleanup("r-1")
    assert injector.leftovers() == []


def test_safe_cleanup_records_incomplete_on_timeout():
    """기한 초과는 runner의 safe_cleanup에서 INCOMPLETE로 기록된다 (예외가 밖으로 나가지 않음)."""
    api = FakeApi(finalizer_polls=10_000)
    injector, _ = make(api, timeout=3)
    injector.apply("r-1", 0, FAULT)
    status, removed, err = safe_cleanup(injector, "r-1")
    assert status == "INCOMPLETE"
    assert removed == []
    assert "CleanupIncomplete" in err


def test_incomplete_cleanup_is_recorded_and_next_recover_retries(tmp_path):
    """정리에 실패하면 cleanup=INCOMPLETE로 남고, 다음 recover가 다시 정리해 OK가 된다."""
    store = StateStore(str(tmp_path / "state.json"))
    store.save({"run_id": "r-1", "phase": "OBSERVING", "history": []})
    api = FakeApi(finalizer_polls=10_000)
    injector, _ = make(api, timeout=3)
    injector.apply("r-1", 0, FAULT)

    state = recover(store, injector)
    assert state["phase"] == "ABORTED"
    assert state["cleanup"] == "INCOMPLETE"  # 다음 run의 previous_cleanup 게이트가 막음

    api.objects[("podchaos", "r-1-s0")]["deleting"] = 0  # finalizer가 풀림
    state = recover(store, injector)
    assert (
        state["cleanup"] == "OK"
    )  # 이미 사라졌으므로 이번에 지운 목록은 비어 있어도 OK
    assert injector.leftovers() == []
    assert api.objects == {}

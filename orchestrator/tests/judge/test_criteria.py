"""판정 기준 병합·검증(설계 6)."""

import hashlib
import textwrap
from pathlib import Path

import pytest

from orchestrator.judge.criteria import (
    RESOLVED_NAME,
    duration_ms,
    integrity_cfg,
    load_criteria,
    write_resolved,
)
from orchestrator.judge.errors import InputError
from orchestrator.judge.model import IntegrityCfg

REPO = Path(__file__).resolve().parents[3]
CRITERIA_DIR = REPO / "experiments" / "criteria"
CRITERIA_FILES = [
    "default.yaml",
    "order-api-pod-kill.yaml",
    "order-api-crash-after-commit.yaml",
    "demo-min.yaml",
]

BASE = """\
schema_version: 1
safety: {symbol_prefix: "REPLAY:UPBIT:", account_mode: replay}
phases: {warmup: 60s, baseline: {before: inject_start, duration: 150s, guard: 2s},
         fault: {from: inject_start, to: inject_end, extra: 60s},
         tail: {before: k6_end, duration: 120s}}
coverage: {min_filled: {baseline: 100, tail: 100}, min_sell_filled: 20, min_achieved_ratio: 0.98}
integrity: {response_fields: {status: status, reject_reason: reason, price: price, fee: fee},
            response_fields_confirmed: false, price_max_age_ms: 60000}
guards: [{id: safety}, {id: fault_effect}]
checks:
  - {id: lost, kind: integrity, class: [LOST], op: '==', value: 0}
  - {id: p95, kind: client, stat: ack_p95_ms, op: '<', value: 300, phases: [baseline, tail]}
"""


def put(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return p


def ids(items):
    return [i["id"] for i in items]


# ---------------------------------------------------------------- 저장소 기준 파일 4개


@pytest.mark.parametrize("name", CRITERIA_FILES)
def test_repo_criteria_load_and_deterministic(name, tmp_path):
    a = load_criteria(CRITERIA_DIR / name)
    b = load_criteria(CRITERIA_DIR / name)
    assert a.sha256 == b.sha256
    assert a.text == b.text
    assert a.sha256 == hashlib.sha256(a.text.encode("utf-8")).hexdigest()
    # resolved를 다시 읽으면 같은 내용·같은 해시(evaluate는 resolved만 읽는다, 5.2)
    out = write_resolved(a, tmp_path)
    assert out.name == RESOLVED_NAME
    again = load_criteria(out)
    assert again.sha256 == a.sha256
    assert again.data == a.data


def test_repo_criteria_hashes_differ():
    shas = {load_criteria(CRITERIA_DIR / n).sha256 for n in CRITERIA_FILES}
    assert len(shas) == len(CRITERIA_FILES)


def test_default_contents():
    c = load_criteria(CRITERIA_DIR / "default.yaml")
    assert ids(c.checks) == [
        "lost",
        "mismatch",
        "duplicates",
        "orphans",
        "invariants",
        "i7_stale_fill",
        "arithmetic",
        "protocol",
        "order_p95_k6",
        "order_err_k6",
        "k6_exit",
    ]
    assert ids(c.guards) == [
        "safety",
        "account_set",
        "fill_coverage",
        "sell_coverage",
        "load_coverage",
        "fault_in_load",
        "fault_effect",
        "db_quiescent",
    ]
    assert c.fault_effect == ()
    assert c.item("lost")["required"] is True
    assert c.item("lost")["on_violation"] == "fail"
    assert c.item("arithmetic")["required"] is False
    assert c.item("protocol")["on_violation"] == "inconclusive"
    assert c.item("safety")["on_violation"] == "inconclusive"
    assert c.item("safety")["kind"] == "guard"
    assert c.data["require_extra_accounts"] is False
    assert c.chain == ("default.yaml",)


def test_pod_kill_contents():
    c = load_criteria(CRITERIA_DIR / "order-api-pod-kill.yaml")
    assert ids(c.checks)[-2:] == ["unresolved_zero", "order_recovery_k6"]
    assert ids(c.fault_effect) == ["pod_replaced"]
    assert c.item("pod_replaced")["on_violation"] == "inconclusive"
    assert c.item("order_recovery_k6")["required"] is False
    assert "op" not in c.item("order_recovery_k6")


def test_crash_after_commit_contents():
    c = load_criteria(CRITERIA_DIR / "order-api-crash-after-commit.yaml")
    assert c.chain == (
        "default.yaml",
        "order-api-pod-kill.yaml",
        "order-api-crash-after-commit.yaml",
    )
    assert c.disabled == ("pod_replaced", "unresolved_zero")
    assert c.item("unresolved_zero") is None
    assert c.item("pod_replaced") is None
    assert ids(c.fault_effect) == ["crash_restart", "crash_seen_by_k6"]
    assert c.item("crash_seen_by_k6")["class_sub"] == ["after_commit", "lookup"]
    assert c.item("crash_seen_by_k6")["account"] == "idx0"
    assert c.item("order_recovery_k6") is not None  # pod-kill에서 상속
    assert ids(c.checks)[-1] == "unresolved_committed"


def test_demo_min_field_level_override():
    c = load_criteria(CRITERIA_DIR / "demo-min.yaml")
    p95 = c.item("order_p95_k6")
    assert p95["required"] is False
    assert p95["stat"] == "ack_p95_ms"  # 나머지 필드는 상속
    assert p95["value"] == 300
    assert p95["phases"] == ["baseline", "tail", "steady"]
    assert c.item("order_err_k6")["required"] is False
    assert c.data["require_extra_accounts"] is True
    assert ids(c.fault_effect) == ["presenter_crash"]


def test_integrity_cfg():
    cfg = integrity_cfg(load_criteria(CRITERIA_DIR / "default.yaml"))
    assert cfg == IntegrityCfg(
        response_fields={
            "fee": "fee",
            "price": "price",
            "reject_reason": "reason",
            "status": "status",
        },
        response_fields_confirmed=False,
        price_max_age_ms=60000,
    )


def test_resolved_text_sorted_and_flat():
    c = load_criteria(CRITERIA_DIR / "order-api-crash-after-commit.yaml")
    top = [
        ln.split(":")[0]
        for ln in c.text.splitlines()
        if ln and not ln[0].isspace() and ln[0] != "-"
    ]
    assert top == sorted(top)
    assert "extends" not in c.data
    assert "disable" not in c.data
    assert "replace" not in c.text


# ---------------------------------------------------------------- 병합 규칙


def test_map_deep_merge(tmp_path):
    put(tmp_path, "base.yaml", BASE)
    child = put(
        tmp_path,
        "child.yaml",
        """\
        extends: base.yaml
        coverage: {min_filled: {tail: 5, steady: 7}}
        integrity: {response_fields_confirmed: true}
        """,
    )
    c = load_criteria(child)
    assert c.data["coverage"]["min_filled"] == {"baseline": 100, "tail": 5, "steady": 7}
    assert c.data["coverage"]["min_sell_filled"] == 20
    assert c.data["integrity"]["response_fields_confirmed"] is True
    assert c.data["integrity"]["response_fields"]["reject_reason"] == "reason"


def test_map_replace(tmp_path):
    put(tmp_path, "base.yaml", BASE)
    child = put(
        tmp_path,
        "child.yaml",
        """\
        extends: base.yaml
        coverage:
          replace: true
          min_filled: {steady: 1}
          min_sell_filled: 0
          min_achieved_ratio: 0.5
        """,
    )
    c = load_criteria(child)
    assert c.data["coverage"] == {
        "min_filled": {"steady": 1},
        "min_sell_filled": 0,
        "min_achieved_ratio": 0.5,
    }


def test_items_field_merge_append_and_replace(tmp_path):
    put(tmp_path, "base.yaml", BASE)
    child = put(
        tmp_path,
        "child.yaml",
        """\
        extends: base.yaml
        checks:
          - {id: p95, value: 500}
          - {id: lost, replace: true, kind: integrity, sql: [i2], op: '==', value: 0}
          - {id: new_one, kind: unresolved, class: [PROTOCOL], op: '==', value: 0}
        """,
    )
    c = load_criteria(child)
    assert ids(c.checks) == ["lost", "p95", "new_one"]  # 부모 순서 유지, 새 id는 뒤
    assert c.item("p95")["value"] == 500
    assert c.item("p95")["phases"] == ["baseline", "tail"]
    assert c.item("lost") == {
        "id": "lost",
        "kind": "integrity",
        "sql": ["i2"],
        "op": "==",
        "value": 0,
        "required": True,
        "on_violation": "fail",
    }


def test_disable(tmp_path):
    put(tmp_path, "base.yaml", BASE)
    child = put(
        tmp_path, "child.yaml", "extends: base.yaml\ndisable: [p95, fault_effect]\n"
    )
    c = load_criteria(child)
    assert ids(c.checks) == ["lost"]
    assert ids(c.guards) == ["safety"]
    assert c.disabled == ("fault_effect", "p95")


def test_disable_unknown_id(tmp_path):
    put(tmp_path, "base.yaml", BASE)
    child = put(tmp_path, "child.yaml", "extends: base.yaml\ndisable: [nope]\n")
    with pytest.raises(InputError, match="nope"):
        load_criteria(child)


def test_three_level_chain_and_absolute_extends(tmp_path):
    base = put(tmp_path, "base.yaml", BASE)
    sub = tmp_path / "sub"
    sub.mkdir()
    put(sub, "mid.yaml", f"extends: {base}\nchecks: [{{id: p95, value: 400}}]\n")
    leaf = put(
        sub, "leaf.yaml", "extends: mid.yaml\nchecks: [{id: p95, required: false}]\n"
    )
    c = load_criteria(leaf)
    assert c.chain == ("base.yaml", "mid.yaml", "leaf.yaml")
    assert c.item("p95")["value"] == 400
    assert c.item("p95")["required"] is False


def test_hash_ignores_chain_layout(tmp_path):
    """같은 결과면 사슬 모양이 달라도 해시가 같다."""
    one = put(tmp_path, "one.yaml", BASE)
    put(tmp_path, "base.yaml", BASE)
    two = put(tmp_path, "two.yaml", "extends: base.yaml\n")
    assert load_criteria(one).sha256 == load_criteria(two).sha256


def test_cycle_detected(tmp_path):
    put(tmp_path, "a.yaml", "extends: b.yaml\n")
    put(tmp_path, "b.yaml", "extends: a.yaml\n")
    with pytest.raises(InputError, match="순환"):
        load_criteria(tmp_path / "a.yaml")


def test_self_cycle(tmp_path):
    p = put(tmp_path, "self.yaml", "extends: self.yaml\n")
    with pytest.raises(InputError, match="순환"):
        load_criteria(p)


def test_missing_parent(tmp_path):
    p = put(tmp_path, "c.yaml", "extends: nowhere.yaml\n")
    with pytest.raises(InputError):
        load_criteria(p)


# ---------------------------------------------------------------- 거부


@pytest.mark.parametrize(
    "patch",
    [
        "surprise: 1\n",  # 모르는 최상위 키
        "replace: true\n",  # replace는 맵·항목 안에만
        "integrity: {foo: 1}\n",  # 모르는 하위 키
        "phases: {fault: {extra: 1m, until: x}}\n",
        "schema_version: 2\n",
        "checks: [{id: lost, kind: integrity, class: [LOST], sql: [i2]}]\n",  # class와 sql 둘 다
        "checks: [{id: x1, kind: integrity, op: '==', value: 0}]\n",  # class|sql|check 없음
        "checks: [{id: x1, kind: integrity, class: [LOST]}]\n",  # op·value 없음
        "checks: [{id: x1, kind: integrity, class: [LOSTT], op: '==', value: 0}]\n",
        "checks: [{id: x1, kind: integrity, sql: [i99], op: '==', value: 0}]\n",
        "checks: [{id: x1, kind: integrity, check: i8, op: '==', value: 0}]\n",
        "checks: [{id: x1, kind: integrity, class: [LOST], op: '=', value: 0}]\n",
        "checks: [{id: x1, kind: integrity, class: [LOST], op: '==', value: zero}]\n",
        "checks: [{id: x1, kind: integrity, class: [LOST], op: '==', value: 0, stat: x}]\n",
        "checks: [{id: x1, kind: wat}]\n",
        "checks: [{id: x1}]\n",  # 새 id인데 kind 없음
        "checks: [{id: x1, kind: client, stat: p99, op: '<', value: 1}]\n",
        "checks: [{id: x1, kind: client, stat: ack_p95_ms}]\n",  # recovery만 op 생략 가능
        "checks: [{id: x1, kind: client, stat: recovery, phases: [later]}]\n",
        "checks: [{id: x1, kind: unresolved, op: '==', value: 0}]\n",
        "checks: [{id: x1, kind: k6, check: oom}]\n",
        "checks: [{id: p95, required: maybe}]\n",
        "checks: [{id: p95, on_violation: warn}]\n",
        "checks: [{id: Bad-Id, kind: k6, check: exit_code}]\n",
        "checks: [{id: x1, kind: k6, check: exit_code}, {id: x1, kind: k6, check: exit_code}]\n",
        "checks: [{id: safety, kind: k6, check: exit_code}]\n",  # 가드 id와 겹침
        "checks: [{id: p95, replace: yes please}]\n",
        "checks: {id: p95}\n",
        "guards: [{id: unknown_guard}]\n",
        "guards: [{id: safety, kind: integrity}]\n",
        "fault_effect: [{id: f1, observe: pod_replaced}]\n",  # target 없음
        "fault_effect: [{id: f1, observe: node_drain, target: x}]\n",
        "fault_effect: [{id: f1, target: x}]\n",  # observe|class_sub 없음
        "fault_effect: [{id: f1, observe: crash_restart, target: x, class_sub: [lookup]}]\n",
        "fault_effect: [{id: f1, class_sub: [lookup]}]\n",  # op·value 없음
        "fault_effect: [{id: f1, class_sub: [clean2], op: '>=', value: 1}]\n",
        "fault_effect: [{id: f1, class_sub: [lookup], op: '>=', value: 1, account: idxA}]\n",
        "fault_effect: [{id: f1, observe: crash_restart, target: x, within_s: 0}]\n",
        "require_extra_accounts: yes please\n",
        "disable: lost\n",
    ],
)
def test_rejects(tmp_path, patch):
    put(tmp_path, "base.yaml", BASE)
    child = put(tmp_path, "child.yaml", "extends: base.yaml\n" + patch)
    with pytest.raises(InputError):
        load_criteria(child)


@pytest.mark.parametrize("section", ["safety", "phases", "coverage", "integrity"])
def test_missing_required_section(tmp_path, section):
    put(tmp_path, "base.yaml", BASE)
    child = put(
        tmp_path,
        "child.yaml",
        f"extends: base.yaml\n{section}: {{replace: true}}\n",
    )
    with pytest.raises(InputError):
        load_criteria(child)


def test_missing_schema_version(tmp_path):
    p = put(tmp_path, "c.yaml", BASE.replace("schema_version: 1\n", ""))
    with pytest.raises(InputError, match="schema_version"):
        load_criteria(p)


def test_duplicate_yaml_key(tmp_path):
    p = put(tmp_path, "c.yaml", BASE + "schema_version: 1\n")
    with pytest.raises(InputError, match="중복"):
        load_criteria(p)


@pytest.mark.parametrize("text", ["", "- a\n- b\n", "a: 1\n---\nb: 2\n", "a: [\n"])
def test_bad_documents(tmp_path, text):
    with pytest.raises(InputError):
        load_criteria(put(tmp_path, "c.yaml", text))


def test_duration_ms():
    assert duration_ms("60s") == 60_000
    assert duration_ms("500ms") == 500
    assert duration_ms("2m") == 120_000
    for bad in ("60", "1h", "1.5s", 60):
        with pytest.raises(InputError):
            duration_ms(bad)

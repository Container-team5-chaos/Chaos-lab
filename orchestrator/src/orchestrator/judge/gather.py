"""collect 구현: fetch → DB 스냅샷 → Pod 관측 → RELEASE 순서는 설계 5.2를 따른다.

모듈 이름을 collect로 하면 패키지의 collect 함수를 가려서 gather로 둔다.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from . import fetch as fetch_mod
from .criteria import RESOLVED_NAME, integrity_cfg, write_resolved
from .db import build_bundle, exec_bundle, parse_bundle_output
from .model import Criteria, RunContext, sha256_bytes
from .transport import TransportError, Transports

__all__ = ["collect", "observe_pods", "pod_counts", "snapshot_db", "write_evidence"]

QUIET_S = 10  # 늦게 끝난 주문 트랜잭션을 기다린다
RETRY_S = 30
ORDER_SELECTOR = "app.kubernetes.io/name=order"  # workloads/order/chart 라벨

DB_DIR = "raw/db"
SNAPSHOT_NAME = "snapshot.txt"
PSQL_STDERR_NAME = "psql-stderr.txt"
K8S_DIR = "raw/k8s"
PODS_NAME = "pods.json"
OBS_NAME = "obs.json"
EVIDENCE_NAME = "evidence.json"
EVIDENCE_DIRS = ("k6", "raw")


def collect(
    ctx: RunContext,
    crit: Criteria,
    tr: Transports,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> Path:
    """증거를 runs/<run_id>/에 모으고 그 폴더를 돌려준다."""
    run_dir = Path(ctx.run_dir)
    fetched = fetch_mod.fetch(ctx, tr, run_dir)
    sleep(QUIET_S)
    snapshot_db(ctx, crit, tr, run_dir, sleep=sleep)
    observe_pods(ctx, crit, tr, run_dir)
    try:
        fetch_mod.release(tr, fetched.pod)
    except TransportError:
        pass  # 못 풀어도 holder가 HOLD_S 뒤 스스로 끝난다
    write_resolved(crit, run_dir)
    write_evidence(run_dir)
    return run_dir


def snapshot_db(
    ctx: RunContext,
    crit: Criteria,
    tr: Transports,
    run_dir: Path,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """psql 1회, 지문 F1 ≠ F2면 RETRY_S 뒤 1회 더. 마지막 출력을 snapshot.txt에 쓴다."""
    bundle = build_bundle(ctx, stale_ms=integrity_cfg(crit).stale_ms)
    out = run_dir / DB_DIR
    out.mkdir(parents=True, exist_ok=True)
    for attempt in (1, 2):
        try:
            res = exec_bundle(tr, bundle, target=ctx.db)
        except TransportError as e:  # 스냅샷이 없으면 evaluate가 DB_ERROR로 본다
            (out / PSQL_STDERR_NAME).write_text(f"{e}\n", encoding="utf-8")
            return
        (out / SNAPSHOT_NAME).write_text(res.stdout, encoding="utf-8")
        _write_or_drop(out / PSQL_STDERR_NAME, res.stderr)
        if attempt == 2 or parse_bundle_output(res.stdout).quiescent:
            return
        sleep(RETRY_S)


def observe_pods(
    ctx: RunContext, crit: Criteria, tr: Transports, run_dir: Path
) -> dict[str, Any]:
    """주문 서비스 Pod를 한 번 조회해 pods.json과 obs.json을 쓴다."""
    out = run_dir / K8S_DIR
    out.mkdir(parents=True, exist_ok=True)
    obs: dict[str, Any] = {"selector": ORDER_SELECTOR}
    try:
        cp = tr.kubectl("get", "pods", "-l", ORDER_SELECTOR, "-o", "json")
    except TransportError as e:
        obs["error"] = str(e)
    else:
        if cp.returncode != 0:
            err = (cp.stderr or b"").decode("utf-8", errors="replace").strip()
            obs["error"] = err[-500:] or f"kubectl 종료 코드 {cp.returncode}"
        else:
            (out / PODS_NAME).write_bytes(cp.stdout)
            items = json.loads(cp.stdout)["items"]
            within = _within_ms(crit, "pod_replaced")
            obs.update(pod_counts(items, ctx.inject_start, ctx.inject_end, within))
    _write_json(out / OBS_NAME, obs)
    return obs


def pod_counts(
    items: Iterable[Mapping[str, Any]],
    inject_start: int | None,
    inject_end: int | None,
    within_ms: int | None = None,
) -> dict[str, Any]:
    """pod_replaced(장애 뒤 새로 뜬 Ready Pod)와 crash_restart(장애 중 비정상 종료) 수."""
    if inject_start is None:
        return {"note": "marks.inject_start가 없어 Pod 관측값을 만들지 않았다"}
    # k8s 시각은 초 단위로 잘려서, 같은 초에 뜬 교체 Pod가 장애 전으로 보이지 않게 내린다
    start_s = inject_start // 1000 * 1000
    created_by = None if within_ms is None else inject_start + within_ms
    replaced = crashed = 0
    for pod in items:
        status = pod["status"]
        created = _ms(pod["metadata"]["creationTimestamp"])
        if (
            created >= start_s
            and (created_by is None or created <= created_by)
            and _ready(status)
        ):
            replaced += 1
        for c in status.get("containerStatuses", ()):
            term = c.get("lastState", {}).get("terminated")
            if term and term["exitCode"] != 0:
                finished = _ms(term["finishedAt"])
                if finished >= start_s and (
                    inject_end is None or finished <= inject_end
                ):
                    crashed += 1
    out: dict[str, Any] = {"pod_replaced": replaced, "crash_restart": crashed}
    if within_ms is not None:
        out["pod_replaced_within_ms"] = within_ms
    return out


def write_evidence(run_dir: Path) -> Path:
    """k6/·raw/ 파일과 criteria.resolved.yaml의 sha256을 경로 순으로 쓴다."""
    paths = [p for d in EVIDENCE_DIRS for p in (run_dir / d).rglob("*") if p.is_file()]
    resolved = run_dir / RESOLVED_NAME
    if resolved.is_file():
        paths.append(resolved)
    files = {
        p.relative_to(run_dir).as_posix(): sha256_bytes(p.read_bytes()) for p in paths
    }
    path = run_dir / EVIDENCE_NAME
    _write_json(path, {"files": dict(sorted(files.items()))})
    return path


def _within_ms(crit: Criteria, observe: str) -> int | None:
    """같은 observe를 쓰는 fault_effect 항목의 가장 작은 within_s(ms)."""
    vals = [
        int(item["within_s"]) * 1000
        for item in crit.data.get("fault_effect") or ()
        if item.get("observe") == observe and "within_s" in item
    ]
    return min(vals) if vals else None


def _ready(status: Mapping[str, Any]) -> bool:
    return any(
        c["type"] == "Ready" and c["status"] == "True"
        for c in status.get("conditions", ())
    )


def _ms(ts: str) -> int:
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


def _write_or_drop(path: Path, text: str) -> None:
    if text:
        path.write_text(text, encoding="utf-8")
    else:
        path.unlink(missing_ok=True)  # 첫 시도의 stderr가 남지 않게


def _write_json(path: Path, doc: Mapping[str, Any]) -> None:
    text = json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")

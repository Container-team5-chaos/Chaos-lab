"""k6 기록 회수와 정지(설계 4.8). 1차.

`judge fetch` 순서(설계 4.8)
1. `-l batch.kubernetes.io/job-name=<k6.job>`으로 Pod를 찾고 k6 컨테이너가 모두
   terminated가 될 때까지 기다린다. exitCode·reason을 k6/pod-status.json에 쓴다.
   2단계 sha256sum 목록에 STOP_REQUESTED가 있으면 stop_requested: true도 적는다(105 인정).
2. `exec -c holder -- sh -c 'sha256sum /out/*'`(출력은 k6/sha256sum.txt에 그대로),
   `exec -c holder -- gzip -c /out/orders.jsonl`(k6/orders.jsonl.gz). summary.json도 받는다.
   `kubectl cp`(tar 의존)는 쓰지 않는다. kubectl exec는 셸을 거치지 않아 글롭은 Pod 안 sh가 푼다.
3. 받은 것을 풀어 sha256을 비교한다. 다르거나 받지 못하면 2단계를 FETCH_ATTEMPTS번까지 다시 한다.
   - 끝내 받지 못하면 기록 파일을 쓰지 않는다 → evaluate가 SENT_LOG_MISSING.
   - 받았지만 끝내 다르면 마지막 것을 남긴다 → evaluate가 sha_mismatch(기록 불완전)로 본다.
4. `exec -c holder -- date -u +%s`를 RTT로 감싸 노드 시계 차이를 derived.json에 남긴다.
   실패하면 derived.json을 쓰지 않는다(시계 차이 모름).
5. DB 수집 뒤 `exec -c holder -- touch /out/RELEASE`(release). fetch는 하지 않는다.

fetch 전에 `kubectl delete job`을 하면 emptyDir와 함께 기록이 사라진다(설계 3).
"""

from __future__ import annotations

import gzip
import json
import re
import subprocess
import time
import zlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import JudgeError
from .model import RunContext, sha256_bytes
from .transport import Transports, TransportError

__all__ = [
    "ClockOffset",
    "FetchResult",
    "PodStatus",
    "fetch",
    "find_pod",
    "measure_clock",
    "release",
    "remote_sha256",
    "stop",
    "copy_gz",
    "wait_terminated",
]

JOB_NAME_LABEL = "batch.kubernetes.io/job-name"
K6_CONTAINER = "k6"
K6_WS_CONTAINER = "k6-ws"
HOLDER_CONTAINER = "holder"

REMOTE_OUT = "/out"
ORDERS_LOG = "orders.jsonl"
WS_LOG = "ws.jsonl"
SUMMARY = "summary.json"
RELEASE_PATH = "/out/RELEASE"
# 정지 표시(k6load.STOP_FLAG). SIGINT로 멈춘 k6는 105로 끝난다. 이 파일이 있으면 우리가 멈춘 것
STOP_FLAG = "STOP_REQUESTED"

# runs/<run_id>/ 아래 산출물(설계 5.2). evaluate(pipeline)가 이 이름으로 읽는다
LOCAL_K6_DIR = "k6"
POD_STATUS_NAME = "pod-status.json"  # {"pod", "containers", "stop_requested"}
DERIVED_NAME = "derived.json"  # {"offset_ms", "rtt_ms"}(ClockOffset)
REMOTE_SHA_NAME = "sha256sum.txt"  # 원격 `sha256sum /out/*` 출력 그대로(2단계)
LOCAL_ORDERS_GZ = "orders.jsonl.gz"

FETCH_ATTEMPTS = 3  # 4.8 3단계
CLOCK_SKEW_MAX_MS = 2000  # 5.5 시간 기준 CLOCK_SKEW 경계
WAIT_TIMEOUT_S = 600.0  # 1단계 최대 대기(우리 설정값)
POLL_S = 2.0
COPY_TIMEOUT_S = 300.0  # gzip 전송 한 번(우리 설정값)

_SHA_LINE = re.compile(r"^([0-9a-fA-F]{64})\s+\*?(.+)$")  # pipeline._SHA_LINE과 같다


@dataclass(frozen=True)
class PodStatus:
    """k6 Pod 컨테이너 종료 상태(설계 4.8 1단계). containers: 이름 → {exitCode, reason}."""

    pod: str
    containers: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict, hash=False
    )


@dataclass(frozen=True)
class ClockOffset:
    """노드 시계 − 노트북 시계(설계 4.8 4단계, 5.5).

    offset_ms: 노드 `date -u +%s`(초)에 500ms를 더해 그 초의 가운데로 놓고,
      RTT 중앙 노트북 시각을 뺀 값.
    rtt_ms: exec 왕복 시간. 초 단위 해상도라 오차는 ±(500 + rtt/2)ms다.
    """

    offset_ms: int
    rtt_ms: int


@dataclass(frozen=True)
class FetchResult:
    """회수 결과(설계 4.8).

    files: 로컬에 쓴 기록 이름 → 압축 해제 기준 sha256. orders.jsonl이 없으면 회수 실패다.
    remote: 원격 sha256sum 결과(파일 이름 → sha256). 받지 못했으면 비어 있다.
    sha_ok: orders.jsonl의 로컬·원격 sha256이 같다.
    attempts: 2–3단계를 돈 횟수. errors: 실패한 시도와 단계의 사유.
    """

    pod: str
    k6_dir: Path
    pod_status: PodStatus
    files: Mapping[str, str] = field(default_factory=dict, hash=False)
    remote: Mapping[str, str] = field(default_factory=dict, hash=False)
    sha_ok: bool = False
    attempts: int = 0
    errors: tuple[str, ...] = ()
    clock: ClockOffset | None = None
    stop_requested: bool = False  # 원격 /out에 STOP_FLAG가 있었다


def _check(cp: subprocess.CompletedProcess[bytes], what: str) -> bytes:
    """0이 아닌 종료 코드를 TransportError로 바꾼다. stdout을 돌려준다."""
    if cp.returncode != 0:
        err = (cp.stderr or b"").decode("utf-8", errors="replace").strip()[-300:]
        raise TransportError(f"{what}: 종료 코드 {cp.returncode} {err}".rstrip())
    return cp.stdout or b""


def _exec(
    tr: Transports, pod: str, container: str, *cmd: str, timeout: float | None = None
) -> bytes:
    cp = tr.run(tr.exec_argv(pod, container, *cmd), timeout=timeout)
    return _check(cp, f"{container}: {' '.join(cmd)}")


def _json_obj(data: bytes, what: str) -> dict[str, Any]:
    try:
        doc = json.loads(data)
    except ValueError as e:
        raise TransportError(f"{what}: JSON이 아니다") from e
    if not isinstance(doc, dict):
        raise TransportError(f"{what}: JSON 객체가 아니다")
    return doc


def _write_json(path: Path, obj: Any) -> None:
    text = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def find_pod(ctx: RunContext, tr: Transports) -> str:
    """`-l batch.kubernetes.io/job-name=<ctx.k6_job>`인 Pod 이름(설계 4.8 1단계).

    Pod가 0개이거나 2개 이상이면(재실행 흔적, 4.8) 오류다.
    """
    sel = f"{JOB_NAME_LABEL}={ctx.k6_job}"
    out = _check(tr.kubectl("get", "pods", "-l", sel, "-o", "json"), f"get pods {sel}")
    items = _json_obj(out, f"get pods {sel}").get("items") or []
    names = sorted(
        str((i.get("metadata") or {}).get("name") or "")
        for i in items
        if isinstance(i, dict)
    )
    if not names:
        raise JudgeError(f"{ctx.k6_job}: k6 Pod가 없다(Job이 없거나 지워졌다)")
    if len(names) > 1:
        raise JudgeError(
            f"{ctx.k6_job}: k6 Pod가 {len(names)}개다({', '.join(names)}). "
            "재실행 흔적이라 기록을 고를 수 없다(설계 4.8)"
        )
    return names[0]


def _terminated(doc: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Pod JSON에서 terminated인 컨테이너 → {exitCode, reason}."""
    out: dict[str, dict[str, Any]] = {}
    for s in (doc.get("status") or {}).get("containerStatuses") or []:
        if not isinstance(s, dict):
            continue
        t = (s.get("state") or {}).get("terminated")
        if isinstance(t, dict):
            out[str(s.get("name"))] = {
                "exitCode": t.get("exitCode"),
                "reason": t.get("reason"),
            }
    return out


def wait_terminated(
    ctx: RunContext,
    tr: Transports,
    pod: str,
    *,
    containers: Sequence[str] = (K6_CONTAINER,),
    timeout_s: float = WAIT_TIMEOUT_S,
    poll_s: float = POLL_S,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> PodStatus:
    """k6 컨테이너가 모두 terminated가 될 때까지 기다린다(설계 4.8 1단계).

    timeout_s 안에 끝나지 않으면 오류다(k6가 아직 돈다. judge stop 뒤 다시 fetch한다).
    """
    deadline = clock() + timeout_s
    while True:
        out = _check(tr.kubectl("get", "pod", pod, "-o", "json"), f"get pod {pod}")
        done = _terminated(_json_obj(out, f"get pod {pod}"))
        left = [c for c in containers if c not in done]
        if not left:
            return PodStatus(pod=pod, containers={c: done[c] for c in containers})
        if clock() >= deadline:
            raise JudgeError(
                f"{ctx.k6_job}: {timeout_s:g}s 안에 {', '.join(left)} 컨테이너가 "
                "끝나지 않았다. judge stop 뒤 다시 fetch한다"
            )
        sleep(poll_s)


def remote_sha256(tr: Transports, pod: str) -> tuple[str, dict[str, str]]:
    """`exec -c holder -- sh -c 'sha256sum /out/*'`(설계 4.8 2단계).

    (출력 원문, 파일 이름 → sha256). orders.jsonl 줄이 없으면 TransportError다.
    """
    out = _exec(tr, pod, HOLDER_CONTAINER, "sh", "-c", f"sha256sum {REMOTE_OUT}/*")
    text = out.decode("utf-8", errors="replace")
    sums: dict[str, str] = {}
    for line in text.splitlines():
        m = _SHA_LINE.match(line.strip())
        if m:
            sums[m.group(2).strip().rsplit("/", 1)[-1]] = m.group(1).lower()
    if ORDERS_LOG not in sums:
        raise TransportError(f"{REMOTE_OUT}/{ORDERS_LOG}의 sha256이 없다")
    return text, sums


def copy_gz(tr: Transports, pod: str, name: str) -> tuple[bytes, bytes]:
    """`exec -c holder -- gzip -c /out/<name>`을 한 번 받는다. (받은 gzip, 푼 원본).

    전송이 잘리면 gzip 끝의 길이·CRC가 맞지 않아 풀리지 않는다 → TransportError.
    다시 받기와 sha256 비교는 fetch가 한다(설계 4.8 2–3단계).
    """
    gz = _exec(
        tr, pod, HOLDER_CONTAINER, "gzip", "-c", f"{REMOTE_OUT}/{name}",
        timeout=COPY_TIMEOUT_S,
    )  # fmt: skip
    try:
        raw = gzip.decompress(gz)
    except (OSError, EOFError, zlib.error) as e:
        raise TransportError(f"{name}: 받은 gzip을 풀 수 없다({e})") from e
    return gz, raw


def measure_clock(
    tr: Transports, pod: str, *, now_ms: Callable[[], int] = _now_ms
) -> ClockOffset:
    """`exec -c holder -- date -u +%s`를 RTT로 감싸 시계 차이를 잰다(설계 4.8 4단계)."""
    t0 = now_ms()
    out = _exec(tr, pod, HOLDER_CONTAINER, "date", "-u", "+%s")
    t1 = now_ms()
    try:
        node_s = int(out.decode("utf-8", errors="replace").strip())
    except ValueError as e:
        raise TransportError(f"date 출력이 정수가 아니다: {out[:40]!r}") from e
    return ClockOffset(offset_ms=node_s * 1000 + 500 - (t0 + t1) // 2, rtt_ms=t1 - t0)


def release(tr: Transports, pod: str) -> None:
    """`exec -c holder -- touch /out/RELEASE`. DB 수집 뒤에만 부른다(설계 4.8 5단계)."""
    _exec(tr, pod, HOLDER_CONTAINER, "touch", RELEASE_PATH)


def fetch(
    ctx: RunContext,
    tr: Transports,
    run_dir: Path,
    *,
    wait_timeout_s: float = WAIT_TIMEOUT_S,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchResult:
    """1–4단계를 돌아 run_dir/k6/ 아래에 기록·pod-status.json·derived.json을 쓴다(설계 4.8).

    Pod를 못 찾거나 k6가 끝나지 않으면 오류다. 기록 회수·시계 측정 실패는 결과에 남기고
    파일을 쓰지 않는다(evaluate가 SENT_LOG_MISSING 등으로 판단한다). RELEASE는 하지 않는다.
    """
    k6_dir = run_dir / LOCAL_K6_DIR
    k6_dir.mkdir(parents=True, exist_ok=True)
    pod = find_pod(ctx, tr)
    status = wait_terminated(ctx, tr, pod, timeout_s=wait_timeout_s, sleep=sleep)

    errors: list[str] = []
    got: tuple[str, dict[str, str], bytes, bytes] | None = None
    attempts = 0
    while attempts < FETCH_ATTEMPTS:
        attempts += 1
        try:
            text, remote = remote_sha256(tr, pod)
            gz, raw = copy_gz(tr, pod, ORDERS_LOG)
        except TransportError as e:
            errors.append(f"{attempts}회: {e}")
            continue
        got = (text, remote, gz, raw)
        if sha256_bytes(raw) == remote[ORDERS_LOG]:
            break
        errors.append(f"{attempts}회: {ORDERS_LOG} sha256이 원격과 다르다")

    files: dict[str, str] = {}
    remote = {}
    if got is not None:  # 같은 시도의 sha256sum 출력과 기록을 함께 쓴다
        text, remote, gz, raw = got
        (k6_dir / REMOTE_SHA_NAME).write_text(text, encoding="utf-8")
        (k6_dir / LOCAL_ORDERS_GZ).write_bytes(gz)
        files[ORDERS_LOG] = sha256_bytes(raw)
        if SUMMARY in remote:
            try:
                _, summary = copy_gz(tr, pod, SUMMARY)
            except TransportError as e:
                errors.append(f"{SUMMARY}: {e}")
            else:
                (k6_dir / SUMMARY).write_bytes(summary)
                files[SUMMARY] = sha256_bytes(summary)
    stop_requested = STOP_FLAG in remote  # sha256sum 목록에 있으면 정지 표시가 있다
    pod_doc = {"pod": pod, "containers": status.containers}
    _write_json(k6_dir / POD_STATUS_NAME, {**pod_doc, "stop_requested": stop_requested})

    clock: ClockOffset | None = None
    try:
        clock = measure_clock(tr, pod)
    except TransportError as e:
        errors.append(f"시계: {e}")
    else:
        _write_json(
            k6_dir / DERIVED_NAME,
            {"offset_ms": clock.offset_ms, "rtt_ms": clock.rtt_ms},
        )

    return FetchResult(
        pod=pod,
        k6_dir=k6_dir,
        pod_status=status,
        files=files,
        remote=remote,
        sha_ok=files.get(ORDERS_LOG) is not None
        and files.get(ORDERS_LOG) == remote.get(ORDERS_LOG),
        attempts=attempts,
        errors=tuple(errors),
        clock=clock,
        stop_requested=stop_requested,
    )


def stop(ctx: RunContext, tr: Transports, *, ws: bool = False) -> None:
    """`judge stop`: 정지 표시 → `exec -c k6 -- kill -INT 1`(WS run은 `-c k6-ws`도)(설계 4.8).

    k6는 SIGINT에 테스트를 멈추고 teardown과 handleSummary를 돈다. holder는 남는다.
    표시를 먼저 만든다(k6load.K6JobLoad.stop과 같은 순서).
    """
    pod = find_pod(ctx, tr)
    _exec(tr, pod, HOLDER_CONTAINER, "touch", f"{REMOTE_OUT}/{STOP_FLAG}")
    for c in (K6_CONTAINER, K6_WS_CONTAINER) if ws else (K6_CONTAINER,):
        _exec(tr, pod, c, "kill", "-INT", "1")

"""k6 기록 회수와 정지(설계 4.8). 1차.

`judge fetch` 순서(설계 4.8)
1. `-l batch.kubernetes.io/job-name=<k6.job>`으로 Pod를 찾고 k6 컨테이너가 모두
   terminated가 될 때까지 기다린다. exitCode·reason을 k6/pod-status.json에 쓴다.
2. `exec -c holder -- sha256sum /out/*`(출력은 k6/sha256sum.txt에 그대로),
   `exec -c holder -- gzip -c /out/orders.jsonl`(k6/orders.jsonl.gz). summary.json도 받는다.
   `kubectl cp`(tar 의존)는 쓰지 않는다.
3. sha256을 비교한다. 3회 실패하면 SENT_LOG_MISSING이다.
4. `exec -c holder -- date -u +%s`를 RTT로 감싸 노드 시계 차이를 derived.json에 남긴다.
5. DB 수집 뒤 `exec -c holder -- touch /out/RELEASE`(release).

fetch 전에 `kubectl delete job`을 하면 emptyDir와 함께 기록이 사라진다(설계 3).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .model import RunContext
from .transport import Transports

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

# runs/<run_id>/ 아래 산출물(설계 5.2). evaluate(pipeline)가 이 이름으로 읽는다
LOCAL_K6_DIR = "k6"
POD_STATUS_NAME = "pod-status.json"  # {"pod", "containers": {이름: {exitCode, reason}}}
DERIVED_NAME = "derived.json"  # {"offset_ms", "rtt_ms"}(ClockOffset)
REMOTE_SHA_NAME = "sha256sum.txt"  # 원격 `sha256sum /out/*` 출력 그대로(2단계)
LOCAL_ORDERS_GZ = "orders.jsonl.gz"

FETCH_ATTEMPTS = 3  # 4.8 3단계
CLOCK_SKEW_MAX_MS = 2000  # 5.5 시간 기준 CLOCK_SKEW 경계


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

    offset_ms: 노드 `date -u +%s`(초)를 RTT 중앙 노트북 시각과 비교한 값.
    rtt_ms: exec 왕복 시간. 초 단위 해상도라 오차는 ±(500 + rtt/2)ms다.
    """

    offset_ms: int
    rtt_ms: int


@dataclass(frozen=True)
class FetchResult:
    """회수 결과. files: 로컬 파일 이름 → 원격 원본 sha256(압축 해제 기준)."""

    pod: str
    k6_dir: Path
    pod_status: PodStatus
    files: Mapping[str, str] = field(default_factory=dict, hash=False)
    clock: ClockOffset | None = None


def find_pod(ctx: RunContext, tr: Transports) -> str:
    """`-l batch.kubernetes.io/job-name=<ctx.k6_job>`인 Pod 이름(설계 4.8 1단계).

    Pod가 0개이거나 2개 이상이면(재실행 흔적, 4.8) 오류다.
    """
    raise NotImplementedError("설계 4.8, 1차")


def wait_terminated(
    ctx: RunContext,
    tr: Transports,
    pod: str,
    *,
    containers: Sequence[str] = (K6_CONTAINER,),
    timeout_s: float = 600.0,
) -> PodStatus:
    """k6 컨테이너가 모두 terminated가 될 때까지 기다린다(설계 4.8 1단계)."""
    raise NotImplementedError("설계 4.8, 1차")


def remote_sha256(tr: Transports, pod: str) -> dict[str, str]:
    """`exec -c holder -- sha256sum /out/*` 결과. 파일 이름 → sha256(설계 4.8 2단계)."""
    raise NotImplementedError("설계 4.8, 1차")


def copy_gz(tr: Transports, pod: str, name: str, dest: Path) -> str:
    """`exec -c holder -- gzip -c /out/<name>`을 dest에 쓰고 압축 해제 sha256을 낸다.

    원격 sha256과 다르면 FETCH_ATTEMPTS번까지 다시 받는다(설계 4.8 2–3단계).
    """
    raise NotImplementedError("설계 4.8, 1차")


def measure_clock(tr: Transports, pod: str) -> ClockOffset:
    """`exec -c holder -- date -u +%s`를 RTT로 감싸 시계 차이를 잰다(설계 4.8 4단계)."""
    raise NotImplementedError("설계 4.8, 1차")


def release(tr: Transports, pod: str) -> None:
    """`exec -c holder -- touch /out/RELEASE`. DB 수집 뒤에만 부른다(설계 4.8 5단계)."""
    raise NotImplementedError("설계 4.8, 1차")


def fetch(ctx: RunContext, tr: Transports, run_dir: Path) -> FetchResult:
    """1–4단계를 돌아 run_dir/k6/ 아래에 기록·pod-status.json·derived.json을 쓴다(설계 4.8).

    3회 실패하면 SENT_LOG_MISSING을 낼 수 있게 결과에 남긴다. RELEASE는 하지 않는다.
    """
    raise NotImplementedError("설계 4.8, 1차")


def stop(ctx: RunContext, tr: Transports, *, ws: bool = False) -> None:
    """`judge stop`: `exec -c k6 -- kill -INT 1`(WS run은 `-c k6-ws`도)(설계 4.8).

    k6는 SIGINT에 테스트를 멈추고 teardown과 handleSummary를 돈다. holder는 남는다.
    """
    raise NotImplementedError("설계 4.8, 1차")

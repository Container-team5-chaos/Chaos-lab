"""k6 Job 부하 생성기 (설계 b-measure-judge 1.4, 3, 4.5, 4.8).

runner가 WARMUP에서 start, RECOVERING·정리에서 stop을 부른다.
- start: ConfigMap k6-scripts 갱신 → Job k6-<run_id> apply → 첫 intent 줄이 나올 때까지 대기
- stop: holder에 /out/STOP_REQUESTED를 만든 뒤 k6 컨테이너 PID 1에 SIGINT를 보낸다.
  Job·Pod는 지우지 않는다(judge fetch 전 삭제 금지)
kubectl은 주입 가능한 runner로 부른다(테스트는 가짜 runner, 클러스터 없음).
"""

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Callable, Sequence

# 저장소 루트의 load/k6 (orchestrator/src/orchestrator/k6load.py 기준)
K6_DIR = Path(__file__).resolve().parents[3] / "load" / "k6"
DEFAULT_IMAGE = "grafana/k6:2.3.0"  # 공식 이미지(alpine: sh, cat, date, kill 포함)
PROFILES = ("smoke", "steady", "accept", "demo")
LATER_PROFILES = ("step", "replay")  # 2차
LOAD_DEFAULTS = {
    "profile": "steady",
    "rate": 30,
    "duration_s": 600,
    "accounts": 300,
    "start_timeout_s": 90,
}
SETUP_TIMEOUT_S, GRACEFUL_STOP_S, FETCH_BUDGET_S = 180, 60, 600  # 설계 4.8
# SIGINT로 멈춘 k6는 종료 코드 105다. judge fetch가 이 파일을 보고 우리가 멈춘 것으로 기록한다
STOP_FLAG = "/out/STOP_REQUESTED"
ANTI_AFFINITY = (
    "{podAntiAffinity: {preferredDuringSchedulingIgnoredDuringExecution: [{weight: 100,"
    " podAffinityTerm: {labelSelector: {matchLabels: {app.kubernetes.io/name: order}},"
    " topologyKey: kubernetes.io/hostname}}]}}"
)
RUN_ID_RE = re.compile(r"^[a-z0-9-]{3,40}$")
PLACEHOLDER_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")


class LoadStartError(RuntimeError):
    """k6가 start_timeout_s 안에 첫 intent를 쓰지 못했다 → runner가 중단한다."""


def load_settings(scenario: dict) -> dict:
    """시나리오 load: 블록을 기본값과 합치고 검사한다."""
    block = (scenario or {}).get("load") or {}
    if not isinstance(block, dict):
        raise ValueError("load는 매핑이어야 한다")
    unknown = set(block) - set(LOAD_DEFAULTS)
    if unknown:
        raise ValueError(f"load에 모르는 키: {sorted(unknown)}")
    s = {**LOAD_DEFAULTS, **block}
    if s["profile"] in LATER_PROFILES:
        raise ValueError(f"profile={s['profile']}는 2차(아직 지원 안 함)")
    if s["profile"] not in PROFILES:
        raise ValueError(f"profile은 {'|'.join(PROFILES)} 중 하나여야 한다")

    def num(key, lo, hi, integer=True):
        v = s[key]
        ok = isinstance(v, int) if integer else isinstance(v, (int, float))
        if isinstance(v, bool) or not ok or not lo <= v <= hi:
            raise ValueError(f"load.{key}={v!r}가 {lo}..{hi} 밖이다")

    num("rate", 0.1, 1000, integer=False)  # orders.js RATE 범위
    num("duration_s", 1, 86400)
    num("accounts", 1, 1000)  # seed 계좌 수
    num("start_timeout_s", 1, 3600)
    return s


def _profile_length_s(s: dict) -> int:
    # 설계 4.5 표. steady·demo는 DURATION_S, accept는 300+10+180
    return {"smoke": 10, "accept": 490}.get(s["profile"], s["duration_s"])


def render_job(run_id: str, scenario: dict, image: str = DEFAULT_IMAGE) -> str:
    """job.yaml의 ${NAME}을 채운다. 남은 자리표시자가 있으면 실패."""
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"run_id {run_id!r}는 소문자·숫자·하이픈 3–40자여야 한다")
    s = load_settings(scenario)
    hold_s = SETUP_TIMEOUT_S + _profile_length_s(s) + GRACEFUL_STOP_S + FETCH_BUDGET_S
    values = {
        "RUN_ID": run_id,
        "K6_IMAGE": image,
        "PROFILE": s["profile"],
        "ACCOUNTS": str(s["accounts"]),
        "USER_BASE": "",  # 빈 값 = orders.js 기본값
        "RATE": str(s["rate"]),
        # smoke·accept는 길이가 고정이라 orders.js 기본값을 쓴다
        "DURATION_S": str(s["duration_s"])
        if s["profile"] in ("steady", "demo")
        else "",
        "SPIKE_AT_S": "",
        "THRESHOLDS": "",
        "RUN_SEED": "",
        "HOLD_S": str(hold_s),
        "DEADLINE": str(hold_s + 120),
        "DISRUPT_ANNOTATION": "{}",
        "ANTI_AFFINITY": ANTI_AFFINITY,
    }
    text = (K6_DIR / "k8s" / "job.yaml").read_text(encoding="utf-8")
    # 머리 주석에도 ${...} 설명이 있어 주석 줄은 그대로 두고 본문만 채운다
    lines = []
    for line in text.splitlines(keepends=True):
        if line.lstrip().startswith("#"):
            lines.append(line)
            continue
        line = PLACEHOLDER_RE.sub(
            lambda m: values[m.group(1)] if m.group(1) in values else m.group(0), line
        )
        left = PLACEHOLDER_RE.findall(line.split(" #")[0])
        if left:
            raise ValueError(f"채우지 못한 자리표시자: {left}")
        lines.append(line)
    return "".join(lines)


def _subprocess_kubectl(args: Sequence[str], stdin: str | None = None) -> str:
    r = subprocess.run(
        ["kubectl", *args], input=stdin, capture_output=True, text=True, check=False
    )
    if r.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args[:3])} 실패: {r.stderr.strip()}")
    return r.stdout


class K6JobLoad:
    """LoadGenerator 구현. 같은 run_id로 Job을 다시 apply하지 않는다."""

    def __init__(
        self,
        namespace: str = "exchange",
        image: str = DEFAULT_IMAGE,
        kubectl: Callable[..., str] = _subprocess_kubectl,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        poll_s: float = 2.0,
    ):
        self.namespace = namespace
        self.image = image
        self._kubectl = kubectl
        self._sleep = sleep
        self._clock = clock
        self.poll_s = poll_s
        self._stopped: set[str] = set()

    def _k(self, *args: str, stdin: str | None = None) -> str:
        return self._kubectl(["--namespace", self.namespace, *args], stdin=stdin)

    def _job_exists(self, job: str) -> bool:
        out = self._k("get", "job", job, "--ignore-not-found", "-o", "name")
        return bool(out.strip())

    def _pod(self, job: str) -> str | None:
        out = self._k(
            "get", "pods", "-l", f"job-name={job}",
            "-o", "jsonpath={.items[*].metadata.name}",
        )  # fmt: skip
        names = out.split()
        return names[0] if names else None

    def _has_intent(self, pod: str) -> bool:
        try:
            out = self._k(
                "exec", pod, "-c", "holder", "--",
                "sh", "-c", "cat /out/orders.jsonl 2>/dev/null || true",
            )  # fmt: skip
        except RuntimeError:
            return False  # 컨테이너가 아직 안 떴다
        for line in out.splitlines():
            try:
                if json.loads(line).get("type") == "intent":
                    return True
            except (ValueError, AttributeError):
                continue  # 쓰는 중인 마지막 줄
        return False

    def _apply_scripts(self) -> None:
        # load/k6/ 최상위 파일만(k8s/ 제외). create --dry-run | apply와 같다
        files = sorted(p for p in K6_DIR.iterdir() if p.is_file())
        args = ["create", "configmap", "k6-scripts", "--dry-run=client", "-o", "yaml"]
        args += [f"--from-file={p}" for p in files]
        self._k("apply", "-f", "-", stdin=self._k(*args))

    def start(self, run_id: str, scenario: dict) -> None:
        s = load_settings(scenario)
        manifest = render_job(run_id, scenario, self.image)
        job = f"k6-{run_id}"
        if not self._job_exists(job):  # 같은 RUN_ID 재apply 금지(같은 키가 다시 나간다)
            self._apply_scripts()
            self._k("create", "-f", "-", stdin=manifest)
        deadline = self._clock() + s["start_timeout_s"]
        while True:
            pod = self._pod(job)
            if pod and self._has_intent(pod):
                return
            if self._clock() >= deadline:
                raise LoadStartError(
                    f"{job}: {s['start_timeout_s']}s 안에 첫 intent가 없다"
                )
            self._sleep(self.poll_s)

    def stop(self, run_id: str) -> None:
        """표시 파일 → k6에 SIGINT(judge stop과 같다). 두 번·start 실패 후에도 안전.

        표시를 먼저 만들어야 "105인데 표시 없음"(의도하지 않은 중단으로 보임)이 생기지 않는다.
        Job·Pod는 지우지 않는다.
        """
        if run_id in self._stopped:
            return
        job = f"k6-{run_id}"
        try:
            pod = self._pod(job)
            if pod:
                try:
                    self._k("exec", pod, "-c", "holder", "--", "touch", STOP_FLAG)
                except RuntimeError:
                    pass  # holder가 없으면 표시 없이 멈춘다(판정은 기록 불완전으로 본다)
                self._k("exec", pod, "-c", "k6", "--", "kill", "-INT", "1")
        except RuntimeError:
            pass  # k6가 이미 끝났거나 Job이 없다. 정리 경로라 삼킨다
        self._stopped.add(run_id)

"""실서비스 e2e 도우미(계약 명세 2, 10). test_e2e_order_service.py가 쓴다.

- docker 명령, 주문 서비스 프로세스, 시세 수집기 대역(Ticker), k6와 같은 JSONL v2를 쓰는
  파이썬 주문 클라이언트(Recorder), 증거 폴더 작성(write_evidence, take_snapshot)을 둔다.
- HTTP는 표준 라이브러리(http.client)만 쓴다. 전송 오류·시간 초과는 st 0이고 ec는 k6 error_code를 흉내 낸다.
- 요청에 Idempotency-Key 같은 헤더를 넣지 않는다. 멱등 키는 본문 client_order_id다(계약 명세 1, 2).
- 비밀번호를 코드에 두지 않는다. DB 비밀번호는 테스트가 만들고 docker·앱에 환경변수로만 넘긴다.
  judge db 번들과 시세 기록은 docker exec psql(컨테이너 안 Unix 소켓, 비밀번호 없음)로 돈다.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from orchestrator.judge import db as D
from orchestrator.judge.criteria import integrity_cfg
from orchestrator.judge.model import JSONL_VERSION, Criteria, RunContext
from orchestrator.judge.transport import Transports

MARKET = "KRW-BTC"
RAW_MAX = 2048  # attempt.raw 상한(계약 명세 2)
HTTP_TIMEOUT_S = 10.0
PG_USER = "exchange"  # 서비스 로컬 개발 기본값(docker-compose.dev.yml). 이름만 쓴다
PG_DB = "exchange"
# k6 error_code 흉내(전송 오류 st 0의 ec)
K6_EC_TIMEOUT = "1050"
K6_EC_REFUSED = "1212"
K6_EC_RESET = "1220"
K6_EC_GENERIC = "1000"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


# ---------------------------------------------------------------- docker


def docker(
    *args: str,
    input: bytes | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float = 120,
) -> subprocess.CompletedProcess[bytes]:
    """`docker <args>`. 셸을 거치지 않는다. env를 주면 그 환경으로 docker CLI를 띄운다."""
    return subprocess.run(
        ["docker", *args],
        input=input,
        capture_output=True,
        timeout=timeout,
        env=None if env is None else dict(env),
        check=False,
    )


def docker_ok(cp: subprocess.CompletedProcess[bytes], what: str) -> str:
    if cp.returncode != 0:
        err = cp.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"{what} 실패(종료 코드 {cp.returncode}): {err[-500:]}")
    return cp.stdout.decode("utf-8", "replace")


def docker_available() -> bool:
    try:
        return docker("info", timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def host_port(container: str, port: str) -> int:
    """docker가 127.0.0.1에 고른 호스트 포트(`docker port`)."""
    out = docker_ok(docker("port", container, port), f"docker port {container}")
    for line in out.splitlines():
        host, _, p = line.strip().rpartition(":")
        if host in ("127.0.0.1", "0.0.0.0") and p.isdigit():
            return int(p)
    raise RuntimeError(f"{container} {port}의 호스트 포트를 찾지 못했다: {out!r}")


def free_port() -> int:
    """127.0.0.1의 빈 포트 하나. 앱이 쓰기 전까지 잠깐 비어 있는 것만 보장한다."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def psql(container: str, sql: str, *, dbname: str = PG_DB) -> str:
    """컨테이너 안 Unix 소켓으로 psql을 돌린다(비밀번호 없음). 실패하면 RuntimeError."""
    cp = docker(
        "exec",
        "-i",
        container,
        "psql",
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        PG_USER,
        "-d",
        dbname,
        "-f",
        "-",
        input=sql.encode("utf-8"),
    )
    return docker_ok(cp, "psql")


def wait_pg_ready(container: str, deadline_s: float = 90) -> None:
    """TCP로 준비를 확인한다. 초기화 중 임시 서버는 Unix 소켓만 연다."""
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        cp = docker("exec", container, "pg_isready", "-h", "127.0.0.1", "-U", PG_USER)
        if cp.returncode == 0:
            return
        time.sleep(0.5)
    logs = docker("logs", "--tail", "30", container).stderr.decode("utf-8", "replace")
    raise RuntimeError(f"postgres가 {deadline_s}s 안에 준비되지 않았다\n{logs}")


def wait_redis_ready(container: str, deadline_s: float = 30) -> None:
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        cp = docker("exec", container, "redis-cli", "ping")
        if cp.returncode == 0 and cp.stdout.strip() == b"PONG":
            return
        time.sleep(0.3)
    raise RuntimeError(f"redis가 {deadline_s}s 안에 준비되지 않았다")


# ---------------------------------------------------------------- HTTP


def http_get(port: int, path: str, timeout: float = 2.0) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", "replace")
    finally:
        conn.close()


def wait_app_ready(
    port: int, alive: Callable[[], bool], log_tail: Callable[[], str], deadline_s=60
) -> None:
    """GET /readyz가 200이 될 때까지 기다린다. 프로세스가 먼저 끝나면 로그와 함께 실패한다."""
    end = time.monotonic() + deadline_s
    last = ""
    while time.monotonic() < end:
        if not alive():
            raise RuntimeError(f"주문 서비스가 일찍 끝났다\n{log_tail()}")
        try:
            st, body = http_get(port, "/readyz")
            if st == 200:
                return
            last = f"{st} {body[:200]}"
        except OSError as e:
            last = type(e).__name__
        time.sleep(0.3)
    raise RuntimeError(
        f"/readyz가 {deadline_s}s 안에 200이 아니다({last})\n{log_tail()}"
    )


def _ec_of(e: OSError) -> str:
    if isinstance(e, (TimeoutError, socket.timeout)):
        return K6_EC_TIMEOUT
    if isinstance(e, ConnectionRefusedError):
        return K6_EC_REFUSED
    if isinstance(e, ConnectionResetError):
        return K6_EC_RESET
    return K6_EC_GENERIC


def _json_obj(text: str | None) -> dict[str, Any] | None:
    if text is None:
        return None
    try:
        v = json.loads(text)
    except ValueError:
        return None
    return v if isinstance(v, dict) else None


def post_order(port: int, body: Mapping[str, Any]) -> dict[str, Any]:
    """POST /orders 한 번. JSONL v2 시도 하나(n 없이)를 돌려준다(계약 명세 2).

    {t0, t1, st, ec, ms, rp, raw}. st 0 = 전송 오류. raw는 200·4xx·503 본문(2048자 이하).
    ec는 거절 본문의 reason(200은 null), 전송 오류면 k6 error_code 흉내. rp는 200 본문의 replayed.
    """
    data = json.dumps(body, separators=(",", ":")).encode("utf-8")
    t0 = now_ms()
    p0 = time.perf_counter()
    st, text, ec = 0, None, None
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=HTTP_TIMEOUT_S)
    try:
        conn.request(
            "POST", "/orders", body=data, headers={"Content-Type": "application/json"}
        )
        resp = conn.getresponse()
        st = resp.status
        text = resp.read().decode("utf-8", "replace")
    except OSError as e:
        st, text, ec = 0, None, _ec_of(e)
    finally:
        conn.close()
    ms = round((time.perf_counter() - p0) * 1000, 3)
    t1 = max(t0, now_ms())
    obj = _json_obj(text)
    raw = text if st == 200 or 400 <= st < 500 or st == 503 else None
    rp = None
    if st == 200:
        if obj is not None and isinstance(obj.get("replayed"), bool):
            rp = obj["replayed"]
    elif st != 0 and obj is not None and isinstance(obj.get("reason"), str):
        ec = obj["reason"]
    if raw is not None and len(raw) > RAW_MAX:
        raw = raw[:RAW_MAX]
    return {"t0": t0, "t1": t1, "st": st, "ec": ec, "ms": ms, "rp": rp, "raw": raw}


# ---------------------------------------------------------------- 주문 서비스 프로세스


@dataclass
class OrderService:
    """`python -m app`(cwd=workloads/order). env는 app_env로 만든다.

    서비스 설정은 DATABASE_URL·REDIS_URL·PORT만 주고, 셸에서는 실행에 필요한 값(APP_ENV_PASS)만
    넘긴다. STALE_MS·DB_POOL_SIZE는 서비스 기본값(5000, 10)이다.
    """

    python: str
    app_dir: Path
    env: Mapping[str, str]
    log_path: Path
    port: int
    proc: subprocess.Popen[bytes] | None = None
    _log: Any = field(default=None, repr=False)

    def start(self) -> None:
        self._log = self.log_path.open("wb")
        self.proc = subprocess.Popen(
            [self.python, "-m", "app"],
            cwd=self.app_dir,
            env=dict(self.env),
            stdin=subprocess.DEVNULL,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def log_tail(self, n: int = 40) -> str:
        try:
            lines = self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
        return "\n".join(lines.splitlines()[-n:])

    def stop(self) -> None:
        """SIGTERM(서비스의 정상 종료 경로) 뒤 25초 안에 안 끝나면 SIGKILL."""
        p = self.proc
        if p is not None and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=25)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=10)
        if self._log is not None:
            self._log.close()
            self._log = None


# ---------------------------------------------------------------- 시세 수집기 대역


def price_series(base: str = "98000000", step: str = "1234.56789012") -> Iterator[str]:
    """체결가가 매번 다르게 오르내리는 시세(소수 8자리)."""
    b, s = Decimal(base), Decimal(step)
    i = 0
    while True:
        p = b + s * ((i * 7) % 11) - s * Decimal((i * 3) % 5) / 2
        yield f"{p.quantize(Decimal('0.00000001')):f}"
        i += 1


class Ticker:
    """수집기(다른 파트) 대역. period마다 ticks 행을 넣고 Redis price:<market>를 지금 시각으로 쓴다.

    ticks 행을 먼저 넣고(received_at = DB now()) Redis를 쓴다. 그래서 그 시세로 체결된 주문의
    filled_at(DB now())보다 tick이 앞선다(계약 명세 7 fresh). paused() 안에서는 주기 갱신을 멈춘다.
    """

    def __init__(
        self,
        pg: str,
        redis: str,
        *,
        prices: Iterator[str],
        period_s: float = 1.0,
        market: str = MARKET,
    ) -> None:
        self.pg, self.redis, self.market = pg, redis, market
        self.prices = prices
        self.period_s = period_s
        self.price: str | None = None
        self.errors: list[str] = []
        self.ticks = 0
        self._lock = threading.Lock()
        self._paused = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _set_redis(self, price: str, ts: int) -> None:
        value = json.dumps({"price": price, "ts": ts}, separators=(",", ":"))
        cp = docker(
            "exec", self.redis, "redis-cli", "SET", f"price:{self.market}", value
        )
        out = docker_ok(cp, "redis-cli SET")
        if out.strip() != "OK":
            raise RuntimeError(f"redis-cli SET 응답이 OK가 아니다: {out.strip()!r}")

    def _tick(self) -> None:
        price = next(self.prices)
        Decimal(price)  # 숫자만 SQL에 넣는다
        psql(
            self.pg,
            "INSERT INTO ticks (market, price, received_at) "
            f"VALUES ('{self.market}', {price}, now());\n",
        )
        self._set_redis(price, now_ms())
        self.price = price
        self.ticks += 1

    def tick(self) -> str:
        """지금 새 시세를 낸다(paused와 상관없이). 낸 가격을 돌려준다."""
        with self._lock:
            self._tick()
            assert self.price is not None
            return self.price

    def make_stale(self, age_ms: int) -> None:
        """마지막 시세를 age_ms만큼 오래된 것으로 다시 쓴다(tick 행은 넣지 않는다). paused 안에서 쓴다."""
        with self._lock:
            assert self._paused and self.price is not None
            self._set_redis(self.price, now_ms() - age_ms)

    @contextmanager
    def paused(self) -> Iterator[None]:
        with self._lock:  # 진행 중인 tick이 끝난 뒤 멈춘다
            self._paused = True
        try:
            yield
        finally:
            with self._lock:
                self._paused = False

    def _loop(self) -> None:
        while not self._stop.wait(self.period_s):
            with self._lock:
                if self._paused:
                    continue
                try:
                    self._tick()
                except Exception as e:  # noqa: BLE001 — 스레드 오류는 모아 두고 테스트가 본다
                    self.errors.append(f"{type(e).__name__}: {e}")

    def start(self) -> None:
        self.tick()
        self._thread = threading.Thread(target=self._loop, name="ticker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)


# ---------------------------------------------------------------- JSONL v2 기록(계약 명세 2)


def run_key(run_id: str, vu: int, seq: int) -> str:
    """k6 client_order_id `k6-<run_id>-<vu 4자리>-<seq 6자리>`(계약 명세 11. 예 k6-demo-1106-0001-000042)."""
    return f"k6-{run_id}-{vu:04d}-{seq:06d}"


def dump_line(obj: Mapping[str, Any]) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


@dataclass
class Recorder:
    """k6 스크립트처럼 주문을 보내며 JSONL v2를 쓴다(계약 명세 2).

    run_start → (intent → 시도들 → result)… → run_end. 키는 k6와 같은
    `k6-<run_id>-<vu 4자리>-<seq 6자리>`(계약 명세 11, run_key), 1 VU = 1 user(vu n은 user-<n:04>)다.
    시도는 같은 키로 보낸 모든 전송이다.
    """

    port: int
    run_id: str
    plan: Mapping[str, Any]
    vus: int
    seed: int = 7
    lines: list[dict[str, Any]] = field(default_factory=list)
    http_reqs: int = 0
    _seq: dict[int, int] = field(default_factory=dict)

    @staticmethod
    def user(vu: int) -> str:
        return f"user-{vu:04d}"

    def run_start(self) -> int:
        t = now_ms()
        self.lines.append(
            {
                "type": "run_start",
                "v": JSONL_VERSION,
                "run": self.run_id,
                "script_ver": "e2e-py-0.1.0",
                "seed": self.seed,
                "profile": "e2e",
                "plan": dict(self.plan),
                "symbols": [MARKET],
                "initial_cash": 10000000,
                "vus": self.vus,
                "t": t,
            }
        )
        return t

    def intent(self, vu: int, side: str, volume: str) -> tuple[str, dict[str, str]]:
        """intent 줄을 쓴다(보내기 전). (키, 본문)."""
        seq = self._seq.get(vu, 0) + 1
        self._seq[vu] = seq
        key = run_key(self.run_id, vu, seq)
        body = {
            "client_order_id": key,
            "user_id": self.user(vu),
            "market": MARKET,
            "side": side,
            "volume": volume,
        }
        self.lines.append(
            {
                "type": "intent",
                "key": key,
                "acct": self.user(vu),
                "vu": vu,
                "t": now_ms(),
                "body": body,
            }
        )
        return key, body

    def send(self, body: Mapping[str, Any]) -> dict[str, Any]:
        self.http_reqs += 1
        return post_order(self.port, body)

    def send_concurrently(
        self, body: Mapping[str, Any], n: int
    ) -> list[dict[str, Any]]:
        """같은 본문을 n개 연결로 동시에 보낸다. 시도는 보낸 시각(t0), 받은 시각(t1) 순이다.

        k6는 같은 키를 겹쳐 보내지 않는다(계약 명세 2). 겹친 시도에서는 이 순서의 n=1이 진 쪽(replayed=true)일
        수 있다. judge는 겹친 시도를 커밋했을 수 있는 시도로 본다(classify.ack_path).
        """
        barrier = threading.Barrier(n)
        out: list[dict[str, Any] | None] = [None] * n
        errors: list[BaseException] = []

        def worker(i: int) -> None:
            try:
                barrier.wait(timeout=10)
                out[i] = self.send(body)
            except BaseException as e:  # noqa: BLE001 — 주 스레드에서 다시 던진다
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=HTTP_TIMEOUT_S + 15)
        if errors:
            raise errors[0]
        atts = [a for a in out if a is not None]
        if len(atts) != n:
            raise RuntimeError(f"동시 전송 {n}건 중 {len(atts)}건만 끝났다")
        return sorted(atts, key=lambda a: (a["t0"], a["t1"]))

    def result(self, key: str, atts: Sequence[Mapping[str, Any]]) -> None:
        numbered = [{"n": i, **a} for i, a in enumerate(atts, start=1)]
        self.lines.append({"type": "result", "key": key, "atts": numbered})

    def run_end(self) -> int:
        t = now_ms()
        self.lines.append({"type": "run_end", "run": self.run_id, "t": t})
        return t

    def jsonl(self) -> bytes:
        return "".join(dump_line(x) + "\n" for x in self.lines).encode("utf-8")

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for x in self.lines:
            out[x["type"]] = out.get(x["type"], 0) + 1
        return out


# ---------------------------------------------------------------- 증거 폴더(설계 4.8, 5.2)


def write_evidence(
    run_dir: Path,
    rec: Recorder,
    *,
    criteria_text: str,
    duration_ms: int,
) -> None:
    """run.json, criteria.yaml, k6/(orders.jsonl, summary.json, pod-status.json, derived.json,
    sha256sum.txt)를 fetch가 남기는 모양으로 쓴다. DB 스냅샷은 take_snapshot이 쓴다.
    """
    k6 = run_dir / "k6"
    k6.mkdir(parents=True, exist_ok=True)
    orders = rec.jsonl()
    (k6 / "orders.jsonl").write_bytes(orders)
    counts = rec.counts()
    summary = {
        "state": {"testRunDurationMs": duration_ms},
        "metrics": {
            "order_intents": {
                "type": "counter",
                "contains": "default",
                "values": {"count": counts.get("intent", 0)},
            },
            "order_results": {
                "type": "counter",
                "contains": "default",
                "values": {"count": counts.get("result", 0)},
            },
            "http_reqs": {
                "type": "counter",
                "contains": "default",
                "values": {"count": rec.http_reqs},
            },
        },
    }
    summary_bytes = (json.dumps(summary, indent=2) + "\n").encode("utf-8")
    (k6 / "summary.json").write_bytes(summary_bytes)
    sha = (
        f"{hashlib.sha256(orders).hexdigest()}  /out/orders.jsonl\n"
        f"{hashlib.sha256(summary_bytes).hexdigest()}  /out/summary.json\n"
    )
    (k6 / "sha256sum.txt").write_text(sha, encoding="utf-8")
    pod = {
        "pod": f"k6-{rec.run_id}-local",
        "containers": {"k6": {"exitCode": 0, "reason": "Completed"}},
    }
    (k6 / "pod-status.json").write_text(json.dumps(pod, indent=2) + "\n", "utf-8")
    # 클라이언트와 판정이 같은 노트북 시계를 쓴다
    (k6 / "derived.json").write_text(
        json.dumps({"offset_ms": 0, "rtt_ms": 0}) + "\n", "utf-8"
    )
    (run_dir / "criteria.yaml").write_text(criteria_text, encoding="utf-8")
    run = {
        "schema": "chaos.run/v1",
        "run_id": rec.run_id,
        "experiment": "e2e-order-service",
        "criteria": "criteria.yaml",
        "k6": {"job": f"k6-{rec.run_id}"},
        "db": {"user": PG_USER, "database": PG_DB},
    }
    (run_dir / "run.json").write_text(json.dumps(run, indent=2) + "\n", "utf-8")


def take_snapshot(
    run_dir: Path, ctx: RunContext, crit: Criteria, container: str
) -> D.PsqlRun:
    """judge db 번들을 `docker exec -i <container> psql …`로 돌려 raw/db/snapshot.txt를 쓴다(계약 명세 6)."""
    bundle = D.build_bundle(ctx, stale_ms=integrity_cfg(crit).stale_ms)
    run = D.exec_bundle(
        Transports.for_local_pg(container), bundle, target=ctx.db, timeout_s=120
    )
    out = run_dir / "raw" / "db"
    out.mkdir(parents=True, exist_ok=True)
    (out / "bundle.sql").write_text(bundle, encoding="utf-8")
    (out / "snapshot.txt").write_text(run.stdout, encoding="utf-8")
    if run.returncode != 0:
        raise RuntimeError(f"psql 번들 종료 코드 {run.returncode}: {run.stderr[-500:]}")
    return run


def wait_until(t_ms: int) -> None:
    while True:
        left = t_ms - now_ms()
        if left <= 0:
            return
        time.sleep(min(left, 50) / 1000)


def env_with(**overrides: str) -> dict[str, str]:
    """docker CLI 환경. 셸 환경 전체(PYTHONPATH 제외, DOCKER_HOST 등이 필요하다)에 overrides를 더한다."""
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH",)}
    env.update(overrides)
    return env


# 주문 서비스 프로세스에 셸에서 넘기는 환경변수(실행에 필요한 것만). 서비스 설정(STALE_MS,
# DB_POOL_SIZE, app/config.py)과 libpq 계열(PG*, asyncpg가 읽는다)은 넘기지 않는다.
# 셸 값이 시나리오를 바꾸지 않게 한다(예: STALE_MS > 10000이면 stale 첫 시도가 200이 된다)
APP_ENV_PASS = ("PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "SYSTEMROOT")


def app_env(**overrides: str) -> dict[str, str]:
    """주문 서비스 환경. 셸에서는 APP_ENV_PASS만 가져오고 overrides(DATABASE_URL 등)를 더한다."""
    env = {k: os.environ[k] for k in APP_ENV_PASS if k in os.environ}
    env.update(overrides)
    return env

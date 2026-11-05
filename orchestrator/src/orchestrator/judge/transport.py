"""클러스터 명령 전송(설계 5.1, 5.4, 7.1, 7.2).

- judge는 노트북에서 kubectl을 부른다. 모든 클러스터 명령은 CMD_PREFIX로 시작한다.
- 테스트는 접두어만 바꿔 같은 경로를 돈다(설계 8 `-m pg`).
  `Transports.for_local_pg()`는 psql 접두어를 `docker exec -i pgtest`로 바꾼다.
  `runner`를 가짜 함수로 바꾸면 프로세스를 띄우지 않고 argv만 확인할 수 있다.
- 0이 아닌 종료 코드는 예외로 바꾸지 않는다. 판단은 부르는 쪽이 한다.
  실행 파일이 없거나 시간이 넘으면 TransportError(judge 오류, 종료 코드 1)다.
"""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Protocol

from .errors import JudgeError

__all__ = [
    "CMD_PREFIX",
    "DEFAULT_TIMEOUT_S",
    "PG_CONTAINER",
    "PG_POD",
    "PSQL_ARGS",
    "TEST_PSQL_PREFIX",
    "Runner",
    "TransportError",
    "Transports",
    "subprocess_runner",
]

NAMESPACE = "papertrade"
CMD_PREFIX = ("kubectl", "-n", NAMESPACE)  # 설계 5.1

# 설계 5.4, 7.2: postgres Pod 안 Unix 소켓, pt_ro, 표준 입력으로 bundle.sql을 넘긴다
PG_POD = "postgres-0"
PG_CONTAINER = "postgres"
PG_USER = "pt_ro"
PG_DB = "papertrade"
PSQL_ARGS = (
    "psql",
    "-X",
    "-q",
    "-v",
    "ON_ERROR_STOP=1",
    "-U",
    PG_USER,
    "-d",
    PG_DB,
    "-f",
    "-",
)
# 설계 5.1, 8: 로컬 docker postgres 컨테이너(pgtest)로 같은 psql을 돌린다
TEST_PSQL_PREFIX = ("docker", "exec", "-i", "pgtest")

DEFAULT_TIMEOUT_S = 60.0


class TransportError(JudgeError):
    """명령을 실행하지 못했다(실행 파일 없음, 시간 초과). judge 오류(설계 1.4)."""


class Runner(Protocol):
    """argv 하나를 실행한다. subprocess.run(capture_output=True, check=False)와 같은 모양."""

    def __call__(
        self,
        argv: Sequence[str],
        *,
        input: bytes | None = None,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]: ...


def subprocess_runner(
    argv: Sequence[str],
    *,
    input: bytes | None = None,
    timeout: float,
) -> subprocess.CompletedProcess[bytes]:
    """실제 프로세스를 띄운다. 셸을 거치지 않는다."""
    args = list(argv)
    if not args:
        raise TransportError("빈 명령은 실행할 수 없다")
    try:
        return subprocess.run(
            args,
            input=input,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as e:
        raise TransportError(f"{args[0]}: 실행 파일을 찾을 수 없다") from e
    except subprocess.TimeoutExpired as e:
        raise TransportError(f"{args[0]}: {timeout}s 안에 끝나지 않았다") from e


@dataclass(frozen=True)
class Transports:
    """클러스터 명령 전송 경로(설계 5.1).

    prefix: kubectl 명령 접두어. 기본은 CMD_PREFIX.
    psql_prefix: psql 앞에 붙일 접두어. None이면
        `prefix + exec -i postgres-0 -c postgres --`를 쓴다(설계 5.4).
    runner: argv 실행 함수. 테스트는 가짜 함수를 넣는다.
    timeout_s: run()에 timeout을 주지 않을 때 쓰는 값.
    2차: Prometheus direct HTTP 경로를 여기에 더한다(설계 5.9, 7.3). 주소는 코드에 두지 않는다.
    """

    prefix: tuple[str, ...] = CMD_PREFIX
    psql_prefix: tuple[str, ...] | None = None
    runner: Runner = field(default=subprocess_runner, compare=False, repr=False)
    timeout_s: float = DEFAULT_TIMEOUT_S

    @classmethod
    def for_local_pg(cls, container: str = "pgtest") -> Transports:
        """로컬 docker postgres로 psql을 보내는 전송(설계 8 `-m pg`)."""
        return cls(psql_prefix=("docker", "exec", "-i", container))

    def with_prefix(self, prefix: Sequence[str]) -> Transports:
        """kubectl 접두어만 바꾼 사본."""
        return replace(self, prefix=tuple(prefix))

    def run(
        self,
        argv: Sequence[str],
        *,
        input: bytes | None = None,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        """argv를 그대로 실행한다. 접두어를 붙이지 않는다."""
        t = self.timeout_s if timeout is None else timeout
        return self.runner(tuple(argv), input=input, timeout=t)

    def kubectl_argv(self, *args: str) -> tuple[str, ...]:
        return self.prefix + tuple(args)

    def kubectl(
        self,
        *args: str,
        input: bytes | None = None,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        """`prefix + args`를 실행한다."""
        return self.run(self.kubectl_argv(*args), input=input, timeout=timeout)

    def exec_argv(
        self, pod: str, container: str, *cmd: str, stdin: bool = False
    ) -> tuple[str, ...]:
        """`kubectl -n papertrade exec [-i] <pod> -c <container> -- <cmd>`(설계 4.8, 5.4)."""
        head = ("exec", "-i") if stdin else ("exec",)
        return self.kubectl_argv(*head, pod, "-c", container, "--", *cmd)

    def psql_argv(self) -> tuple[str, ...]:
        """bundle.sql을 표준 입력으로 받는 psql argv(설계 5.4)."""
        if self.psql_prefix is not None:
            return self.psql_prefix + PSQL_ARGS
        return self.exec_argv(PG_POD, PG_CONTAINER, *PSQL_ARGS, stdin=True)

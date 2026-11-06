"""클러스터 명령 전송(설계 5.1, 5.4, 7.1, 7.2, 계약 정렬 명세 6).

- judge는 노트북에서 kubectl을 부른다. 모든 클러스터 명령은 CMD_PREFIX로 시작한다.
- psql 대상(Pod, 컨테이너, DB 사용자, DB 이름)은 run.json db(model.DbTarget)에서 받는다.
  비밀번호는 넘기지 않는다(Pod 안 Unix 소켓 trust 전제. 아니면 psql이 실패해 DB_ERROR가 된다).
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
from .model import DbTarget

__all__ = [
    "CMD_PREFIX",
    "DEFAULT_TIMEOUT_S",
    "PSQL_ARGS",
    "TEST_PSQL_PREFIX",
    "Runner",
    "TransportError",
    "Transports",
    "psql_args",
    "subprocess_runner",
]

NAMESPACE = "papertrade"  # 옛 설계 명세 값. 주문 서비스 namespace는 확인 필요(인프라, 명세 6 <ns>)
CMD_PREFIX = ("kubectl", "-n", NAMESPACE)  # 설계 5.1

# 명세 6: postgres Pod 안 Unix 소켓, 표준 입력으로 bundle.sql을 넘긴다.
# -X: ~/.psqlrc를 읽지 않는다. ON_ERROR_STOP=1: 첫 오류에서 멈춘다(남은 구획은 DB_ERROR).
_PSQL_HEAD = ("psql", "-X", "-q", "-v", "ON_ERROR_STOP=1")


def psql_args(target: DbTarget | None = None) -> tuple[str, ...]:
    """`psql -X -q -v ON_ERROR_STOP=1 -U <user> -d <database> -f -`(명세 6)."""
    t = target if target is not None else DbTarget()
    return (*_PSQL_HEAD, "-U", t.user, "-d", t.database, "-f", "-")


PSQL_ARGS = psql_args()  # 기본 대상(DbTarget 기본값)
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
        `prefix + exec -i <pod> [-c <container>] --`를 쓴다(명세 6, DbTarget).
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
        self, pod: str, container: str | None, *cmd: str, stdin: bool = False
    ) -> tuple[str, ...]:
        """`kubectl -n papertrade exec [-i] <pod> [-c <container>] -- <cmd>`(설계 4.8, 명세 6).

        container가 None이거나 빈 문자열이면 `-c`를 붙이지 않는다(Pod 기본 컨테이너).
        """
        head = ("exec", "-i") if stdin else ("exec",)
        pick = ("-c", container) if container else ()
        return self.kubectl_argv(*head, pod, *pick, "--", *cmd)

    def psql_argv(self, target: DbTarget | None = None) -> tuple[str, ...]:
        """bundle.sql을 표준 입력으로 받는 psql argv(명세 6). target 기본값은 DbTarget()."""
        t = target if target is not None else DbTarget()
        args = psql_args(t)
        if self.psql_prefix is not None:
            return self.psql_prefix + args
        return self.exec_argv(t.pod, t.container, *args, stdin=True)

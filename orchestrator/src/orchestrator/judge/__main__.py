"""chaos-judge CLI(설계 1.4, 5.1).

    chaos-judge <명령> --run runs/<run_id>/run.json [--criteria x.yaml] [--offline]

1차 명령: plan accounts doctor fetch collect evaluate run mark stop. 2차: precheck limit.

종료 코드(설계 1.4, errors.py)
- 0: PASS(run, evaluate), 준비됨(doctor, precheck), 그 밖 명령 성공
- 10: FAIL. 11: INCONCLUSIVE, 미비(doctor, precheck). 12: 입력 오류
- 1: judge 오류(예외). 2: 사용법 오류(argparse)
- 3: 아직 구현하지 않은 명령(EXIT_NOT_IMPLEMENTED). runner에게는 "그 밖 = judge 오류"라
  PASS로 오인되지 않는다.

구현 범위
- mark: marks.local.json에 원자적으로 쓴다(--at 없으면 현재 시각).
- evaluate, collect, run: pipeline 모듈의 collect·assess(설계 5.1)를 지연 import로 부르는
  얇은 래퍼다. pipeline이 없으면 3이다. evaluate·run은 runs/<run_id>/에 verdict.json,
  report.md, classified.csv를 원자적으로 쓰고 verdict JSON을 stdout에 낸다(설계 5.2).
  파일 쓰기는 CLI만 한다. 현재 시각은 mark와 verdict의 generated_at(digest 밖)에만 쓴다.
- 나머지는 각 모듈의 스텁(NotImplementedError)을 부르거나 직접 3으로 끝난다.
- 입력(run.json, 기준 파일)은 늘 먼저 검증한다. 입력이 틀리면 구현 여부와 상관없이 12다.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import importlib
import json
import os
import re
import sys
import tempfile
import time
import traceback
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import ModuleType
from typing import Any

from . import __version__
from . import client as client_mod
from . import fetch as fetch_mod
from . import limit as limit_mod
from .context import (
    EPOCH_MS_MAX,
    EPOCH_MS_MIN,
    MARKS_LOCAL_NAME,
    check_user_id,
    load_run,
)
from .criteria import select_criteria
from .errors import (
    EXIT_BY_VERDICT,
    EXIT_INCONCLUSIVE,
    EXIT_JUDGE_ERROR,
    EXIT_NOT_IMPLEMENTED,
    EXIT_PASS,
    InputError,
    JudgeError,
)
from .model import Criteria, Outcome, RunContext, Verdict, to_jsonable
from .transport import Transports
from .verdict import exit_code_for

__all__ = ["EXIT_NOT_IMPLEMENTED", "build_parser", "main"]

PROG = "chaos-judge"
MARK_NAMES = ("inject_start", "inject_end")
OBS_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_INT_RE = re.compile(r"^-?(0|[1-9][0-9]*)$")

EPILOG = f"""\
종료 코드(설계 1.4):
  0   PASS(run, evaluate) / 준비됨(doctor, precheck) / 그 밖 명령 성공
  10  FAIL
  11  INCONCLUSIVE / 미비(doctor, precheck)
  12  입력 오류(run.json, 기준 파일, marks.local.json)
  1   judge 오류
  2   사용법 오류
  {EXIT_NOT_IMPLEMENTED}   아직 구현하지 않은 명령
"""


# ---------------------------------------------------------------- 인자 형식


def _epoch_ms_arg(s: str) -> int:
    """--at: 정수 epoch ms. 초 단위 실수를 막는다(context와 같은 범위)."""
    if not _INT_RE.fullmatch(s.strip()):
        raise argparse.ArgumentTypeError(f"정수 epoch ms가 아니다: {s!r}")
    v = int(s)
    if not EPOCH_MS_MIN <= v < EPOCH_MS_MAX:
        raise argparse.ArgumentTypeError(
            f"{v}는 epoch ms 범위가 아니다(초 단위인지 확인)"
        )
    return v


def _speed_arg(s: str) -> Decimal:
    """--speed: 0보다 큰 유한한 수. 리플레이 배속 기록이다(참고용, 판정에 쓰지 않는다. 계약 명세 7)."""
    try:
        v = Decimal(s.strip())
    except InvalidOperation as e:
        raise argparse.ArgumentTypeError(f"수가 아니다: {s!r}") from e
    if not v.is_finite() or v <= 0:
        raise argparse.ArgumentTypeError(f"배속은 0보다 커야 한다: {s!r}")
    return v


def _user_id_arg(s: str) -> str:
    """--extra-account: 발표자 user_id(계약 명세 8). 형식은 context.check_user_id와 같다."""
    try:
        return check_user_id(s, "--extra-account")
    except InputError as e:
        raise argparse.ArgumentTypeError(str(e)) from e


def _obs_value(s: str) -> Any:
    """true·false → bool, 10진 정수 → int, 그 밖은 문자열. 실수는 문자열로 둔다(float 금지)."""
    if s == "true":
        return True
    if s == "false":
        return False
    if _INT_RE.fullmatch(s):
        return int(s)
    return s


def _obs_arg(s: str) -> tuple[str, Any]:
    key, sep, value = s.partition("=")
    if not sep or not OBS_KEY_RE.fullmatch(key):
        raise argparse.ArgumentTypeError(
            f"--obs는 key=value 형식이고 key는 ^[a-z][a-z0-9_]*$여야 한다: {s!r}"
        )
    if value == "":
        raise argparse.ArgumentTypeError(f"--obs 값이 비었다: {s!r}")
    return key, _obs_value(value)


# ---------------------------------------------------------------- 파서


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--run",
        required=True,
        type=Path,
        metavar="RUN_JSON",
        help="runs/<run_id>/run.json(설계 1.4)",
    )
    common.add_argument(
        "--criteria",
        type=Path,
        metavar="YAML",
        help="판정 기준 파일. 주면 run.json의 criteria 대신 쓴다(현재 디렉터리 기준)",
    )
    common.add_argument(
        "--offline",
        action="store_true",
        help="클러스터를 만지지 않는다. run은 수집 없이 기존 증거로 판정한다",
    )

    parser = argparse.ArgumentParser(
        prog=PROG,
        description="측정과 판정(judge). 설계 docs/design/b-measure-judge.md",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    sub = parser.add_subparsers(dest="command", metavar="<명령>", required=True)

    def add(name: str, func: Any, help_: str) -> argparse.ArgumentParser:
        p = sub.add_parser(
            name,
            parents=[common],
            help=help_,
            description=help_,
            epilog=EPILOG,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        p.set_defaults(func=func, parser=p)
        return p

    add("plan", _cmd_plan, "k6 Job 템플릿 렌더와 구간 길이 검사(설계 1.4, 4.8, 5.9)")
    accounts = add(
        "accounts",
        _cmd_accounts,
        "k6 기록의 계좌 확인, idx0 user_id 출력(설계 3, 계약 명세 8)",
    )
    accounts.add_argument(
        "--wait", action="store_true", help="첫 intent 줄이 나올 때까지 기다린다"
    )
    add("doctor", _cmd_doctor, "권한·라벨·psql 사전 점검(설계 7.5)")
    add("fetch", _cmd_fetch, "k6 기록 회수(설계 4.8)")
    add("collect", _cmd_collect, "fetch → 정지 확인 → DB → Pod 관측(설계 5.2)")
    add("evaluate", _cmd_evaluate, "증거만으로 판정(설계 5.2 6단계)")
    add("run", _cmd_run, "collect 후 evaluate. --offline이면 evaluate만(설계 5.2)")

    mark = add(
        "mark", _cmd_mark, "marks.local.json에 시각·관측값을 쓴다(설계 1.4, 5.1)"
    )
    mark.add_argument(
        "name",
        nargs="?",
        choices=MARK_NAMES,
        help="표시 이름. --speed·--extra-account·--obs만 쓸 때는 생략한다",
    )
    mark.add_argument(
        "--at",
        type=_epoch_ms_arg,
        metavar="EPOCH_MS",
        help="시각(epoch ms). 없으면 현재 시각",
    )
    mark.add_argument(
        "--speed",
        type=_speed_arg,
        metavar="N",
        help="--at(또는 현재) 시각부터 리플레이 배속 N(replay_speed에 추가). 참고용, 판정에 쓰지 않는다",
    )
    mark.add_argument(
        "--extra-account",
        dest="extra_accounts",
        action="append",
        type=_user_id_arg,
        default=[],
        metavar="USER_ID",
        help="DB 전용 검사만 받는 계좌 user_id(시연 발표자, 계약 명세 8). 여러 번 줄 수 있다",
    )
    mark.add_argument(
        "--obs",
        action="append",
        type=_obs_arg,
        default=[],
        metavar="KEY=VALUE",
        help="관측값(설계 5.2 4단계). true·false·정수는 그 타입, 나머지는 문자열",
    )
    mark.add_argument(
        "--force",
        action="store_true",
        help="marks.local.json의 기존 값을 덮어쓴다",
    )

    stop = add(
        "stop", _cmd_stop, "k6에 SIGINT를 보내 handleSummary 후 끝낸다(설계 4.8)"
    )
    stop.add_argument("--ws", action="store_true", help="k6-ws 컨테이너도 멈춘다")

    add("precheck", _cmd_precheck, "[2차] 주입 전 게이트(설계 1.4)")
    add("limit", _cmd_limit, "[2차] 한계 찾기 판정(설계 4.10)")
    return parser


# ---------------------------------------------------------------- 공용


def _now_ms() -> int:
    """현재 시각 epoch ms. mark와 generated_at(digest 밖)만 쓴다(판정 결정성)."""
    return time.time_ns() // 1_000_000


def _transports() -> Transports:
    """클러스터 전송. 테스트는 이 함수를 바꿔 실제 kubectl 호출을 막는다."""
    return Transports()


def _usage(args: argparse.Namespace, msg: str) -> None:
    """사용법 오류. argparse 규칙대로 종료 코드 2."""
    args.parser.error(msg)


def _require_online(args: argparse.Namespace) -> None:
    if args.offline:
        _usage(args, f"{args.command}는 클러스터 명령이라 --offline과 함께 쓸 수 없다")


def _criteria(
    args: argparse.Namespace, ctx: RunContext, *, prefer_resolved: bool
) -> Criteria:
    """--criteria > (증거 판정이면) runs/<id>/criteria.resolved.yaml > run.json criteria.

    evaluate는 증거와 함께 저장된 resolved 기준만 읽는다(설계 5.2 6단계).
    """
    return select_criteria(
        ctx.criteria,
        ctx.run_dir,
        override=args.criteria,
        prefer_resolved=prefer_resolved,
    )


def _run_dir(ctx: RunContext) -> Path:
    if ctx.run_dir is None:
        raise JudgeError("run 디렉터리를 알 수 없다")
    return ctx.run_dir


def _pipeline(section: str) -> ModuleType:
    """pipeline 모듈(설계 5.1 collect·evaluate). 모듈 자체가 없을 때만 미구현으로 본다."""
    name = f"{__package__}.pipeline"
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as e:
        if e.name != name:
            raise
        raise NotImplementedError(section) from None


def _emit_verdict(v: Any) -> int:
    """Verdict를 stdout JSON으로 내고 종료 코드로 바꾼다(설계 1.4)."""
    if not isinstance(v, Verdict):
        raise JudgeError(f"evaluate가 Verdict가 아닌 {type(v).__name__}를 냈다")
    code = exit_code_for(v.verdict)
    sys.stdout.write(
        json.dumps(v.to_dict(), ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    )
    reasons = ",".join(v.reasons) or "-"
    print(f"{PROG}: verdict={v.verdict} reasons={reasons}", file=sys.stderr)
    return code


def _evaluate_and_write(
    pipeline: ModuleType, ctx: RunContext, crit: Criteria, evidence: Path
) -> int:
    """pipeline.assess로 판정하고 runs/<run_id>/에 산출물을 쓴다(설계 5.2, 5.8).

    verdict가 PASS·FAIL·INCONCLUSIVE가 아니면 아무것도 쓰지 않고 judge 오류다.
    """
    a = pipeline.assess(ctx, crit, evidence, generated_at=_now_ms())
    v = a.verdict
    if not isinstance(v, Verdict):
        raise JudgeError(f"evaluate가 Verdict가 아닌 {type(v).__name__}를 냈다")
    exit_code_for(v.verdict)
    run_dir = _run_dir(ctx)
    files = pipeline.render_outputs(a)
    for name in sorted(files):
        _write_atomic(run_dir / name, files[name].encode("utf-8"))
    if files:
        print(f"{PROG}: wrote {', '.join(sorted(files))} in {run_dir}", file=sys.stderr)
    return _emit_verdict(v)


def _print_json(obj: Any) -> None:
    sys.stdout.write(
        json.dumps(to_jsonable(obj), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n"
    )


# ---------------------------------------------------------------- 명령


def _cmd_plan(args: argparse.Namespace) -> int:
    ctx = load_run(args.run)
    _criteria(args, ctx, prefer_resolved=False)
    raise NotImplementedError("설계 1.4, 4.8, 1차")


def _cmd_accounts(args: argparse.Namespace) -> int:
    _require_online(args)
    load_run(args.run)
    raise NotImplementedError("설계 3, 1차")


def _cmd_doctor(args: argparse.Namespace) -> int:
    _require_online(args)
    load_run(args.run)
    raise NotImplementedError("설계 7.5, 1차")


def _cmd_fetch(args: argparse.Namespace) -> int:
    _require_online(args)
    ctx = load_run(args.run)
    result = fetch_mod.fetch(ctx, _transports(), _run_dir(ctx))
    _print_json(result)
    # 기록을 못 받았거나 원격과 다르다. Job을 지우지 않고 다시 돈다
    if not result.sha_ok:
        print(f"{PROG} fetch: 기록 회수 실패. 다시 fetch한다", file=sys.stderr)
        return EXIT_JUDGE_ERROR
    return EXIT_PASS


def _cmd_stop(args: argparse.Namespace) -> int:
    _require_online(args)
    ctx = load_run(args.run)
    fetch_mod.stop(ctx, _transports(), ws=args.ws)
    return EXIT_PASS


def _cmd_collect(args: argparse.Namespace) -> int:
    _require_online(args)
    ctx = load_run(args.run)
    crit = _criteria(args, ctx, prefer_resolved=False)
    pipeline = _pipeline("설계 5.2, 1차")
    evidence = pipeline.collect(ctx, crit, _transports())
    print(evidence)
    return EXIT_PASS


def _cmd_evaluate(args: argparse.Namespace) -> int:
    ctx = load_run(args.run)
    crit = _criteria(args, ctx, prefer_resolved=True)
    pipeline = _pipeline("설계 5.2 6단계, 1차")
    return _evaluate_and_write(pipeline, ctx, crit, _run_dir(ctx))


def _cmd_run(args: argparse.Namespace) -> int:
    ctx = load_run(args.run)
    crit = _criteria(args, ctx, prefer_resolved=args.offline)
    pipeline = _pipeline("설계 5.2, 1차")
    if args.offline:
        evidence = _run_dir(ctx)
    else:
        evidence = pipeline.collect(ctx, crit, _transports())
    return _evaluate_and_write(pipeline, ctx, crit, evidence)


def _cmd_precheck(args: argparse.Namespace) -> int:
    _require_online(args)
    ctx = load_run(args.run)
    crit = _criteria(args, ctx, prefer_resolved=False)
    res = client_mod.precheck(ctx, crit, _transports())
    _print_json(res)
    return EXIT_PASS if res.outcome == Outcome.PASS else EXIT_INCONCLUSIVE


def _cmd_limit(args: argparse.Namespace) -> int:
    ctx = load_run(args.run)
    crit = _criteria(args, ctx, prefer_resolved=True)
    res = limit_mod.find_limit(ctx, crit, _run_dir(ctx))
    _print_json(res)
    return EXIT_BY_VERDICT.get(str(res.outcome), EXIT_JUDGE_ERROR)


# ---------------------------------------------------------------- mark


def _is_blank(v: Any) -> bool:
    """context의 빈 칸 규칙과 같다: None, 빈 문자열·배열·객체."""
    return v is None or (isinstance(v, (str, list, dict)) and len(v) == 0)


def _same(a: Any, b: Any) -> bool:
    """타입까지 같아야 같은 값이다(True == 1을 막는다)."""
    return type(a) is type(b) and a == b


def _section(doc: dict[str, Any], name: str, kind: type) -> Any:
    v = doc.get(name)
    if v is None:
        v = kind()
        doc[name] = v
    if not isinstance(v, kind):
        raise InputError(f"{MARKS_LOCAL_NAME}: {name}의 형식이 틀렸다")
    return v


def _speed_json(v: Decimal) -> int | str:
    """정수면 JSON 정수, 아니면 10진 문자열(context가 둘 다 받는다)."""
    return int(v) if v == v.to_integral_value() else str(v)


def _shadowed(run_doc: Mapping[str, Any], name: str, key: str | None = None) -> bool:
    """run.json에 값이 있어 marks.local.json 값이 쓰이지 않는가(설계 1.4)."""
    v = run_doc.get(name)
    if key is not None:
        v = v.get(key) if isinstance(v, dict) else None
    return not _is_blank(v)


def _apply_mark(
    local: Mapping[str, Any],
    run_doc: Mapping[str, Any],
    ctx: RunContext,
    args: argparse.Namespace,
    at: int,
) -> tuple[dict[str, Any], list[str]]:
    """marks.local.json의 새 내용과 바뀐 항목. 충돌이면 InputError."""
    new = copy.deepcopy(dict(local))
    new["run_id"] = ctx.run_id
    changes: list[str] = []
    force = args.force

    if args.name is not None:
        name = args.name
        if _shadowed(run_doc, "marks", name):
            raise InputError(
                f"run.json에 marks.{name}이 이미 있다. run.json 값이 우선하므로 "
                f"{MARKS_LOCAL_NAME}에 쓰지 않는다"
            )
        marks = _section(new, "marks", dict)
        cur = marks.get(name)
        if not _same(cur, at):
            if not _is_blank(cur) and not force:
                raise InputError(
                    f"{MARKS_LOCAL_NAME}에 marks.{name}={cur}가 이미 있다. "
                    "덮어쓰려면 --force"
                )
            marks[name] = at
            changes.append(f"marks.{name}={at}")
        eff = dict(ctx.marks)
        eff[name] = at
        start, end = eff.get("inject_start"), eff.get("inject_end")
        if start is not None and end is not None and end < start:
            raise InputError(f"inject_end({end})가 inject_start({start})보다 이르다")

    if args.speed is not None:
        if _shadowed(run_doc, "replay_speed"):
            raise InputError(
                "run.json에 replay_speed가 이미 있다. run.json 값이 우선하므로 "
                f"{MARKS_LOCAL_NAME}에 쓰지 않는다"
            )
        points = _section(new, "replay_speed", list)
        same_t = [p for p in points if isinstance(p, dict) and p.get("from_ms") == at]
        if same_t:
            p = same_t[0]
            if Decimal(str(p.get("speed"))) != args.speed:
                if not force:
                    raise InputError(
                        f"{MARKS_LOCAL_NAME}에 from_ms={at} 배속 {p.get('speed')}가 "
                        "이미 있다. 덮어쓰려면 --force"
                    )
                p["speed"] = _speed_json(args.speed)
                changes.append(f"replay_speed[from_ms={at}].speed={args.speed}")
        else:
            points.append({"from_ms": at, "speed": _speed_json(args.speed)})
            points.sort(key=lambda p: p["from_ms"])
            changes.append(f"replay_speed+=(from_ms={at}, speed={args.speed})")

    if args.extra_accounts:
        if _shadowed(run_doc, "extra_accounts"):
            raise InputError(
                "run.json에 extra_accounts가 이미 있다. run.json 값이 우선하므로 "
                f"{MARKS_LOCAL_NAME}에 쓰지 않는다"
            )
        accts = _section(new, "extra_accounts", list)
        known = {str(a) for a in accts}  # user_id는 대소문자를 구분한다(TEXT PK)
        for a in args.extra_accounts:
            if a not in known:
                accts.append(a)
                known.add(a)
                changes.append(f"extra_accounts+={a}")

    if args.obs:
        keys = [k for k, _ in args.obs]
        dup = sorted({k for k in keys if keys.count(k) > 1})
        if dup:
            _usage(args, f"--obs 키가 겹친다: {dup}")
        for key, value in args.obs:
            if _shadowed(run_doc, "obs", key):
                raise InputError(
                    f"run.json에 obs.{key}가 이미 있다. run.json 값이 우선하므로 "
                    f"{MARKS_LOCAL_NAME}에 쓰지 않는다"
                )
            obs = _section(new, "obs", dict)
            cur = obs.get(key)
            if _same(cur, value):
                continue
            if not _is_blank(cur) and not force:
                raise InputError(
                    f"{MARKS_LOCAL_NAME}에 obs.{key}={cur!r}가 이미 있다. 덮어쓰려면 --force"
                )
            obs[key] = value
            changes.append(f"obs.{key}={value!r}")
    return new, changes


def _write_atomic(path: Path, data: bytes) -> None:
    """같은 디렉터리 임시 파일에 쓰고 fsync 후 os.replace로 바꾼다."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise
    with contextlib.suppress(OSError):
        dfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)


def _cmd_mark(args: argparse.Namespace) -> int:
    """`judge mark`(설계 1.4, 5.1). run.json은 읽기만 하고 marks.local.json에 쓴다.

    - run.json에 이미 값이 있는 항목은 거부한다(run.json이 우선이라 쓰여도 무시된다).
    - marks.local.json의 기존 값과 다르면 --force가 있어야 덮어쓴다. 같으면 아무것도 안 한다.
    - 쓰고 나서 load_run으로 다시 검증하고, 실패하면 이전 내용으로 되돌린다.
    - 동시에 여러 mark를 돌리는 경우는 막지 않는다(운영자 한 명 전제).
    """
    if (
        args.name is None
        and args.speed is None
        and not args.extra_accounts
        and not args.obs
    ):
        _usage(
            args,
            "이름(inject_start|inject_end), --speed, --extra-account, --obs 중 하나가 필요하다",
        )
    ctx = load_run(args.run)
    run_dir = _run_dir(ctx)
    run_doc = json.loads(
        Path(args.run).read_text(encoding="utf-8"), parse_float=Decimal
    )
    path = run_dir / MARKS_LOCAL_NAME
    old = path.read_bytes() if path.exists() else None
    local: dict[str, Any] = {}
    if old is not None:
        try:
            local = json.loads(old.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as e:
            raise InputError(f"{path}: JSON 형식 오류({e})") from e

    at = args.at if args.at is not None else _now_ms()
    new, changes = _apply_mark(local, run_doc, ctx, args, at)
    if not changes:
        print(f"{MARKS_LOCAL_NAME}: 변경 없음")
        return EXIT_PASS

    text = json.dumps(
        new, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False
    )
    _write_atomic(path, (text + "\n").encode("utf-8"))
    try:
        load_run(args.run)
    except JudgeError:
        if old is None:
            path.unlink(missing_ok=True)
        else:
            _write_atomic(path, old)
        raise
    for c in changes:
        print(f"{MARKS_LOCAL_NAME}: {c}")
    return EXIT_PASS


# ---------------------------------------------------------------- main


def _exit_code(e: SystemExit) -> int:
    if e.code is None:
        return EXIT_PASS
    return e.code if isinstance(e.code, int) else 2


def main(argv: Sequence[str] | None = None) -> int:
    """CLI 진입점. 종료 코드를 돌려준다(설계 1.4). --help는 0, 사용법 오류는 2."""
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return _exit_code(e)
    try:
        return args.func(args)
    except SystemExit as e:  # 명령 안 사용법 오류(parser.error)
        return _exit_code(e)
    except NotImplementedError as e:
        print(
            f"{PROG} {args.command}: not implemented yet ({e or '설계 미정'})",
            file=sys.stderr,
        )
        return EXIT_NOT_IMPLEMENTED
    except JudgeError as e:
        print(f"{PROG} {args.command}: {e}", file=sys.stderr)
        return e.exit_code
    except Exception:
        print(f"{PROG} {args.command}: judge 오류", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return EXIT_JUDGE_ERROR


if __name__ == "__main__":
    sys.exit(main())

"""k6 주문 기록(orders.jsonl) 엄격 파서와 기록 완전성(설계 4.6, 4.8, 5.3).

- 줄마다 JSON을 읽고 kind별 필수 필드를 검사한다. 문제는 예외로 멈추지 않고
  K6Log.problems에 줄 번호와 함께 남긴다. 파일을 못 읽을 때만 InputError다.
- problems 원소는 "<code> L<줄>: <설명>" 또는 "<code>: <설명>"이다. code는 이 모듈의
  소문자 문제 코드다(verdict 사유 코드가 아니다). problem_code()로 꺼낸다.
- 설명에 문자열 필드 값을 넣지 않는다(길이만 적는다). 토큰이 섞여도 새지 않는다(설계 7.4).
- 필드가 없거나 핵심 필드 값이 틀리면 그 줄을 버린다. 보조 값(plan, symbols,
  initial_cash, conn_reuse, body 내용, ack·lookup 일관성)이 틀리면 줄은 살리고 문제만 남긴다.
- .jsonl.gz도 읽는다. sha256은 압축을 푼 원본 바이트 기준이다(4.8 sha256sum과 같다).
- WS 줄(ws_*, wsagg)은 2차라 kind_counts에 세기만 한다(4.9).
"""

from __future__ import annotations

import json
import math
import os
import re
import uuid
import zlib
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

from .context import EPOCH_MS_MAX, EPOCH_MS_MIN
from .errors import InputError
from .model import (
    JSONL_VERSION,
    AccountRec,
    Ack,
    Attempt,
    CheckResult,
    Intent,
    K6Log,
    Lookup,
    Outcome,
    Resend,
    Result,
    RunStart,
    SetupDone,
    sha256_bytes,
)
from .reasons import SENT_LOG_INCOMPLETE

__all__ = [
    "COUNTER_INTENTS",
    "COUNTER_RESULTS",
    "K6_OK_EXIT_CODES",
    "LOG_COMPLETE_CONDITIONS",
    "LOG_COMPLETE_ID",
    "LOG_COMPLETE_KIND",
    "find_token_keys",
    "load_summary",
    "log_complete",
    "parse_k6log",
    "problem_code",
    "problem_counts",
    "sha_matches",
    "summary_count",
]

# ---------------------------------------------------------------- 상수

KIND_RUN_START = "run_start"
KIND_ACCOUNT = "account"
KIND_SETUP_DONE = "setup_done"
KIND_INTENT = "intent"
KIND_RESULT = "result"
KNOWN_KINDS = frozenset(
    {KIND_RUN_START, KIND_ACCOUNT, KIND_SETUP_DONE, KIND_INTENT, KIND_RESULT}
)
WS_KIND_PREFIXES = ("ws_", "wsagg")  # 4.9 WS 줄. 2차라 세기만 한다

RUN_START_FIELDS = (
    "v",
    "run",
    "script_ver",
    "seed",
    "profile",
    "plan",
    "symbols",
    "initial_cash",
    "conn_reuse",
    "t",
)
ACCOUNT_FIELDS = ("run", "idx", "acct", "nick")
SETUP_DONE_FIELDS = ("run", "accounts", "t")
INTENT_FIELDS = ("run", "key", "acct", "vu", "t", "body")
RESULT_FIELDS = ("run", "key", "atts", "lookup", "ack", "resend", "t")
ATTEMPT_FIELDS = ("n", "t0", "t1", "st", "ec", "ms")
BODY_FIELDS = ("symbol", "side", "qty", "limit_price")  # 4.3 주문 본문

COUNTER_INTENTS = "order_intents"  # 4.7 완전성 대조 Counter
COUNTER_RESULTS = "order_results"
_COUNTERS = ((KIND_INTENT, COUNTER_INTENTS), (KIND_RESULT, COUNTER_RESULTS))
K6_OK_EXIT_CODES = frozenset({0, 99})  # 5.3. 99는 threshold 실패(abortOnFail 포함, 4.7)

MAX_PROBLEMS_PER_CODE = 100  # 코드별 상한. 넘으면 "<code>: 그 밖 N건 생략" 한 줄
GZIP_MAGIC = b"\x1f\x8b"
_GZIP_CHUNK = 1 << 16

# 문제 코드(소문자). verdict 사유가 아니라 problems 접두어다.
BAD_UTF8 = "bad_utf8"
BAD_JSON = "bad_json"
NOT_OBJECT = "not_object"
DUP_KEY = "dup_key"
MISSING_FIELD = "missing_field"
BAD_FIELD = "bad_field"
UNKNOWN_KIND = "unknown_kind"
BAD_VERSION = "bad_version"
GZIP_ERROR = "gzip_error"
RUN_MIXED = "run_mixed"
RUN_START_COUNT = "run_start_count"
SETUP_DONE_COUNT = "setup_done_count"
ACCOUNT_COUNT = "account_count"
DUP_ACCOUNT = "dup_account"
DUP_INTENT = "dup_intent"
DUP_RESULT = "dup_result"
ORPHAN_RESULT = "orphan_result"
COUNTER_SHORT = "counter_short"
SUMMARY_INVALID = "summary_invalid"
SHA_MISMATCH = "sha_mismatch"
TOKEN_KEY = "token_key"
OTHER = "other"  # 코드 접두어가 없는 문제(직접 만든 K6Log 등)

# log_complete 조건(설계 5.3). records_consistent는 이 모듈이 더한 조건이다.
COND_LINES = "lines_parsed"
COND_RUN = "run_single"
COND_RUN_START = "run_start_one"
COND_RECORDS = "records_consistent"
COND_COUNTERS = "counters"
COND_SHA = "sha256"
COND_EXIT = "k6_exit"
LOG_COMPLETE_CONDITIONS = (
    COND_LINES,
    COND_RUN,
    COND_RUN_START,
    COND_RECORDS,
    COND_COUNTERS,
    COND_SHA,
    COND_EXIT,
)
LOG_COMPLETE_ID = "log_complete"
LOG_COMPLETE_KIND = "log"

# 문제 코드 → 깨지는 완전성 조건. None은 완전성과 무관(safety 가드 몫).
# 여기 없는 코드는 lines_parsed로 본다(모르는 문제는 보수적으로 다룬다).
PROBLEM_CONDITION: Mapping[str, str | None] = {
    BAD_UTF8: COND_LINES,
    BAD_JSON: COND_LINES,
    NOT_OBJECT: COND_LINES,
    DUP_KEY: COND_LINES,
    MISSING_FIELD: COND_LINES,
    BAD_FIELD: COND_LINES,
    UNKNOWN_KIND: COND_LINES,
    BAD_VERSION: COND_LINES,
    GZIP_ERROR: COND_LINES,
    OTHER: COND_LINES,
    RUN_MIXED: COND_RUN,
    RUN_START_COUNT: COND_RUN_START,
    SETUP_DONE_COUNT: COND_RECORDS,
    ACCOUNT_COUNT: COND_RECORDS,
    DUP_ACCOUNT: COND_RECORDS,
    DUP_INTENT: COND_RECORDS,
    DUP_RESULT: COND_RECORDS,
    ORPHAN_RESULT: COND_RECORDS,
    COUNTER_SHORT: COND_COUNTERS,
    SUMMARY_INVALID: COND_COUNTERS,
    SHA_MISMATCH: COND_SHA,
    TOKEN_KEY: None,
}

_CODE_RE = re.compile(r"^([a-z][a-z0-9_]*)(?: L\d+)?:")
_OMITTED_RE = re.compile(r"^([a-z][a-z0-9_]*): 그 밖 (\d+)건 생략$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
# 문자열 안 JSON 텍스트의 "...token": 키(잘린 응답 원문도 잡는다)
_TOKEN_TEXT_RE = re.compile(r'"[^"\\]*token"\s*:', re.IGNORECASE)


# ---------------------------------------------------------------- 공개 함수


def parse_k6log(
    path_or_lines: str | os.PathLike[str] | bytes | Iterable[str | bytes],
    *,
    summary: Mapping[str, Any] | None = None,
    expected_sha256: str | None = None,
) -> K6Log:
    """orders.jsonl(.gz)을 엄격하게 읽어 K6Log를 만든다(설계 4.6, 5.3).

    path_or_lines: 파일 경로(str·PathLike), 파일 바이트, 또는 줄 목록(str·bytes).
      gzip 매직(1f 8b)이면 압축을 푼다. 줄 목록이면 sha256은 줄마다 "\\n"을 붙인 바이트 기준이다.
    summary: k6 handleSummary JSON. 주면 Counter 대 줄 수(counter_short)와 token 키도 본다.
    expected_sha256: fetch가 받은 sha256(sha256sum 출력 그대로도 된다). 다르면 sha_mismatch.

    줄 단위 문제는 problems에 남기고 계속 읽는다. 파일을 못 읽으면 InputError다.
    has_token_key는 JSONL과 (주면) summary 어디든 'token'으로 끝나는 키가 있으면 True다.
    """
    lines, sha, pre = _read_source(path_or_lines)
    p = _Parser()
    for msg in pre:
        p.problem(GZIP_ERROR, msg)
    for lineno, raw in enumerate(lines, start=1):
        p.feed(lineno, raw)
    return p.finish(sha=sha, summary=summary, expected_sha256=expected_sha256)


def log_complete(
    log: K6Log,
    summary: Mapping[str, Any] | None,
    k6_exit_code: int | None,
    sha_ok: bool | None,
) -> CheckResult:
    """기록 완전성(설계 5.3 log_complete).

    조건(LOG_COMPLETE_CONDITIONS 순서):
    - lines_parsed: 모든 줄이 파싱되고 필드 검사를 통과했다.
    - run_single: run 값이 하나다. run_start_one: run_start 줄이 정확히 1개다.
    - records_consistent: intent·result 키 중복 없음, intent 없는 result 없음,
      setup_done 1개, account 줄 수 = setup_done.accounts(이 모듈이 더한 조건).
    - counters: intent·result 줄 수 ≥ summary의 order_intents·order_results.
      summary가 없으면(OOM 등) 실패다.
    - sha256: sha_ok가 True다. None은 미검증으로 실패다(sha_matches()로 구한다).
    - k6_exit: 종료 코드가 0 또는 99다. None(모름)은 실패다.

    결과: 완전하면 PASS, 아니면 INCONCLUSIVE + SENT_LOG_INCOMPLETE. value는 bool.
    required=False다. verdict 집계에 직접 넣지 않고 ORPHAN·unresolved 기준을 낮추는 데 쓴다.
    LOST·MISMATCH·중복은 있는 줄이 근거라 이 결과와 상관없이 판정한다(5.3).
    """
    failing: dict[str, list[str]] = {}
    counts = problem_counts(log.problems)
    for code, n in counts.items():
        cond = PROBLEM_CONDITION.get(code, COND_LINES)
        if cond is not None:
            failing.setdefault(cond, []).append(f"{code} {n}건")

    cdetail, cprobs = _counter_check(summary, log.kind_counts)
    for _code, msg in cprobs:
        failing.setdefault(COND_COUNTERS, []).append(msg)

    if sha_ok is not True:
        msg = "sha256 불일치" if sha_ok is False else "sha256 미검증"
        failing.setdefault(COND_SHA, []).append(msg)

    if not _exit_ok(k6_exit_code):
        msg = (
            "k6 종료 코드 모름"
            if k6_exit_code is None
            else f"k6 종료 코드 {k6_exit_code!r}(0 또는 99여야 한다)"
        )
        failing.setdefault(COND_EXIT, []).append(msg)

    failed = [c for c in LOG_COMPLETE_CONDITIONS if c in failing]
    ok = not failed
    detail: dict[str, Any] = {
        "failed": failed,
        "why": {c: "; ".join(failing[c]) for c in failed},
        **cdetail,
        "k6_exit_code": k6_exit_code if _plain_int(k6_exit_code) else None,
        "sha_ok": sha_ok if isinstance(sha_ok, bool) else None,
        "problems": counts,
    }
    return CheckResult(
        id=LOG_COMPLETE_ID,
        kind=LOG_COMPLETE_KIND,
        outcome=Outcome.PASS if ok else Outcome.INCONCLUSIVE,
        required=False,
        on_violation="inconclusive",
        value=ok,
        reasons=() if ok else (SENT_LOG_INCOMPLETE,),
        detail=detail,
    )


def load_summary(path: str | os.PathLike[str]) -> Mapping[str, Any] | None:
    """k6 summary.json(handleSummary 출력)을 읽는다. 파일이 없으면 None(OOM 등).

    JSON 객체가 아니면 InputError다. 실수는 Decimal로 읽는다.
    """
    p = Path(path)
    try:
        data = p.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as e:
        raise InputError(f"summary를 읽을 수 없다: {p} ({e.strerror or e})") from e
    try:
        doc = json.loads(data.decode("utf-8"), parse_float=Decimal)
    except (UnicodeDecodeError, ValueError, RecursionError) as e:
        raise InputError(f"summary가 JSON이 아니다: {p}") from e
    if not isinstance(doc, dict):
        raise InputError(f"summary 최상위가 객체가 아니다: {p}")
    return doc


def summary_count(summary: Mapping[str, Any] | None, metric: str) -> int | None:
    """handleSummary JSON의 Counter 값(metrics.<metric>.values.count).

    --summary-export 모양(metrics.<metric>.count)과 {"data": {...}} 감싸기도 읽는다.
    지표가 없거나 값이 음이 아닌 정수가 아니면 None.
    """
    metrics = _summary_metrics(summary)
    if metrics is None:
        return None
    entry = metrics.get(metric)
    if not isinstance(entry, Mapping):
        return None
    values = entry.get("values", entry)
    if not isinstance(values, Mapping):
        return None
    return _count_value(values.get("count"))


def sha_matches(log: K6Log, expected: str | None) -> bool:
    """log.sha256이 기대값과 같은가. 기대값이 없거나 형식이 틀리면 False."""
    e = _norm_sha(expected)
    return e is not None and e == log.sha256


def find_token_keys(obj: Any, prefix: str = "$") -> tuple[str, ...]:
    """'token'으로 끝나는 키(대소문자 무시)의 경로(설계 5.6 safety, 7.4).

    중첩 객체·배열을 모두 보고, 문자열 안 JSON 텍스트의 "...token": 키도 찾는다
    (응답 원문 raw, 본문 문자열 body). 값은 돌려주지 않는다.
    """
    out: list[str] = []
    _walk_tokens(obj, prefix, out)
    return tuple(out)


def problem_code(problem: str) -> str:
    """problems 원소의 코드 접두어. 없으면 빈 문자열."""
    m = _CODE_RE.match(problem)
    return m.group(1) if m else ""


def problem_counts(problems: Iterable[str]) -> dict[str, int]:
    """문제 코드별 건수(키 정렬). 생략 줄("그 밖 N건 생략")은 N건으로 센다."""
    c: Counter[str] = Counter()
    for p in problems:
        m = _OMITTED_RE.match(p)
        if m:
            c[m.group(1)] += int(m.group(2))
            continue
        c[problem_code(p) or OTHER] += 1
    return dict(sorted(c.items()))


# ---------------------------------------------------------------- 입력 읽기


def _read_source(src: Any) -> tuple[list[bytes], str, list[str]]:
    """(물리 줄 목록, sha256, gzip 문제)."""
    if isinstance(src, (str, os.PathLike)):
        path = Path(src)
        try:
            data = path.read_bytes()
        except OSError as e:
            raise InputError(
                f"k6 기록을 읽을 수 없다: {path} ({e.strerror or e})"
            ) from e
        return _split_data(data)
    if isinstance(src, (bytes, bytearray)):
        return _split_data(bytes(src))
    lines: list[bytes] = []
    for item in src:
        if isinstance(item, str):
            b = item.encode("utf-8", "surrogatepass")
        elif isinstance(item, (bytes, bytearray)):
            b = bytes(item)
        else:
            raise TypeError(f"줄은 str 또는 bytes여야 한다: {type(item).__name__}")
        if b.endswith(b"\n"):
            b = b[:-1]
        lines.append(b)
    sha = sha256_bytes(b"".join(x + b"\n" for x in lines))
    return lines, sha, []


def _split_data(data: bytes) -> tuple[list[bytes], str, list[str]]:
    problems: list[str] = []
    if data[:2] == GZIP_MAGIC:
        data, problems = _gunzip(data)
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return lines, sha256_bytes(data), problems


def _gunzip(data: bytes) -> tuple[bytes, list[str]]:
    """gzip을 푼다. 손상·잘림이면 그때까지 푼 바이트와 문제를 돌려준다."""
    out: list[bytes] = []
    problems: list[str] = []
    pos = 0
    while pos < len(data):
        d = zlib.decompressobj(wbits=31)
        try:
            while pos < len(data) and not d.eof:
                chunk = data[pos : pos + _GZIP_CHUNK]
                pos += len(chunk)
                out.append(d.decompress(chunk))
            out.append(d.flush())
        except zlib.error as e:
            problems.append(f"gzip이 손상됐다({e}). 앞부분만 읽는다")
            break
        if not d.eof:
            problems.append("gzip이 중간에 끝났다(잘림). 앞부분만 읽는다")
            break
        pos -= len(d.unused_data)
        if pos < len(data) and data[pos : pos + 2] != GZIP_MAGIC:
            problems.append("gzip 뒤에 알 수 없는 바이트가 있다")
            break
    return b"".join(out), problems


# ---------------------------------------------------------------- 파서


class _Bad(Exception):
    """줄 하나의 검사 실패. 그 줄(또는 그 값)을 버린다."""

    def __init__(self, code: str, msg: str) -> None:
        super().__init__(msg)
        self.code = code
        self.msg = msg


class _DupKey(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key


Soft = list[tuple[str, str]]


class _Parser:
    def __init__(self) -> None:
        self.problems: list[str] = []
        self._code_n: Counter[str] = Counter()
        self._omitted: Counter[str] = Counter()
        self.kind_counts: Counter[str] = Counter()
        self.line_count = 0
        self.runs: dict[str, int] = {}  # run 값 → 처음 나온 줄
        self.run_start: RunStart | None = None
        self.setup_done: SetupDone | None = None
        self.accounts: list[AccountRec] = []
        self._acct_ids: set[str] = set()
        self._acct_idx: set[int] = set()
        self.intents: dict[str, Intent] = {}
        self.results: dict[str, Result] = {}
        self._result_line: dict[str, int] = {}
        self.has_token_key = False
        self._handlers: dict[str, Callable[[dict[str, Any], Soft], None]] = {
            KIND_RUN_START: self._on_run_start,
            KIND_ACCOUNT: self._on_account,
            KIND_SETUP_DONE: self._on_setup_done,
            KIND_INTENT: self._on_intent,
            KIND_RESULT: self._on_result,
        }
        self._lineno = 0

    def problem(self, code: str, msg: str, lineno: int | None = None) -> None:
        self._code_n[code] += 1
        if self._code_n[code] > MAX_PROBLEMS_PER_CODE:
            self._omitted[code] += 1
            return
        where = "" if lineno is None else f" L{lineno}"
        self.problems.append(f"{code}{where}: {msg}")

    def feed(self, lineno: int, raw: bytes) -> None:
        if not raw.strip():
            return
        self.line_count += 1
        self._lineno = lineno
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            self.problem(BAD_UTF8, "UTF-8이 아니다", lineno)
            return
        try:
            obj = _loads(text)
        except _DupKey as e:
            self.problem(DUP_KEY, f"키 '{_clip(e.key)}'가 두 번 나온다", lineno)
            return
        except (ValueError, RecursionError) as e:
            self.problem(BAD_JSON, f"JSON이 아니다({_json_err(e)})", lineno)
            return

        paths = find_token_keys(obj)
        if paths:
            self.has_token_key = True
            self.problem(TOKEN_KEY, f"'token' 키가 있다({_paths(paths)})", lineno)

        if not isinstance(obj, dict):
            self.problem(NOT_OBJECT, f"JSON 객체가 아니다({_desc(obj)})", lineno)
            return
        if "kind" not in obj:
            self.problem(MISSING_FIELD, "필드가 없다: kind", lineno)
            return
        kind = obj["kind"]
        if not isinstance(kind, str) or not kind:
            self.problem(BAD_FIELD, f"kind가 문자열이 아니다({_desc(kind)})", lineno)
            return
        self.kind_counts[kind] += 1
        if kind.startswith(WS_KIND_PREFIXES):
            return
        handler = self._handlers.get(kind)
        if handler is None:
            self.problem(UNKNOWN_KIND, f"모르는 kind {_clip(kind)!r}", lineno)
            return

        soft: Soft = []
        try:
            _require(obj, ("run",))
            run = _str(obj["run"], "run")
            self.runs.setdefault(run, lineno)
            handler(obj, soft)
        except _Bad as e:
            self.problem(e.code, e.msg, lineno)
        for code, msg in soft:
            self.problem(code, msg, lineno)

    # -- kind별

    def _on_run_start(self, o: dict[str, Any], soft: Soft) -> None:
        _require(o, RUN_START_FIELDS)
        v = _int(o["v"], "v")
        if v != JSONL_VERSION:
            raise _Bad(BAD_VERSION, f"JSONL v{v}는 읽지 않는다(v{JSONL_VERSION}만)")
        script_ver = _str(o["script_ver"], "script_ver")
        seed = _int(o["seed"], "seed")
        profile = _str(o["profile"], "profile")
        t = _epoch_ms(o["t"], "t")
        rs = RunStart(
            v=v,
            run=o["run"],
            script_ver=script_ver,
            seed=seed,
            profile=profile,
            plan=_plan(o["plan"], soft),
            symbols=_symbols(o["symbols"], soft),
            initial_cash=_initial_cash(o["initial_cash"], soft),
            conn_reuse=_conn_reuse(o["conn_reuse"], soft),
            t=t,
            extra={
                k: o[k] for k in sorted(o) if k != "kind" and k not in RUN_START_FIELDS
            },
        )
        if self.run_start is None:
            self.run_start = rs

    def _on_account(self, o: dict[str, Any], soft: Soft) -> None:
        _require(o, ACCOUNT_FIELDS)
        idx = _int(o["idx"], "idx", lo=0)
        acct = _uuid(o["acct"], "acct")
        nick = _str(o["nick"], "nick")
        if idx in self._acct_idx or acct in self._acct_ids:
            raise _Bad(DUP_ACCOUNT, f"계좌 idx {idx} 또는 같은 계좌 id가 이미 나왔다")
        self._acct_idx.add(idx)
        self._acct_ids.add(acct)
        self.accounts.append(AccountRec(idx=idx, acct=acct, nick=nick))

    def _on_setup_done(self, o: dict[str, Any], soft: Soft) -> None:
        _require(o, SETUP_DONE_FIELDS)
        sd = SetupDone(
            accounts=_int(o["accounts"], "accounts", lo=0),
            t=_epoch_ms(o["t"], "t"),
        )
        if self.setup_done is None:
            self.setup_done = sd

    def _on_intent(self, o: dict[str, Any], soft: Soft) -> None:
        _require(o, INTENT_FIELDS)
        key = _str(o["key"], "key")
        acct = _str(o["acct"], "acct")
        vu = _int(o["vu"], "vu", lo=0)
        t = _epoch_ms(o["t"], "t")
        body, body_raw = _body(o["body"], soft)
        it = Intent(key=key, acct=acct, vu=vu, t=t, body=body, body_raw=body_raw)
        prev = self.intents.get(key)
        if prev is not None:
            same = "같은 내용" if prev == it else "다른 내용"
            raise _Bad(
                DUP_INTENT,
                f"intent 키 {_clip(key)}가 이미 나왔다({same}). 첫 줄을 쓴다",
            )
        self.intents[key] = it

    def _on_result(self, o: dict[str, Any], soft: Soft) -> None:
        _require(o, RESULT_FIELDS)
        key = _str(o["key"], "key")
        atts = _attempts(o["atts"], "atts")
        lookup = _lookup(o["lookup"], soft)
        ack = _ack(o["ack"], "ack")
        resend = _resend(o["resend"], soft)
        t = _epoch_ms(o["t"], "t")
        first = next((a for a in atts if a.is_ack), None)
        if ack is None and first is not None:
            soft.append((BAD_FIELD, f"ack가 null인데 시도 n={first.n}이 {first.st}다"))
        elif ack is not None and (first is None or ack.st != first.st):
            soft.append((BAD_FIELD, f"ack.st {ack.st}가 첫 ACK 시도와 다르다"))
        rs = Result(key=key, atts=atts, lookup=lookup, ack=ack, resend=resend, t=t)
        if key in self.results:
            raise _Bad(
                DUP_RESULT, f"result 키 {_clip(key)}가 이미 나왔다. 첫 줄을 쓴다"
            )
        self.results[key] = rs
        self._result_line[key] = self._lineno

    # -- 마무리

    def finish(
        self,
        *,
        sha: str,
        summary: Mapping[str, Any] | None,
        expected_sha256: str | None,
    ) -> K6Log:
        n_rs = self.kind_counts.get(KIND_RUN_START, 0)
        if n_rs != 1:
            self.problem(RUN_START_COUNT, f"run_start가 {n_rs}개다(1개여야 한다)")
        n_sd = self.kind_counts.get(KIND_SETUP_DONE, 0)
        if n_sd != 1:
            self.problem(SETUP_DONE_COUNT, f"setup_done이 {n_sd}개다(1개여야 한다)")
        if self.setup_done is not None:
            n_acc = self.kind_counts.get(KIND_ACCOUNT, 0)
            if n_acc != self.setup_done.accounts:
                self.problem(
                    ACCOUNT_COUNT,
                    f"account 줄 {n_acc}개 ≠ setup_done.accounts "
                    f"{self.setup_done.accounts}",
                )
        if len(self.runs) > 1:
            seen = ", ".join(
                f"{_clip(r, 40)!r}(L{n})" for r, n in sorted(self.runs.items())
            )
            self.problem(RUN_MIXED, f"run 값이 {len(self.runs)}개다: {seen}")
        for key, lineno in self._result_line.items():
            if key not in self.intents:
                self.problem(
                    ORPHAN_RESULT, f"result 키 {_clip(key)}에 intent가 없다", lineno
                )

        if summary is not None:
            paths = find_token_keys(summary)
            if paths:
                self.has_token_key = True
                self.problem(TOKEN_KEY, f"summary에 'token' 키가 있다({_paths(paths)})")
            _, cprobs = _counter_check(summary, self.kind_counts)
            for code, msg in cprobs:
                self.problem(code, msg)

        if expected_sha256 is not None:
            exp = _norm_sha(expected_sha256)
            if exp is None:
                self.problem(SHA_MISMATCH, "기대 sha256 형식이 틀리다")
            elif exp != sha:
                self.problem(
                    SHA_MISMATCH,
                    f"sha256 {sha[:12]}…이 기대값 {exp[:12]}…과 다르다",
                )

        for code in sorted(self._omitted):
            self.problems.append(f"{code}: 그 밖 {self._omitted[code]}건 생략")

        if self.run_start is not None:
            run_id: str | None = self.run_start.run
        elif len(self.runs) == 1:
            run_id = next(iter(self.runs))
        else:
            run_id = None

        return K6Log(
            run_id=run_id,
            run_start=self.run_start,
            accounts=tuple(sorted(self.accounts, key=lambda a: (a.idx, a.acct))),
            setup_done=self.setup_done,
            intents=dict(self.intents),
            results=dict(self.results),
            problems=tuple(self.problems),
            line_count=self.line_count,
            has_token_key=self.has_token_key,
            sha256=sha,
            kind_counts=dict(sorted(self.kind_counts.items())),
        )


# ---------------------------------------------------------------- 값 검사


def _loads(text: str) -> Any:
    return json.loads(
        text,
        parse_float=Decimal,
        parse_constant=_reject_constant,
        object_pairs_hook=_pairs_no_dup,
    )


def _pairs_no_dup(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in pairs:
        if k in out:
            raise _DupKey(k)
        out[k] = v
    return out


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name}는 받지 않는다")


def _json_err(e: BaseException) -> str:
    if isinstance(e, json.JSONDecodeError):
        return f"{e.msg}, {e.colno}열"
    if isinstance(e, RecursionError):
        return "중첩이 너무 깊다"
    return str(e)[:60]


def _clip(s: str, n: int = 64) -> str:
    return s if len(s) <= n else s[:n] + "…"


def _paths(paths: tuple[str, ...]) -> str:
    shown = ", ".join(_clip(p, 80) for p in paths[:3])
    return shown if len(paths) <= 3 else f"{shown} 외 {len(paths) - 3}곳"


def _desc(v: Any) -> str:
    """문제 설명용 값 표시. 문자열은 값 대신 길이만 쓴다(토큰 유출 방지)."""
    if isinstance(v, str):
        return f"문자열(길이 {len(v)})"
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, Decimal)):
        return _clip(str(v), 24)
    if isinstance(v, list):
        return f"배열(길이 {len(v)})"
    if isinstance(v, dict):
        return "객체"
    return type(v).__name__


def _require(o: Mapping[str, Any], keys: Iterable[str], prefix: str = "") -> None:
    missing = [k for k in keys if k not in o]
    if missing:
        raise _Bad(
            MISSING_FIELD, "필드가 없다: " + ", ".join(prefix + k for k in missing)
        )


def _plain_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _int(v: Any, name: str, *, lo: int | None = None, hi: int | None = None) -> int:
    if not _plain_int(v):
        raise _Bad(BAD_FIELD, f"{name}는 정수여야 한다({_desc(v)})")
    if (lo is not None and v < lo) or (hi is not None and v > hi):
        raise _Bad(BAD_FIELD, f"{name} 값 {v}가 범위 밖이다")
    return v


def _epoch_ms(v: Any, name: str) -> int:
    t = _int(v, name)
    if not (EPOCH_MS_MIN <= t < EPOCH_MS_MAX):
        raise _Bad(BAD_FIELD, f"{name} 값 {t}가 epoch ms 범위 밖이다(초 단위?)")
    return t


def _num(v: Any, name: str, *, lo: int | None = None) -> int | Decimal:
    if not (_plain_int(v) or isinstance(v, Decimal)):
        raise _Bad(BAD_FIELD, f"{name}는 숫자여야 한다({_desc(v)})")
    if lo is not None and v < lo:
        raise _Bad(BAD_FIELD, f"{name} 값 {_desc(v)}가 범위 밖이다")
    return v


def _str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise _Bad(BAD_FIELD, f"{name}는 문자열이어야 한다({_desc(v)})")
    if not v:
        raise _Bad(BAD_FIELD, f"{name}가 비어 있다")
    return v


def _opt_str(v: Any, name: str) -> str | None:
    if v is None or isinstance(v, str):
        return v
    raise _Bad(BAD_FIELD, f"{name}는 문자열 또는 null이어야 한다({_desc(v)})")


def _obj(v: Any, name: str) -> dict[str, Any]:
    if not isinstance(v, dict):
        raise _Bad(BAD_FIELD, f"{name}는 객체여야 한다({_desc(v)})")
    return v


def _list(v: Any, name: str, *, nonempty: bool = False) -> list[Any]:
    if not isinstance(v, list):
        raise _Bad(BAD_FIELD, f"{name}는 배열이어야 한다({_desc(v)})")
    if nonempty and not v:
        raise _Bad(BAD_FIELD, f"{name}가 비어 있다")
    return v


def _uuid(v: Any, name: str) -> str:
    s = _str(v, name)
    try:
        canon = str(uuid.UUID(s))
    except ValueError:
        canon = ""
    if canon != s:
        raise _Bad(BAD_FIELD, f"{name}가 정규 UUID 형식(소문자, 하이픈)이 아니다")
    return s


def _ec(v: Any, name: str) -> str | None:
    if v is None or isinstance(v, str):
        return v
    if _plain_int(v):
        return str(v)
    if isinstance(v, Decimal) and v == v.to_integral_value():
        return str(int(v))
    raise _Bad(BAD_FIELD, f"{name}는 문자열·정수·null이어야 한다({_desc(v)})")


def _attempt(v: Any, name: str) -> Attempt:
    o = _obj(v, name)
    p = name + "."
    _require(o, ATTEMPT_FIELDS, p)
    return Attempt(
        n=_int(o["n"], p + "n", lo=1),
        t0=_epoch_ms(o["t0"], p + "t0"),
        t1=_epoch_ms(o["t1"], p + "t1"),
        st=_int(o["st"], p + "st", lo=0, hi=599),
        ec=_ec(o["ec"], p + "ec"),
        ms=float(_num(o["ms"], p + "ms", lo=0)),
    )


def _attempts(v: Any, name: str) -> tuple[Attempt, ...]:
    items = _list(v, name, nonempty=True)
    return tuple(_attempt(a, f"{name}[{i}]") for i, a in enumerate(items))


def _ack(v: Any, name: str) -> Ack | None:
    if v is None:
        return None
    o = _obj(v, name)
    _require(o, ("st", "raw"), name + ".")
    return Ack(
        st=_int(o["st"], name + ".st", lo=0, hi=599),
        raw=_opt_str(o["raw"], name + ".raw"),
    )


def _lookup(v: Any, soft: Soft) -> Lookup | None:
    if v is None:
        return None
    o = _obj(v, "lookup")
    _require(o, ("atts", "st", "raw"), "lookup.")
    lk = Lookup(
        atts=_attempts(o["atts"], "lookup.atts"),
        st=_int(o["st"], "lookup.st", lo=0, hi=599),
        raw=_opt_str(o["raw"], "lookup.raw"),
    )
    if lk.st != lk.atts[-1].st:
        soft.append((BAD_FIELD, f"lookup.st {lk.st}가 마지막 조회 시도와 다르다"))
    return lk


def _resend(v: Any, soft: Soft) -> Resend | None:
    if v is None:
        return None
    o = _obj(v, "resend")
    _require(o, ("atts", "ack"), "resend.")
    atts = _attempts(o["atts"], "resend.atts")
    ack = _ack(o["ack"], "resend.ack")
    first = next((a for a in atts if 200 <= a.st < 300), None)
    if ack is None and first is not None:
        soft.append(
            (BAD_FIELD, f"resend.ack가 null인데 시도 n={first.n}이 {first.st}다")
        )
    elif ack is not None and (first is None or ack.st != first.st):
        soft.append((BAD_FIELD, f"resend.ack.st {ack.st}가 첫 2xx 시도와 다르다"))
    return Resend(atts=atts, ack=ack)


def _body(v: Any, soft: Soft) -> tuple[dict[str, Any], str | None]:
    """intent body. JSON 문자열이면 풀고 원문을 둔다. 내용 문제는 soft로 남긴다."""
    if isinstance(v, str):
        raw: str | None = v
        try:
            b = _loads(v)
        except (ValueError, RecursionError, _DupKey):
            soft.append((BAD_FIELD, "body 문자열이 JSON이 아니다"))
            return {}, raw
        if not isinstance(b, dict):
            soft.append((BAD_FIELD, f"body가 JSON 객체가 아니다({_desc(b)})"))
            return {}, raw
    elif isinstance(v, dict):
        b, raw = v, None
    else:
        raise _Bad(BAD_FIELD, f"body는 객체 또는 JSON 문자열이어야 한다({_desc(v)})")
    missing = [k for k in BODY_FIELDS if k not in b]
    if missing:
        soft.append((BAD_FIELD, "body에 필드가 없다: " + ", ".join(missing)))
    return dict(b), raw


def _plan(v: Any, soft: Soft) -> dict[str, Any]:
    """run_start.plan(설계 4.5). ramping-arrival-rate면 모양을 검사한다."""
    if not isinstance(v, dict):
        soft.append((BAD_FIELD, f"plan은 객체여야 한다({_desc(v)})"))
        return {}
    executor = v.get("executor")
    if not isinstance(executor, str):
        soft.append((BAD_FIELD, f"plan.executor가 문자열이 아니다({_desc(executor)})"))
    elif executor == "ramping-arrival-rate":
        try:
            _check_ramping(v)
        except _Bad as e:
            soft.append((e.code, e.msg))
    return dict(v)


def _check_ramping(p: Mapping[str, Any]) -> None:
    _require(p, ("time_unit_s", "start_rate", "stages"), "plan.")
    _int(p["time_unit_s"], "plan.time_unit_s", lo=1)
    _num(p["start_rate"], "plan.start_rate", lo=0)
    for i, s in enumerate(_list(p["stages"], "plan.stages", nonempty=True)):
        name = f"plan.stages[{i}]"
        so = _obj(s, name)
        _require(so, ("duration_s", "target"), name + ".")
        _num(so["duration_s"], name + ".duration_s", lo=0)
        _num(so["target"], name + ".target", lo=0)


def _symbols(v: Any, soft: Soft) -> tuple[str, ...]:
    if isinstance(v, list) and all(isinstance(s, str) and s for s in v):
        return tuple(v)
    soft.append((BAD_FIELD, f"symbols는 문자열 배열이어야 한다({_desc(v)})"))
    return ()


def _initial_cash(v: Any, soft: Soft) -> int | None:
    """원 단위 정수. 숫자 문자열과 정수값 Decimal도 받는다. 틀리면 None."""
    if v is None:
        return None
    if _plain_int(v) and v >= 0:
        return v
    if isinstance(v, str) and v.isascii() and v.isdigit():
        return int(v)
    if isinstance(v, Decimal) and v >= 0 and v == v.to_integral_value():
        return int(v)
    soft.append((BAD_FIELD, f"initial_cash는 음이 아닌 정수여야 한다({_desc(v)})"))
    return None


def _conn_reuse(v: Any, soft: Soft) -> bool | None:
    """false가 아니면 FIRST_200을 낮춘다(4.4). 틀린 값은 None(재사용 가능)으로 둔다."""
    if v is None or isinstance(v, bool):
        return v
    soft.append((BAD_FIELD, f"conn_reuse는 true·false·null이어야 한다({_desc(v)})"))
    return None


def _walk_tokens(obj: Any, path: str, out: list[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}"
            if isinstance(k, str) and k.lower().endswith("token"):
                out.append(p)
            _walk_tokens(v, p, out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _walk_tokens(v, f"{path}[{i}]", out)
    elif isinstance(obj, str):
        if "token" in obj.lower() and _TOKEN_TEXT_RE.search(obj):
            out.append(f"{path}(텍스트)")


# ---------------------------------------------------------------- summary·완전성 도우미


def _summary_metrics(summary: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if not isinstance(summary, Mapping):
        return None
    m = summary.get("metrics")
    if isinstance(m, Mapping):
        return m
    data = summary.get("data")
    if isinstance(data, Mapping) and isinstance(data.get("metrics"), Mapping):
        return data["metrics"]
    return None


def _count_value(v: Any) -> int | None:
    if _plain_int(v):
        return v if v >= 0 else None
    if isinstance(v, Decimal):
        if v.is_finite() and v >= 0 and v == v.to_integral_value():
            return int(v)
        return None
    if isinstance(v, float) and math.isfinite(v) and v >= 0 and v.is_integer():
        return int(v)
    return None


def _counter_check(
    summary: Mapping[str, Any] | None, kind_counts: Mapping[str, int]
) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    """(detail, [(문제 코드, 설명)]). 줄 수 ≥ Counter면 문제가 없다(설계 5.3)."""
    detail: dict[str, Any] = {
        "intent_lines": kind_counts.get(KIND_INTENT, 0),
        "result_lines": kind_counts.get(KIND_RESULT, 0),
        COUNTER_INTENTS: None,
        COUNTER_RESULTS: None,
    }
    probs: list[tuple[str, str]] = []
    if summary is None:
        probs.append((SUMMARY_INVALID, "summary가 없다"))
        return detail, probs
    metrics = _summary_metrics(summary)
    if metrics is None:
        probs.append((SUMMARY_INVALID, "summary에 metrics가 없다"))
        return detail, probs
    for kind, metric in _COUNTERS:
        lines = kind_counts.get(kind, 0)
        if metric not in metrics:
            # k6는 표본이 없는 지표를 빼므로, 줄이 없을 때만 0으로 본다.
            if lines > 0:
                probs.append(
                    (SUMMARY_INVALID, f"summary에 {metric}가 없다({kind} 줄 {lines}개)")
                )
            else:
                detail[metric] = 0
            continue
        c = summary_count(summary, metric)
        if c is None:
            probs.append((SUMMARY_INVALID, f"summary {metric} 값 형식이 틀리다"))
            continue
        detail[metric] = c
        if lines < c:
            probs.append((COUNTER_SHORT, f"{kind} 줄 {lines}개 < {metric} {c}"))
    return detail, probs


def _exit_ok(code: Any) -> bool:
    return _plain_int(code) and code in K6_OK_EXIT_CODES


def _norm_sha(s: str | None) -> str | None:
    """sha256 16진 문자열로 정규화한다. sha256sum 출력("hex  파일")도 받는다."""
    if not isinstance(s, str):
        return None
    parts = s.strip().split()
    if not parts:
        return None
    h = parts[0].lower()
    return h if _SHA_RE.match(h) else None

"""DB 스냅샷: bundle.sql 조립, psql 1회 실행, 출력 파싱(계약 명세 6).

대상은 실제 주문 서비스 스키마(workloads/order/db/schema.sql)다. accounts·positions·orders는
전체 행을 가져온다(시연 규모라 작다). 불변식은 이 스냅샷으로 invariants가 Python으로 계산한다.

bundle.sql 순서
1. `\\set` 변수(run_key_re, extra, schema_tables, stale_ms, tick_slack_ms). 세션 설정(UTC,
   ISO 날짜, 기본 읽기 전용, 시간 제한).
2. 지문 F1(트랜잭션 밖).
3. BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY → schema → accounts → positions → orders → COMMIT.
4. 지문 F2(트랜잭션 밖).
5. `\\set ON_ERROR_STOP off` → ticks(선택). ticks 오류가 필수 증거를 지우지 않는다.
   ticks 테이블이 없으면 머리줄만 있는 빈 구획을 낸다(fresh는 NOT_EVALUATED).

정한 것
- F1·F2는 트랜잭션 밖에서 잰다. REPEATABLE READ 안에서 재면 늘 같아 db_quiescent가 아무것도 못 본다.
  F1 == F2면 그 사이 스냅샷도 같은 상태로 본다(invariants.snapshot_consistent가 대조한다).
- schema를 트랜잭션 첫 구획으로 둔다. 열이 없어져 뒤 구획이 실패해도 드리프트 근거는 남는다.
- run 주문은 client_order_id 전체 형식 `^k6-<run_id>-[0-9]+-[0-9]+$`로 고른다(model.run_key_re와 같다).
  접두어만 보면 r1117-crash가 r1117-crash-2의 키까지 잡는다.

출력 형식(psql stdout, raw/db/에 그대로 남긴다)
- 구획은 `--@ <name>` 줄로 시작하고 `--@end <name>` 줄로 끝난다.
- 본문은 COPY ... TO STDOUT (FORMAT csv, HEADER, FORCE_QUOTE *) 출력이다. 첫 줄이 열 이름이다.
  NULL은 따옴표 없는 빈 값, 그 밖 값은 모두 따옴표 안에 있다. 그래서 데이터 줄이 `--@`로 시작하지 않는다.
- 출력에 `--@end` 줄이 하나라도 있으면 끝 표지가 없는 구획은 버린다(psql이 그 구획에서 멈췄다).
  끝 표지가 전혀 없으면 머리줄이 있는 구획을 받는다(가짜 API 등 단순 출력 호환).
- 버린 구획은 DbSnapshot.sections에 없고 errors에 이름이 남는다. 그 구획을 쓰는 기준만 DB_ERROR다.

주입 방지: run_id는 RUN_ID_RE 전체 일치, extra user_id는 USER_ID_RE(context.check_user_id)만 받는다.
그 밖은 InputError다. psql 접속 정보는 RunContext.db(DbTarget)에서 받고 비밀번호는 다루지 않는다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from importlib import resources
from typing import Any

from .context import RUN_ID_RE, check_user_id
from .errors import InputError, JudgeError
from .model import (
    ACCOUNT_COLUMNS,
    DEFAULT_STALE_MS,
    FINGERPRINT_COLUMNS,
    ORDER_COLUMNS,
    POSITION_COLUMNS,
    TICK_COLUMNS,
    DbAccount,
    DbOrder,
    DbPosition,
    DbSnapshot,
    DbTarget,
    DbTick,
    Fingerprint,
    SchemaItem,
    dec,
    iso_us,
    ts_us,
)
from .transport import Transports

__all__ = [
    "BUNDLE_SECTIONS",
    "END_MARK",
    "MARK",
    "PSQL_TIMEOUT_S",
    "PsqlRun",
    "SCHEMA_COLUMNS",
    "SCHEMA_KINDS",
    "SCHEMA_TABLES",
    "SQL_FILES",
    "TICK_SLACK_MS",
    "build_bundle",
    "exec_bundle",
    "format_section",
    "load_sql",
    "parse_bundle_output",
    "render_snapshot",
    "run_key_regex",
    "run_snapshot",
]

MARK = "--@ "
END_MARK = "--@end "

# psql 전체 시간 상한(초). 안쪽 statement_timeout보다 길게 둔다(우리 설정값).
PSQL_TIMEOUT_S = 180.0
STATEMENT_TIMEOUT = "120s"
LOCK_TIMEOUT = "5s"

# schema 구획(계약 명세 6, 7 constraints). 열 이름 table은 SQL 예약어라 table_name으로 낸다.
SCHEMA_COLUMNS = ("kind", "table_name", "name", "detail")
SCHEMA_KINDS = ("table", "column", "constraint")
# 메타데이터를 가져올 서비스 테이블(workloads/order/db/schema.sql)
SCHEMA_TABLES = ("accounts", "positions", "orders", "ticks")
# fresh 창의 시계 여유(계약 명세 7: filled_at − stale_ms − 1000ms ~ filled_at + 1000ms)
TICK_SLACK_MS = 1000
# 패키지 sql/<name>.sql(긴 질의만 파일로 둔다)
SQL_FILES = ("snapshot_schema", "snapshot_ticks")

# 출력 구획 순서(bundle 순서). 이름 집합은 model.SNAPSHOT_SECTIONS와 같다.
BUNDLE_SECTIONS = (
    "fingerprint1",
    "schema",
    "accounts",
    "positions",
    "orders",
    "fingerprint2",
    "ticks",
)

_INT_RE = re.compile(r"-?[0-9]+")

# ---------------------------------------------------------------- 고정 SQL(계약 명세 6)

_FINGERPRINT_SQL = """SELECT (SELECT count(*) FROM orders) AS orders,
       (SELECT max(id) FROM orders) AS max_order_id,
       (SELECT sum(krw) FROM accounts) AS krw_sum,
       (SELECT count(*) FROM positions) AS positions,
       (SELECT sum(volume) FROM positions) AS volume_sum"""


def _select(table: str, columns: Sequence[str], order_by: str) -> str:
    return f"SELECT {', '.join(columns)}\nFROM {table}\nORDER BY {order_by}"


# 정렬은 바이트 순(COLLATE "C")이다. 파서도 다시 정렬하지만 원본 출력도 결정적이게 둔다.
_ACCOUNTS_SQL = _select("accounts", ACCOUNT_COLUMNS, 'user_id COLLATE "C"')
_POSITIONS_SQL = _select(
    "positions", POSITION_COLUMNS, 'user_id COLLATE "C", market COLLATE "C"'
)
_ORDERS_SQL = _select("orders", ORDER_COLUMNS, "id")


# ---------------------------------------------------------------- bundle 조립


def load_sql(name: str) -> str:
    """패키지의 sql/<name>.sql 원문."""
    if name not in SQL_FILES:
        raise JudgeError(f"모르는 SQL 파일: {name!r}")
    path = resources.files(__package__) / "sql" / f"{name}.sql"
    return path.read_text(encoding="utf-8")


def _embed(sql_text: str, where: str) -> str:
    """COPY ( ... ) 안에 넣을 본문. 줄 전체 주석과 끝 세미콜론을 뺀다.

    문장 하나만 허용한다. psql 메타 명령(역슬래시)이 섞이면 거부한다.
    """
    lines = [
        ln.rstrip()
        for ln in sql_text.splitlines()
        if ln.strip() and not ln.lstrip().startswith("--")
    ]
    body = "\n".join(lines).strip()
    if body.endswith(";"):
        body = body[:-1].rstrip()
    if not body:
        raise JudgeError(f"{where}: SQL이 비었다")
    if ";" in body or "\\" in body:
        raise JudgeError(f"{where}: SELECT 문 하나만 둔다(세미콜론·역슬래시 금지)")
    return body


def _regex_escape(s: str) -> str:
    """PostgreSQL ARE용 이스케이프. 영숫자가 아닌 글자 앞에 역슬래시를 붙인다.

    ARE에서 역슬래시 + 비영숫자는 그 글자 자체다(예: `\\-`는 `-`). Python re도 같다.
    """
    return "".join(c if c.isascii() and c.isalnum() else "\\" + c for c in s)


def _check_run_id(run_id: Any) -> str:
    # match는 끝 줄바꿈을 허용하므로 fullmatch로 본다
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise InputError(f"run_id {run_id!r}가 ^[a-z0-9-]{{3,40}}$에 맞지 않다")
    return run_id


def run_key_regex(run_id: str) -> str:
    """run 주문 client_order_id 정규식 `^k6-<run_id>-[0-9]+-[0-9]+$`(계약 명세 2). run_id는 이스케이프한다."""
    return f"^k6-{_regex_escape(_check_run_id(run_id))}-[0-9]+-[0-9]+$"


def _user_list(values: Any, where: str) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes)):
        raise InputError(f"{where}는 user_id 목록이어야 한다")
    out = {check_user_id(v, where) for v in values}
    return tuple(sorted(out))


def _check_stale_ms(v: Any) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise InputError(f"stale_ms {v!r}는 0 이상 정수여야 한다")
    return v


def _pg_text_array(values: Sequence[str]) -> str:
    """text[] 리터럴. 원소를 큰따옴표로 감싼다(값 'NULL'이 NULL이 되지 않게).

    원소는 검증된 이름(USER_ID_RE, 테이블 이름)이라 큰따옴표·역슬래시가 없다.
    """
    for v in values:
        if any(c in v for c in '"\\{},'):
            raise InputError(f"배열 원소에 쓸 수 없는 글자가 있다: {v!r}")
    return "{" + ",".join(f'"{v}"' for v in values) + "}"


def _psql_literal(value: str) -> str:
    """psql 메타 명령 인자용 작은따옴표 문자열. 역슬래시와 따옴표를 이스케이프한다."""
    if any(c in value for c in "\r\n\0"):
        raise InputError("psql 변수 값에 줄바꿈을 넣을 수 없다")
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _copy(select: str) -> str:
    return (
        f"COPY (\n{select}\n) TO STDOUT WITH (FORMAT csv, HEADER true, FORCE_QUOTE *);"
    )


def _section(name: str, select: str) -> list[str]:
    """`--@ name` → COPY → `--@end name`. ON_ERROR_STOP=1이라 실패하면 끝 표지까지 오지 않는다."""
    return [f"\\qecho '{MARK}{name}'", _copy(select), f"\\qecho '{END_MARK}{name}'"]


def _ticks_section() -> list[str]:
    """ticks(선택). ON_ERROR_STOP off 뒤에 둔다. 성공했을 때만 끝 표지를 낸다.

    ticks 테이블이 없으면 머리줄만 있는 빈 구획을 낸다(수집기가 아직 없을 수 있다).
    """
    select = _embed(load_sql("snapshot_ticks"), "snapshot_ticks.sql")
    return [
        "SELECT to_regclass('ticks') IS NOT NULL AS has_ticks \\gset",
        f"\\qecho '{MARK}ticks'",
        "\\if :has_ticks",
        _copy(select),
        "\\if :ERROR",
        "\\else",
        f"\\qecho '{END_MARK}ticks'",
        "\\endif",
        "\\else",
        f"\\qecho '{','.join(TICK_COLUMNS)}'",
        f"\\qecho '{END_MARK}ticks'",
        "\\endif",
    ]


def build_bundle(
    ctx_like: Any = None,
    *,
    run_id: str | None = None,
    extra_ids: Iterable[str] | None = None,
    stale_ms: int | None = None,
) -> str:
    """bundle.sql 텍스트(계약 명세 6). 같은 입력이면 같은 바이트다.

    ctx_like: run_id·extra_accounts 속성이 있으면 기본값으로 쓴다(RunContext 등). None 가능.
    run_id: None이면 ctx_like.run_id. extra_ids: 발표자 user_id. None이면 ctx_like.extra_accounts.
    stale_ms: ticks 범위(integrity.stale_ms). None이면 DEFAULT_STALE_MS.
    """
    rid = _check_run_id(
        run_id if run_id is not None else getattr(ctx_like, "run_id", None)
    )
    if extra_ids is None:
        extra_ids = getattr(ctx_like, "extra_accounts", ())
    extra = _user_list(extra_ids, "extra_ids")
    ms = _check_stale_ms(DEFAULT_STALE_MS if stale_ms is None else stale_ms)

    lines: list[str] = [
        f"-- bundle.sql (chaos-judge db.build_bundle, 계약 명세 6) run_id={rid}",
        f"\\set run_key_re {_psql_literal(run_key_regex(rid))}",
        f"\\set extra {_psql_literal(_pg_text_array(extra))}",
        f"\\set schema_tables {_psql_literal(_pg_text_array(SCHEMA_TABLES))}",
        f"\\set stale_ms {ms}",
        f"\\set tick_slack_ms {TICK_SLACK_MS}",
        "SET client_encoding = 'UTF8';",
        "SET TIME ZONE 'UTC';",
        "SET DateStyle = 'ISO, YMD';",
        f"SET statement_timeout = '{STATEMENT_TIMEOUT}';",
        f"SET lock_timeout = '{LOCK_TIMEOUT}';",
        "SET default_transaction_read_only = on;",
    ]
    lines += _section("fingerprint1", _FINGERPRINT_SQL)
    lines.append("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
    lines += _section("schema", _embed(load_sql("snapshot_schema"), "snapshot_schema"))
    lines += _section("accounts", _ACCOUNTS_SQL)
    lines += _section("positions", _POSITIONS_SQL)
    lines += _section("orders", _ORDERS_SQL)
    lines.append("COMMIT;")
    lines += _section("fingerprint2", _FINGERPRINT_SQL)
    lines.append("\\set ON_ERROR_STOP off")
    lines += _ticks_section()
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- 실행


@dataclass(frozen=True)
class PsqlRun:
    """psql 1회 실행 결과. returncode 3은 ON_ERROR_STOP 중단이다(남은 구획만 판정한다)."""

    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


def exec_bundle(
    transports: Transports | None,
    bundle: str,
    *,
    target: DbTarget | None = None,
    timeout_s: float = PSQL_TIMEOUT_S,
) -> PsqlRun:
    """bundle.sql을 psql 표준 입력으로 한 번 보낸다(계약 명세 6).

    명령은 transports.psql_argv(target)다. 기본은
    `kubectl -n exchange exec -i <pod> [-c <container>] -- psql -X -q -v ON_ERROR_STOP=1
    -U <user> -d <database> -f -`. target은 RunContext.db(기본 DbTarget()).
    테스트는 Transports.for_local_pg()로 docker exec를 쓴다.
    0이 아닌 종료 코드는 예외가 아니다(빠진 구획이 DB_ERROR가 된다).
    실행 파일이 없거나 시간이 넘으면 TransportError(judge 오류)다.
    """
    tr = transports if transports is not None else Transports()
    argv = tuple(tr.psql_argv(target))
    cp = tr.run(argv, input=bundle.encode("utf-8"), timeout=timeout_s)
    try:
        stdout = (cp.stdout or b"").decode("utf-8")
    except UnicodeDecodeError as e:
        raise JudgeError(f"psql 출력이 UTF-8이 아니다({e})") from e
    stderr = (cp.stderr or b"").decode("utf-8", errors="replace")
    return PsqlRun(argv=argv, returncode=cp.returncode, stdout=stdout, stderr=stderr)


def run_snapshot(
    transports: Transports | None,
    bundle: str,
    *,
    target: DbTarget | None = None,
    timeout_s: float = PSQL_TIMEOUT_S,
) -> str:
    """exec_bundle의 stdout만 돌려준다. 종료 코드·stderr가 필요하면 exec_bundle을 쓴다."""
    return exec_bundle(transports, bundle, target=target, timeout_s=timeout_s).stdout


# ---------------------------------------------------------------- 출력 파싱

Field = str | None  # None = NULL(따옴표 없는 빈 값)


def _read_record(text: str, i: int) -> tuple[list[Field], int, bool]:
    """COPY CSV 레코드 하나. (필드들, 다음 위치, 완결 여부). 따옴표가 안 닫히면 완결 아님."""
    n = len(text)
    fields: list[Field] = []
    while True:
        if i < n and text[i] == '"':
            i += 1
            buf: list[str] = []
            while True:
                if i >= n:
                    return fields, n, False
                c = text[i]
                if c == '"':
                    if i + 1 < n and text[i + 1] == '"':
                        buf.append('"')
                        i += 2
                        continue
                    i += 1
                    break
                buf.append(c)
                i += 1
            j = i
            while j < n and text[j] not in ",\r\n":
                j += 1
            buf.append(text[i:j])  # 닫는 따옴표 뒤 글자(정상 출력에는 없다)
            fields.append("".join(buf))
            i = j
        else:
            j = i
            while j < n and text[j] not in ",\r\n":
                j += 1
            raw = text[i:j]
            fields.append(raw if raw != "" else None)
            i = j
        if i >= n:
            return fields, n, True
        c = text[i]
        if c == ",":
            i += 1
            continue
        if c == "\r":
            i += 1
            if i < n and text[i] == "\n":
                i += 1
            return fields, i, True
        return fields, i + 1, True  # "\n"


def _scan(text: str) -> Iterable[tuple[str, Any]]:
    """("mark"|"end", 이름), ("row", 필드들), ("broken", None) 사건을 차례로 낸다."""
    i, n = 0, len(text)
    while i < n:
        if text.startswith("--@", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            line = text[i:j].rstrip("\r")
            i = j + 1
            if line.startswith(END_MARK):
                yield "end", line[len(END_MARK) :].strip()
            elif line.startswith(MARK):
                yield "mark", line[len(MARK) :].strip()
            else:
                raise InputError(f"모르는 구획 표지 줄: {line[:80]!r}")
            continue
        fields, i, complete = _read_record(text, i)
        if not complete:
            yield "broken", None
            return
        yield "row", fields


def _split_sections(
    text: str,
) -> tuple[dict[str, list[list[Field]]], tuple[str, ...]]:
    """(완결된 구획 이름 → 레코드(머리줄 포함), 버린 구획 설명)."""
    sections: dict[str, list[list[Field]]] = {}
    closed: set[str] = set()
    broken: set[str] = set()
    has_end = False
    current: str | None = None
    for kind, val in _scan(text):
        if kind == "mark":
            if not val:
                raise InputError("구획 이름이 비었다")
            if val in sections:
                raise InputError(f"구획 '{val}'가 두 번 나온다")
            sections[val] = []
            current = val
        elif kind == "end":
            has_end = True
            if val != current:
                raise InputError(f"끝 표지 '{val}'가 열린 구획 '{current}'와 다르다")
            closed.add(val)
            current = None
        elif kind == "row":
            if current is None or val == [None]:  # 구획 밖 줄, 빈 줄
                continue
            sections[current].append(val)
        elif kind == "broken" and current is not None:
            broken.add(current)
    kept: dict[str, list[list[Field]]] = {}
    dropped: list[str] = []
    for name, recs in sections.items():
        if name in broken:
            dropped.append(f"구획 '{name}': 출력이 따옴표 안에서 끊겼다")
        elif has_end and name not in closed:
            dropped.append(f"구획 '{name}': 끝 표지가 없다(psql 오류로 멈춤)")
        elif not recs:
            dropped.append(f"구획 '{name}': 머리줄이 없다")
        else:
            kept[name] = recs
    return kept, tuple(dropped)


def _table(name: str, records: list[list[Field]]) -> tuple[tuple[str, ...], list[dict]]:
    header = records[0]
    if any(h is None or h == "" for h in header):
        raise InputError(f"구획 '{name}': 머리줄에 빈 열 이름이 있다")
    names = tuple(str(h) for h in header)
    if len(set(names)) != len(names):
        raise InputError(f"구획 '{name}': 머리줄 열 이름이 중복된다")
    rows = []
    for k, rec in enumerate(records[1:], start=1):
        if len(rec) != len(names):
            raise InputError(
                f"구획 '{name}' {k}행: 열 {len(rec)}개, 머리줄 {len(names)}개"
            )
        rows.append(dict(zip(names, rec)))
    return names, rows


def _need(name: str, header: Sequence[str], cols: Sequence[str]) -> None:
    missing = [c for c in cols if c not in header]
    if missing:
        raise InputError(f"구획 '{name}': 열 {missing}가 없다")


def _int(v: Field, where: str) -> int | None:
    if v is None:
        return None
    if not _INT_RE.fullmatch(v):
        raise InputError(f"{where}: 정수가 아니다({v!r})")
    return int(v)


def _req_int(v: Field, where: str) -> int:
    out = _int(v, where)
    if out is None:
        raise InputError(f"{where}: NULL일 수 없다")
    return out


def _dec(v: Field, where: str) -> Decimal | None:
    try:
        out = dec(v)
    except (TypeError, ValueError) as e:
        raise InputError(f"{where}: {e}") from e
    if out is not None and not out.is_finite():
        raise InputError(f"{where}: 유한한 수가 아니다({v!r})")
    return out


def _req_str(v: Field, where: str) -> str:
    if v is None or v == "":
        raise InputError(f"{where}: 비어 있을 수 없다")
    return v


def _ts(v: Field, where: str) -> int | None:
    try:
        return ts_us(v)
    except ValueError as e:
        raise InputError(f"{where}: {e}") from e


def _accounts(rows: list[dict], header: Sequence[str]) -> tuple[DbAccount, ...]:
    _need("accounts", header, ACCOUNT_COLUMNS)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"accounts {k}행"
        out.append(
            DbAccount(
                user_id=_req_str(r["user_id"], f"{w}.user_id"),
                krw=_dec(r["krw"], f"{w}.krw"),
            )
        )
    return tuple(sorted(out, key=lambda a: a.user_id))


def _positions(rows: list[dict], header: Sequence[str]) -> tuple[DbPosition, ...]:
    _need("positions", header, POSITION_COLUMNS)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"positions {k}행"
        out.append(
            DbPosition(
                user_id=_req_str(r["user_id"], f"{w}.user_id"),
                market=_req_str(r["market"], f"{w}.market"),
                volume=_dec(r["volume"], f"{w}.volume"),
                avg_price=_dec(r["avg_price"], f"{w}.avg_price"),
            )
        )
    return tuple(sorted(out, key=lambda p: (p.user_id, p.market)))


def _orders(rows: list[dict], header: Sequence[str]) -> tuple[DbOrder, ...]:
    _need("orders", header, ORDER_COLUMNS)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"orders {k}행"
        out.append(
            DbOrder(
                id=_req_int(r["id"], f"{w}.id"),
                # 스키마가 NOT NULL이다. NULL이면 ""로 두고 constraints가 드리프트로 본다
                client_order_id=r["client_order_id"] or "",
                user_id=r["user_id"] or "",
                market=r["market"],
                side=r["side"],
                volume=_dec(r["volume"], f"{w}.volume"),
                status=r["status"],
                fill_price=_dec(r["fill_price"], f"{w}.fill_price"),
                amount=_dec(r["amount"], f"{w}.amount"),
                created_us=_ts(r["created_at"], f"{w}.created_at"),
                filled_us=_ts(r["filled_at"], f"{w}.filled_at"),
            )
        )
    return tuple(sorted(out, key=lambda o: o.id))


def _ticks(rows: list[dict], header: Sequence[str]) -> tuple[DbTick, ...]:
    _need("ticks", header, TICK_COLUMNS)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"ticks {k}행"
        out.append(
            DbTick(
                market=_req_str(r["market"], f"{w}.market"),
                price=_dec(r["price"], f"{w}.price"),
                received_us=_ts(r["received_at"], f"{w}.received_at"),
            )
        )
    return tuple(sorted(out, key=lambda t: (t.market, t.received_us or 0)))


def _schema(rows: list[dict], header: Sequence[str]) -> tuple[SchemaItem, ...]:
    """schema 구획. 행 순서는 출력 순서 그대로다(SQL이 kind, table, name 순으로 낸다)."""
    _need("schema", header, SCHEMA_COLUMNS)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"schema {k}행"
        kind = _req_str(r["kind"], f"{w}.kind")
        if kind not in SCHEMA_KINDS:
            raise InputError(
                f"{w}.kind: {kind!r}는 {list(SCHEMA_KINDS)} 중 하나가 아니다"
            )
        out.append(
            SchemaItem(
                kind=kind,
                table=_req_str(r["table_name"], f"{w}.table_name"),
                name=_req_str(r["name"], f"{w}.name"),
                detail=r["detail"],
            )
        )
    return tuple(out)


def _fingerprint(name: str, rows: list[dict], header: Sequence[str]) -> Fingerprint:
    _need(name, header, FINGERPRINT_COLUMNS)
    if len(rows) != 1:
        raise InputError(f"구획 '{name}': 행이 1개여야 한다(받음 {len(rows)}개)")
    r = rows[0]
    return Fingerprint(
        orders=_req_int(r["orders"], f"{name}.orders"),
        max_order_id=_int(r["max_order_id"], f"{name}.max_order_id"),
        krw_sum=_dec(r["krw_sum"], f"{name}.krw_sum"),
        positions=_req_int(r["positions"], f"{name}.positions"),
        volume_sum=_dec(r["volume_sum"], f"{name}.volume_sum"),
    )


def parse_bundle_output(text: str) -> DbSnapshot:
    """psql 출력(--@ 구획 + COPY CSV)을 DbSnapshot으로 바꾼다(계약 명세 6).

    - numeric은 Decimal(문자열 그대로, 자릿수 유지), bigint는 int, timestamptz는 epoch µs, NULL은 None.
    - 빠지거나 끝나지 않은 구획은 sections에 없고 errors에 남는다. 그 표는 빈 tuple, 지문은 None이다.
    - 불변식 구획(inv_<id>)이 있으면 행을 invariant_rows[<id>]에 열 이름 → 텍스트로 둔다.
      지금 bundle은 이 구획을 내지 않는다(불변식은 invariants가 Python으로 계산한다).
    - 행 순서를 정렬해 같은 증거면 같은 결과다(accounts·positions는 키, orders는 id, ticks는 시각).
    - 형식이 깨졌으면(열 수 불일치, 중복 구획, 필수 열 없음 등) InputError다.
    """
    split, dropped = _split_sections(text)
    parsed = {name: _table(name, recs) for name, recs in split.items()}

    def take(name: str, conv: Any, empty: Any) -> Any:
        if name not in parsed:
            return empty
        header, rows = parsed[name]
        return conv(rows, header)

    fps: dict[str, Fingerprint | None] = {}
    for name in ("fingerprint1", "fingerprint2"):
        if name in parsed:
            header, rows = parsed[name]
            fps[name] = _fingerprint(name, rows, header)
        else:
            fps[name] = None

    inv_rows: dict[str, tuple[dict[str, Field], ...]] = {}
    for name in sorted(parsed):
        rows = parsed[name][1]
        if name.startswith("inv_") and rows:  # 빈 구획은 sections로만 안다
            inv_rows[name[len("inv_") :]] = tuple(rows)

    return DbSnapshot(
        accounts=take("accounts", _accounts, ()),
        positions=take("positions", _positions, ()),
        orders=take("orders", _orders, ()),
        ticks=take("ticks", _ticks, ()),
        schema=take("schema", _schema, ()),
        fingerprint1=fps["fingerprint1"],
        fingerprint2=fps["fingerprint2"],
        sections=frozenset(parsed),
        invariant_rows=inv_rows,
        errors=dropped,
    )


# ---------------------------------------------------------------- 출력 만들기(테스트·픽스처용)


def _pg_ts(us: int | None) -> str | None:
    """epoch µs → psql 출력 모양("2026-11-06 01:02:03.1234+00", UTC, 끝 0은 뺀다)."""
    if us is None:
        return None
    s = iso_us(us, sep=" ")
    if not s.endswith("+00:00"):
        raise JudgeError(f"UTC가 아닌 시각: {s}")
    body = s[: -len("+00:00")]
    if "." in body:
        body = body.rstrip("0").rstrip(".")
    return body + "+00"


def _csv_field(v: Any) -> str:
    """FORCE_QUOTE * 모양. None → 빈 값(NULL), 그 밖 → 따옴표 안."""
    if v is None:
        return ""
    if isinstance(v, bool):
        s = "t" if v else "f"
    elif isinstance(v, Decimal):
        s = format(v, "f")
    else:
        s = str(v)
    return '"' + s.replace('"', '""') + '"'


def format_section(
    name: str, header: Sequence[str], rows: Iterable[Sequence[Any]]
) -> str:
    """구획 하나를 psql 출력과 같은 모양으로 쓴다(None → NULL, "" → `""`)."""
    lines = [f"{MARK}{name}", ",".join(header)]
    lines += [",".join(_csv_field(v) for v in r) for r in rows]
    lines.append(f"{END_MARK}{name}")
    return "\n".join(lines) + "\n"


def _fp_row(fp: Fingerprint) -> tuple[Any, ...]:
    return (fp.orders, fp.max_order_id, fp.krw_sum, fp.positions, fp.volume_sum)


def _inv_section(name: str, rows: Sequence[Mapping[str, Field]]) -> str:
    header = tuple(rows[0].keys()) if rows else ("subject",)
    return format_section(
        f"inv_{name}", header, [[r.get(c) for c in header] for r in rows]
    )


def render_snapshot(db: DbSnapshot) -> str:
    """DbSnapshot을 bundle 출력 모양으로 되돌린다. parse_bundle_output의 역(구획 순서는 bundle 순서).

    db.sections에 있는 구획만 쓴다. inv_<id> 구획은 orders 뒤에 이름 순으로 쓴다(열은 행의 키).
    """
    out: list[str] = []
    secs = db.sections

    def fp(name: str, v: Fingerprint | None) -> None:
        if name in secs and v is not None:
            out.append(format_section(name, FINGERPRINT_COLUMNS, [_fp_row(v)]))

    tables: dict[str, tuple[tuple[str, ...], list[tuple[Any, ...]]]] = {
        "schema": (
            SCHEMA_COLUMNS,
            [(s.kind, s.table, s.name, s.detail) for s in db.schema],
        ),
        "accounts": (ACCOUNT_COLUMNS, [(a.user_id, a.krw) for a in db.accounts]),
        "positions": (
            POSITION_COLUMNS,
            [(p.user_id, p.market, p.volume, p.avg_price) for p in db.positions],
        ),
        "orders": (
            ORDER_COLUMNS,
            [
                (
                    o.id,
                    o.client_order_id,
                    o.user_id,
                    o.market,
                    o.side,
                    o.volume,
                    o.status,
                    o.fill_price,
                    o.amount,
                    _pg_ts(o.created_us),
                    _pg_ts(o.filled_us),
                )
                for o in db.orders
            ],
        ),
        "ticks": (
            TICK_COLUMNS,
            [(t.market, t.price, _pg_ts(t.received_us)) for t in db.ticks],
        ),
    }
    for name in BUNDLE_SECTIONS:
        if name in ("fingerprint1", "fingerprint2"):
            fp(name, db.fingerprint1 if name == "fingerprint1" else db.fingerprint2)
            continue
        if name in secs:
            header, rows = tables[name]
            out.append(format_section(name, header, rows))
        if name == "orders":
            for sec in sorted(s for s in secs if s.startswith("inv_")):
                inv = sec[len("inv_") :]
                out.append(_inv_section(inv, db.invariant_rows.get(inv, ())))
    return "".join(out)

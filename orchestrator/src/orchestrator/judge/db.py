"""DB 스냅샷: bundle.sql 조립, psql 1회 실행, 출력 파싱(설계 5.4).

bundle.sql 순서(설계 5.4)
1. `\\set ids`, `\\set extra`, `\\set nick_re`. 세션 설정(UTC, 읽기 전용, 시간 제한).
2. 지문 F1.
3. BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY → 스냅샷 COPY 4개 → 필수 불변식 → COMMIT.
4. 지문 F2.
5. `\\set ON_ERROR_STOP off` → 선택 검사(arith, privileges). 선택 검사 오류가 필수 증거를 지우지 않는다.

출력 형식(psql stdout, raw/db/에 그대로 남긴다)
- 구획은 `--@ <name>` 줄로 시작하고 `--@end <name>` 줄로 끝난다.
- 본문은 COPY ... TO STDOUT (FORMAT csv, HEADER) 출력이다. 첫 줄이 열 이름이다.
- NULL은 따옴표 없는 빈 값, 빈 문자열은 `""`다(COPY CSV 규칙).
- 출력에 `--@end` 줄이 하나라도 있으면 끝 표지가 없는 구획은 버린다(psql이 그 구획에서 멈췄다).
  끝 표지가 전혀 없으면 머리줄이 있는 구획을 받는다(가짜 API 등 단순 출력 호환).
- 버린 구획은 DbSnapshot.sections에 없다. 그 구획을 쓰는 기준만 INCONCLUSIVE(DB_ERROR)다.
- 모든 구획의 첫 열은 uuid·정수·고정 문자열이다. 그래서 줄 머리 `--@`가 데이터와 섞이지 않는다.

주입 방지: UUID는 정규 형식만, run_id는 RUN_ID_RE 전체 일치만 받는다. 그 밖은 InputError다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from importlib import resources
from typing import Any

from .context import RUN_ID_RE
from .errors import InputError, JudgeError
from .model import (
    INVARIANT_SQL,
    OPTIONAL_INVARIANT_SQL,
    DbAccount,
    DbFill,
    DbOrder,
    DbPosition,
    DbSnapshot,
    dec,
)
from .transport import Transports

__all__ = [
    "ACCOUNT_COLUMNS",
    "FILL_COLUMNS",
    "FINGERPRINT_COLUMNS",
    "INVARIANT_COLUMNS",
    "MARK",
    "END_MARK",
    "OPTIONAL_SQL",
    "ORDER_COLUMNS",
    "POSITION_COLUMNS",
    "PSQL_TIMEOUT_S",
    "PsqlRun",
    "REQUIRED_SQL",
    "build_bundle",
    "exec_bundle",
    "format_section",
    "load_sql",
    "nick_regex",
    "parse_bundle_output",
    "render_snapshot",
    "run_snapshot",
]

MARK = "--@ "
END_MARK = "--@end "

# psql 전체 시간 상한(초). 안쪽 statement_timeout보다 길게 둔다(우리 설정값).
PSQL_TIMEOUT_S = 180.0
STATEMENT_TIMEOUT = "120s"
LOCK_TIMEOUT = "5s"

REQUIRED_SQL = tuple(n for n in INVARIANT_SQL if n not in OPTIONAL_INVARIANT_SQL)
OPTIONAL_SQL = tuple(n for n in INVARIANT_SQL if n in OPTIONAL_INVARIANT_SQL)

UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_INT_RE = re.compile(r"-?[0-9]+")

# 스냅샷 구획 열(이 순서로 COPY한다). k6 열은 끝에 붙인다(설계 5.4).
ACCOUNT_COLUMNS = (
    "id",
    "nickname",
    "mode",
    "initial_cash",
    "cash",
    "version",
    "created_at",
    "created_ms",
    "k6",
)
ORDER_COLUMNS = (
    "id",
    "account_id",
    "idem_key",
    "body_hash",
    "symbol",
    "side",
    "qty",
    "limit_price",
    "status",
    "reject_reason",
    "ref_source",
    "ref_seq",
    "ref_event_ts",
    "ref_recv_ts",
    "ref_price",
    "ref_age_ms",
    "received_at",
    "received_ms",
    "k6",
)
FILL_COLUMNS = (
    "id",
    "order_id",
    "account_id",
    "symbol",
    "side",
    "qty",
    "price",
    "amount",
    "fee",
    "fee_rate",
    "realized_pnl",
    "created_at",
    "created_ms",
    "k6",
)
POSITION_COLUMNS = ("account_id", "symbol", "qty", "cost_basis", "k6")
FINGERPRINT_COLUMNS = ("orders", "fills", "version_sum")

# 파싱 때 반드시 있어야 하는 열(나머지는 없으면 None)
_ACCOUNT_REQUIRED = ("id", "nickname", "mode", "initial_cash", "cash", "version")
_ORDER_REQUIRED = ORDER_COLUMNS[:17]  # received_ms, k6는 선택
_FILL_REQUIRED = FILL_COLUMNS[:11]
_POSITION_REQUIRED = POSITION_COLUMNS[:4]

# 불변식 SQL 결과 열(sql/inv_<name>.sql의 SELECT 목록과 같다. -m pg 테스트가 확인한다)
INVARIANT_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "i2": ("account_id", "field", "symbol", "value"),
    "nulls": ("table_name", "row_key", "column_name", "problem"),
    "constraints": ("kind", "table_name", "columns"),
    "i4": ("problem", "account_id", "idem_key", "order_id", "n", "detail"),
    "i5_cash": (
        "account_id",
        "initial_cash",
        "cash",
        "expect_cash",
        "cost_basis_sum",
        "realized_pnl_sum",
    ),
    "i5_qty": ("account_id", "symbol", "position_qty", "fill_net_qty"),
    "i5_flat": ("account_id", "symbol", "qty", "cost_basis"),
    "i6_db": (
        "problem",
        "order_id",
        "account_id",
        "symbol",
        "fill_price",
        "ref_price",
        "ref_source",
    ),
    "i8": ("fill_id", "account_id", "mode", "symbol", "order_id"),
    "arith": ("problem", "fill_id", "order_id", "actual", "expect"),
    "privileges": ("role_name", "table_name", "privilege"),
}

# ---------------------------------------------------------------- 고정 SQL(설계 5.4)

# 대상 계좌 ra와 k6 여부. k6: JSONL 계좌 또는 nickname 정규식이고 extra가 아니다.
_RA_K6 = """WITH ra AS (
    SELECT id,
           coalesce(
               (id = ANY (:'ids'::uuid[]) OR (mode = 'replay' AND nickname ~ :'nick_re'))
               AND NOT (id = ANY (:'extra'::uuid[])),
               false
           ) AS k6
    FROM accounts
    WHERE id = ANY (:'ids'::uuid[])
       OR id = ANY (:'extra'::uuid[])
       OR (mode = 'replay' AND nickname ~ :'nick_re')
)"""

_FINGERPRINT_SQL = f"""{_RA_K6},
ro AS (
    SELECT id FROM orders WHERE account_id IN (SELECT id FROM ra)
)
SELECT (SELECT count(*) FROM ro) AS orders,
       (SELECT count(*) FROM fills
        WHERE account_id IN (SELECT id FROM ra) OR order_id IN (SELECT id FROM ro)) AS fills,
       (SELECT coalesce(sum(version), 0) FROM accounts
        WHERE id IN (SELECT id FROM ra)) AS version_sum"""

_ACCOUNTS_SQL = f"""{_RA_K6}
SELECT a.id, a.nickname, a.mode, a.initial_cash, a.cash, a.version, a.created_at,
       floor(extract(epoch FROM a.created_at) * 1000)::bigint AS created_ms,
       ra.k6
FROM accounts a
JOIN ra ON ra.id = a.id
ORDER BY a.id"""

_ORDERS_SQL = f"""{_RA_K6}
SELECT o.id, o.account_id, o.idem_key, o.body_hash, o.symbol, o.side, o.qty, o.limit_price,
       o.status, o.reject_reason, o.ref_source, o.ref_seq, o.ref_event_ts, o.ref_recv_ts,
       o.ref_price, o.ref_age_ms, o.received_at,
       floor(extract(epoch FROM o.received_at) * 1000)::bigint AS received_ms,
       ra.k6
FROM orders o
JOIN ra ON ra.id = o.account_id
ORDER BY o.account_id, o.idem_key, o.id"""

_FILLS_SQL = f"""{_RA_K6},
ro AS (
    SELECT o.id, ra.k6 FROM orders o JOIN ra ON ra.id = o.account_id
)
SELECT f.id, f.order_id, f.account_id, f.symbol, f.side, f.qty, f.price, f.amount, f.fee,
       f.fee_rate, f.realized_pnl, f.created_at,
       floor(extract(epoch FROM f.created_at) * 1000)::bigint AS created_ms,
       coalesce(fa.k6, ro.k6, false) AS k6
FROM fills f
LEFT JOIN ra fa ON fa.id = f.account_id
LEFT JOIN ro ON ro.id = f.order_id
WHERE fa.id IS NOT NULL OR ro.id IS NOT NULL
ORDER BY f.id"""

_POSITIONS_SQL = f"""{_RA_K6}
SELECT p.account_id, p.symbol, p.qty, p.cost_basis, ra.k6
FROM positions p
JOIN ra ON ra.id = p.account_id
ORDER BY p.account_id, p.symbol"""

_SNAPSHOT_SQL = (
    ("accounts", _ACCOUNTS_SQL),
    ("orders", _ORDERS_SQL),
    ("fills", _FILLS_SQL),
    ("positions", _POSITIONS_SQL),
)


# ---------------------------------------------------------------- bundle 조립


def load_sql(name: str) -> str:
    """패키지의 sql/inv_<name>.sql 원문(설계 5.4 표)."""
    if name not in INVARIANT_SQL:
        raise JudgeError(f"모르는 불변식 SQL: {name!r}")
    path = resources.files(__package__) / "sql" / f"inv_{name}.sql"
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

    ARE에서 역슬래시 + 비영숫자는 그 글자 자체다(예: `\\-`는 `-`).
    """
    return "".join(c if c.isascii() and c.isalnum() else "\\" + c for c in s)


def _check_run_id(run_id: Any) -> str:
    # match는 끝 줄바꿈을 허용하므로 fullmatch로 본다
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise InputError(f"run_id {run_id!r}가 ^[a-z0-9-]{{3,40}}$에 맞지 않다")
    return run_id


def nick_regex(run_id: str) -> str:
    """보조 계좌 정규식 `^k6-<run_id>-[0-9]+$`(설계 5.3). run_id는 이스케이프한다."""
    return f"^k6-{_regex_escape(_check_run_id(run_id))}-[0-9]+$"


def _uuid_list(values: Any, where: str) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes)):
        raise InputError(f"{where}는 UUID 목록이어야 한다")
    out: set[str] = set()
    for v in values:
        if not isinstance(v, str) or not UUID_RE.fullmatch(v):
            raise InputError(f"{where}: {v!r}는 UUID 형식이 아니다")
        out.add(v.lower())
    return tuple(sorted(out))


def _psql_literal(value: str) -> str:
    """psql 메타 명령 인자용 작은따옴표 문자열. 역슬래시와 따옴표를 이스케이프한다."""
    if any(c in value for c in "\r\n\0"):
        raise InputError("psql 변수 값에 줄바꿈을 넣을 수 없다")
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _copy(select: str) -> str:
    return f"COPY (\n{select}\n) TO STDOUT WITH (FORMAT csv, HEADER true);"


def _section(name: str, select: str, *, optional: bool = False) -> list[str]:
    """`--@ name` → COPY → `--@end name`. 선택 검사는 성공했을 때만 끝 표지를 낸다."""
    out = [f"\\qecho '{MARK}{name}'", _copy(select)]
    if optional:
        out += ["\\if :ERROR", "\\else", f"\\qecho '{END_MARK}{name}'", "\\endif"]
    else:
        # ON_ERROR_STOP=1이라 실패하면 여기까지 오지 않는다
        out.append(f"\\qecho '{END_MARK}{name}'")
    return out


def build_bundle(
    ctx_like: Any = None,
    account_ids: Iterable[str] = (),
    extra_ids: Iterable[str] | None = None,
    run_id: str | None = None,
) -> str:
    """bundle.sql 텍스트(설계 5.4). 같은 입력이면 같은 바이트다.

    ctx_like: run_id·extra_accounts 속성이 있으면 기본값으로 쓴다(RunContext 등). None 가능.
    account_ids: JSONL account 줄의 계좌 UUID(1차 키). 정렬·중복 제거한다.
    extra_ids: extra_accounts. None이면 ctx_like.extra_accounts.
    run_id: None이면 ctx_like.run_id.
    """
    rid = _check_run_id(
        run_id if run_id is not None else getattr(ctx_like, "run_id", None)
    )
    if extra_ids is None:
        extra_ids = getattr(ctx_like, "extra_accounts", ())
    ids = _uuid_list(account_ids, "account_ids")
    extra = _uuid_list(extra_ids, "extra_ids")

    lines: list[str] = [
        f"-- bundle.sql (chaos-judge db.build_bundle, 설계 5.4) run_id={rid}",
        f"\\set ids {_psql_literal('{' + ','.join(ids) + '}')}",
        f"\\set extra {_psql_literal('{' + ','.join(extra) + '}')}",
        f"\\set nick_re {_psql_literal(nick_regex(rid))}",
        "SET client_encoding = 'UTF8';",
        "SET TIME ZONE 'UTC';",
        "SET DateStyle = 'ISO, YMD';",
        f"SET statement_timeout = '{STATEMENT_TIMEOUT}';",
        f"SET lock_timeout = '{LOCK_TIMEOUT}';",
        "SET default_transaction_read_only = on;",
    ]
    lines += _section("fingerprint1", _FINGERPRINT_SQL)
    lines.append("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
    for name, select in _SNAPSHOT_SQL:
        lines += _section(name, select)
    for name in REQUIRED_SQL:
        lines += _section(f"inv_{name}", _embed(load_sql(name), f"inv_{name}.sql"))
    lines.append("COMMIT;")
    lines += _section("fingerprint2", _FINGERPRINT_SQL)
    lines.append("\\set ON_ERROR_STOP off")
    for name in OPTIONAL_SQL:
        lines += _section(
            f"inv_{name}", _embed(load_sql(name), f"inv_{name}.sql"), optional=True
        )
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
    timeout_s: float = PSQL_TIMEOUT_S,
) -> PsqlRun:
    """bundle.sql을 psql 표준 입력으로 한 번 보낸다(설계 5.4).

    명령은 transports.psql_argv()다. 기본은
    `kubectl -n papertrade exec -i postgres-0 -c postgres -- psql -X -q -v ON_ERROR_STOP=1
    -U pt_ro -d papertrade -f -`. 테스트는 Transports.for_local_pg()로 docker exec를 쓴다.
    0이 아닌 종료 코드는 예외가 아니다(빠진 구획이 DB_ERROR가 된다).
    실행 파일이 없거나 시간이 넘으면 TransportError(judge 오류)다.
    """
    tr = transports if transports is not None else Transports()
    argv = tuple(tr.psql_argv())
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
    timeout_s: float = PSQL_TIMEOUT_S,
) -> str:
    """exec_bundle의 stdout만 돌려준다. 종료 코드·stderr가 필요하면 exec_bundle을 쓴다."""
    return exec_bundle(transports, bundle, timeout_s=timeout_s).stdout


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


def _split_sections(text: str) -> dict[str, list[list[Field]]]:
    """완결된 구획만 이름 → 레코드(머리줄 포함)."""
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
    return {
        name: recs
        for name, recs in sections.items()
        if recs and name not in broken and (name in closed or not has_end)
    }


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
        return dec(v)
    except (TypeError, ValueError) as e:
        raise InputError(f"{where}: {e}") from e


def _bool(v: Field, where: str) -> bool:
    if v is None:
        return False
    if v in ("t", "true"):
        return True
    if v in ("f", "false"):
        return False
    raise InputError(f"{where}: 참거짓 값이 아니다({v!r})")


def _req_str(v: Field, where: str) -> str:
    if v is None or v == "":
        raise InputError(f"{where}: 비어 있을 수 없다")
    return v


def _ts_ms(raw: Field, where: str) -> int | None:
    """timestamptz 텍스트 → epoch ms. *_ms 열이 없을 때만 쓴다."""
    if raw is None:
        return None
    try:
        t = datetime.fromisoformat(raw)
    except ValueError as e:
        raise InputError(f"{where}: 시각 형식이 아니다({raw!r})") from e
    if t.tzinfo is None:
        raise InputError(f"{where}: 시간대가 없다({raw!r})")
    return (t - _EPOCH) // timedelta(milliseconds=1)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ms_or_ts(
    row: Mapping[str, Field], ms_col: str, ts_col: str, where: str
) -> int | None:
    if ms_col in row:
        return _int(row[ms_col], f"{where}.{ms_col}")
    return _ts_ms(row.get(ts_col), f"{where}.{ts_col}")


def _accounts(rows: list[dict], header: Sequence[str]) -> tuple[DbAccount, ...]:
    _need("accounts", header, _ACCOUNT_REQUIRED)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"accounts {k}행"
        out.append(
            DbAccount(
                id=_req_str(r["id"], f"{w}.id"),
                nickname=r["nickname"],
                mode=r["mode"],
                initial_cash=_int(r["initial_cash"], f"{w}.initial_cash"),
                cash=_int(r["cash"], f"{w}.cash"),
                version=_int(r["version"], f"{w}.version"),
                k6=_bool(r.get("k6"), f"{w}.k6"),
                created_ms=_ms_or_ts(r, "created_ms", "created_at", w),
            )
        )
    return tuple(sorted(out, key=lambda a: a.id))


def _orders(rows: list[dict], header: Sequence[str]) -> tuple[DbOrder, ...]:
    _need("orders", header, _ORDER_REQUIRED)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"orders {k}행"
        out.append(
            DbOrder(
                id=_req_str(r["id"], f"{w}.id"),
                account_id=_req_str(r["account_id"], f"{w}.account_id"),
                idem_key=r["idem_key"] or "",  # NULL 키는 ""(inv_nulls가 위반으로 낸다)
                body_hash=r["body_hash"],
                symbol=r["symbol"],
                side=r["side"],
                qty=_dec(r["qty"], f"{w}.qty"),
                limit_price=_dec(r["limit_price"], f"{w}.limit_price"),
                status=r["status"],
                reject_reason=r["reject_reason"],
                ref_source=r["ref_source"],
                ref_seq=r["ref_seq"],
                ref_event_ts=_int(r["ref_event_ts"], f"{w}.ref_event_ts"),
                ref_recv_ts=_int(r["ref_recv_ts"], f"{w}.ref_recv_ts"),
                ref_price=_dec(r["ref_price"], f"{w}.ref_price"),
                ref_age_ms=_int(r["ref_age_ms"], f"{w}.ref_age_ms"),
                received_at=r["received_at"],
                received_ms=_ms_or_ts(r, "received_ms", "received_at", w),
            )
        )
    return tuple(sorted(out, key=lambda o: (o.account_id, o.idem_key, o.id)))


def _fills(rows: list[dict], header: Sequence[str]) -> tuple[DbFill, ...]:
    _need("fills", header, _FILL_REQUIRED)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"fills {k}행"
        out.append(
            DbFill(
                id=_req_int(r["id"], f"{w}.id"),
                order_id=r["order_id"] or "",  # NULL은 ""(inv_nulls가 위반으로 낸다)
                account_id=r["account_id"],
                symbol=r["symbol"],
                side=r["side"],
                qty=_dec(r["qty"], f"{w}.qty"),
                price=_dec(r["price"], f"{w}.price"),
                amount=_int(r["amount"], f"{w}.amount"),
                fee=_int(r["fee"], f"{w}.fee"),
                fee_rate=_dec(r["fee_rate"], f"{w}.fee_rate"),
                realized_pnl=_int(r["realized_pnl"], f"{w}.realized_pnl"),
                created_at=r.get("created_at"),
                created_ms=_ms_or_ts(r, "created_ms", "created_at", w),
            )
        )
    return tuple(sorted(out, key=lambda f: f.id))


def _positions(rows: list[dict], header: Sequence[str]) -> tuple[DbPosition, ...]:
    _need("positions", header, _POSITION_REQUIRED)
    out = []
    for k, r in enumerate(rows, start=1):
        w = f"positions {k}행"
        out.append(
            DbPosition(
                account_id=_req_str(r["account_id"], f"{w}.account_id"),
                symbol=_req_str(r["symbol"], f"{w}.symbol"),
                qty=_dec(r["qty"], f"{w}.qty"),
                cost_basis=_int(r["cost_basis"], f"{w}.cost_basis"),
            )
        )
    return tuple(sorted(out, key=lambda p: (p.account_id, p.symbol)))


def _fingerprint(
    name: str, rows: list[dict], header: Sequence[str]
) -> tuple[int, int, int]:
    _need(name, header, FINGERPRINT_COLUMNS)
    if len(rows) != 1:
        raise InputError(f"구획 '{name}': 행이 1개여야 한다(받음 {len(rows)}개)")
    r = rows[0]
    return (
        _req_int(r["orders"], f"{name}.orders"),
        _req_int(r["fills"], f"{name}.fills"),
        _req_int(r["version_sum"], f"{name}.version_sum"),
    )


def parse_bundle_output(text: str) -> DbSnapshot:
    """psql 출력(--@ 구획 + COPY CSV)을 DbSnapshot으로 바꾼다(설계 5.4).

    - numeric은 Decimal(문자열 그대로), bigint·int는 int, NULL은 None.
    - 빠지거나 끝나지 않은 구획은 sections에 없다. 그 표는 빈 tuple, 지문은 None이다.
    - 불변식 구획(inv_<name>)의 위반 행은 invariant_rows[<name>]에 열 이름 → 텍스트(NULL은 None)로
      둔다. 위반이 없는 구획은 sections에만 있다(DbSnapshot.invariant_violations가 빈 tuple을 준다).
    - 행 순서를 정렬해 같은 증거면 같은 결과다.
    - 형식이 깨졌으면(열 수 불일치, 중복 구획 등) InputError다.
    """
    parsed = {name: _table(name, recs) for name, recs in _split_sections(text).items()}

    def take(name: str, conv: Any, empty: Any) -> Any:
        if name not in parsed:
            return empty
        header, rows = parsed[name]
        return conv(rows, header)

    fps: dict[str, tuple[int, int, int] | None] = {}
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
        orders=take("orders", _orders, ()),
        fills=take("fills", _fills, ()),
        positions=take("positions", _positions, ()),
        fingerprint1=fps["fingerprint1"],
        fingerprint2=fps["fingerprint2"],
        sections=frozenset(parsed),
        invariant_rows=inv_rows,
    )


# ---------------------------------------------------------------- 출력 만들기(테스트·가짜 API 참고용)


def _csv_field(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        s = "t" if v else "f"
    elif isinstance(v, Decimal):
        s = format(v, "f")
    else:
        s = str(v)
    if (
        s == ""
        or s != s.strip()
        or s.startswith("--@")
        or any(c in s for c in ',"\r\n')
    ):
        return '"' + s.replace('"', '""') + '"'
    return s


def format_section(
    name: str, header: Sequence[str], rows: Iterable[Sequence[Any]]
) -> str:
    """구획 하나를 psql 출력과 같은 모양으로 쓴다(None → NULL, "" → `""`)."""
    lines = [f"{MARK}{name}", ",".join(_csv_field(h) for h in header)]
    lines += [",".join(_csv_field(v) for v in r) for r in rows]
    lines.append(f"{END_MARK}{name}")
    return "\n".join(lines) + "\n"


def render_snapshot(db: DbSnapshot) -> str:
    """DbSnapshot을 bundle 출력 모양으로 되돌린다. parse_bundle_output의 역(구획 순서는 bundle 순서).

    k6 열은 계좌의 k6 값으로 채운다. invariant_rows의 열은 INVARIANT_COLUMNS를 따른다.
    """
    k6 = {a.id: a.k6 for a in db.accounts}
    order_acct = {o.id: o.account_id for o in db.orders}
    out: list[str] = []

    def fp(name: str, v: tuple[int, int, int] | None) -> None:
        if name in db.sections and v is not None:
            out.append(format_section(name, FINGERPRINT_COLUMNS, [v]))

    fp("fingerprint1", db.fingerprint1)
    tables: list[tuple[str, tuple[str, ...], list[tuple[Any, ...]]]] = [
        (
            "accounts",
            ACCOUNT_COLUMNS,
            [
                (
                    a.id,
                    a.nickname,
                    a.mode,
                    a.initial_cash,
                    a.cash,
                    a.version,
                    None,
                    a.created_ms,
                    a.k6,
                )
                for a in db.accounts
            ],
        ),
        (
            "orders",
            ORDER_COLUMNS,
            [
                (
                    o.id,
                    o.account_id,
                    o.idem_key,
                    o.body_hash,
                    o.symbol,
                    o.side,
                    o.qty,
                    o.limit_price,
                    o.status,
                    o.reject_reason,
                    o.ref_source,
                    o.ref_seq,
                    o.ref_event_ts,
                    o.ref_recv_ts,
                    o.ref_price,
                    o.ref_age_ms,
                    o.received_at,
                    o.received_ms,
                    k6.get(o.account_id, False),
                )
                for o in db.orders
            ],
        ),
        (
            "fills",
            FILL_COLUMNS,
            [
                (
                    f.id,
                    f.order_id,
                    f.account_id,
                    f.symbol,
                    f.side,
                    f.qty,
                    f.price,
                    f.amount,
                    f.fee,
                    f.fee_rate,
                    f.realized_pnl,
                    f.created_at,
                    f.created_ms,
                    k6.get(
                        f.account_id or "",
                        k6.get(order_acct.get(f.order_id, ""), False),
                    ),
                )
                for f in db.fills
            ],
        ),
        (
            "positions",
            POSITION_COLUMNS,
            [
                (
                    p.account_id,
                    p.symbol,
                    p.qty,
                    p.cost_basis,
                    k6.get(p.account_id, False),
                )
                for p in db.positions
            ],
        ),
    ]
    for name, header, rows in tables:
        if name in db.sections:
            out.append(format_section(name, header, rows))
    for name in REQUIRED_SQL:
        out.append(_render_inv(db, name))
    fp("fingerprint2", db.fingerprint2)
    for name in OPTIONAL_SQL:
        out.append(_render_inv(db, name))
    return "".join(out)


def _render_inv(db: DbSnapshot, name: str) -> str:
    rows = db.invariant_violations(name)
    if rows is None:
        return ""
    cols = INVARIANT_COLUMNS[name]
    return format_section(f"inv_{name}", cols, [[r.get(c) for c in cols] for r in rows])

"""판정 기준 YAML 읽기·병합·검증(설계 6).

규칙
- schema_version: 1. 모르는 키는 InputError(12).
- extends: 상대 경로는 그 파일 위치 기준. 순환은 InputError.
- 맵은 깊은 병합. checks·guards·fault_effect는 id별 필드 단위 덮어쓰기, 새 id는 뒤에 추가.
- replace: true를 맵이나 목록 항목 안에 두면 그 맵·항목을 병합 없이 통째로 바꾼다.
- disable: [id]는 그 파일까지 병합한 결과에서 id를 지운다(없는 id면 InputError).
- 병합 뒤 필수 키를 검증하고 기본값을 채운다. required 기본 true.
  on_violation 기본값은 checks가 fail, guards·fault_effect가 inconclusive(5.6 가드).
- 결과는 정렬된 키의 YAML(criteria.resolved.yaml)과 그 sha256이다.
  resolved 파일을 다시 load_criteria로 읽으면 같은 내용·같은 해시가 나온다.
- 계약 정렬 명세 9, 11: safety는 markets·user_pattern·user_max, integrity는 response_fields(응답
  필드 → orders 열)·response_fields_confirmed·initial_krw·stale_ms다. user_max·initial_krw·stale_ms는
  없으면 기본값을 채운다(resolved에 남는다). 옛 키(symbol_prefix, account_mode, price_max_age_ms)는
  거부한다.
- integrity 항목의 sql:은 DB 불변식 id 목록(model.INVARIANTS), check:는 fresh다(명세 7).
"""

from __future__ import annotations

import copy
import math
import re
from collections.abc import Callable, Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from .errors import InputError
from .model import (
    DEFAULT_INITIAL_KRW,
    DEFAULT_STALE_MS,
    DEFAULT_USER_MAX,
    INTEGRITY_CHECKS,
    INVARIANTS,
    NON_COMPARE_FIELDS,
    ORDER_COLUMNS,
    Criteria,
    IntegrityCfg,
    OkSub,
    OrderClass,
    SafetyCfg,
    sha256_text,
)

__all__ = [
    "Criteria",
    "load_criteria",
    "integrity_cfg",
    "safety_cfg",
    "write_resolved",
    "dump_resolved",
    "duration_ms",
    "RESOLVED_NAME",
]

SCHEMA_VERSION = 1
RESOLVED_NAME = "criteria.resolved.yaml"
MAX_EXTENDS_DEPTH = 16

LIST_KEYS = ("checks", "guards", "fault_effect")
TOP_KEYS = frozenset(
    {
        "schema_version",
        "extends",
        "disable",
        "safety",
        "phases",
        "coverage",
        "integrity",
        "prometheus",
        "require_extra_accounts",
        *LIST_KEYS,
    }
)
# 병합 지시 키. resolved에는 남지 않는다.
DIRECTIVE_KEYS = frozenset({"extends", "disable"})
REPLACE_KEY = "replace"

PHASE_NAMES = frozenset({"warmup", "baseline", "fault", "recovery", "tail", "steady"})
OPS = frozenset({"==", "!=", "<", "<=", ">", ">="})
ON_VIOLATION = frozenset({"fail", "inconclusive"})
CHECK_KINDS = frozenset({"integrity", "unresolved", "client", "prom", "k6"})
KNOWN_CHECKS = frozenset(INTEGRITY_CHECKS)  # check: 이름(명세 7 fresh)
K6_CHECKS = frozenset({"exit_code"})
CLIENT_STATS = frozenset(
    {
        "ack_p95_ms",
        "sys_err_ratio",
        "achieved_ratio",
        "recovery",
        "filled",
        "sell_filled",
    }
)
KNOWN_SQL = frozenset(INVARIANTS)  # sql: 이름(명세 7 DB 불변식)
KNOWN_GUARDS = frozenset(
    {
        "safety",
        "account_set",
        "fill_coverage",
        "sell_coverage",
        "load_coverage",
        "fault_in_load",
        "fault_effect",
        "db_quiescent",
        # 2차(gateway-pod-kill, 6절 끝)
        "ws_load_present",
        "orders_unaffected",
        "ws_restored",
    }
)
FAULT_OBSERVE = frozenset({"pod_replaced", "crash_restart"})
# 없앤 키 → 안내(명세 9). 모르는 키보다 먼저 알려 준다.
REMOVED_KEYS = {
    "safety": {
        "symbol_prefix": "safety.markets",
        "account_mode": "safety.user_pattern",
    },
    "integrity": {"price_max_age_ms": "integrity.stale_ms"},
}
# 없으면 채우는 기본값(명세 7. 근거 seed.sql, app/config.py STALE_MS)
INTEGRITY_DEFAULTS = {"initial_krw": DEFAULT_INITIAL_KRW, "stale_ms": DEFAULT_STALE_MS}
# 명세 11. 근거 seed.sql user-0001..1000
SAFETY_DEFAULTS = {"user_max": DEFAULT_USER_MAX}
RESPONSE_FIELD_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
MARKET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")

ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
MARK_RE = re.compile(r"^[a-z][a-z0-9_]*$")
DURATION_RE = re.compile(r"^(\d+)(ms|s|m)$")
ACCOUNT_SEL_RE = re.compile(r"^(idx\d+|extra|any)$")

_COMMON_ITEM_KEYS = frozenset({"id", "required", "on_violation", "requires"})
CHECK_KEYS_BY_KIND = {
    "integrity": _COMMON_ITEM_KEYS
    | {"kind", "phases", "class", "sql", "check", "op", "value"},
    "unresolved": _COMMON_ITEM_KEYS | {"kind", "phases", "class", "op", "value"},
    "client": _COMMON_ITEM_KEYS | {"kind", "phases", "stat", "op", "value"},
    "k6": _COMMON_ITEM_KEYS | {"kind", "phases", "check", "op", "value"},
    "prom": _COMMON_ITEM_KEYS | {"kind", "phases", "query", "op", "value"},
}
GUARD_KEYS = _COMMON_ITEM_KEYS | {"kind", "phases", "op", "value"}
FAULT_EFFECT_KEYS = _COMMON_ITEM_KEYS | {
    "observe",
    "target",
    "within_s",
    "min",
    "class_sub",
    "account",
    "op",
    "value",
}


# ---------------------------------------------------------------- 공개 함수


def load_criteria(path: str | Path) -> Criteria:
    """기준 파일을 extends 사슬까지 읽어 병합·검증한다(설계 6)."""
    src = Path(path)
    merged, chain, disabled = _load_chain(src, ())
    data = _finalize(merged, src.name)
    text = dump_resolved(data)
    return Criteria(
        data=data,
        text=text,
        sha256=sha256_text(text),
        source=src,
        chain=tuple(chain),
        disabled=tuple(sorted(disabled)),
    )


def dump_resolved(data: Mapping[str, Any]) -> str:
    """정렬된 키, 블록 스타일의 결정적 YAML."""
    return yaml.safe_dump(
        _plain(data),
        sort_keys=True,
        allow_unicode=True,
        default_flow_style=False,
        width=4096,
    )


def write_resolved(crit: Criteria, path: str | Path) -> Path:
    """criteria.resolved.yaml을 쓴다. 디렉터리를 주면 그 안에 RESOLVED_NAME으로 쓴다."""
    p = Path(path)
    if p.is_dir():
        p = p / RESOLVED_NAME
    p.write_text(crit.text, encoding="utf-8")
    return p


def integrity_cfg(crit: Criteria) -> IntegrityCfg:
    """기준의 integrity 맵을 IntegrityCfg로 바꾼다."""
    sec = crit.data["integrity"]
    return IntegrityCfg(
        response_fields=dict(sorted(sec["response_fields"].items())),
        response_fields_confirmed=sec["response_fields_confirmed"],
        initial_krw=Decimal(sec["initial_krw"]),
        stale_ms=sec["stale_ms"],
    )


def safety_cfg(crit: Criteria) -> SafetyCfg:
    """기준의 safety 맵을 SafetyCfg로 바꾼다(명세 8)."""
    sec = crit.data["safety"]
    return SafetyCfg(
        markets=tuple(sec["markets"]),
        user_pattern=sec["user_pattern"],
        user_max=sec["user_max"],
    )


def duration_ms(v: str) -> int:
    """'60s', '500ms', '2m' → ms."""
    m = DURATION_RE.fullmatch(v) if isinstance(v, str) else None
    if not m:
        raise InputError(f"기간 {v!r}는 <정수>(ms|s|m) 형식이어야 한다")
    n, unit = int(m.group(1)), m.group(2)
    return n * {"ms": 1, "s": 1000, "m": 60000}[unit]


# ---------------------------------------------------------------- YAML 읽기


class _StrictLoader(yaml.SafeLoader):
    """중복 키를 거부하는 SafeLoader."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> Any:
        self.flatten_mapping(node)
        seen: set[Any] = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=True)
            try:
                dup = key in seen
            except TypeError:
                continue
            if dup:
                raise yaml.constructor.ConstructorError(
                    None, None, f"중복 키 {key!r}", key_node.start_mark
                )
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise InputError(f"{path}: 읽을 수 없다({e.strerror or e})") from e
    try:
        docs = list(yaml.load_all(text, Loader=_StrictLoader))
    except yaml.YAMLError as e:
        raise InputError(f"{path}: YAML 오류({e})") from e
    if len(docs) != 1:
        raise InputError(f"{path}: 문서는 하나여야 한다(받음 {len(docs)}개)")
    doc = docs[0]
    if not isinstance(doc, dict):
        raise InputError(f"{path}: 최상위는 맵이어야 한다")
    for k in doc:
        if not isinstance(k, str):
            raise InputError(f"{path}: 키는 문자열이어야 한다({k!r})")
    return doc


# ---------------------------------------------------------------- 병합


def _load_chain(
    path: Path, stack: tuple[Path, ...]
) -> tuple[dict[str, Any], list[str], list[str]]:
    real = path.resolve()
    if real in stack:
        names = " -> ".join(p.name for p in (*stack, real))
        raise InputError(f"extends 순환: {names}")
    if len(stack) >= MAX_EXTENDS_DEPTH:
        raise InputError(f"{path}: extends 깊이가 {MAX_EXTENDS_DEPTH}을 넘는다")
    doc = _read_yaml(path)
    where = path.name

    unknown = sorted(set(doc) - TOP_KEYS)
    if REPLACE_KEY in unknown:
        raise InputError(f"{where}: replace는 맵이나 목록 항목 안에만 둔다")
    if unknown:
        raise InputError(f"{where}: 모르는 키 {unknown}")
    if "schema_version" in doc and doc["schema_version"] != SCHEMA_VERSION:
        raise InputError(
            f"{where}: schema_version은 {SCHEMA_VERSION}이어야 한다"
            f"(받음: {doc['schema_version']!r})"
        )

    ext = doc.get("extends")
    if ext is None:
        base: dict[str, Any] = {}
        chain: list[str] = []
        disabled: list[str] = []
    else:
        if not isinstance(ext, str) or not ext.strip():
            raise InputError(f"{where}: extends는 파일 경로 문자열이어야 한다")
        parent = Path(ext)
        if not parent.is_absolute():
            parent = path.parent / parent
        base, chain, disabled = _load_chain(parent, (*stack, real))

    patch = {k: v for k, v in doc.items() if k not in DIRECTIVE_KEYS}
    merged = _merge_top(base, patch, where)

    dis = doc.get("disable")
    if dis is not None:
        if not isinstance(dis, list) or not all(isinstance(x, str) for x in dis):
            raise InputError(f"{where}: disable은 id 문자열 목록이어야 한다")
        for item_id in dis:
            if not _remove_id(merged, item_id):
                raise InputError(f"{where}: disable한 id '{item_id}'가 없다")
        disabled = disabled + list(dis)
    return merged, chain + [where], disabled


def _merge_top(
    base: dict[str, Any], patch: Mapping[str, Any], where: str
) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in patch.items():
        if k in LIST_KEYS:
            out[k] = _merge_items(out.get(k, []), v, f"{where}: {k}")
        else:
            out[k] = _merge_value(out.get(k), v, f"{where}: {k}")
    return out


def _merge_value(base: Any, patch: Any, where: str) -> Any:
    if not isinstance(patch, dict):
        return copy.deepcopy(patch)
    if _take_replace(patch, where):
        return _strip_replace(patch, where)
    if not isinstance(base, dict):
        return _strip_replace(patch, where)
    out = copy.deepcopy(base)
    for k, v in patch.items():
        if k == REPLACE_KEY:
            continue
        out[k] = _merge_value(out.get(k), v, f"{where}.{k}")
    return out


def _merge_items(base: Any, patch: Any, where: str) -> list[dict[str, Any]]:
    if not isinstance(patch, list):
        raise InputError(f"{where}는 목록이어야 한다")
    out = [copy.deepcopy(x) for x in base]
    index = {x["id"]: i for i, x in enumerate(out)}
    seen: set[str] = set()
    for n, item in enumerate(patch):
        if not isinstance(item, dict):
            raise InputError(f"{where}[{n}]는 맵이어야 한다")
        item_id = item.get("id")
        if not isinstance(item_id, str) or not ID_RE.fullmatch(item_id):
            raise InputError(
                f"{where}[{n}]: id {item_id!r}가 ^[a-z][a-z0-9_]*$에 맞지 않다"
            )
        if item_id in seen:
            raise InputError(f"{where}: 같은 파일 안에 id '{item_id}'가 두 번 있다")
        seen.add(item_id)
        iwhere = f"{where}[{item_id}]"
        replace = _take_replace(item, iwhere)
        fresh = _strip_replace(item, iwhere)
        if item_id in index:
            i = index[item_id]
            out[i] = fresh if replace else {**out[i], **fresh}
        else:
            index[item_id] = len(out)
            out.append(fresh)
    return out


def _take_replace(m: Mapping[str, Any], where: str) -> bool:
    v = m.get(REPLACE_KEY, False)
    if not isinstance(v, bool):
        raise InputError(f"{where}: replace는 true|false여야 한다")
    return v


def _strip_replace(m: Mapping[str, Any], where: str) -> dict[str, Any]:
    """replace 키를 지운 깊은 사본. 안쪽 맵의 replace도 지운다."""
    out: dict[str, Any] = {}
    for k, v in m.items():
        if k == REPLACE_KEY:
            continue
        if isinstance(v, dict):
            _take_replace(v, f"{where}.{k}")
            out[k] = _strip_replace(v, f"{where}.{k}")
        else:
            out[k] = copy.deepcopy(v)
    return out


def _remove_id(merged: dict[str, Any], item_id: str) -> bool:
    found = False
    for k in LIST_KEYS:
        items = merged.get(k)
        if not items:
            continue
        kept = [x for x in items if x.get("id") != item_id]
        if len(kept) != len(items):
            found = True
            merged[k] = kept
    return found


# ---------------------------------------------------------------- 검증·기본값


Validator = Callable[[Any, str], None]


def _err(where: str, msg: str) -> InputError:
    return InputError(f"{where}: {msg}")


def _v_str(v: Any, where: str) -> None:
    if not isinstance(v, str) or not v:
        raise _err(where, "비어 있지 않은 문자열이어야 한다")


def _v_bool(v: Any, where: str) -> None:
    if not isinstance(v, bool):
        raise _err(where, "true|false여야 한다")


def _v_nonneg_int(v: Any, where: str) -> None:
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise _err(where, "0 이상 정수여야 한다")


def _v_pos_int(v: Any, where: str) -> None:
    if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
        raise _err(where, "양의 정수여야 한다")


def _v_number(v: Any, where: str) -> None:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise _err(where, "유한한 수여야 한다")


def _v_ratio(v: Any, where: str) -> None:
    _v_number(v, where)
    if not 0 <= v <= 1:
        raise _err(where, "0과 1 사이여야 한다")


def _v_duration(v: Any, where: str) -> None:
    if not isinstance(v, str) or not DURATION_RE.fullmatch(v):
        raise _err(where, "기간은 <정수>(ms|s|m) 형식이어야 한다(예: 60s)")


def _v_mark(v: Any, where: str) -> None:
    if not isinstance(v, str) or not MARK_RE.fullmatch(v):
        raise _err(where, "표지 이름이어야 한다(예: inject_start)")


def _v_markets(v: Any, where: str) -> None:
    if not isinstance(v, list) or not v:
        raise _err(where, "비어 있지 않은 종목 목록이어야 한다(예: [KRW-BTC])")
    for x in v:
        if not isinstance(x, str) or not MARKET_RE.fullmatch(x):
            raise _err(where, f"종목 이름 {x!r}가 올바르지 않다")
    if len(set(v)) != len(v):
        raise _err(where, "종목이 중복된다")


def _v_regex(v: Any, where: str) -> None:
    _v_str(v, where)
    try:
        re.compile(v)
    except re.error as e:
        raise _err(where, f"정규식 오류({e})") from e


def _v_krw(v: Any, where: str) -> None:
    """원화 금액. 문자열 Decimal(권장) 또는 정수. 0 이상, 소수 4자리 이하(NUMERIC(24,4))."""
    if isinstance(v, bool) or not isinstance(v, (str, int)):
        raise _err(where, '문자열 금액이어야 한다(예: "10000000")')
    try:
        d = Decimal(str(v).strip())
    except InvalidOperation as e:
        raise _err(where, f"금액 형식이 아니다({v!r})") from e
    if not d.is_finite() or d < 0:
        raise _err(where, "0 이상 유한한 금액이어야 한다")
    exp = d.as_tuple().exponent
    if isinstance(exp, int) and exp < -4:
        raise _err(where, "소수 4자리까지다(accounts.krw NUMERIC(24,4))")


def _v_response_fields(v: Any, where: str) -> None:
    """응답 필드 → orders 열(명세 5). replayed·reason은 비교 필드가 아니다."""
    if not isinstance(v, dict) or not v:
        raise _err(where, "비어 있지 않은 맵이어야 한다(응답 필드 → orders 열)")
    for k, col in v.items():
        if not isinstance(k, str) or not RESPONSE_FIELD_RE.fullmatch(k):
            raise _err(where, f"응답 필드 이름 {k!r}가 올바르지 않다")
        if k in NON_COMPARE_FIELDS:
            raise _err(where, f"{k}는 분류 입력이라 비교 필드로 쓸 수 없다")
        if col not in ORDER_COLUMNS:
            raise _err(
                f"{where}.{k}",
                f"orders 열이 아니다({col!r}, 허용: {list(ORDER_COLUMNS)})",
            )


# 맵 구획 명세: 키 → (검사 함수 또는 하위 명세, 필수 여부)
Spec = Mapping[str, tuple[Any, bool]]

SECTION_SPECS: Mapping[str, tuple[Spec, bool]] = {
    "safety": (
        {
            "markets": (_v_markets, True),
            "user_pattern": (_v_regex, True),
            "user_max": (_v_pos_int, False),
        },
        True,
    ),
    "phases": (
        {
            "warmup": (_v_duration, True),
            "baseline": (
                {
                    "before": (_v_mark, True),
                    "duration": (_v_duration, True),
                    "guard": (_v_duration, True),
                },
                True,
            ),
            "fault": (
                {
                    "from": (_v_mark, True),
                    "to": (_v_mark, True),
                    "extra": (_v_duration, True),
                },
                True,
            ),
            "tail": (
                {"before": (_v_mark, True), "duration": (_v_duration, True)},
                True,
            ),
        },
        True,
    ),
    "coverage": (
        {
            "min_filled": ({p: (_v_nonneg_int, False) for p in PHASE_NAMES}, True),
            "min_sell_filled": (_v_nonneg_int, True),
            "min_achieved_ratio": (_v_ratio, True),
        },
        True,
    ),
    "integrity": (
        {
            "response_fields": (_v_response_fields, True),
            "response_fields_confirmed": (_v_bool, True),
            "initial_krw": (_v_krw, False),
            "stale_ms": (_v_pos_int, False),
        },
        True,
    ),
    "prometheus": (
        {"enabled": (_v_bool, True), "scrape_interval": (_v_duration, False)},
        False,
    ),
}


def _validate_map(v: Any, spec: Spec, where: str) -> None:
    if not isinstance(v, dict):
        raise _err(where, "맵이어야 한다")
    unknown = sorted(set(v) - set(spec))
    if unknown:
        raise _err(where, f"모르는 키 {unknown}")
    for k, (check, required) in spec.items():
        if k not in v:
            if required:
                raise _err(where, f"필수 키 '{k}'가 없다")
            continue
        if isinstance(check, Mapping):
            _validate_map(v[k], check, f"{where}.{k}")
        else:
            check(v[k], f"{where}.{k}")


def _v_list_of(v: Any, allowed: frozenset[str], where: str, what: str) -> None:
    if not isinstance(v, list) or not v:
        raise _err(where, f"비어 있지 않은 {what} 목록이어야 한다")
    for x in v:
        if x not in allowed:
            raise _err(where, f"모르는 {what} {x!r}(허용: {sorted(allowed)})")
    if len(set(v)) != len(v):
        raise _err(where, f"{what}이 중복된다")


def _v_requires(v: Any, where: str) -> None:
    items = [v] if isinstance(v, str) else v
    if not isinstance(items, list) or not items:
        raise _err(where, "문자열 또는 문자열 목록이어야 한다")
    for x in items:
        if not isinstance(x, str) or not ID_RE.fullmatch(x):
            raise _err(where, f"requires 이름 {x!r}가 올바르지 않다")


def _v_item_common(item: Mapping[str, Any], where: str) -> None:
    if "required" in item:
        _v_bool(item["required"], f"{where}.required")
    if "on_violation" in item and item["on_violation"] not in ON_VIOLATION:
        raise _err(f"{where}.on_violation", "fail|inconclusive여야 한다")
    if "requires" in item:
        _v_requires(item["requires"], f"{where}.requires")
    if "phases" in item:
        _v_list_of(item["phases"], PHASE_NAMES, f"{where}.phases", "구간")


def _v_op_value(item: Mapping[str, Any], where: str, required: bool) -> None:
    has_op, has_value = "op" in item, "value" in item
    if required and not (has_op and has_value):
        raise _err(where, "op와 value가 필요하다")
    if has_op != has_value:
        raise _err(where, "op와 value는 함께 써야 한다")
    if has_op:
        if item["op"] not in OPS:
            raise _err(f"{where}.op", f"허용: {sorted(OPS)}")
        _v_number(item["value"], f"{where}.value")


def _check_keys(item: Mapping[str, Any], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(item) - allowed)
    if unknown:
        raise _err(where, f"모르는 키 {unknown}")


def _validate_check(item: dict[str, Any], where: str) -> None:
    kind = item.get("kind")
    if kind not in CHECK_KINDS:
        raise _err(f"{where}.kind", f"허용: {sorted(CHECK_KINDS)}(받음: {kind!r})")
    _check_keys(item, CHECK_KEYS_BY_KIND[kind], where)
    _v_item_common(item, where)
    classes = frozenset(c.value for c in OrderClass)
    if kind == "integrity":
        present = [k for k in ("class", "sql", "check") if k in item]
        if len(present) != 1:
            raise _err(where, "integrity는 class|sql|check 중 정확히 하나가 필요하다")
        if "class" in item:
            _v_list_of(item["class"], classes, f"{where}.class", "클래스")
        if "sql" in item:
            _v_list_of(item["sql"], KNOWN_SQL, f"{where}.sql", "불변식")
        if "check" in item and item["check"] not in KNOWN_CHECKS:
            raise _err(f"{where}.check", f"허용: {sorted(KNOWN_CHECKS)}")
        _v_op_value(item, where, required=True)
    elif kind == "unresolved":
        if "class" not in item:
            raise _err(where, "unresolved는 class가 필요하다")
        _v_list_of(item["class"], classes, f"{where}.class", "클래스")
        _v_op_value(item, where, required=True)
    elif kind == "client":
        if item.get("stat") not in CLIENT_STATS:
            raise _err(f"{where}.stat", f"허용: {sorted(CLIENT_STATS)}")
        _v_op_value(item, where, required=item["stat"] != "recovery")
    elif kind == "k6":
        if item.get("check") not in K6_CHECKS:
            raise _err(f"{where}.check", f"허용: {sorted(K6_CHECKS)}")
        _v_op_value(item, where, required=False)
    elif kind == "prom":
        _v_str(item.get("query"), f"{where}.query")
        _v_op_value(item, where, required=True)


def _validate_guard(item: dict[str, Any], where: str) -> None:
    _check_keys(item, GUARD_KEYS, where)
    if item["id"] not in KNOWN_GUARDS:
        raise _err(where, f"모르는 가드 id(허용: {sorted(KNOWN_GUARDS)})")
    if item.get("kind", "guard") != "guard":
        raise _err(f"{where}.kind", "guards 항목의 kind는 guard다")
    _v_item_common(item, where)
    _v_op_value(item, where, required=False)


def _validate_fault_effect(item: dict[str, Any], where: str) -> None:
    _check_keys(item, FAULT_EFFECT_KEYS, where)
    _v_item_common(item, where)
    has_obs, has_sub = "observe" in item, "class_sub" in item
    if has_obs == has_sub:
        raise _err(where, "observe|class_sub 중 정확히 하나가 필요하다")
    if has_obs:
        if item["observe"] not in FAULT_OBSERVE:
            raise _err(f"{where}.observe", f"허용: {sorted(FAULT_OBSERVE)}")
        _check_keys(
            item, _COMMON_ITEM_KEYS | {"observe", "target", "within_s", "min"}, where
        )
        _v_str(item.get("target"), f"{where}.target")
        if "within_s" in item:
            _v_pos_int(item["within_s"], f"{where}.within_s")
        if "min" in item:
            _v_pos_int(item["min"], f"{where}.min")
    else:
        _check_keys(
            item, _COMMON_ITEM_KEYS | {"class_sub", "account", "op", "value"}, where
        )
        subs = frozenset(s.value for s in OkSub)
        _v_list_of(item["class_sub"], subs, f"{where}.class_sub", "OK 세부")
        if "account" in item and (
            not isinstance(item["account"], str)
            or not ACCOUNT_SEL_RE.fullmatch(item["account"])
        ):
            raise _err(f"{where}.account", "idx<n>|extra|any여야 한다")
        _v_op_value(item, where, required=True)


def _finalize(merged: dict[str, Any], where: str) -> dict[str, Any]:
    data = copy.deepcopy(merged)
    if data.get("schema_version") != SCHEMA_VERSION:
        raise _err(where, f"병합 결과에 schema_version: {SCHEMA_VERSION}이 없다")
    for name, (spec, required) in SECTION_SPECS.items():
        if name not in data:
            if required:
                raise _err(where, f"병합 결과에 필수 키 '{name}'가 없다")
            continue
        _reject_removed(data[name], name, where)
        _validate_map(data[name], spec, f"{where}: {name}")
    _fill_integrity_defaults(data["integrity"])
    for k, v in SAFETY_DEFAULTS.items():
        data["safety"].setdefault(k, v)
    rea = data.setdefault("require_extra_accounts", False)
    _v_bool(rea, f"{where}: require_extra_accounts")

    validators = {
        "checks": (_validate_check, "fail"),
        "guards": (_validate_guard, "inconclusive"),
        "fault_effect": (_validate_fault_effect, "inconclusive"),
    }
    ids: set[str] = set()
    for key, (validate, default_ov) in validators.items():
        items = data.setdefault(key, [])
        for item in items:
            iwhere = f"{where}: {key}[{item['id']}]"
            if item["id"] in ids:
                raise _err(
                    iwhere, "id는 checks·guards·fault_effect 전체에서 유일해야 한다"
                )
            ids.add(item["id"])
            validate(item, iwhere)
            item.setdefault("required", True)
            item.setdefault("on_violation", default_ov)
            if key == "guards":
                item.setdefault("kind", "guard")
    return data


def _reject_removed(sec: Any, name: str, where: str) -> None:
    if not isinstance(sec, dict):
        return
    old = sorted(set(sec) & set(REMOVED_KEYS.get(name, {})))
    if old:
        hint = ", ".join(f"{k} → {REMOVED_KEYS[name][k]}" for k in old)
        raise _err(f"{where}: {name}", f"옛 계약 키 {old}는 없앴다({hint}, 명세 9)")


def _fill_integrity_defaults(sec: dict[str, Any]) -> None:
    """initial_krw·stale_ms 기본값을 채운다. initial_krw는 문자열로 맞춘다(해시 안정)."""
    for k, v in INTEGRITY_DEFAULTS.items():
        sec.setdefault(k, v)
    sec["initial_krw"] = str(sec["initial_krw"]).strip()


def _plain(v: Any) -> Any:
    """YAML 직렬화용 순수 dict·list 사본."""
    if isinstance(v, Mapping):
        return {str(k): _plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    return v

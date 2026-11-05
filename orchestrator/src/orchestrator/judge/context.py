"""run.json 읽기와 검증(설계 1.4).

- run.json은 runner가 쓰고 judge는 읽기만 한다.
- 같은 폴더의 marks.local.json(`judge mark`가 쓴다)은 run.json의 빈 칸만 채운다.
- 위반은 모두 InputError(종료 코드 12)다.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .errors import InputError
from .model import RUN_SCHEMA, Fault, RunContext, SpeedPoint, sha256_bytes

__all__ = [
    "MARKS_LOCAL_NAME",
    "RUN_ID_RE",
    "RunContext",
    "Fault",
    "SpeedPoint",
    "load_run",
]

RUN_ID_RE = re.compile(r"^[a-z0-9-]{3,40}$")
MARK_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
MARKS_LOCAL_NAME = "marks.local.json"

# epoch ms 범위. 초 단위 실수를 막는다(2001-09-09 ~ 5138년).
EPOCH_MS_MIN = 10**12
EPOCH_MS_MAX = 10**14

REQUIRED_KEYS = ("schema", "run_id", "experiment", "criteria")
OPTIONAL_KEYS = (
    "scenario",
    "k6",
    "marks",
    "faults",
    "replay_speed",
    "extra_accounts",
    "obs",  # inject·운영자 관측값(5.2 4단계). `judge mark --obs`와 같은 모양
)
RUN_KEYS = frozenset(REQUIRED_KEYS + OPTIONAL_KEYS)
K6_KEYS = frozenset({"job"})
FAULT_KEYS = frozenset({"kind", "target", "start_ms", "end_ms"})  # 그 밖 키는 extra
MARKS_LOCAL_KEYS = frozenset(
    {"run_id", "marks", "faults", "replay_speed", "extra_accounts", "obs"}
)


def load_run(path: str | Path) -> RunContext:
    """run.json을 읽어 RunContext를 만든다(설계 1.4)."""
    run_json = Path(path)
    raw_bytes = _read_bytes(run_json)
    doc = _parse_object(raw_bytes, run_json)
    _check_keys(doc, RUN_KEYS, "run.json")
    for k in REQUIRED_KEYS:
        if k not in doc or doc[k] is None:
            raise InputError(f"run.json: 필수 키 '{k}'가 없다")

    if doc["schema"] != RUN_SCHEMA:
        raise InputError(
            f"run.json: schema는 '{RUN_SCHEMA}'여야 한다(받음: {doc['schema']!r})"
        )
    run_id = doc["run_id"]
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise InputError(
            f"run.json: run_id {run_id!r}가 ^[a-z0-9-]{{3,40}}$에 맞지 않다"
        )
    experiment = _nonempty_str(doc["experiment"], "experiment")
    run_dir = run_json.parent

    local_path = run_dir / MARKS_LOCAL_NAME
    local_sha: str | None = None
    filled: list[str] = []
    if local_path.exists():
        local_bytes = _read_bytes(local_path)
        local_sha = sha256_bytes(local_bytes)
        local = _parse_object(local_bytes, local_path)
        _check_keys(local, MARKS_LOCAL_KEYS, MARKS_LOCAL_NAME)
        if "run_id" in local and local["run_id"] != run_id:
            raise InputError(
                f"{MARKS_LOCAL_NAME}: run_id {local['run_id']!r}가 run.json({run_id})과 다르다"
            )
        doc = _fill_blanks(doc, local, filled)

    criteria = _resolve(run_dir, _nonempty_str(doc["criteria"], "criteria"))
    scenario_raw = doc.get("scenario")
    scenario = (
        None
        if scenario_raw is None
        else _resolve(run_dir, _nonempty_str(scenario_raw, "scenario"))
    )

    return RunContext(
        run_id=run_id,
        experiment=experiment,
        criteria=criteria,
        scenario=scenario,
        k6_job=_k6_job(doc.get("k6"), run_id),
        marks=_marks(doc.get("marks")),
        faults=_faults(doc.get("faults")),
        replay_speed=_replay_speed(doc.get("replay_speed")),
        extra_accounts=_extra_accounts(doc.get("extra_accounts")),
        obs=_obs(doc.get("obs")),
        run_json=run_json,
        run_dir=run_dir,
        run_json_sha256=sha256_bytes(raw_bytes),
        marks_local_sha256=local_sha,
        filled_from_marks_local=tuple(filled),
    )


# ---------------------------------------------------------------- 읽기


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as e:
        raise InputError(f"{path}: 읽을 수 없다({e.strerror or e})") from e


def _parse_object(data: bytes, path: Path) -> dict[str, Any]:
    try:
        doc = json.loads(data.decode("utf-8"), parse_float=Decimal)
    except (UnicodeDecodeError, ValueError) as e:
        raise InputError(f"{path}: JSON 형식 오류({e})") from e
    if not isinstance(doc, dict):
        raise InputError(f"{path}: 최상위는 JSON 객체여야 한다")
    return doc


def _check_keys(doc: Mapping[str, Any], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(doc) - allowed)
    if unknown:
        raise InputError(f"{where}: 모르는 키 {unknown}")


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, (list, dict, str)) and len(v) == 0)


def _fill_blanks(
    doc: dict[str, Any], local: Mapping[str, Any], filled: list[str]
) -> dict[str, Any]:
    """run.json 값을 우선하고 빈 칸만 marks.local.json으로 채운다(설계 1.4).

    marks·obs는 키 단위로, faults·replay_speed·extra_accounts는 통째로 채운다.
    """
    out = dict(doc)
    for name in ("marks", "obs"):
        if name not in local or local[name] is None:
            continue
        src = local[name]
        if not isinstance(src, dict):
            raise InputError(f"{MARKS_LOCAL_NAME}: {name}는 객체여야 한다")
        base = out.get(name)
        if base is None:
            base = {}
        if not isinstance(base, dict):
            raise InputError(f"run.json: {name}는 객체여야 한다")
        merged = dict(base)
        for k in sorted(src):
            if _is_blank(merged.get(k)) and not _is_blank(src[k]):
                merged[k] = src[k]
                filled.append(f"{name}.{k}")
        out[name] = merged
    for name in ("faults", "replay_speed", "extra_accounts"):
        if name in local and _is_blank(out.get(name)) and not _is_blank(local[name]):
            out[name] = local[name]
            filled.append(name)
    return out


# ---------------------------------------------------------------- 필드 검증


def _nonempty_str(v: Any, where: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise InputError(f"run.json: {where}는 비어 있지 않은 문자열이어야 한다")
    return v


def _resolve(base: Path, rel: str) -> Path:
    p = Path(rel)
    return p if p.is_absolute() else base / p


def _epoch_ms(v: Any, where: str) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise InputError(f"run.json: {where}는 정수 epoch ms여야 한다(받음: {v!r})")
    if not EPOCH_MS_MIN <= v < EPOCH_MS_MAX:
        raise InputError(
            f"run.json: {where}={v}는 epoch ms 범위가 아니다(초 단위인지 확인)"
        )
    return v


def _k6_job(v: Any, run_id: str) -> str:
    if v is None:
        return f"k6-{run_id}"
    if not isinstance(v, dict):
        raise InputError("run.json: k6는 객체여야 한다")
    _check_keys(v, K6_KEYS, "run.json k6")
    job = v.get("job")
    if job is None:
        return f"k6-{run_id}"
    return _nonempty_str(job, "k6.job")


def _marks(v: Any) -> dict[str, int]:
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise InputError("run.json: marks는 객체여야 한다")
    out: dict[str, int] = {}
    for name in sorted(v):
        if not isinstance(name, str) or not MARK_NAME_RE.fullmatch(name):
            raise InputError(f"run.json: marks 이름 {name!r}가 올바르지 않다")
        if v[name] is None:
            continue
        out[name] = _epoch_ms(v[name], f"marks.{name}")
    start, end = out.get("inject_start"), out.get("inject_end")
    if start is not None and end is not None and end < start:
        raise InputError("run.json: marks.inject_end가 inject_start보다 이르다")
    return out


def _faults(v: Any) -> tuple[Fault, ...]:
    if v is None:
        return ()
    if not isinstance(v, list):
        raise InputError("run.json: faults는 배열이어야 한다")
    out = []
    for i, f in enumerate(v):
        where = f"faults[{i}]"
        if not isinstance(f, dict):
            raise InputError(f"run.json: {where}는 객체여야 한다")
        for k in ("kind", "target", "start_ms"):
            if f.get(k) is None:
                raise InputError(f"run.json: {where}.{k}가 없다")
        end = f.get("end_ms")
        fault = Fault(
            kind=_nonempty_str(f["kind"], f"{where}.kind"),
            target=_nonempty_str(f["target"], f"{where}.target"),
            start_ms=_epoch_ms(f["start_ms"], f"{where}.start_ms"),
            end_ms=None if end is None else _epoch_ms(end, f"{where}.end_ms"),
            extra={k: f[k] for k in sorted(f) if k not in FAULT_KEYS},
        )
        if fault.end_ms is not None and fault.end_ms < fault.start_ms:
            raise InputError(f"run.json: {where}.end_ms가 start_ms보다 이르다")
        out.append(fault)
    return tuple(out)


def _replay_speed(v: Any) -> tuple[SpeedPoint, ...]:
    if v is None:
        return ()
    if not isinstance(v, list):
        raise InputError("run.json: replay_speed는 배열이어야 한다")
    out: list[SpeedPoint] = []
    for i, p in enumerate(v):
        where = f"replay_speed[{i}]"
        if not isinstance(p, dict) or set(p) != {"from_ms", "speed"}:
            raise InputError(f"run.json: {where}는 {{from_ms, speed}} 객체여야 한다")
        from_ms = _epoch_ms(p["from_ms"], f"{where}.from_ms")
        speed = p["speed"]
        if isinstance(speed, bool) or not isinstance(speed, (int, Decimal, str)):
            raise InputError(f"run.json: {where}.speed는 수여야 한다")
        try:
            sp = Decimal(str(speed))
        except InvalidOperation as e:
            raise InputError(f"run.json: {where}.speed {speed!r}는 수가 아니다") from e
        if not sp.is_finite() or sp <= 0:
            raise InputError(f"run.json: {where}.speed는 0보다 커야 한다")
        if out and from_ms <= out[-1].from_ms:
            raise InputError("run.json: replay_speed의 from_ms는 오름차순이어야 한다")
        out.append(SpeedPoint(from_ms=from_ms, speed=sp))
    return tuple(out)


def _extra_accounts(v: Any) -> tuple[str, ...]:
    if v is None:
        return ()
    if not isinstance(v, list):
        raise InputError("run.json: extra_accounts는 배열이어야 한다")
    out: list[str] = []
    for i, a in enumerate(v):
        try:
            norm = str(uuid.UUID(a)) if isinstance(a, str) else None
        except ValueError:
            norm = None
        if norm is None:
            raise InputError(f"run.json: extra_accounts[{i}] {a!r}는 UUID가 아니다")
        if norm not in out:
            out.append(norm)
    return tuple(out)


def _obs(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise InputError("run.json: obs는 객체여야 한다")
    return {k: v[k] for k in sorted(v)}

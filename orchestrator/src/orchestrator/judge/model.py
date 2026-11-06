"""judge 공용 데이터 모델(설계 4.6, 5.1–5.8, 계약 정렬 명세 1–8).

- 계약은 실제 주문 서비스(workloads/order, 커밋 6e8f38b)다. 옛 설계 명세의 Idempotency-Key 헤더,
  `/api/orders`, 주문 조회, fills·reconcile_runs 테이블, 수수료·cost_basis는 없다.
- 모든 dataclass는 frozen이다. Mapping 필드는 읽기 전용으로 다룬다(hash에서 뺀다).
- 시각은 k6 기록이 epoch ms(int), DB가 epoch µs(int)다. 금액·수량·가격은 Decimal(문자열에서 생성)이다.
- 판정 결과에 현재 시각·난수·dict 순서를 넣지 않는다. 직렬화는 canonical_json을 쓴다.
"""

from __future__ import annotations

import functools
import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any

RUN_SCHEMA = "chaos.run/v1"
VERDICT_SCHEMA = "papertrade.verdict/v1"
JSONL_VERSION = 2  # 명세 2. v1(lookup·resend·ack)을 대체한다

EXAMPLES_MAX = 20  # 설계 5.8 클래스·불변식별 예시 상한

# ---------------------------------------------------------------- 주문 서비스 계약(명세 1)

ORDER_PATH = "/orders"
ORDER_ID_FIELD = "order_id"  # ACK 판정 필드(명세 3)
REPLAYED_FIELD = "replayed"
STATUS_FILLED = "filled"
STATUS_REJECTED = "rejected"
ORDER_STATUSES = frozenset({"accepted", "pending", STATUS_FILLED, STATUS_REJECTED})
SIDE_BID = "bid"
SIDE_ASK = "ask"
ORDER_SIDES = frozenset({SIDE_BID, SIDE_ASK})
CLIENT_ORDER_ID_MAX = 64  # schemas.py OrderRequest

# 사용자 id 형식. 서비스는 1..64자 아무 문자열이나 받지만 judge 입력(run.json, CLI)은
# psql 변수·argv에 들어가므로 이 형식만 받는다(우리 설정값).
USER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$")


class RejectReason(StrEnum):
    """거절 본문 reason(명세 1). 값은 서비스 문자열 그대로다."""

    INVALID_REQUEST = "invalid_request"  # 400
    USER_NOT_FOUND = "user_not_found"  # 404
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"  # 409
    INSUFFICIENT_BALANCE = "insufficient_balance"  # 422
    INSUFFICIENT_VOLUME = "insufficient_volume"  # 422
    PRICE_UNAVAILABLE = "price_unavailable"  # 503, 트랜잭션 전
    STALE_PRICE = "stale_price"  # 503, 트랜잭션 전
    NOT_READY = "not_ready"  # 503, 종료 중 또는 연결 오류(커밋 뒤일 수 있다)


# 시도 성격(명세 3). 상수 집합으로만 판정한다.
ACK_STATUS = 200
# 확정 거절. 그 시도는 저장하지 않는다. 같은 키의 처리 중인 앞 시도는 나중에 커밋될 수 있다
# (앞 시도가 잠금을 쥔 동안 재전송이 기다렸다가 바뀐 잔고로 422를 받는 경우, app/orders.py)
REJECT_STATUSES = frozenset({400, 404, 409, 422})
REJECT_503_REASONS = frozenset(
    {RejectReason.PRICE_UNAVAILABLE.value, RejectReason.STALE_PRICE.value}
)
TRANSPORT_ERROR_STATUS = 0  # k6 전송 오류·타임아웃

# 기본값(근거: app/config.py, db/seed.sql)
DEFAULT_MARKETS = ("KRW-BTC",)
DEFAULT_USER_PATTERN = r"^user-[0-9]{4}$"
DEFAULT_USER_MAX = 1000  # seed user-0001..user-1000(명세 11)
_USER_NUM_RE = re.compile(r"([0-9]+)\Z")  # user_id 끝 숫자 묶음(user_max 범위 검사)
DEFAULT_INITIAL_KRW = "10000000"
DEFAULT_STALE_MS = 5000

# ---------------------------------------------------------------- DB 계약(명세 1, 6)

ACCOUNT_COLUMNS = ("user_id", "krw")
POSITION_COLUMNS = ("user_id", "market", "volume", "avg_price")
ORDER_COLUMNS = (
    "id",
    "client_order_id",
    "user_id",
    "market",
    "side",
    "volume",
    "status",
    "fill_price",
    "amount",
    "created_at",
    "filled_at",
)
TICK_COLUMNS = ("market", "price", "received_at")
FINGERPRINT_COLUMNS = (
    "orders",
    "max_order_id",
    "krw_sum",
    "positions",
    "volume_sum",
)
# 비교 규칙(명세 5): 숫자는 Decimal, 시각은 instant(µs), id는 int, 나머지는 문자열 그대로.
# 응답 시각은 응답에 적힌 소수 자릿수까지만 비교한다(명세 11, classify._same_instant)
ORDER_DECIMAL_COLUMNS = frozenset({"volume", "fill_price", "amount"})
ORDER_TIME_COLUMNS = frozenset({"created_at", "filled_at"})
ORDER_INT_COLUMNS = frozenset({"id"})
# 보낸 본문 대 DB 행 비교 필드(BODY_MISMATCH, 명세 4). 서비스 same_request와 같다
ORDER_BODY_FIELDS = ("user_id", "market", "side", "volume")
# intent.body 필드(명세 2)
INTENT_BODY_FIELDS = ("client_order_id", *ORDER_BODY_FIELDS)

# DB 스냅샷 구획 이름(psql `--@ <name>`, 명세 6). 불변식 SQL 구획은 "inv_<id>"다.
SNAPSHOT_SECTIONS = (
    "fingerprint1",
    "accounts",
    "positions",
    "orders",
    "ticks",
    "schema",
    "fingerprint2",
)
ORDERS_WARN_ROWS = 1_000_000  # 넘으면 경고만(SNAPSHOT_LARGE, 명세 6)

# 불변식 id(명세 7). 기준 YAML integrity 항목 sql: 목록 값이고 결과 id다.
# 키 이름 sql:은 옛 이름이다. "DB 스냅샷만으로 판정하는 불변식"이라는 뜻이고,
# 계산 위치(SQL 구획 inv_<id> 또는 스냅샷 Python)는 invariants 모듈이 정한다.
INVARIANTS = ("cash", "qty", "avg", "nonneg", "arith", "constraints")
# 스냅샷 밖 자료(ticks)와 대조하는 Python 검사. 기준 YAML check: 값이다.
INTEGRITY_CHECKS = ("fresh",)
INVARIANT_IDS = INVARIANTS + INTEGRITY_CHECKS

# 응답 필드 매핑 기본값(명세 5). 키 = 응답 JSON 필드, 값 = orders 열.
# 근거: workloads/order/app/schemas.py order_to_dict(커밋 6e8f38b)
DEFAULT_RESPONSE_FIELDS = {
    "order_id": "id",
    "client_order_id": "client_order_id",
    "user_id": "user_id",
    "market": "market",
    "side": "side",
    "volume": "volume",
    "status": "status",
    "fill_price": "fill_price",
    "amount": "amount",
    "filled_at": "filled_at",
}
# 분류 입력이지 비교 필드가 아니다(명세 5). response_fields 키로 쓸 수 없다.
NON_COMPARE_FIELDS = frozenset({REPLAYED_FIELD, "reason"})


def _mfield(**kw: Any) -> Any:
    """Mapping·Any 필드. hash에서 빼서 dataclass hash가 깨지지 않게 한다."""
    return field(hash=False, **kw)


def _mdefault() -> Any:
    return field(default_factory=dict, hash=False)


# ---------------------------------------------------------------- 값 변환


def dec(v: Any) -> Decimal | None:
    """수량·가격을 Decimal로 바꾼다. None·빈 문자열은 None.

    float는 받지 않는다(돈을 float로 다루지 않는다). JSON은 parse_float=Decimal로 읽는다.
    """
    if v is None or v == "":
        return None
    if isinstance(v, bool) or isinstance(v, float):
        raise TypeError(f"Decimal로 바꿀 수 없는 타입: {type(v).__name__}")
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v).strip())
    except InvalidOperation as e:
        raise ValueError(f"Decimal 형식이 아니다: {v!r}") from e


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_US = timedelta(microseconds=1)


def ts_us(v: Any) -> int | None:
    """시각 문자열(ISO 8601, 시간대 필수) → epoch µs. None·빈 문자열은 None.

    psql 출력("2026-11-06 01:02:03.123456+00")과 서비스 응답
    ("2026-11-06T01:02:03.123456+00:00")을 모두 읽는다. 시간대가 없거나 형식이 틀리면 ValueError.
    """
    if v is None or v == "":
        return None
    if not isinstance(v, str):
        raise ValueError(f"시각 문자열이 아니다: {type(v).__name__}")
    try:
        t = datetime.fromisoformat(v.strip())
    except ValueError as e:
        raise ValueError(f"시각 형식이 아니다: {v!r}") from e
    if t.tzinfo is None:
        raise ValueError(f"시간대가 없다: {v!r}")
    return (t - _EPOCH) // _US


# 날짜·시·분·초와 초의 소수부(ISO 8601 확장·기본 형식). 소수부 자릿수를 센다
_TS_FRAC_RE = re.compile(
    r"\s*[0-9]{4}-?[0-9]{2}-?[0-9]{2}.[0-9]{2}:?[0-9]{2}:?[0-9]{2}(?:[.,]([0-9]+))?"
)


def ts_digits(v: str) -> int | None:
    """시각 문자열의 초 소수 자릿수(명세 11). 초까지 적지 않은 형식이면 None.

    "…:40.005Z" → 3, "…:40.005123+00:00" → 6, "…:40+00:00" → 0(isoformat은 µs가 0이면 소수부를 뺀다).
    """
    m = _TS_FRAC_RE.match(v)
    if m is None:
        return None
    return len(m.group(1) or "")


def iso_us(us: int, sep: str = "T") -> str:
    """epoch µs → UTC ISO 8601(마이크로초 6자리, +00:00). ts_us의 역이다."""
    t = _EPOCH + timedelta(microseconds=us)
    return t.isoformat(sep=sep, timespec="microseconds")


def order_value(column: str, v: Any) -> Any:
    """orders 열 하나의 비교용 값(명세 5). 응답 값과 DB 값을 같은 함수로 바꿔 == 로 비교한다.

    ORDER_DECIMAL_COLUMNS → Decimal(표기 차이 무시), ORDER_TIME_COLUMNS → epoch µs
    (문자열은 ts_us, int는 이미 µs로 본다), id → int, 나머지 → 그대로. None은 None.
    바꿀 수 없으면 TypeError 또는 ValueError다(호출자는 불일치로 다룬다).
    """
    if v is None:
        return None
    if column in ORDER_DECIMAL_COLUMNS:
        return dec(v)
    if column in ORDER_TIME_COLUMNS:
        if isinstance(v, int) and not isinstance(v, bool):
            return v
        return ts_us(v)
    if column in ORDER_INT_COLUMNS:
        if isinstance(v, bool):
            raise TypeError("bool은 id가 아니다")
        if isinstance(v, int):
            return v
        if isinstance(v, str) and re.fullmatch(r"-?[0-9]+", v.strip()):
            return int(v)
        if isinstance(v, Decimal) and v.is_finite() and v == v.to_integral_value():
            return int(v)
        raise ValueError(f"정수 id가 아니다: {v!r}")
    return v


def _parse_raw(raw: str | None) -> Mapping[str, Any] | None:
    if not raw:
        return None
    try:
        v = json.loads(raw, parse_float=Decimal)
    except ValueError:
        return None
    return v if isinstance(v, dict) else None


# ---------------------------------------------------------------- run 키(명세 2)


def run_key_prefix(run_id: str) -> str:
    """client_order_id 접두어 `k6-<run_id>-`."""
    return f"k6-{run_id}-"


@functools.lru_cache(maxsize=64)
def run_key_re(run_id: str) -> re.Pattern[str]:
    """client_order_id 전체 형식 `k6-<run_id>-<vu>-<seq>`.

    접두어만 보면 r1117-crash가 r1117-crash-2의 키(k6-r1117-crash-2-…)까지 잡는다.
    끝의 두 숫자 묶음까지 맞춰야 run이 하나로 정해진다(ORPHAN·자기 검사에 쓴다).
    """
    return re.compile(rf"^k6-{re.escape(run_id)}-([0-9]+)-([0-9]+)$")


def is_run_key(key: Any, run_id: str) -> bool:
    return isinstance(key, str) and run_key_re(run_id).fullmatch(key) is not None


# ---------------------------------------------------------------- Enum


class Outcome(StrEnum):
    """기준·가드·verdict 결과(설계 5.7). NA는 requires 불일치."""

    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    NA = "N/A"


class AttemptKind(StrEnum):
    """시도 하나의 성격(명세 3)."""

    ACK = "ACK"  # 200이고 raw가 order_id 있는 JSON 객체
    # 확정 거절. 서버가 INSERT 전에 거절해 그 시도는 저장하지 않는다(같은 키의 처리 중인
    # 앞 시도는 나중에 커밋될 수 있다)
    REJECT = "REJECT"
    UNKNOWN = "UNKNOWN"  # 결과 불명(커밋됐을 수도 있다)
    PROTOCOL = "PROTOCOL"  # 그 밖(201, 200인데 본문이 주문이 아님, 405 등)


class KState(StrEnum):
    """키별 클라이언트 상태 K(명세 3)."""

    K_ACK = "K_ACK"  # ACK 시도가 하나라도 있다. 첫 ACK 본문이 원 응답이다
    K_REJECT = "K_REJECT"  # ACK 없음, 마지막 시도가 확정 거절
    K_UNKNOWN = "K_UNKNOWN"  # ACK 없음, 마지막 시도가 결과 불명
    INTERRUPTED = "INTERRUPTED"  # intent만 있고 result 없음


class OrderClass(StrEnum):
    """주문 분류 클래스(명세 4). 값은 기준 YAML class: 목록과 같다."""

    OK = "OK"
    MISMATCH = "MISMATCH"  # K_ACK × D1, 원 응답 필드 ≠ DB 행
    BODY_MISMATCH = "BODY_MISMATCH"  # D1 행의 본문 필드 ≠ intent.body(also로 붙는다)
    LOST = "LOST"  # K_ACK × D0
    REJECT_COMMITTED = (
        "REJECT_COMMITTED"  # K_REJECT × D1. 거절로 안내됐는데 돈이 움직였다
    )
    UNRESOLVED_COMMITTED = "UNRESOLVED_COMMITTED"  # K_UNKNOWN × D1
    UNRESOLVED_ABSENT = "UNRESOLVED_ABSENT"  # K_UNKNOWN × D0
    PROTOCOL = "PROTOCOL"  # 3절 "그 밖" 시도, intent 형식 위반
    DUP_ORDER = "DUP_ORDER"  # DB 전용: 같은 client_order_id 행 2개 이상
    FILL_STATE = (
        "FILL_STATE"  # DB 전용: filled인데 체결 열 NULL, 또는 filled가 아닌 run 주문
    )
    ORPHAN = "ORPHAN"  # DB 전용: run 키 형식인데 intent가 없다
    IDEMPOTENCY_VIOLATION = "IDEMPOTENCY_VIOLATION"  # IdemViolation이 하나라도 있다
    RESPONSE_SCHEMA_UNCONFIRMED = (
        "RESPONSE_SCHEMA_UNCONFIRMED"  # 매핑 미확정인데 응답≠DB
    )
    INTERRUPTED = "INTERRUPTED"


# 완료 기준 용어 대응(명세 4, 9 default.yaml)
LOSS_CLASSES = frozenset({OrderClass.LOST, OrderClass.REJECT_COMMITTED})
DUPLICATE_CLASSES = frozenset(
    {
        OrderClass.DUP_ORDER,
        OrderClass.FILL_STATE,
        OrderClass.IDEMPOTENCY_VIOLATION,
    }
)
MISMATCH_CLASSES = frozenset({OrderClass.MISMATCH, OrderClass.BODY_MISMATCH})
ORPHAN_CLASSES = frozenset({OrderClass.ORPHAN})
UNRESOLVED_CLASSES = frozenset(
    {OrderClass.UNRESOLVED_COMMITTED, OrderClass.UNRESOLVED_ABSENT}
)
USER_IMPACT_CLASSES = UNRESOLVED_CLASSES | {OrderClass.INTERRUPTED}
DB_ONLY_CLASSES = frozenset(
    {OrderClass.DUP_ORDER, OrderClass.FILL_STATE, OrderClass.ORPHAN}
)


class OkSub(StrEnum):
    """OK 세부(명세 4). 값은 기준 YAML class_sub: 목록과 같다."""

    CLEAN = "clean"  # 첫 시도가 ACK이고 rp=false
    AFTER_COMMIT = (
        "after_commit"  # 앞에 결과 불명 시도, 첫 ACK rp=true(앞 시도가 커밋됐다)
    )
    NOT_COMMITTED = "not_committed"  # 앞에 결과 불명·확정 거절 시도, 첫 ACK rp=false
    AFTER_503 = (
        "after_503"  # 앞에 503 not_ready, 첫 ACK rp=true. 경고 ROLLBACK_REPLY_COMMITTED
    )
    REJECTED = "rejected"  # K_REJECT × D0


class IdemViolation(StrEnum):
    """멱등 위반(명세 4). 하나라도 있으면 IDEMPOTENCY_VIOLATION으로 센다."""

    DOUBLE_CREATE = "DOUBLE_CREATE"  # rp=false ACK가 2번 이상이고 order_id가 다르다
    FIRST_REPLAYED = "FIRST_REPLAYED"  # n=1 ACK인데 rp=true(키 충돌·서버 결함)
    RESEND_DIFF = "RESEND_DIFF"  # 두 번째 이후 ACK의 의미 필드가 원 응답과 다르다
    SAME_BODY_409 = "409_SAME_BODY"  # 409를 받았는데 그 키의 전송 본문이 모두 같다


# RESEND_DIFF 비교 필드(명세 4). 응답 JSON 필드 이름이다.
RESEND_COMPARE_FIELDS = (
    ORDER_ID_FIELD,
    "user_id",
    "market",
    "side",
    "volume",
    "status",
    "fill_price",
    "amount",
)


# ---------------------------------------------------------------- k6 기록(JSONL v2, 명세 2)


def attempt_kind(st: int, ec: str | None, raw: str | None) -> AttemptKind:
    """시도 성격(명세 3). 상수 집합만 본다.

    - ACK: st == 200이고 raw가 JSON 객체이며 order_id 값이 있다.
    - REJECT: st ∈ {400, 404, 409, 422}, 또는 st == 503이고 ec ∈ {price_unavailable, stale_price}.
    - UNKNOWN: st == 0, 또는 5xx(503은 ec가 not_ready이거나 모르는 값·None).
    - PROTOCOL: 그 밖. 200인데 raw가 없거나 깨져 order_id를 확인하지 못한 것도 여기다.
    """
    if st == ACK_STATUS:
        body = _parse_raw(raw)
        if body is not None and body.get(ORDER_ID_FIELD) is not None:
            return AttemptKind.ACK
        return AttemptKind.PROTOCOL
    if st in REJECT_STATUSES:
        return AttemptKind.REJECT
    if st == 503 and ec in REJECT_503_REASONS:
        return AttemptKind.REJECT
    if st == TRANSPORT_ERROR_STATUS or 500 <= st <= 599:
        return AttemptKind.UNKNOWN
    return AttemptKind.PROTOCOL


@dataclass(frozen=True)
class Attempt:
    """같은 키로 보낸 전송 한 번. JSONL v2 atts[] 원소 = {n, t0, t1, st, ec, ms, rp, raw}.

    n: 1부터 시작하는 전송 번호(첫 전송과 재전송을 시간순으로 함께 센다).
    t0·t1: 요청 시작·끝 epoch ms. st: HTTP 상태(전송 오류·타임아웃은 0). ms: k6 timings.duration.
    ec: 거절 본문의 reason 또는 k6 error_code 문자열. 없으면 None. 숫자면 파서가 문자열로 바꾼다.
    rp: 200 본문의 replayed(bool). 그 밖 None.
    raw: 200·409·4xx·503 본문 텍스트(2048자 이하). 없으면 None.
    """

    n: int
    t0: int
    t1: int
    st: int
    ec: str | None
    ms: float
    rp: bool | None = None
    raw: str | None = None

    def json(self) -> Mapping[str, Any] | None:
        """raw를 JSON 객체로 읽는다(실수는 Decimal). 객체가 아니거나 깨지면 None."""
        return _parse_raw(self.raw)

    @property
    def kind(self) -> AttemptKind:
        return attempt_kind(self.st, self.ec, self.raw)

    @property
    def is_ack(self) -> bool:
        return self.kind is AttemptKind.ACK

    @property
    def is_reject(self) -> bool:
        return self.kind is AttemptKind.REJECT

    @property
    def is_unknown(self) -> bool:
        return self.kind is AttemptKind.UNKNOWN

    @property
    def is_protocol(self) -> bool:
        return self.kind is AttemptKind.PROTOCOL

    @property
    def is_not_ready(self) -> bool:
        """503 not_ready(종료 중 또는 연결 오류). 결과 불명이다(after_503 판정용)."""
        return self.st == 503 and self.ec == RejectReason.NOT_READY.value


@dataclass(frozen=True)
class Intent:
    """JSONL v2 intent = {type:"intent", key, acct, vu, t, body}. 보내기 전에 쓴다.

    key: client_order_id(`k6-<run_id>-<vu>-<seq>`). acct: user_id.
    body: 보낸 본문 {client_order_id, user_id, market, side, volume(10진 문자열)}.
          JSONL에서 body가 JSON 문자열이면 파서가 객체로 풀고 원문은 body_raw에 둔다.
    key == body.client_order_id, acct == body.user_id이어야 한다(consistent).
    """

    key: str
    acct: str
    vu: int
    t: int
    body: Mapping[str, Any] = _mfield()
    body_raw: str | None = None

    @property
    def consistent(self) -> bool:
        """key·acct가 본문과 맞으면 True. 아니면 PROTOCOL·안전 위반이다(명세 2)."""
        return (
            self.body.get("client_order_id") == self.key
            and self.body.get("user_id") == self.acct
        )


@dataclass(frozen=True)
class Result:
    """JSONL v2 result = {type:"result", key, atts}. 끝난 뒤 쓴다.

    atts: 같은 키로 보낸 모든 전송(첫 전송과 재전송, n 순서). 옛 lookup·ack·resend는 없다.
    """

    key: str
    atts: tuple[Attempt, ...]

    @property
    def acks(self) -> tuple[Attempt, ...]:
        return tuple(a for a in self.atts if a.is_ack)

    @property
    def first_ack(self) -> Attempt | None:
        """첫 ACK. 그 본문이 원 응답이다(명세 3)."""
        return next((a for a in self.atts if a.is_ack), None)

    @property
    def last(self) -> Attempt | None:
        return self.atts[-1] if self.atts else None

    @property
    def has_protocol(self) -> bool:
        return any(a.is_protocol for a in self.atts)


@dataclass(frozen=True)
class RunStart:
    """JSONL v2 run_start = {type:"run_start", v:2, run, script_ver, seed, profile, plan,
    symbols, initial_cash, conn_reuse, vus, t, ...}. v1 필드를 유지한다(명세 2).

    plan = {executor, time_unit_s, start_rate, stages:[{duration_s, target}]}.
    symbols: 주문 종목(market, 예: KRW-BTC). initial_cash: 참고용(판정은 integrity.initial_krw).
    conn_reuse: 남겨 두지만 판정에 쓰지 않는다(명세 2, 4).
    vus: VU 수(1 VU = 1 user). extra: 위에 없는 키.
    """

    v: int
    run: str
    script_ver: str
    seed: int
    profile: str
    plan: Mapping[str, Any] = _mfield()
    symbols: tuple[str, ...] = ()
    initial_cash: int | None = None
    conn_reuse: bool | None = None
    vus: int | None = None
    t: int = 0
    extra: Mapping[str, Any] = _mdefault()


@dataclass(frozen=True)
class AccountRec:
    """이번 run이 쓴 계좌(user_id). JSONL v2에는 account 줄이 없어 intent에서 만든다.

    idx: VU 오름차순 순번(0부터). 한 user를 여러 VU가 쓰면 가장 작은 VU로 정한다.
      기준 YAML fault_effect의 `account: idx<n>`이 이 idx다.
    vus: 이 user를 쓴 VU(정렬). 1 VU = 1 user면 원소가 하나다(safety 가드, 명세 8).
    """

    idx: int
    acct: str
    vus: tuple[int, ...]


def derive_accounts(intents: Iterable[Intent]) -> tuple[AccountRec, ...]:
    """intent들에서 계좌 목록을 만든다. 순서는 (가장 작은 VU, user_id)다."""
    vus: dict[str, set[int]] = {}
    for it in intents:
        vus.setdefault(it.acct, set()).add(it.vu)
    order = sorted(vus, key=lambda a: (min(vus[a]), a))
    return tuple(
        AccountRec(idx=i, acct=a, vus=tuple(sorted(vus[a])))
        for i, a in enumerate(order)
    )


@dataclass(frozen=True)
class K6Log:
    """orders.jsonl(v2) 파싱 결과(명세 2, 설계 5.3 기록 완전성).

    intents·results: client_order_id → 기록. 같은 키가 두 번 나오면 첫 줄을 쓰고 problems에 적는다.
    problems: 파싱 문제 설명(깨진 줄, run 값 여러 개, run_start 2개 등). 비면 문제 없음.
    line_count: 빈 줄을 뺀 전체 줄 수. kind_counts: type별 줄 수.
    has_token_key: 어느 줄이든 token·authorization 키가 있으면 True(safety 가드).
    sha256: 원본 파일(압축 해제 뒤) sha256.
    accounts·accounts_by_vu: intent에서 만든다(derive_accounts). 순회 순서는 파일 순서다.
    """

    run_id: str | None
    run_start: RunStart | None
    intents: Mapping[str, Intent] = _mfield()
    results: Mapping[str, Result] = _mfield()
    problems: tuple[str, ...] = ()
    line_count: int = 0
    has_token_key: bool = False
    sha256: str = ""
    kind_counts: Mapping[str, int] = _mdefault()

    @functools.cached_property
    def accounts(self) -> tuple[AccountRec, ...]:
        return derive_accounts(self.intents.values())

    @functools.cached_property
    def accounts_by_vu(self) -> Mapping[int, tuple[str, ...]]:
        """VU → 그 VU가 쓴 user_id(정렬). 원소가 2개 이상이면 1 VU = 1 user 위반이다."""
        out: dict[int, set[str]] = {}
        for it in self.intents.values():
            out.setdefault(it.vu, set()).add(it.acct)
        return {vu: tuple(sorted(out[vu])) for vu in sorted(out)}

    @property
    def account_ids(self) -> frozenset[str]:
        return frozenset(a.acct for a in self.accounts)

    def account_idx(self, acct: str) -> int | None:
        for a in self.accounts:
            if a.acct == acct:
                return a.idx
        return None

    def account_by_idx(self, idx: int) -> str | None:
        if 0 <= idx < len(self.accounts):
            return self.accounts[idx].acct
        return None


# ---------------------------------------------------------------- DB 스냅샷(명세 6)


@dataclass(frozen=True)
class DbAccount:
    """accounts 행(user_id, krw). krw는 NUMERIC(24,4)."""

    user_id: str
    krw: Decimal | None


@dataclass(frozen=True)
class DbPosition:
    """positions 행. 매도로 volume이 0이 돼도 행과 avg_price가 남는다."""

    user_id: str
    market: str
    volume: Decimal | None
    avg_price: Decimal | None


_ORDER_TIME_ATTR = {"created_at": "created_us", "filled_at": "filled_us"}


@dataclass(frozen=True)
class DbOrder:
    """orders 행(명세 6 열 전부). 시각은 epoch µs다(timestamptz는 µs 정밀도).

    지금 서비스는 status 'filled'만 넣는다. 체결 열(fill_price, amount, filled_us)은
    filled가 아니면 NULL일 수 있다.
    """

    id: int
    client_order_id: str
    user_id: str
    market: str | None
    side: str | None
    volume: Decimal | None
    status: str | None
    fill_price: Decimal | None
    amount: Decimal | None
    created_us: int | None
    filled_us: int | None

    def value(self, column: str) -> Any:
        """열 이름(ORDER_COLUMNS) → 비교용 값(order_value). 모르는 열이면 KeyError."""
        if column not in ORDER_COLUMNS:
            raise KeyError(column)
        attr = _ORDER_TIME_ATTR.get(column, column)
        return order_value(column, getattr(self, attr))

    @property
    def filled_ms(self) -> int | None:
        return None if self.filled_us is None else self.filled_us // 1000

    @property
    def created_ms(self) -> int | None:
        return None if self.created_us is None else self.created_us // 1000


@dataclass(frozen=True)
class DbTick:
    """ticks 행(run 종목, run 창 ± stale_ms 범위만). 수집기가 쓰며 비어 있을 수 있다."""

    market: str
    price: Decimal | None
    received_us: int | None


@dataclass(frozen=True)
class SchemaItem:
    """schema 구획 한 행(constraints 검사 입력, 명세 6–7).

    kind: "table" | "column" | "constraint".
    table: 테이블 이름. name: table이면 테이블 이름, column이면 열 이름, constraint면 제약 이름.
    detail: column이면 형식(예: "numeric(24,4) not null"),
            constraint면 pg_get_constraintdef 결과(예: "UNIQUE (client_order_id)"). 없으면 None.
    """

    kind: str
    table: str
    name: str
    detail: str | None = None


@dataclass(frozen=True)
class Fingerprint:
    """지문(명세 6). 스냅샷 트랜잭션 앞(F1)·뒤(F2)에 잰다. 빈 표의 max·sum은 None(SQL NULL).

    Decimal 비교라 "10.0"과 "10.00"은 같다.
    """

    orders: int
    max_order_id: int | None
    krw_sum: Decimal | None
    positions: int
    volume_sum: Decimal | None


def compute_fingerprint(
    accounts: Iterable[DbAccount],
    positions: Iterable[DbPosition],
    orders: Iterable[DbOrder],
) -> Fingerprint:
    """스냅샷 행에서 지문을 계산한다(SQL 지문과 같은 정의, 테스트·교차 확인용)."""
    accs, poss, ords = list(accounts), list(positions), list(orders)

    def total(vals: list[Decimal | None]) -> Decimal | None:
        xs = [v for v in vals if v is not None]
        return sum(xs, Decimal(0)) if xs else None

    return Fingerprint(
        orders=len(ords),
        max_order_id=max((o.id for o in ords), default=None),
        krw_sum=total([a.krw for a in accs]),
        positions=len(poss),
        volume_sum=total([p.volume for p in poss]),
    )


@dataclass(frozen=True)
class DbSnapshot:
    """psql 1회 결과(명세 6). accounts·positions·orders는 전체 행, ticks는 run 범위만이다.

    fingerprint1·2: 구획이 없으면 None.
    sections: 받은 구획 이름(SNAPSHOT_SECTIONS, "inv_<id>"). 빠진 구획의 기준만 DB_ERROR다.
    invariant_rows: SQL로 계산한 불변식 id(INVARIANTS) → 위반 행(열 이름 → 텍스트 값).
    errors: psql·파싱 오류 요약(구획 이름 포함). 비면 없음. 접속 정보·비밀번호를 넣지 않는다.
    행 순서: 파서가 정렬해 둔다(accounts·positions는 키, orders는 id, ticks는 시각).
    """

    accounts: tuple[DbAccount, ...] = ()
    positions: tuple[DbPosition, ...] = ()
    orders: tuple[DbOrder, ...] = ()
    ticks: tuple[DbTick, ...] = ()
    schema: tuple[SchemaItem, ...] = ()
    fingerprint1: Fingerprint | None = None
    fingerprint2: Fingerprint | None = None
    sections: frozenset[str] = frozenset()
    invariant_rows: Mapping[str, tuple[Mapping[str, str | None], ...]] = _mdefault()
    errors: tuple[str, ...] = ()

    @property
    def quiescent(self) -> bool:
        """F1 == F2이고 둘 다 있으면 True(db_quiescent 가드)."""
        return self.fingerprint1 is not None and self.fingerprint1 == self.fingerprint2

    def has_section(self, name: str) -> bool:
        return name in self.sections

    def account(self, user_id: str) -> DbAccount | None:
        for a in self.accounts:
            if a.user_id == user_id:
                return a
        return None

    def position(self, user_id: str, market: str) -> DbPosition | None:
        for p in self.positions:
            if p.user_id == user_id and p.market == market:
                return p
        return None

    def has_invariant(self, name: str) -> bool:
        return f"inv_{name}" in self.sections

    def invariant_violations(
        self, name: str
    ) -> tuple[Mapping[str, str | None], ...] | None:
        """SQL 불변식 위반 행. 구획이 없으면 None(SQL로 계산하지 않았거나 DB_ERROR)."""
        if not self.has_invariant(name):
            return None
        return tuple(self.invariant_rows.get(name, ()))

    def orders_by_key(self) -> dict[str, tuple[DbOrder, ...]]:
        """client_order_id → 주문들(id 순). 2건 이상이면 DUP_ORDER다."""
        out: dict[str, list[DbOrder]] = {}
        for o in self.orders:
            out.setdefault(o.client_order_id, []).append(o)
        return {k: tuple(sorted(v, key=lambda o: o.id)) for k, v in sorted(out.items())}

    def orders_by_user(self) -> dict[str, tuple[DbOrder, ...]]:
        """user_id → 주문들(id 순 = 사용자별 처리 순서, 명세 1)."""
        out: dict[str, list[DbOrder]] = {}
        for o in self.orders:
            out.setdefault(o.user_id, []).append(o)
        return {k: tuple(sorted(v, key=lambda o: o.id)) for k, v in sorted(out.items())}

    def run_orders(self, run_id: str) -> tuple[DbOrder, ...]:
        """client_order_id가 이 run 키 형식(run_key_re)인 주문(id 순)."""
        return tuple(
            sorted(
                (o for o in self.orders if is_run_key(o.client_order_id, run_id)),
                key=lambda o: o.id,
            )
        )


# ---------------------------------------------------------------- 불변식 결과(명세 7)


@dataclass(frozen=True)
class InvariantViolation:
    """불변식 위반 한 건(detail.examples 원소).

    invariant: 불변식 id(INVARIANT_IDS). subject: 대상(user_id, "user_id/market", client_order_id 등).
    expected·actual: 기대값·실제값(Decimal은 str). detail: 그 밖 근거(JSON 직렬화 가능).
    """

    invariant: str
    subject: str
    expected: str | None = None
    actual: str | None = None
    detail: Mapping[str, Any] = _mdefault()


@dataclass(frozen=True)
class InvariantResult:
    """불변식 하나의 평가(명세 7). invariants가 만들고 CheckResult로 바꾼다.

    id: 불변식 id(INVARIANT_IDS). source: "sql" | "python".
    violations: 위반 수. 평가하지 못하면 None(DB_ERROR, SCHEMA_DRIFT, ticks 없음 등).
    examples: 위반 예(subject 순, EXAMPLES_MAX개까지). reasons: 사유 코드(reasons.py).
    """

    id: str
    source: str
    violations: int | None
    examples: tuple[InvariantViolation, ...] = ()
    reasons: tuple[str, ...] = ()
    detail: Mapping[str, Any] = _mdefault()


# ---------------------------------------------------------------- 분류 결과(명세 4)


@dataclass(frozen=True)
class Classified:
    """주문(키) 하나의 분류 결과. classified.csv 한 줄이다.

    key: client_order_id. account_id: user_id(열 이름은 그대로 둔다).
    k_state: DB 전용 클래스(ORPHAN, DUP_ORDER 등 intent 없는 행)면 None.
    sub: cls가 OK일 때만 OkSub.
    idem: 멱등 위반. 비어 있지 않으면 cls와 별도로 IDEMPOTENCY_VIOLATION으로 센다.
    t: intent 시각(epoch ms). intent가 없으면 DB created 시각(ms). 구간 배정에 쓴다.
    order_ids: 이 키에 매칭된 DB orders.id(정렬).
    detail: 근거(불일치 필드, 상태 코드 등). 값은 JSON으로 직렬화 가능해야 한다.
    """

    key: str
    account_id: str
    k_state: KState | None
    cls: OrderClass
    sub: OkSub | None = None
    idem: tuple[IdemViolation, ...] = ()
    t: int | None = None
    order_ids: tuple[int, ...] = ()
    detail: Mapping[str, Any] = _mdefault()


# ---------------------------------------------------------------- 실행 맥락(설계 1.4)


@dataclass(frozen=True)
class Fault:
    """run.json faults[] 원소 = {kind, target, start_ms, end_ms?, ...}. 나머지 키는 extra."""

    kind: str
    target: str
    start_ms: int
    end_ms: int | None = None
    extra: Mapping[str, Any] = _mdefault()


@dataclass(frozen=True)
class SpeedPoint:
    """run.json replay_speed[] 원소 = {from_ms, speed}. 판정에 쓰지 않는다(명세 7, I7 삭제)."""

    from_ms: int
    speed: Decimal


@dataclass(frozen=True)
class DbTarget:
    """psql exec 대상(run.json db, 명세 6). 비밀번호는 받지 않는다(로컬 소켓 trust 전제).

    기본값은 로컬 개발 기본값이다. 클러스터 값은 확인 필요(앱·인프라).
    container가 None이면 `-c`를 붙이지 않는다.
    """

    pod: str = "postgres-0"
    container: str | None = None
    user: str = "exchange"
    database: str = "exchange"


@dataclass(frozen=True)
class RunContext:
    """run.json(+ marks.local.json 빈 칸 채움)을 검증한 결과. context.load_run이 만든다.

    criteria·scenario의 상대 경로는 run.json 위치 기준으로 푼다.
    marks: 이름 → epoch ms(값이 있는 것만). inject_start, inject_end 외 이름도 받는다.
    extra_accounts: 발표자 user_id(명세 8). db: psql 대상.
    filled_from_marks_local: marks.local.json에서 채운 항목(예: "marks.inject_end").
    """

    run_id: str
    experiment: str
    criteria: Path
    scenario: Path | None
    k6_job: str
    marks: Mapping[str, int] = _mdefault()
    faults: tuple[Fault, ...] = ()
    replay_speed: tuple[SpeedPoint, ...] = ()
    extra_accounts: tuple[str, ...] = ()
    obs: Mapping[str, Any] = _mdefault()
    db: DbTarget = DbTarget()
    run_json: Path | None = None
    run_dir: Path | None = None
    run_json_sha256: str = ""
    marks_local_sha256: str | None = None
    filled_from_marks_local: tuple[str, ...] = ()
    schema: str = RUN_SCHEMA

    @property
    def inject_start(self) -> int | None:
        return self.marks.get("inject_start")

    @property
    def inject_end(self) -> int | None:
        return self.marks.get("inject_end")

    @property
    def key_prefix(self) -> str:
        return run_key_prefix(self.run_id)


# ---------------------------------------------------------------- 기준·결과(설계 5.6–5.8, 6)


@dataclass(frozen=True)
class Criteria:
    """병합·검증된 판정 기준(설계 6). criteria.load_criteria가 만든다.

    data: 병합 결과(dict). checks·guards·fault_effect는 늘 list다. 바꾸지 않는다.
    text: criteria.resolved.yaml 내용(정렬된 키). sha256 = sha256(text UTF-8).
    chain: extends 사슬 파일 이름(뿌리 → 잎). 해시에 들어가지 않는다.
    disabled: disable로 끈 id(정렬). 정보용이다. resolved에 남지 않아 판정에 쓰지 않는다(설계 6).
    """

    data: Mapping[str, Any] = _mfield()
    text: str = ""
    sha256: str = ""
    source: Path | None = None
    chain: tuple[str, ...] = ()
    disabled: tuple[str, ...] = ()

    @property
    def checks(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self.data.get("checks", ()))

    @property
    def guards(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self.data.get("guards", ()))

    @property
    def fault_effect(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self.data.get("fault_effect", ()))

    def item(self, item_id: str) -> Mapping[str, Any] | None:
        """checks·guards·fault_effect에서 id로 찾는다(id는 셋 전체에서 유일)."""
        for group in (self.checks, self.guards, self.fault_effect):
            for it in group:
                if it.get("id") == item_id:
                    return it
        return None


@dataclass(frozen=True)
class IntegrityCfg:
    """분류기·불변식 설정(기준 YAML integrity, 명세 5, 7).

    response_fields: 응답 JSON 필드 → orders 열(DEFAULT_RESPONSE_FIELDS). 원 응답 대 DB 비교에 쓴다.
    response_fields_confirmed: false면 응답 대 DB 불일치는 RESPONSE_SCHEMA_UNCONFIRMED.
    initial_krw: 계좌 시작 잔고(cash 불변식). stale_ms: 서비스 STALE_MS(fresh 검사 창).
    """

    response_fields: Mapping[str, str] = field(
        default_factory=lambda: dict(DEFAULT_RESPONSE_FIELDS), hash=False
    )
    response_fields_confirmed: bool = True
    initial_krw: Decimal = Decimal(DEFAULT_INITIAL_KRW)
    stale_ms: int = DEFAULT_STALE_MS


@functools.lru_cache(maxsize=32)
def _compiled(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern)


@dataclass(frozen=True)
class SafetyCfg:
    """안전장치 설정(기준 YAML safety, 명세 8). criteria.safety_cfg가 만든다.

    markets: 주문해도 되는 종목. user_pattern: user_id 정규식(전체 일치로 본다).
    user_max: user_id 끝 숫자 묶음의 범위 1..user_max(명세 11, seed user-0001..user-1000).
      끝이 숫자가 아니면 범위를 확인할 수 없어 위반으로 본다.
    """

    markets: tuple[str, ...] = DEFAULT_MARKETS
    user_pattern: str = DEFAULT_USER_PATTERN
    user_max: int = DEFAULT_USER_MAX

    def market_ok(self, market: Any) -> bool:
        return isinstance(market, str) and market in self.markets

    def user_ok(self, user_id: Any) -> bool:
        if not isinstance(user_id, str):
            return False
        if _compiled(self.user_pattern).fullmatch(user_id) is None:
            return False
        m = _USER_NUM_RE.search(user_id)
        return m is not None and 1 <= int(m.group(1)) <= self.user_max


@dataclass(frozen=True)
class CheckResult:
    """기준 또는 가드 하나의 결과(설계 5.6, 5.7).

    kind: integrity | unresolved | client | prom | k6 | guard | fault_effect.
    on_violation: fail | inconclusive. value: 측정값(JSON 직렬화 가능).
    reasons: 대문자 사유 코드(reasons.py).
    """

    id: str
    kind: str
    outcome: Outcome
    required: bool = True
    on_violation: str = "fail"
    value: Any = _mfield(default=None)
    reasons: tuple[str, ...] = ()
    detail: Mapping[str, Any] = _mdefault()


@dataclass(frozen=True, kw_only=True)
class Verdict:
    """verdict.json v1(설계 5.8). to_dict()가 그대로 JSON이 된다.

    integrity = {intents, log_complete, classes, subs, invariants, examples}.
    inputs = {criteria_sha256, run_json_sha256, evidence_manifest_sha256, judge_version,
              response_fields_confirmed}.
    verdict_digest: generated_at·run_info·verdict_digest를 뺀 canonical JSON의 sha256.
    generated_at: epoch ms(digest에 들어가지 않는다).
    """

    schema: str = VERDICT_SCHEMA
    run_id: str
    experiment: str
    verdict: Outcome
    reasons: tuple[str, ...] = ()
    checks: tuple[CheckResult, ...] = ()
    guards: tuple[CheckResult, ...] = ()
    integrity: Mapping[str, Any] = _mdefault()
    client: Mapping[str, Any] = _mdefault()
    load: Mapping[str, Any] = _mdefault()
    inputs: Mapping[str, Any] = _mdefault()
    warnings: tuple[str, ...] = ()
    verdict_digest: str = ""
    run_info: Mapping[str, Any] = _mdefault()
    generated_at: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)

    def compute_digest(self) -> str:
        d = self.to_dict()
        for k in DIGEST_EXCLUDED:
            d.pop(k, None)
        return sha256_text(canonical_json(d))

    def with_digest(self) -> Verdict:
        return replace(self, verdict_digest=self.compute_digest())


DIGEST_EXCLUDED = ("generated_at", "run_info", "verdict_digest")


# ---------------------------------------------------------------- 직렬화


def to_jsonable(obj: Any) -> Any:
    """dataclass·Enum·Decimal·tuple·set을 JSON 값으로 바꾼다.

    Enum → value, Decimal → str, tuple → list, set → 정렬된 list, Path → str.
    """
    if is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: to_jsonable(getattr(obj, f.name)) for f in fields(obj)}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Mapping):
        return {str(to_jsonable(k)): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, (set, frozenset)):
        items = [to_jsonable(v) for v in obj]
        return sorted(items, key=lambda v: json.dumps(v, sort_keys=True))
    if isinstance(obj, Path):
        return str(obj)
    return obj


def canonical_json(obj: Any) -> str:
    """정렬된 키, 공백 없는 JSON. NaN·Inf는 거부한다."""
    return json.dumps(
        to_jsonable(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

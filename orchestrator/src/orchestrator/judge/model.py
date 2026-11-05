"""judge 공용 데이터 모델(설계 4.6, 5.1–5.8).

- 모든 dataclass는 frozen이다. Mapping 필드는 읽기 전용으로 다룬다(hash에서 뺀다).
- 시각은 epoch ms(int), 금액은 원 단위 int, 수량·가격은 Decimal(문자열에서 생성)이다.
- 판정 결과에 현재 시각·난수·dict 순서를 넣지 않는다. 직렬화는 canonical_json을 쓴다.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, is_dataclass, replace
from decimal import Decimal, InvalidOperation
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any

RUN_SCHEMA = "chaos.run/v1"
VERDICT_SCHEMA = "papertrade.verdict/v1"
JSONL_VERSION = 1

ACK_STATUSES = frozenset({200, 201})  # 4.4 ACK
RETRY_STATUSES = frozenset({0, 502, 503, 504})  # 4.4 같은 키로 재시도
UNKNOWN_STATUSES = frozenset({0, 500, 502, 503, 504})  # 결과 불명 또는 롤백

# DB 스냅샷 구획 이름(psql `\qecho '--@ <name>'`, 설계 5.4)
SNAPSHOT_SECTIONS = (
    "fingerprint1",
    "accounts",
    "orders",
    "fills",
    "positions",
    "fingerprint2",
)
# 불변식 SQL 이름. 파일은 sql/inv_<name>.sql, 구획은 inv_<name>, 기준 YAML sql: 목록은 <name>.
INVARIANT_SQL = (
    "i2",
    "nulls",
    "constraints",
    "i4",
    "i5_cash",
    "i5_qty",
    "i5_flat",
    "i6_db",
    "i8",
    "arith",
    "privileges",
)
OPTIONAL_INVARIANT_SQL = frozenset({"arith", "privileges"})  # 5.4 선택 검사
# 응답 필드 매핑 기본값(설계 6 integrity.response_fields, 확인 필요: 앱 팀)
DEFAULT_RESPONSE_FIELDS = {
    "status": "status",
    "reject_reason": "reason",
    "price": "price",
    "fee": "fee",
}


def _mfield(**kw: Any) -> Any:
    """Mapping·Any 필드. hash에서 빼서 dataclass hash가 깨지지 않게 한다."""
    return field(hash=False, **kw)


def _mdefault() -> Any:
    return field(default_factory=dict, hash=False)


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


# ---------------------------------------------------------------- Enum


class Outcome(StrEnum):
    """기준·가드·verdict 결과(설계 5.7). NA는 requires 불일치."""

    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    NA = "N/A"


class KState(StrEnum):
    """1단계 클라이언트 상태 K(설계 5.3)."""

    K_ACK = "K_ACK"  # 원 시도 중 200 또는 201
    K_FOUND = "K_FOUND"  # ACK 없음, 조회 200
    K_404 = "K_404"  # ACK 없음, 조회 404
    K_UNKNOWN = "K_UNKNOWN"  # ACK 없음, 조회 실패 또는 없음
    K_4XX = "K_4XX"  # 마지막 원 시도가 4xx
    INTERRUPTED = "INTERRUPTED"  # intent만 있고 result 없음


class OrderClass(StrEnum):
    """주문 분류 클래스(설계 5.3). 값은 기준 YAML class: 목록과 같다."""

    OK = "OK"
    MISMATCH = "MISMATCH"
    BODY_MISMATCH = "BODY_MISMATCH"
    LOST = "LOST"
    PHANTOM_ACK = "PHANTOM_ACK"
    FALSE_NOT_FOUND = "FALSE_NOT_FOUND"
    NOT_ENTERED = "NOT_ENTERED"
    UNRESOLVED_COMMITTED = "UNRESOLVED_COMMITTED"
    UNRESOLVED_ABSENT = "UNRESOLVED_ABSENT"
    PROTOCOL = "PROTOCOL"
    DUP_ORDER = "DUP_ORDER"
    DUP_FILL = "DUP_FILL"
    FILL_STATE = "FILL_STATE"
    ORPHAN = "ORPHAN"
    IDEMPOTENCY_VIOLATION = "IDEMPOTENCY_VIOLATION"
    RESPONSE_SCHEMA_UNCONFIRMED = "RESPONSE_SCHEMA_UNCONFIRMED"
    INTERRUPTED = "INTERRUPTED"


# 완료 기준 용어 대응(설계 5.3)
LOSS_CLASSES = frozenset(
    {OrderClass.LOST, OrderClass.PHANTOM_ACK, OrderClass.FALSE_NOT_FOUND}
)
DUPLICATE_CLASSES = frozenset(
    {
        OrderClass.DUP_ORDER,
        OrderClass.DUP_FILL,
        OrderClass.FILL_STATE,
        OrderClass.IDEMPOTENCY_VIOLATION,
    }
)
MISMATCH_CLASSES = frozenset({OrderClass.MISMATCH, OrderClass.BODY_MISMATCH})
ORPHAN_CLASSES = frozenset({OrderClass.ORPHAN})
USER_IMPACT_CLASSES = frozenset(
    {
        OrderClass.UNRESOLVED_COMMITTED,
        OrderClass.UNRESOLVED_ABSENT,
        OrderClass.NOT_ENTERED,
        OrderClass.INTERRUPTED,
    }
)


class OkSub(StrEnum):
    """OK 세부(설계 5.3). 값은 기준 YAML class_sub: 목록과 같다."""

    CLEAN = "clean"  # 첫 시도 201
    AFTER_COMMIT = "after_commit"  # 0·502·504 뒤 200(crash 효과)
    NOT_COMMITTED = "not_committed"  # 결과 불명 뒤 201
    LOOKUP = "lookup"  # 조회로 확인(K_FOUND)
    AFTER_503 = "after_503"  # 503 뒤 200. 경고 ROLLBACK_REPLY_COMMITTED


class IdemViolation(StrEnum):
    """2단계 시도 이상(설계 5.3). 하나라도 있으면 IDEMPOTENCY_VIOLATION."""

    DOUBLE_CREATE = "DOUBLE_CREATE"  # 한 키에 201이 2번 이상
    FIRST_200 = "FIRST_200"  # 새 키의 첫 시도가 200
    RESEND_DIFF = "RESEND_DIFF"  # 재송신 200의 의미 필드가 다름
    SAME_BODY_409 = "409_SAME_BODY"  # 같은 바이트인데 409


class ResendOutcome(StrEnum):
    """재송신 결과(설계 4.4)."""

    OK = "OK"
    DOUBLE_CREATE = "DOUBLE_CREATE"
    RESEND_DIFF = "RESEND_DIFF"
    RESEND_UNRESOLVED = "RESEND_UNRESOLVED"  # 끝까지 2xx 없음. 보고만 한다


# ---------------------------------------------------------------- k6 기록(JSONL v1, 설계 4.6)


@dataclass(frozen=True)
class Attempt:
    """HTTP 시도 1회. JSONL v1 atts[] 원소 = {n, t0, t1, st, ec, ms}.

    n: 1부터 시작하는 시도 번호. t0·t1: 요청 시작·끝 epoch ms.
    st: HTTP 상태(전송 오류·타임아웃은 0). ms: k6 timings.duration(ms).
    ec: 오류 코드 문자열. 전송 오류면 k6 error_code, HTTP 오류면 응답 본문 code
        (예: "LEDGER_BUSY", "IDEMPOTENCY_KEY_REUSED"), 없으면 None.
        k6가 숫자로 내면 파서가 문자열로 바꾼다.
    """

    n: int
    t0: int
    t1: int
    st: int
    ec: str | None
    ms: float

    @property
    def is_ack(self) -> bool:
        return self.st in ACK_STATUSES

    @property
    def is_4xx(self) -> bool:
        return 400 <= self.st < 500


def _parse_raw(raw: str | None) -> Mapping[str, Any] | None:
    if not raw:
        return None
    try:
        v = json.loads(raw, parse_float=Decimal)
    except ValueError:
        return None
    return v if isinstance(v, dict) else None


@dataclass(frozen=True)
class Ack:
    """응답 원문. JSONL v1 ack = {st, raw} | null.

    raw는 응답 본문 텍스트(≤2KB, 잘리면 JSON이 아닐 수 있다).
    """

    st: int
    raw: str | None

    def json(self) -> Mapping[str, Any] | None:
        """raw를 JSON 객체로 읽는다(실수는 Decimal). 객체가 아니거나 깨지면 None."""
        return _parse_raw(self.raw)


@dataclass(frozen=True)
class Lookup:
    """멱등키 조회. JSONL v1 lookup = {atts:[Attempt...], st, raw} | null.

    atts: GET /api/orders?idem_key= 시도들(1s 간격 최대 3회).
    st: 마지막 조회 시도의 상태(200 → K_FOUND, 404 → K_404, 그 밖 → K_UNKNOWN).
    raw: 마지막 조회 응답 본문(≤2KB) 또는 None.
    """

    atts: tuple[Attempt, ...]
    st: int
    raw: str | None

    def json(self) -> Mapping[str, Any] | None:
        return _parse_raw(self.raw)


@dataclass(frozen=True)
class Resend:
    """재송신. JSONL v1 resend = {atts:[Attempt...], ack:{st, raw}|null} | null.

    ack는 재송신 시도 중 첫 2xx 응답이다. 끝까지 2xx가 없으면 null(RESEND_UNRESOLVED).
    """

    atts: tuple[Attempt, ...]
    ack: Ack | None


@dataclass(frozen=True)
class Intent:
    """JSONL v1 intent = {kind:"intent", run, key, acct, vu, t, body}. 보내기 전에 쓴다.

    body: 보낸 본문 {symbol, side, qty, limit_price}. qty·limit_price는 10진 문자열.
          JSONL에서 body가 JSON 문자열이면 파서가 객체로 풀고 원문은 body_raw에 둔다.
    """

    key: str
    acct: str
    vu: int
    t: int
    body: Mapping[str, Any] = _mfield()
    body_raw: str | None = None


@dataclass(frozen=True)
class Result:
    """JSONL v1 result = {kind:"result", run, key, atts, lookup, ack, resend, t}. 끝난 뒤 쓴다.

    ack: 원 시도 중 첫 200·201 응답(없으면 None).
    """

    key: str
    atts: tuple[Attempt, ...]
    lookup: Lookup | None
    ack: Ack | None
    resend: Resend | None
    t: int


@dataclass(frozen=True)
class RunStart:
    """JSONL v1 run_start = {kind:"run_start", v, run, script_ver, seed, profile, plan,
    symbols, initial_cash, conn_reuse, t, ...}.

    plan(JSONL v1) = {executor:"ramping-arrival-rate", time_unit_s, start_rate,
                      stages:[{duration_s, target}]}. target은 time_unit_s당 도착 수다.
    conn_reuse: k6 noConnectionReuse의 반대. false가 아니면(None 포함) FIRST_200을 낮춘다(4.4).
    extra: 위에 없는 키(예: ws_reconnect).
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
    t: int = 0
    extra: Mapping[str, Any] = _mdefault()


@dataclass(frozen=True)
class AccountRec:
    """JSONL v1 account = {kind:"account", run, idx, acct, nick}. 토큰은 없다."""

    idx: int
    acct: str
    nick: str


@dataclass(frozen=True)
class SetupDone:
    """JSONL v1 setup_done = {kind:"setup_done", run, accounts, t}. accounts는 계좌 수."""

    accounts: int
    t: int


@dataclass(frozen=True)
class K6Log:
    """orders.jsonl 파싱 결과(설계 4.6, 5.3 기록 완전성).

    intents·results: 멱등키 → 기록. 같은 키가 두 번 나오면 첫 줄을 쓰고 problems에 적는다.
    problems: 파싱 문제 설명(깨진 줄, run 값 여러 개, run_start 2개 등). 비면 문제 없음.
    line_count: 빈 줄을 뺀 전체 줄 수. kind_counts: kind별 줄 수.
    has_token_key: 어느 줄이든 "token" 키가 있으면 True(safety 가드).
    sha256: 원본 파일(압축 해제 뒤) sha256.
    순회 순서는 파일 순서다. 판정에 쓸 때는 키로 정렬한다.
    """

    run_id: str | None
    run_start: RunStart | None
    accounts: tuple[AccountRec, ...]
    setup_done: SetupDone | None
    intents: Mapping[str, Intent] = _mfield()
    results: Mapping[str, Result] = _mfield()
    problems: tuple[str, ...] = ()
    line_count: int = 0
    has_token_key: bool = False
    sha256: str = ""
    kind_counts: Mapping[str, int] = _mdefault()

    @property
    def account_ids(self) -> frozenset[str]:
        return frozenset(a.acct for a in self.accounts)

    def account_idx(self, acct: str) -> int | None:
        for a in self.accounts:
            if a.acct == acct:
                return a.idx
        return None

    def account_by_idx(self, idx: int) -> str | None:
        for a in self.accounts:
            if a.idx == idx:
                return a.acct
        return None


# ---------------------------------------------------------------- DB 스냅샷(설계 5.4, 명세 §5)


@dataclass(frozen=True)
class DbAccount:
    """accounts 행. token_hash는 수집하지 않는다(7.4).

    명세 스키마에 NOT NULL이 없어 NULL 가능 열은 Optional이다.
    k6: 이번 run의 k6 계좌면 True(JSONL account 또는 nickname 정규식). extra 계좌는 False.
    """

    id: str
    nickname: str | None
    mode: str | None
    initial_cash: int | None
    cash: int | None
    version: int | None
    k6: bool = False
    created_ms: int | None = None


@dataclass(frozen=True)
class DbOrder:
    """orders 행(명세 §5 열 전부). received_at은 원문, received_ms는 epoch ms."""

    id: str
    account_id: str
    idem_key: str
    body_hash: str | None
    symbol: str | None
    side: str | None
    qty: Decimal | None
    limit_price: Decimal | None
    status: str | None
    reject_reason: str | None
    ref_source: str | None
    ref_seq: str | None
    ref_event_ts: int | None
    ref_recv_ts: int | None
    ref_price: Decimal | None
    ref_age_ms: int | None
    received_at: str | None
    received_ms: int | None


@dataclass(frozen=True)
class DbFill:
    """fills 행(명세 §5 열 전부). amount·fee·realized_pnl은 원 단위 정수."""

    id: int
    order_id: str
    account_id: str | None
    symbol: str | None
    side: str | None
    qty: Decimal | None
    price: Decimal | None
    amount: int | None
    fee: int | None
    fee_rate: Decimal | None
    realized_pnl: int | None
    created_at: str | None = None
    created_ms: int | None = None


@dataclass(frozen=True)
class DbPosition:
    """positions 행."""

    account_id: str
    symbol: str
    qty: Decimal | None
    cost_basis: int | None


Fingerprint = tuple[
    int, int, int
]  # (count(orders), count(fills), sum(accounts.version))


@dataclass(frozen=True)
class DbSnapshot:
    """psql 1회 결과(설계 5.4). 대상 계좌(ra) 전체의 행이다.

    fingerprint1·2: 구획이 없으면 None.
    sections: 받은 구획 이름(SNAPSHOT_SECTIONS, "inv_<name>"). 빠진 구획의 기준만 DB_ERROR다.
    invariant_rows: 불변식 이름(INVARIANT_SQL, 접두어 없음) → 위반 행(열 이름 → 텍스트 값).
    """

    accounts: tuple[DbAccount, ...]
    orders: tuple[DbOrder, ...]
    fills: tuple[DbFill, ...]
    positions: tuple[DbPosition, ...]
    fingerprint1: Fingerprint | None
    fingerprint2: Fingerprint | None
    sections: frozenset[str] = frozenset()
    invariant_rows: Mapping[str, tuple[Mapping[str, str], ...]] = _mdefault()

    @property
    def quiescent(self) -> bool:
        """F1 == F2이고 둘 다 있으면 True(db_quiescent 가드)."""
        return self.fingerprint1 is not None and self.fingerprint1 == self.fingerprint2

    @property
    def k6_account_ids(self) -> frozenset[str]:
        return frozenset(a.id for a in self.accounts if a.k6)

    def account(self, account_id: str) -> DbAccount | None:
        for a in self.accounts:
            if a.id == account_id:
                return a
        return None

    def has_invariant(self, name: str) -> bool:
        return f"inv_{name}" in self.sections

    def invariant_violations(self, name: str) -> tuple[Mapping[str, str], ...] | None:
        """위반 행. 구획이 없으면 None(DB_ERROR), 빈 tuple이면 PASS."""
        if not self.has_invariant(name):
            return None
        return tuple(self.invariant_rows.get(name, ()))

    def orders_by_key(self) -> dict[tuple[str, str], tuple[DbOrder, ...]]:
        """(account_id, idem_key) → 주문들(id 순). 2건 이상이면 DUP_ORDER다."""
        out: dict[tuple[str, str], list[DbOrder]] = {}
        for o in self.orders:
            out.setdefault((o.account_id, o.idem_key), []).append(o)
        return {k: tuple(sorted(v, key=lambda o: o.id)) for k, v in sorted(out.items())}

    def fills_by_order(self) -> dict[str, tuple[DbFill, ...]]:
        """order_id → 체결들(id 순)."""
        out: dict[str, list[DbFill]] = {}
        for f in self.fills:
            out.setdefault(f.order_id, []).append(f)
        return {k: tuple(sorted(v, key=lambda f: f.id)) for k, v in sorted(out.items())}


# ---------------------------------------------------------------- 분류 결과(설계 5.3)


@dataclass(frozen=True)
class Classified:
    """주문(키) 하나의 분류 결과. classified.csv 한 줄이다.

    k_state: DB 전용 클래스(ORPHAN, DUP_* 등 intent 없는 행)면 None.
    sub: cls가 OK일 때만 OkSub.
    idem: 2단계 시도 이상. 비어 있지 않으면 cls와 별도로 IDEMPOTENCY_VIOLATION으로 센다.
    resend: 재송신이 있었으면 그 결과.
    t: intent 시각(epoch ms). intent가 없으면 DB received_ms. 구간 배정에 쓴다.
    order_ids: 이 키에 매칭된 DB 주문 id(정렬).
    detail: 근거(불일치 필드, 상태 코드 등). 값은 JSON으로 직렬화 가능해야 한다.
    """

    key: str
    account_id: str
    k_state: KState | None
    cls: OrderClass
    sub: OkSub | None = None
    idem: tuple[IdemViolation, ...] = ()
    resend: ResendOutcome | None = None
    t: int | None = None
    order_ids: tuple[str, ...] = ()
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
    """run.json replay_speed[] 원소 = {from_ms, speed}. from_ms부터 배속 speed."""

    from_ms: int
    speed: Decimal


@dataclass(frozen=True)
class RunContext:
    """run.json(+ marks.local.json 빈 칸 채움)을 검증한 결과. context.load_run이 만든다.

    criteria·scenario의 상대 경로는 run.json 위치 기준으로 푼다.
    marks: 이름 → epoch ms(값이 있는 것만). inject_start, inject_end 외 이름도 받는다.
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
    """분류기·불변식 설정(기준 YAML integrity, 설계 5.3, 5.4).

    response_fields: 의미 이름(status, reject_reason, price, fee) → 응답 JSON 필드 이름.
      기본값은 설계 6 default.yaml 매핑이다. 빈 매핑을 주면 응답 대 DB 비교를 하지 않는다.
    response_fields_confirmed: false면 응답 대 DB 비교는 RESPONSE_SCHEMA_UNCONFIRMED.
    price_max_age_ms: I7 신선도 상한(배속 1 기준).
    """

    response_fields: Mapping[str, str] = field(
        default_factory=lambda: dict(DEFAULT_RESPONSE_FIELDS), hash=False
    )
    response_fields_confirmed: bool = False
    price_max_age_ms: int = 60000


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

"""잔고 불변식(계약 명세 7). DB 스냅샷(전체 행)으로 Python이 계산해 InvariantResult를 낸다.

불변식(모두 status='filled' 주문만 센다)
- cash: 모든 계좌 krw + Σ(bid amount) − Σ(ask amount) = initial_krw. 계좌 없는 주문도 위반이다.
- qty: 모든 (user, market) ∈ positions ∪ orders: positions.volume(없으면 0) = Σ(bid volume) − Σ(ask volume).
- avg: 모든 (user, market): 그 주문을 id 순으로 서비스 공식대로 재생한 avg_price = positions.avg_price.
  공식(workloads/order/app/orders.py): bid는 ((held·avg + v·price) / (held + v)).quantize(1e-8),
  ask는 avg를 그대로 둔다(volume이 0이 돼도). 기본 context(28자리, ROUND_HALF_EVEN)로 똑같이 계산한다.
- nonneg: krw ≥ 0, volume ≥ 0, avg_price ≥ 0.
- arith: run 주문(+extra 계좌 주문): amount = ROUND_DOWN(fill_price × volume, 4)(기본 context).
- constraints: schema 구획이 기대 스키마(EXPECTED_*)를 담는다. 다르면 위반 수 대신 None + SCHEMA_DRIFT.
- fresh(선택): run 주문(+extra)마다 같은 market의 tick 중 price = fill_price이고
  received_at ∈ [filled_at − stale_ms − 1s, filled_at + 1s]인 것이 있다. 볼 tick이 없으면 NOT_EVALUATED.

정한 것
- 계산 위치는 모두 Python이다. bundle이 accounts·positions·orders 전체 행과 schema 메타를 가져온다.
  SQL 구획(inv_<id>)은 쓰지 않는다. 같은 스냅샷 하나로 계산하니 SQL·Python 두 결과가 갈릴 일이 없다.
- cash·qty·avg·nonneg는 모든 계좌다(seed 기준 전체 보존식). arith·fresh는 run 주문(+extra)이다.
  run 주문은 client_order_id 전체 형식(model.is_run_key)으로 고른다. ctx가 없으면 모든 filled 주문이다.
- NULL은 위반으로 센다(스키마가 NOT NULL이라 정상이면 없다). fresh만 체결 열 NULL을 건너뛴다(FILL_STATE 몫).
- 필요한 구획이 없으면 violations None + DB_ERROR. 다른 불변식은 그대로 판정한다.
- 스냅샷 자기 검사(snapshot_consistent): F1 == F2(정지)인데 파싱한 행의 지문이 F1과 다르면
  행 기반 불변식(constraints 밖 전부)은 None + JUDGE_SELF_CHECK다(파싱이 행을 잃었다).
- 서비스는 Redis 가격(소수 자릿수 제한 없음)으로 계산하고 DB에는 fill_price를 8자리로 반올림해 넣는다.
  가격 소수가 8자리를 넘으면 avg·arith가 어긋날 수 있다(KRW-BTC 가격은 정수라 지금은 해당 없음).

결과 규칙(verdict.evaluate_count_check와 맞춘다)
- check_invariants는 InvariantResult를 INVARIANT_IDS 순서로 낸다. examples는 subject 순 EXAMPLES_MAX개.
- item_results는 기준 항목(kind integrity, sql: 또는 check:)마다 CheckResult 하나를 낸다. 위반 수 합을
  op·value로 비교한다. 평가 못 한 불변식이 있으면 아는 건수로 정해질 때만 정하고(예: '== 0'인데 이미 1건),
  아니면 INCONCLUSIVE이고 사유는 그 불변식의 사유(DB_ERROR, SCHEMA_DRIFT, NOT_EVALUATED 등)다.
- invariant_counts는 id → 위반 수(평가 못 하면 None)다. verdict.json integrity.invariants에 쓴다.
- run_key_counts는 judge_self_check(분류기 대조)용 run 키 주문 수다.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Context, Decimal, localcontext
from typing import Any

from . import reasons as R
from .criteria import integrity_cfg
from .db import TICK_SLACK_MS
from .model import (
    DEFAULT_STALE_MS,
    EXAMPLES_MAX,
    INTEGRITY_CHECKS,
    INVARIANT_IDS,
    SIDE_ASK,
    SIDE_BID,
    STATUS_FILLED,
    CheckResult,
    Criteria,
    DbOrder,
    DbSnapshot,
    IntegrityCfg,
    InvariantResult,
    InvariantViolation,
    Outcome,
    canonical_json,
    compute_fingerprint,
    is_run_key,
    iso_us,
    to_jsonable,
)
from .verdict import evaluate_count_check

__all__ = [
    "EXPECTED_COLUMNS",
    "EXPECTED_CONSTRAINTS",
    "EXPECTED_TABLES",
    "ROW_BASED",
    "Replay",
    "Scope",
    "check_arith",
    "check_avg",
    "check_cash",
    "check_constraints",
    "check_fresh",
    "check_invariants",
    "check_nonneg",
    "check_qty",
    "invariant_counts",
    "item_results",
    "replay_position",
    "run_key_counts",
    "scope_of",
    "snapshot_consistent",
]

KIND = "integrity"
SOURCE = "python"

# 서비스 계산 context(orders.place_order는 기본 context를 쓴다: 28자리, ROUND_HALF_EVEN)
SERVICE_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN)
# 합계·차이용. 자릿수가 모자라 반올림되는 일이 없게 넉넉히 둔다(우리 설정값)
EXACT_CONTEXT = Context(prec=200, rounding=ROUND_HALF_EVEN)
Q_AMOUNT = Decimal("0.0001")  # amount = (price × volume).quantize(0.0001, ROUND_DOWN)
Q_AVG = Decimal("0.00000001")  # new_avg.quantize(0.00000001)

# 행으로 계산하는 불변식(스냅샷 자기 검사 대상). constraints는 schema 구획만 본다
ROW_BASED = frozenset(INVARIANT_IDS) - {"constraints"}

# 기대 스키마(workloads/order/db/schema.sql, 커밋 6e8f38b). judge가 읽는 열과 그 값을 지키는 제약만 본다.
# 열은 format_type + not null 여부(snapshot_schema.sql), 제약은 pg_get_constraintdef(PostgreSQL 16) 문자열이다.
# 제약은 이름이 아니라 정의로 찾는다(이름은 자동 생성이라 바뀔 수 있다). 더 있는 열·제약은 괜찮다.
# FOREIGN KEY는 보지 않는다(계좌 없는 주문은 cash가 위반으로 센다). ticks는 fresh(선택)만 쓰므로 보지 않는다.
EXPECTED_TABLES = ("accounts", "positions", "orders")
EXPECTED_COLUMNS: Mapping[str, Mapping[str, str]] = {
    "accounts": {"user_id": "text not null", "krw": "numeric(24,4) not null"},
    "positions": {
        "user_id": "text not null",
        "market": "text not null",
        "volume": "numeric(30,8) not null",
        "avg_price": "numeric(24,8) not null",
    },
    "orders": {
        "id": "bigint not null",
        "client_order_id": "text not null",
        "user_id": "text not null",
        "market": "text not null",
        "side": "text not null",
        "volume": "numeric(30,8) not null",
        "status": "text not null",
        "fill_price": "numeric(24,8)",
        "amount": "numeric(24,4)",
        "created_at": "timestamp with time zone not null",
        "filled_at": "timestamp with time zone",
    },
}
STATUS_CHECK = (
    "CHECK ((status = ANY (ARRAY['accepted'::text, 'pending'::text, "
    "'filled'::text, 'rejected'::text])))"
)
# adapter 모드(external_schema.sql)는 'unknown'을 더한다. unknown은 filled가 아니라 불변식에 영향이 없다
STATUS_CHECK_ADAPTER = (
    "CHECK ((status = ANY (ARRAY['accepted'::text, 'pending'::text, "
    "'filled'::text, 'rejected'::text, 'unknown'::text])))"
)
EXPECTED_CONSTRAINTS: Mapping[str, tuple[str, ...]] = {
    "accounts": (
        "PRIMARY KEY (user_id)",
        "CHECK ((krw >= (0)::numeric))",
    ),
    "positions": (
        "PRIMARY KEY (user_id, market)",
        "CHECK ((volume >= (0)::numeric))",
        "CHECK ((avg_price >= (0)::numeric))",
    ),
    "orders": (
        "PRIMARY KEY (id)",
        "UNIQUE (client_order_id)",
        "CHECK ((side = ANY (ARRAY['bid'::text, 'ask'::text])))",
        "CHECK ((volume > (0)::numeric))",
        STATUS_CHECK,
        "CHECK (((status <> 'filled'::text) OR ((fill_price IS NOT NULL) AND "
        "(amount IS NOT NULL) AND (filled_at IS NOT NULL))))",
    ),
}
ALSO_ACCEPTED: Mapping[str, tuple[str, ...]] = {STATUS_CHECK: (STATUS_CHECK_ADAPTER,)}


# ---------------------------------------------------------------- 공용


@dataclass(frozen=True)
class Scope:
    """arith·fresh 대상(계약 명세 7): run 주문(+extra 계좌 주문). run_id가 None이면 모든 주문이다."""

    run_id: str | None = None
    extra: frozenset[str] = frozenset()

    def covers(self, o: DbOrder) -> bool:
        if self.run_id is None:
            return True
        return is_run_key(o.client_order_id, self.run_id) or o.user_id in self.extra


def scope_of(ctx_like: Any) -> Scope:
    """RunContext 등(run_id·extra_accounts 속성)에서 Scope를 만든다. None이면 전체."""
    if ctx_like is None:
        return Scope()
    return Scope(
        run_id=getattr(ctx_like, "run_id", None),
        extra=frozenset(getattr(ctx_like, "extra_accounts", ()) or ()),
    )


def _s(v: Decimal | None) -> str | None:
    """Decimal → 지수 없는 문자열(서비스 응답과 같은 f"{d:f}")."""
    return None if v is None else format(v, "f")


def _filled(orders: Iterable[DbOrder]) -> list[DbOrder]:
    return [o for o in orders if o.status == STATUS_FILLED]


def _pair(user: str, market: str | None) -> str:
    return f"{user}/{market if market is not None else ''}"


def _missing(
    inv_id: str, db: DbSnapshot, need: Sequence[str]
) -> InvariantResult | None:
    miss = [s for s in need if s not in db.sections]
    if not miss:
        return None
    return InvariantResult(
        id=inv_id,
        source=SOURCE,
        violations=None,
        reasons=(R.DB_ERROR,),
        detail={"missing_sections": miss},
    )


def _violation(
    inv_id: str,
    subject: str,
    problem: str,
    expected: Any = None,
    actual: Any = None,
    **detail: Any,
) -> InvariantViolation:
    return InvariantViolation(
        invariant=inv_id,
        subject=subject,
        expected=_s(expected) if isinstance(expected, Decimal) else expected,
        actual=_s(actual) if isinstance(actual, Decimal) else actual,
        detail={"problem": problem, **detail},
    )


def _examples(viols: Sequence[InvariantViolation]) -> tuple[InvariantViolation, ...]:
    def key(v: InvariantViolation) -> tuple[str, str]:
        return (v.subject, canonical_json(v))

    return tuple(sorted(viols, key=key)[:EXAMPLES_MAX])


def _counted(
    inv_id: str,
    viols: Sequence[InvariantViolation],
    detail: Mapping[str, Any],
    *,
    reasons: Sequence[str] = (),
) -> InvariantResult:
    return InvariantResult(
        id=inv_id,
        source=SOURCE,
        violations=len(viols),
        examples=_examples(viols),
        reasons=tuple(reasons),
        detail=dict(detail),
    )


# ---------------------------------------------------------------- cash


@dataclass
class _Flow:
    bid: Decimal = Decimal(0)
    ask: Decimal = Decimal(0)
    bad: list[int] = field(default_factory=list)


def check_cash(db: DbSnapshot, initial_krw: Decimal) -> InvariantResult:
    """모든 계좌: krw + Σ(bid amount) − Σ(ask amount) = initial_krw(계약 명세 7 cash).

    problem: mismatch(식이 안 맞음), krw_null, order_null(filled 주문 amount NULL·side 이상),
    no_account(주문은 있는데 계좌 행이 없다).
    """
    miss = _missing("cash", db, ("accounts", "orders"))
    if miss is not None:
        return miss
    filled = _filled(db.orders)
    flows: dict[str, _Flow] = {}
    accounts = {a.user_id: a for a in db.accounts}
    viols: list[InvariantViolation] = []
    with localcontext(EXACT_CONTEXT):
        for o in filled:
            f = flows.setdefault(o.user_id, _Flow())
            if o.amount is None or o.side not in (SIDE_BID, SIDE_ASK):
                f.bad.append(o.id)
            elif o.side == SIDE_BID:
                f.bid += o.amount
            else:
                f.ask += o.amount
        for uid in sorted(set(accounts) | set(flows)):
            a = accounts.get(uid)
            f = flows.get(uid, _Flow())
            expected = initial_krw - f.bid + f.ask
            actual = None if a is None else a.krw
            if a is None:
                problem = "no_account"
            elif f.bad:
                problem = "order_null"
            elif actual is None:
                problem = "krw_null"
            elif actual != expected:
                problem = "mismatch"
            else:
                continue
            viols.append(
                _violation(
                    "cash",
                    uid,
                    problem,
                    expected,
                    actual,
                    bid_amount=_s(f.bid),
                    ask_amount=_s(f.ask),
                    bad_orders=sorted(f.bad)[:EXAMPLES_MAX],
                )
            )
    detail = {
        "accounts": len(accounts),
        "filled_orders": len(filled),
        "initial_krw": _s(initial_krw),
    }
    return _counted("cash", viols, detail)


# ---------------------------------------------------------------- qty


def check_qty(db: DbSnapshot) -> InvariantResult:
    """모든 (user, market) ∈ positions ∪ orders: volume(없으면 0) = Σ bid − Σ ask(계약 명세 7 qty).

    problem: mismatch, volume_null(positions.volume NULL), order_null(filled 주문 volume NULL·side 이상).
    """
    miss = _missing("qty", db, ("positions", "orders"))
    if miss is not None:
        return miss
    filled = _filled(db.orders)
    net: dict[str, Decimal] = {}
    bad: dict[str, list[int]] = {}
    pos = {_pair(p.user_id, p.market): p for p in db.positions}
    viols: list[InvariantViolation] = []
    with localcontext(EXACT_CONTEXT):
        for o in filled:
            k = _pair(o.user_id, o.market)
            if o.volume is None or o.side not in (SIDE_BID, SIDE_ASK):
                bad.setdefault(k, []).append(o.id)
                continue
            delta = o.volume if o.side == SIDE_BID else -o.volume
            net[k] = net.get(k, Decimal(0)) + delta
        for k in sorted(set(pos) | set(net) | set(bad)):
            p = pos.get(k)
            expected = net.get(k, Decimal(0))
            actual = Decimal(0) if p is None else p.volume
            if k in bad:
                problem = "order_null"
            elif actual is None:
                problem = "volume_null"
            elif actual != expected:
                problem = "mismatch"
            else:
                continue
            viols.append(
                _violation(
                    "qty",
                    k,
                    problem,
                    expected,
                    actual,
                    position_row=p is not None,
                    bad_orders=sorted(bad.get(k, ()))[:EXAMPLES_MAX],
                )
            )
    detail = {
        "pairs": len(set(pos) | set(net) | set(bad)),
        "filled_orders": len(filled),
    }
    return _counted("qty", viols, detail)


# ---------------------------------------------------------------- avg


@dataclass(frozen=True)
class Replay:
    """한 (user, market)의 filled 주문을 서비스 공식대로 재생한 결과.

    oversold: 보유량보다 많이 판 ask 주문 id(서비스는 422로 거절하므로 정상이면 없다).
    error: 계산할 수 없었던 이유(체결 열 NULL, 0으로 나눔 등). 있으면 volume·avg는 그 직전 값이다.
    """

    volume: Decimal
    avg_price: Decimal
    orders: int
    oversold: tuple[int, ...] = ()
    error: str | None = None


def replay_position(orders: Iterable[DbOrder]) -> Replay:
    """filled 주문을 id 순으로 orders.place_order와 같은 식으로 재생한다(계약 명세 7 avg).

    bid: new_volume = held + v, new_avg = ((held·avg + v·price) / new_volume).quantize(1e-8).
    ask: new_volume = held − v, avg 그대로(0이 돼도 유지). 계산은 기본 context(28자리, HALF_EVEN)다.
    price는 DB fill_price(서비스가 쓴 Redis 가격을 NUMERIC(24,8)로 저장한 값)다.
    """
    held = Decimal(0)
    avg = Decimal(0)
    n = 0
    oversold: list[int] = []
    with localcontext(SERVICE_CONTEXT):
        for o in sorted(_filled(orders), key=lambda x: x.id):
            n += 1
            if o.volume is None or o.fill_price is None:
                return Replay(
                    held, avg, n, tuple(oversold), f"주문 {o.id}: 체결 열 NULL"
                )
            try:
                if o.side == SIDE_BID:
                    new_volume = held + o.volume
                    avg = (
                        (held * avg + o.volume * o.fill_price) / new_volume
                    ).quantize(Q_AVG)
                    held = new_volume
                elif o.side == SIDE_ASK:
                    if held < o.volume:
                        oversold.append(o.id)
                    held = held - o.volume
                else:
                    return Replay(
                        held, avg, n, tuple(oversold), f"주문 {o.id}: side {o.side!r}"
                    )
            except ArithmeticError as e:
                return Replay(
                    held, avg, n, tuple(oversold), f"주문 {o.id}: {type(e).__name__}"
                )
    return Replay(held, avg, n, tuple(oversold))


def check_avg(db: DbSnapshot) -> InvariantResult:
    """모든 (user, market): 재생한 avg_price = positions.avg_price, 정확히 같음(계약 명세 7 avg).

    사용자별로는 accounts FOR UPDATE로 직렬화되어 orders.id 순서 = 처리 순서다(계약 명세 1).
    problem: mismatch, avg_null, position_missing(주문은 있는데 positions 행이 없다), replay_error.
    """
    miss = _missing("avg", db, ("positions", "orders"))
    if miss is not None:
        return miss
    by_pair: dict[str, list[DbOrder]] = {}
    for o in _filled(db.orders):
        by_pair.setdefault(_pair(o.user_id, o.market), []).append(o)
    pos = {_pair(p.user_id, p.market): p for p in db.positions}
    viols: list[InvariantViolation] = []
    oversold = 0
    for k in sorted(set(pos) | set(by_pair)):
        rp = replay_position(by_pair.get(k, ()))
        oversold += len(rp.oversold)
        p = pos.get(k)
        actual = None if p is None else p.avg_price
        if rp.error is not None:
            problem = "replay_error"
        elif p is None:
            problem = "position_missing"
        elif actual is None:
            problem = "avg_null"
        elif actual != rp.avg_price:
            problem = "mismatch"
        else:
            continue
        viols.append(
            _violation(
                "avg",
                k,
                problem,
                rp.avg_price,
                actual,
                orders=rp.orders,
                replay_volume=_s(rp.volume),
                error=rp.error,
                oversold=list(rp.oversold[:EXAMPLES_MAX]),
            )
        )
    detail = {"pairs": len(set(pos) | set(by_pair)), "oversold_orders": oversold}
    return _counted("avg", viols, detail)


# ---------------------------------------------------------------- nonneg


def check_nonneg(db: DbSnapshot) -> InvariantResult:
    """krw ≥ 0(accounts), volume ≥ 0, avg_price ≥ 0(positions). NULL도 위반이다(계약 명세 7 nonneg)."""
    miss = _missing("nonneg", db, ("accounts", "positions"))
    if miss is not None:
        return miss
    viols: list[InvariantViolation] = []

    def see(subject: str, table: str, column: str, v: Decimal | None) -> None:
        if v is None or v < 0:
            problem = "null" if v is None else "negative"
            viols.append(
                _violation(
                    "nonneg", subject, problem, ">= 0", v, table=table, column=column
                )
            )

    for a in db.accounts:
        see(a.user_id, "accounts", "krw", a.krw)
    for p in db.positions:
        k = _pair(p.user_id, p.market)
        see(k, "positions", "volume", p.volume)
        see(k, "positions", "avg_price", p.avg_price)
    detail = {"accounts": len(db.accounts), "positions": len(db.positions)}
    return _counted("nonneg", viols, detail)


# ---------------------------------------------------------------- arith


def check_arith(db: DbSnapshot, scope: Scope = Scope()) -> InvariantResult:
    """run 주문(+extra)의 filled 주문: amount = ROUND_DOWN(fill_price × volume, 4)(계약 명세 7 arith).

    곱은 서비스처럼 기본 context(28자리)로 계산한다. problem: mismatch, null, error.
    """
    miss = _missing("arith", db, ("orders",))
    if miss is not None:
        return miss
    targets = [o for o in _filled(db.orders) if scope.covers(o)]
    viols: list[InvariantViolation] = []
    with localcontext(SERVICE_CONTEXT):
        for o in targets:
            ids = {
                "order_id": o.id,
                "fill_price": _s(o.fill_price),
                "volume": _s(o.volume),
            }
            if o.fill_price is None or o.volume is None or o.amount is None:
                viols.append(
                    _violation(
                        "arith", o.client_order_id, "null", None, o.amount, **ids
                    )
                )
                continue
            try:
                expected = (o.fill_price * o.volume).quantize(
                    Q_AMOUNT, rounding=ROUND_DOWN
                )
            except ArithmeticError as e:
                viols.append(
                    _violation(
                        "arith",
                        o.client_order_id,
                        "error",
                        None,
                        o.amount,
                        error=type(e).__name__,
                        **ids,
                    )
                )
                continue
            if o.amount != expected:
                viols.append(
                    _violation(
                        "arith",
                        o.client_order_id,
                        "mismatch",
                        expected,
                        o.amount,
                        **ids,
                    )
                )
    detail = {"orders": len(targets), "run_id": scope.run_id}
    return _counted("arith", viols, detail)


# ---------------------------------------------------------------- constraints


def _norm(s: str | None) -> str:
    return " ".join((s or "").split())


def check_constraints(db: DbSnapshot) -> InvariantResult:
    """schema 구획이 기대 스키마를 담는지 본다(계약 명세 7 constraints).

    빠진 테이블·열, 열 형식(타입·NOT NULL) 차이, 빠진 제약(검증 안 된 NOT VALID 제약도 빠진 것으로 본다)이
    하나라도 있으면 violations None + SCHEMA_DRIFT다(판정 불가. FAIL로 세지 않는다).
    problem: table_missing, column_missing, column_type, constraint_missing.
    """
    miss = _missing("constraints", db, ("schema",))
    if miss is not None:
        return miss
    tables = {s.name for s in db.schema if s.kind == "table"}
    cols = {(s.table, s.name): s.detail for s in db.schema if s.kind == "column"}
    cons: dict[str, list[str]] = {}
    for s in db.schema:
        if s.kind == "constraint":
            cons.setdefault(s.table, []).append(_norm(s.detail))
    drift: list[InvariantViolation] = []
    for t in EXPECTED_TABLES:
        if t not in tables:
            drift.append(_violation("constraints", t, "table_missing", "table", None))
            continue
        for c, want in EXPECTED_COLUMNS[t].items():
            got = cols.get((t, c))
            subject = f"{t}.{c}"
            if got is None:
                drift.append(_violation("constraints", subject, "column_missing", want))
            elif _norm(got) != _norm(want):
                drift.append(
                    _violation("constraints", subject, "column_type", want, got)
                )
        have = cons.get(t, [])
        for want in EXPECTED_CONSTRAINTS[t]:
            w = _norm(want)
            if any(_norm(x) in have for x in (want, *ALSO_ACCEPTED.get(want, ()))):
                continue
            near = next((h for h in have if h.startswith(w)), None)  # 예: ... NOT VALID
            drift.append(
                _violation("constraints", f"{t}: {w}", "constraint_missing", w, near)
            )
    detail = {
        "tables": sorted(tables),
        "expected_tables": list(EXPECTED_TABLES),
        "drift": len(drift),
    }
    if drift:
        return InvariantResult(
            id="constraints",
            source=SOURCE,
            violations=None,
            examples=_examples(drift),
            reasons=(R.SCHEMA_DRIFT,),
            detail=detail,
        )
    return _counted("constraints", (), detail)


# ---------------------------------------------------------------- fresh


def check_fresh(
    db: DbSnapshot, scope: Scope = Scope(), stale_ms: int = DEFAULT_STALE_MS
) -> InvariantResult:
    """run 주문(+extra)의 filled 주문마다 같은 market에서 price = fill_price이고
    received_at ∈ [filled_at − stale_ms − 1s, filled_at + 1s](양끝 포함)인 tick이 있다(계약 명세 7 fresh).

    - 체결 열(filled_at, fill_price, market)이 NULL인 주문은 건너뛴다(skipped. FILL_STATE가 센다).
    - 그 market의 tick이 구획에 하나도 없으면 그 주문은 볼 수 없다(unevaluated).
    - 위반이 있으면 위반 수. 위반 없이 볼 수 없는 주문이 있으면 None + NOT_EVALUATED(ticks가 비면 이것).
    - ticks 구획이 없으면(psql 오류) None + DB_ERROR.
    """
    miss = _missing("fresh", db, ("orders", "ticks"))
    if miss is not None:
        return miss
    times: dict[str, list[int]] = {}
    prices: dict[str, list[Decimal]] = {}
    usable = sorted(
        (t.market, t.received_us, t.price)
        for t in db.ticks
        if t.received_us is not None and t.price is not None
    )
    for market, at, price in usable:
        times.setdefault(market, []).append(at)
        prices.setdefault(market, []).append(price)
    lo_us = (stale_ms + TICK_SLACK_MS) * 1000
    hi_us = TICK_SLACK_MS * 1000
    targets = [o for o in _filled(db.orders) if scope.covers(o)]
    viols: list[InvariantViolation] = []
    skipped = 0
    unevaluated: list[str] = []
    for o in targets:
        if o.filled_us is None or o.fill_price is None or o.market is None:
            skipped += 1
            continue
        ts = times.get(o.market)
        if not ts:
            unevaluated.append(o.client_order_id)
            continue
        lo, hi = o.filled_us - lo_us, o.filled_us + hi_us
        i, j = bisect.bisect_left(ts, lo), bisect.bisect_right(ts, hi)
        if any(prices[o.market][x] == o.fill_price for x in range(i, j)):
            continue
        viols.append(
            _violation(
                "fresh",
                o.client_order_id,
                "no_tick",
                o.fill_price,
                None,
                order_id=o.id,
                market=o.market,
                filled_at=iso_us(o.filled_us),
                window=[iso_us(lo), iso_us(hi)],
                ticks_in_window=j - i,
            )
        )
    detail = {
        "orders": len(targets),
        "ticks": len(db.ticks),
        "skipped": skipped,
        "unevaluated": len(unevaluated),
        "unevaluated_examples": sorted(unevaluated)[:EXAMPLES_MAX],
        "stale_ms": stale_ms,
        "slack_ms": TICK_SLACK_MS,
    }
    if not viols and unevaluated:
        return InvariantResult(
            id="fresh",
            source=SOURCE,
            violations=None,
            reasons=(R.NOT_EVALUATED,),
            detail=detail,
        )
    return _counted("fresh", viols, detail)


# ---------------------------------------------------------------- 스냅샷 자기 검사


def snapshot_consistent(db: DbSnapshot) -> bool | None:
    """F1 == F2(정지)일 때 파싱한 행의 지문이 F1과 같으면 True, 다르면 False.

    정지가 아니거나(db_quiescent 가드 몫) 구획이 모자라면 None(판단하지 않는다).
    F1·F2는 스냅샷 트랜잭션 밖에서 잰다(db.py). 정지 상태면 그 사이 스냅샷도 같은 상태다.
    """
    if not db.quiescent or not {"accounts", "positions", "orders"} <= db.sections:
        return None
    with localcontext(EXACT_CONTEXT):
        fp = compute_fingerprint(db.accounts, db.positions, db.orders)
    return fp == db.fingerprint1


def _untrusted(r: InvariantResult, db: DbSnapshot) -> InvariantResult:
    with localcontext(EXACT_CONTEXT):
        parsed = compute_fingerprint(db.accounts, db.positions, db.orders)
    return replace(
        r,
        violations=None,
        reasons=tuple(sorted({*r.reasons, R.JUDGE_SELF_CHECK})),
        detail={
            **r.detail,
            "snapshot_consistent": False,
            "computed_violations": r.violations,
            "fingerprint1": to_jsonable(db.fingerprint1),
            "parsed_fingerprint": to_jsonable(parsed),
        },
    )


# ---------------------------------------------------------------- 공개 함수


def _cfg(crit: Any) -> IntegrityCfg:
    if crit is None:
        return IntegrityCfg()
    if isinstance(crit, IntegrityCfg):
        return crit
    if isinstance(crit, Mapping):
        crit = Criteria(data=crit)
    if not isinstance(crit, Criteria):
        raise TypeError(
            f"crit는 Criteria|IntegrityCfg|Mapping|None이어야 한다: {type(crit)!r}"
        )
    if "integrity" not in crit.data:
        return IntegrityCfg()
    return integrity_cfg(crit)


def check_invariants(
    db: DbSnapshot | None,
    ctx_like: Any = None,
    crit: Any = None,
) -> list[InvariantResult]:
    """불변식 7개를 INVARIANT_IDS 순서로 평가한다(계약 명세 7).

    ctx_like: run_id·extra_accounts 속성(RunContext 등). arith·fresh 대상을 정한다. None이면 전체.
    crit: Criteria(권장), IntegrityCfg, 맵, 또는 None. initial_krw·stale_ms를 읽는다.
    """
    snap = db if db is not None else DbSnapshot()
    cfg = _cfg(crit)
    scope = scope_of(ctx_like)
    by_id = {
        "cash": check_cash(snap, cfg.initial_krw),
        "qty": check_qty(snap),
        "avg": check_avg(snap),
        "nonneg": check_nonneg(snap),
        "arith": check_arith(snap, scope),
        "constraints": check_constraints(snap),
        "fresh": check_fresh(snap, scope, cfg.stale_ms),
    }
    out = [by_id[i] for i in INVARIANT_IDS]
    if snapshot_consistent(snap) is False:
        out = [_untrusted(r, snap) if r.id in ROW_BASED else r for r in out]
    return out


def invariant_counts(results: Sequence[InvariantResult]) -> dict[str, int | None]:
    """id → 위반 수. 평가하지 못했으면 None(DB_ERROR, SCHEMA_DRIFT, NOT_EVALUATED 등)."""
    return {r.id: r.violations for r in results}


def _targets(item: Mapping[str, Any]) -> tuple[str, ...]:
    if item.get("kind") != KIND:
        return ()
    if "sql" in item:
        raw = item["sql"]
        return (raw,) if isinstance(raw, str) else tuple(str(x) for x in raw)
    if item.get("check") in INTEGRITY_CHECKS:
        return (str(item["check"]),)
    return ()


def _part(r: InvariantResult | None) -> dict[str, Any]:
    if r is None:
        return {"violations": None, "reasons": [R.NOT_EVALUATED], "missing": True}
    return {
        "violations": r.violations,
        "source": r.source,
        "reasons": list(r.reasons),
        "examples": to_jsonable(r.examples),
        "detail": to_jsonable(r.detail),
    }


def item_results(
    results: Sequence[InvariantResult], crit: Criteria
) -> list[CheckResult]:
    """불변식 결과를 기준 항목(kind integrity, sql: 또는 check:) 결과로 묶는다.

    비교는 verdict.evaluate_count_check(위반 수 합, op·value)다. INCONCLUSIVE면 사유를 평가 못 한
    불변식의 사유(DB_ERROR, SCHEMA_DRIFT, NOT_EVALUATED, JUDGE_SELF_CHECK)로 바꾼다.
    detail.parts에 불변식별 위반 수·사유·예시를 둔다.
    """
    by_id = {r.id: r for r in results}
    counts = invariant_counts(results)
    out: list[CheckResult] = []
    for item in crit.checks:
        names = _targets(item)
        if not names:
            continue
        res = evaluate_count_check(item, counts)
        parts = {n: by_id.get(n) for n in names}
        if res.outcome is Outcome.INCONCLUSIVE:
            codes: set[str] = set()
            for p in parts.values():
                if p is None:
                    codes.add(R.NOT_EVALUATED)
                elif p.violations is None:
                    codes.update(p.reasons or (R.NOT_EVALUATED,))
            if codes:
                res = replace(res, reasons=tuple(sorted(codes)))
        detail = {
            **res.detail,
            "parts": {n: _part(p) for n, p in sorted(parts.items())},
        }
        out.append(replace(res, kind=KIND, detail=detail))
    return out


def run_key_counts(db: DbSnapshot, run_id: str) -> dict[str, int] | None:
    """run 키(`k6-<run_id>-<vu>-<seq>` 전체 형식) 주문 수. orders 구획이 없으면 None.

    judge_self_check(계약 명세 7) 대조식: keys = 분류기 D1 키 수 + ORPHAN 키 수,
    orders − keys = 같은 키 중복 행 수(DUP_ORDER. UNIQUE라 정상이면 0).
    접두어 `k6-<run_id>-`만 보면 r1117-crash가 r1117-crash-2 키까지 센다(그래서 전체 형식).
    """
    if "orders" not in db.sections:
        return None
    rows = db.run_orders(run_id)
    return {
        "orders": len(rows),
        "keys": len({o.client_order_id for o in rows}),
        "filled": sum(1 for o in rows if o.status == STATUS_FILLED),
    }

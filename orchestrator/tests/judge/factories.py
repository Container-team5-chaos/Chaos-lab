"""테스트용 빌더. 골든 테스트가 K6Log·DbSnapshot을 파서 없이 직접 만든다(계약 명세 1–7).

기본값은 서로 맞물린다: make_intent 본문 = make_order 본문, make_result의 ACK 응답 =
make_order 행(order_to_dict 모양). make_db는 주문에서 accounts.krw와 positions를 서비스 공식대로
계산한다. 그래서 아무것도 바꾸지 않으면 OK(clean) 한 건이고 불변식도 모두 맞는다.

    k = run_key(1)                       # "k6-r-test-1-1"
    log = make_log([make_intent(k)], [make_result(k)])
    db = make_db(orders=[make_order(ACCT_A, k)])

위반은 값을 직접 주어 심는다: make_db(accounts=[make_account(ACCT_A, krw="1")], ...).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from decimal import ROUND_DOWN, Context, Decimal, localcontext
from typing import Any

from orchestrator.judge.model import (
    INVARIANTS,
    JSONL_VERSION,
    SIDE_BID,
    SNAPSHOT_SECTIONS,
    STATUS_FILLED,
    STATUS_REJECTED,
    Attempt,
    DbAccount,
    DbOrder,
    DbPosition,
    DbSnapshot,
    DbTick,
    Fingerprint,
    Intent,
    K6Log,
    Result,
    RunStart,
    SchemaItem,
    canonical_json,
    compute_fingerprint,
    dec,
    iso_us,
    sha256_text,
)

RUN_ID = "r-test"
T0 = 1_762_300_000_000  # 2025-11-05 epoch ms. 테스트 시각 기준점
MARKET = "KRW-BTC"
PRICE = "100000000"  # 체결가(원)
VOLUME = "0.00010000"  # 기본 주문 수량
AMOUNT = "10000.0000"  # ROUND_DOWN(PRICE × VOLUME, 4)
INITIAL_KRW = "10000000"  # seed.sql
FILLED_US = (
    T0 + 5
) * 1000 + 123  # 기본 체결 시각(epoch µs). ms 아래 자리까지 비교하는지 본다

# seed 계좌(user-0001..1000). vu_of가 끝 네 자리를 VU로 쓴다.
ACCT_A = "user-0001"
ACCT_B = "user-0002"
ACCT_C = "user-0003"
ACCT_EXTRA = "user-0999"  # 발표자(extra_accounts)

_AUTO: Any = object()
_Q4 = Decimal("0.0001")
_Q8 = Decimal("0.00000001")
_USER_RE = re.compile(r"^user-([0-9]{4})$")
_KEY_RE = re.compile(r"^k6-.+-([0-9]+)-([0-9]+)$")

# 상태 코드 → 기본 거절 reason(ec를 안 줄 때). 503은 일부러 비워 둔다(ec 없음 = 결과 불명).
_DEFAULT_REJECT = {
    400: "invalid_request",
    404: "user_not_found",
    409: "idempotency_conflict",
    422: "insufficient_balance",
}


# ---------------------------------------------------------------- 계약 계산(서비스 공식)


def run_key(seq: int, vu: int = 1, run_id: str = RUN_ID) -> str:
    """client_order_id `k6-<run_id>-<vu>-<seq>`."""
    return f"k6-{run_id}-{vu}-{seq}"


def vu_of(acct: str) -> int:
    """user-NNNN → NNNN(1 VU = 1 user). 그 밖 형식은 1."""
    m = _USER_RE.fullmatch(acct)
    return int(m.group(1)) if m else 1


def fill_amount(price: Any, volume: Any) -> Decimal:
    """amount = ROUND_DOWN(price × volume, 4)(orders.place_order)."""
    return (dec(price) * dec(volume)).quantize(_Q4, rounding=ROUND_DOWN)


def order_id_for(key: str, n: int = 0) -> int:
    """키에서 결정적인 orders.id. run 키는 vu·seq 순서를 따른다(같은 VU면 seq 순 = 처리 순)."""
    m = _KEY_RE.fullmatch(key)
    if m:
        base = int(m.group(1)) * 100_000 + int(m.group(2))
    else:
        base = 10**8 + int(hashlib.sha256(key.encode()).hexdigest()[:7], 16)
    return base + n * 10**9


def default_body(
    key: str,
    acct: str = ACCT_A,
    *,
    market: str = MARKET,
    side: str = SIDE_BID,
    volume: str = VOLUME,
) -> dict[str, str]:
    """intent.body = 보낸 주문 본문(계약 명세 2)."""
    return {
        "client_order_id": key,
        "user_id": acct,
        "market": market,
        "side": side,
        "volume": volume,
    }


def _s(d: Decimal | None) -> str | None:
    return None if d is None else f"{d:f}"


def response_body(
    key: str,
    acct: str = ACCT_A,
    *,
    order_id: int | None = None,
    n: int = 0,
    body: Mapping[str, Any] | None = None,
    status: str = STATUS_FILLED,
    fill_price: Any = PRICE,
    amount: Any = _AUTO,
    filled_us: int | None = FILLED_US,
    replayed: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    """200 응답 본문(schemas.order_to_dict 모양). 기본값은 make_order 기본 행과 같다.

    Decimal은 DB 열 자릿수(volume·fill_price 8자리, amount 4자리)로 맞춘 지수 없는 문자열이다.
    extra로 필드를 바꾸거나 더한다(값이 None이면 null).
    """
    b = dict(body) if body is not None else default_body(key, acct)
    vol = dec(b.get("volume"))
    price = dec(fill_price)
    amt = fill_amount(price, vol) if amount is _AUTO else dec(amount)
    out: dict[str, Any] = {
        "order_id": order_id if order_id is not None else order_id_for(key, n),
        "client_order_id": b.get("client_order_id", key),
        "user_id": b.get("user_id", acct),
        "market": b.get("market"),
        "side": b.get("side"),
        "volume": _s(None if vol is None else vol.quantize(_Q8)),
        "status": status,
        "fill_price": _s(None if price is None else price.quantize(_Q8)),
        "amount": _s(None if amt is None else amt.quantize(_Q4)),
        "filled_at": None if filled_us is None else iso_us(filled_us),
        "replayed": replayed,
    }
    out.update(extra)
    return out


def response_raw(key: str, acct: str = ACCT_A, **kw: Any) -> str:
    """response_body의 JSON 텍스트(Attempt.raw)."""
    return canonical_json(response_body(key, acct, **kw))


def reject_raw(reason: str | None, key: str | None) -> str:
    """거절 본문 {status:"rejected", reason, client_order_id}(main.rejected_body)."""
    return canonical_json(
        {"status": STATUS_REJECTED, "reason": reason, "client_order_id": key}
    )


# ---------------------------------------------------------------- k6 기록(JSONL v2)


def make_intent(
    key: str,
    acct: str = ACCT_A,
    *,
    vu: int | None = None,
    t: int = T0,
    body: Mapping[str, Any] | None = None,
    **body_kw: Any,
) -> Intent:
    """intent 한 줄. body를 안 주면 default_body(key, acct, **body_kw). vu 기본값은 vu_of(acct)."""
    b = dict(body) if body is not None else default_body(key, acct, **body_kw)
    return Intent(
        key=key,
        acct=acct,
        vu=vu if vu is not None else vu_of(acct),
        t=t,
        body=b,
        body_raw=canonical_json(b),
    )


def att(
    st: int,
    ec: str | None = _AUTO,
    ms: float = 10,
    *,
    rp: bool | None = _AUTO,
    raw: str | None = _AUTO,
    **response: Any,
) -> dict[str, Any]:
    """시도 명세. make_result가 키·계좌에 맞춰 raw·rp 기본값을 채운다.

    response: 200 기본 응답에서 바꿀 필드(response_body 인자, 예: fill_price="1").
    """
    return {"st": st, "ec": ec, "ms": ms, "rp": rp, "raw": raw, "response": response}


AttSpec = int | tuple | Mapping[str, Any] | Attempt


def _spec(s: AttSpec) -> dict[str, Any]:
    if isinstance(s, int):
        return att(s)
    if isinstance(s, tuple):
        return att(*s)
    return {**att(s["st"]), **s}


def make_attempts(
    specs: Iterable[AttSpec],
    key: str,
    acct: str = ACCT_A,
    *,
    body: Mapping[str, Any] | None = None,
    t: int = T0 + 1,
) -> tuple[Attempt, ...]:
    """시도 명세 → Attempt들. n은 1부터, t0는 앞 시도 끝 뒤로 이어 붙인다.

    명세는 상태 코드, (st, ec, ms) tuple, att(...), Attempt(그대로 쓴다) 중 하나다.
    기본값: 200 → raw = response_raw(key, acct), rp = 앞에 200이 없으면 False, 있으면 True.
    400·404·409·422 → ec = 그 상태의 대표 reason. 4xx·503에 ec가 있으면 raw = reject_raw(ec, key)
    (400은 client_order_id null). 0·502·504·500 → raw None. 503에 ec를 안 주면 None(결과 불명).
    """
    out: list[Attempt] = []
    cur = t
    seen_200 = False
    for i, s in enumerate(specs, start=1):
        if isinstance(s, Attempt):
            out.append(s)
            cur = s.t1 + 1000
            seen_200 = seen_200 or s.st == 200
            continue
        sp = _spec(s)
        st = sp["st"]
        ec = sp["ec"]
        if ec is _AUTO:
            ec = _DEFAULT_REJECT.get(st)
        rp, raw = sp["rp"], sp["raw"]
        if st == 200:
            if rp is _AUTO:
                rp = seen_200
            if raw is _AUTO:
                kw = {"replayed": rp, **sp.get("response", {})}
                raw = response_raw(key, acct, body=body, **kw)
            seen_200 = True
        else:
            if rp is _AUTO:
                rp = None
            if raw is _AUTO:
                rejected = ec is not None and (400 <= st < 500 or st == 503)
                raw = reject_raw(ec, None if st == 400 else key) if rejected else None
        ms = sp["ms"]
        t0 = cur
        t1 = t0 + int(ms)
        out.append(
            Attempt(n=i, t0=t0, t1=t1, st=st, ec=ec, ms=float(ms), rp=rp, raw=raw)
        )
        cur = t1 + 1000  # 백오프 흉내
    return tuple(out)


def make_result(
    key: str,
    atts: Sequence[AttSpec] = (200,),
    *,
    acct: str = ACCT_A,
    body: Mapping[str, Any] | None = None,
    t_start: int = T0 + 1,
) -> Result:
    """result 한 줄. atts는 make_attempts 명세다. 기본값은 첫 시도 200(clean)."""
    return Result(key=key, atts=make_attempts(atts, key, acct, body=body, t=t_start))


def make_run_start(
    *,
    run_id: str = RUN_ID,
    profile: str = "steady",
    vus: int | None = None,
    conn_reuse: bool | None = None,
    symbols: Sequence[str] = (MARKET,),
    initial_cash: int | None = int(INITIAL_KRW),
    t: int = T0 - 60_000,
) -> RunStart:
    return RunStart(
        v=JSONL_VERSION,
        run=run_id,
        script_ver="test",
        seed=1,
        profile=profile,
        plan={
            "executor": "ramping-arrival-rate",
            "time_unit_s": 60,
            "start_rate": 0,
            "stages": [
                {"duration_s": 0, "target": 1800},
                {"duration_s": 600, "target": 1800},
            ],
        },
        symbols=tuple(symbols),
        initial_cash=initial_cash,
        conn_reuse=conn_reuse,
        vus=vus,
        t=t,
    )


def make_log(
    intents: Iterable[Intent],
    results: Iterable[Result],
    *,
    run_id: str = RUN_ID,
    problems: Sequence[str] = (),
    has_token_key: bool = False,
    profile: str = "steady",
    vus: int | None = None,
    with_run_start: bool = True,
) -> K6Log:
    """K6Log. 계좌(accounts)는 K6Log가 intent에서 만든다. vus 기본값은 intent의 VU 수."""
    its = list(intents)
    rss = list(results)
    n_vus = vus if vus is not None else len({it.vu for it in its})
    rstart = (
        make_run_start(run_id=run_id, profile=profile, vus=n_vus)
        if with_run_start
        else None
    )
    kind_counts = {
        "run_start": 1 if with_run_start else 0,
        "intent": len(its),
        "result": len(rss),
    }
    payload = canonical_json({"intents": its, "results": rss})
    return K6Log(
        run_id=run_id,
        run_start=rstart,
        intents={it.key: it for it in its},
        results={r.key: r for r in rss},
        problems=tuple(problems),
        line_count=sum(kind_counts.values()),
        has_token_key=has_token_key,
        sha256=sha256_text(payload),
        kind_counts=kind_counts,
    )


# ---------------------------------------------------------------- DB 스냅샷


def make_account(user_id: str, krw: Any = INITIAL_KRW) -> DbAccount:
    return DbAccount(user_id=user_id, krw=dec(krw))


def make_position(
    user_id: str,
    market: str = MARKET,
    volume: Any = VOLUME,
    avg_price: Any = PRICE,
) -> DbPosition:
    return DbPosition(
        user_id=user_id, market=market, volume=dec(volume), avg_price=dec(avg_price)
    )


def make_order(
    acct: str,
    key: str,
    *,
    order_id: int | None = None,
    n: int = 0,
    body: Mapping[str, Any] | None = None,
    status: str | None = STATUS_FILLED,
    fill_price: Any = PRICE,
    amount: Any = _AUTO,
    filled_us: int | None = FILLED_US,
    created_us: int | None = _AUTO,
    **body_kw: Any,
) -> DbOrder:
    """orders 행. body를 안 주면 default_body(key, acct, **body_kw).

    id 기본값은 order_id_for(key, n). amount 기본값은 fill_amount(fill_price, volume).
    created_us 기본값은 filled_us(서비스는 한 트랜잭션의 now()로 둘 다 쓴다).
    숫자는 DB 열 자릿수로 맞춘다(volume·fill_price 8자리, amount 4자리).
    """
    b = dict(body) if body is not None else default_body(key, acct, **body_kw)
    vol = dec(b.get("volume"))
    price = dec(fill_price)
    if amount is not _AUTO:
        amt = dec(amount)
    elif price is not None and vol is not None:
        amt = fill_amount(price, vol)
    else:
        amt = None
    return DbOrder(
        id=order_id if order_id is not None else order_id_for(key, n),
        client_order_id=b.get("client_order_id", key),
        user_id=b.get("user_id", acct),
        market=b.get("market"),
        side=b.get("side"),
        volume=None if vol is None else vol.quantize(_Q8),
        status=status,
        fill_price=None if price is None else price.quantize(_Q8),
        amount=None if amt is None else amt.quantize(_Q4),
        created_us=filled_us if created_us is _AUTO else created_us,
        filled_us=filled_us,
    )


def make_tick(
    price: Any = PRICE,
    received_us: int = FILLED_US - 1_000_000,
    market: str = MARKET,
) -> DbTick:
    """ticks 행. 기본값은 기본 체결 1초 전, 같은 가격이다(fresh 통과)."""
    return DbTick(market=market, price=dec(price), received_us=received_us)


def replay_positions(
    orders: Iterable[DbOrder],
) -> dict[tuple[str, str], tuple[Decimal, Decimal]]:
    """filled 주문을 id 순으로 서비스 공식대로 재생한다 → (user, market) → (volume, avg_price).

    bid: avg = ((held·avg + v·price) / (held + v)).quantize(1e-8), 기본 context(28자리, HALF_EVEN).
    ask: volume만 줄고 avg는 그대로(0이 돼도 유지).
    """
    state: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}
    with localcontext(Context()):
        for o in sorted(orders, key=lambda o: o.id):
            if o.status != STATUS_FILLED or o.volume is None or o.fill_price is None:
                continue
            k = (o.user_id, o.market)
            held, avg = state.get(k, (Decimal(0), Decimal(0)))
            if o.side == SIDE_BID:
                new_v = held + o.volume
                avg = ((held * avg + o.volume * o.fill_price) / new_v).quantize(_Q8)
            else:
                new_v = held - o.volume
            state[k] = (new_v, avg)
    return state


def replay_krw(orders: Iterable[DbOrder], user_id: str, initial: Any) -> Decimal:
    """krw = initial − Σ(bid amount) + Σ(ask amount), filled 주문만."""
    krw = dec(initial)
    for o in orders:
        if o.user_id != user_id or o.status != STATUS_FILLED or o.amount is None:
            continue
        krw += -o.amount if o.side == SIDE_BID else o.amount
    return krw


# 서비스 스키마(workloads/order/db/schema.sql)를 schema 구획 모양으로 옮긴 것. constraints PASS 기준
_COLUMNS = {
    "accounts": (
        ("user_id", "text not null"),
        ("krw", "numeric(24,4) not null"),
        ("created_at", "timestamp with time zone not null"),
    ),
    "positions": (
        ("user_id", "text not null"),
        ("market", "text not null"),
        ("volume", "numeric(30,8) not null"),
        ("avg_price", "numeric(24,8) not null"),
        ("updated_at", "timestamp with time zone not null"),
    ),
    "orders": (
        ("id", "bigint not null"),
        ("client_order_id", "text not null"),
        ("user_id", "text not null"),
        ("market", "text not null"),
        ("side", "text not null"),
        ("volume", "numeric(30,8) not null"),
        ("status", "text not null"),
        ("fill_price", "numeric(24,8)"),
        ("amount", "numeric(24,4)"),
        ("created_at", "timestamp with time zone not null"),
        ("filled_at", "timestamp with time zone"),
    ),
    "ticks": (
        ("id", "bigint not null"),
        ("market", "text not null"),
        ("price", "numeric not null"),
        ("received_at", "timestamp with time zone not null"),
        ("source_ts", "bigint"),
        ("event_id", "uuid"),
    ),
}
_CONSTRAINTS = (
    ("accounts", "accounts_pkey", "PRIMARY KEY (user_id)"),
    ("accounts", "accounts_krw_check", "CHECK ((krw >= (0)::numeric))"),
    ("positions", "positions_pkey", "PRIMARY KEY (user_id, market)"),
    (
        "positions",
        "positions_user_id_fkey",
        "FOREIGN KEY (user_id) REFERENCES accounts(user_id)",
    ),
    ("positions", "positions_volume_check", "CHECK ((volume >= (0)::numeric))"),
    (
        "positions",
        "positions_avg_price_check",
        "CHECK ((avg_price >= (0)::numeric))",
    ),
    ("orders", "orders_pkey", "PRIMARY KEY (id)"),
    ("orders", "orders_client_order_id_key", "UNIQUE (client_order_id)"),
    (
        "orders",
        "orders_user_id_fkey",
        "FOREIGN KEY (user_id) REFERENCES accounts(user_id)",
    ),
    (
        "orders",
        "orders_side_check",
        "CHECK ((side = ANY (ARRAY['bid'::text, 'ask'::text])))",
    ),
    ("orders", "orders_volume_check", "CHECK ((volume > (0)::numeric))"),
    (
        "orders",
        "orders_status_check",
        "CHECK ((status = ANY (ARRAY['accepted'::text, 'pending'::text, "
        "'filled'::text, 'rejected'::text])))",
    ),
    (
        "orders",
        "orders_check",
        "CHECK (((status <> 'filled'::text) OR ((fill_price IS NOT NULL) AND "
        "(amount IS NOT NULL) AND (filled_at IS NOT NULL))))",
    ),
    ("ticks", "ticks_event_id_key", "UNIQUE (event_id)"),
    ("ticks", "ticks_pkey", "PRIMARY KEY (id)"),
)


def default_schema() -> tuple[SchemaItem, ...]:
    """서비스 스키마의 schema 구획 행(table → column → constraint, 이름 순)."""
    items = [SchemaItem(kind="table", table=t, name=t) for t in sorted(_COLUMNS)]
    items += [
        SchemaItem(kind="column", table=t, name=c, detail=d)
        for t in sorted(_COLUMNS)
        for c, d in _COLUMNS[t]
    ]
    items += [
        SchemaItem(kind="constraint", table=t, name=n, detail=d)
        for t, n, d in sorted(_CONSTRAINTS)
    ]
    return tuple(items)


def make_db(
    accounts: Sequence[str | DbAccount] | None = None,
    orders: Sequence[DbOrder] = (),
    positions: Sequence[DbPosition] | None = None,
    ticks: Sequence[DbTick] = (),
    *,
    extra_accounts: Sequence[str] = (),
    initial_krw: Any = INITIAL_KRW,
    schema: Sequence[SchemaItem] | None = None,
    fingerprint2: Fingerprint | None | Any = _AUTO,
    sections: Iterable[str] | None = None,
    invariant_rows: Mapping[str, Sequence[Mapping[str, str | None]]] | None = None,
    errors: Sequence[str] = (),
) -> DbSnapshot:
    """DbSnapshot. 기본값이면 모든 불변식이 맞는다.

    accounts: None이면 주문 계좌(처음 나온 순서) + extra_accounts. 문자열 원소는 krw를
      replay_krw(initial_krw)로 계산한 행, DbAccount는 그대로 쓴다.
    positions: None이면 replay_positions(orders)로 계산한다. 목록을 주면 그대로 쓴다.
    schema 기본값은 default_schema(). fingerprint1은 compute_fingerprint, fingerprint2 기본값도 같다.
    sections 기본값: SNAPSHOT_SECTIONS 전부 + 모든 "inv_<id>"(INVARIANTS). invariant_rows가
      비어 있으면 SQL 불변식 위반 0이다.
    행은 키 순(accounts·positions), id 순(orders), 시각 순(ticks)으로 정렬해 둔다.
    """
    if accounts is None:
        ids: list[str] = []
        for o in orders:
            if o.user_id not in ids:
                ids.append(o.user_id)
        accounts = ids + [a for a in extra_accounts if a not in ids]
    accs = [
        a
        if isinstance(a, DbAccount)
        else make_account(a, krw=replay_krw(orders, a, initial_krw))
        for a in accounts
    ]
    if positions is None:
        positions = [
            DbPosition(user_id=u, market=m, volume=v, avg_price=p)
            for (u, m), (v, p) in replay_positions(orders).items()
        ]
    accs.sort(key=lambda a: a.user_id)
    poss = sorted(positions, key=lambda p: (p.user_id, p.market))
    ords = sorted(orders, key=lambda o: o.id)
    tks = sorted(ticks, key=lambda t: (t.market, t.received_us or 0))
    fp1 = compute_fingerprint(accs, poss, ords)
    fp2 = fp1 if fingerprint2 is _AUTO else fingerprint2
    secs = (
        frozenset(sections)
        if sections is not None
        else frozenset(SNAPSHOT_SECTIONS) | {f"inv_{k}" for k in INVARIANTS}
    )
    return DbSnapshot(
        accounts=tuple(accs),
        positions=tuple(poss),
        orders=tuple(ords),
        ticks=tuple(tks),
        schema=tuple(schema) if schema is not None else default_schema(),
        fingerprint1=fp1 if "fingerprint1" in secs else None,
        fingerprint2=fp2 if "fingerprint2" in secs else None,
        sections=secs,
        invariant_rows={k: tuple(v) for k, v in (invariant_rows or {}).items()},
        errors=tuple(errors),
    )

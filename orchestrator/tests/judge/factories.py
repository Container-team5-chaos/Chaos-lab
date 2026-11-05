"""테스트용 빌더. 골든 테스트가 K6Log·DbSnapshot을 파서 없이 직접 만든다.

기본값은 서로 맞물린다: make_intent 본문 = make_order 본문, make_result의 ACK 응답 =
make_order(FILLED) + make_fill 결과. 그래서 아무것도 바꾸지 않으면 OK(clean) 한 건이다.

    acct = ACCT_A
    it = make_intent("k1", acct)
    rs = make_result("k1", atts=[(201, None, 12)])
    log = make_log([it], [rs])
    o = make_order(acct, "k1")
    db = make_db(orders=[o], fills=[make_fill(o)])
"""

from __future__ import annotations

import uuid
from dataclasses import replace
from collections.abc import Iterable, Mapping, Sequence
from decimal import ROUND_CEILING, ROUND_HALF_UP, Decimal
from typing import Any

from orchestrator.judge.model import (
    INVARIANT_SQL,
    SNAPSHOT_SECTIONS,
    AccountRec,
    Ack,
    Attempt,
    DbAccount,
    DbFill,
    DbOrder,
    DbPosition,
    DbSnapshot,
    Intent,
    K6Log,
    Lookup,
    Resend,
    Result,
    RunStart,
    SetupDone,
    canonical_json,
    dec,
    sha256_text,
)

RUN_ID = "r-test"
T0 = 1_762_300_000_000  # 2025-11-05 epoch ms. 테스트 시각 기준점
SYMBOL = "REPLAY:UPBIT:KRW-BTC"
PRICE = "100000000"  # 기준가
QTY = "0.00010000"  # floor8(10,000원 ÷ PRICE)
LIMIT_BUY = "105000000"  # PRICE × 1.05
FEE_RATE = "0.0005"
INITIAL_CASH = 10_000_000

# 결정적 계좌 UUID
ACCT_A = "00000000-0000-4000-8000-00000000000a"
ACCT_B = "00000000-0000-4000-8000-00000000000b"
ACCT_C = "00000000-0000-4000-8000-00000000000c"
ACCT_EXTRA = "00000000-0000-4000-8000-0000000000ee"

_NS = uuid.UUID("6f1c2a52-3e1b-4d7e-9a50-000000000000")
_AUTO: Any = object()

AttSpec = tuple  # (st,) | (st, ec) | (st, ec, ms)
_ATT_DEFAULTS = (None, None, 10)


def default_body(
    *,
    symbol: str = SYMBOL,
    side: str = "buy",
    qty: str = QTY,
    limit_price: str = LIMIT_BUY,
) -> dict[str, str]:
    return {"symbol": symbol, "side": side, "qty": qty, "limit_price": limit_price}


def response_raw(
    status: str = "FILLED",
    reason: str | None = None,
    price: str | None = _AUTO,
    fee: int | None = _AUTO,
    **extra: Any,
) -> str:
    """주문 응답 본문(JSON 텍스트). 키는 기본 response_fields 매핑을 따른다.

    {status: status, reject_reason: reason, price: price, fee: fee}
    FILLED 기본값은 make_fill 기본 체결(가격 PRICE, 그 수수료)이다. REJECTED 기본값은 null.
    """
    filled = status == "FILLED"
    if price is _AUTO:
        price = PRICE if filled else None
    if fee is _AUTO:
        fee = _fee(_amount(dec(QTY), dec(price)), dec(FEE_RATE)) if filled else None
    body: dict[str, Any] = {
        "status": status,
        "reason": reason,
        "price": price,
        "fee": fee,
    }
    body.update(extra)
    return canonical_json(body)


# ---------------------------------------------------------------- k6 기록


def make_intent(
    key: str,
    acct: str,
    *,
    body: Mapping[str, Any] | None = None,
    vu: int = 1,
    t: int = T0,
    **body_kw: Any,
) -> Intent:
    """intent 한 줄. body를 안 주면 default_body(**body_kw)."""
    b = dict(body) if body is not None else default_body(**body_kw)
    return Intent(key=key, acct=acct, vu=vu, t=t, body=b, body_raw=canonical_json(b))


def make_attempts(
    specs: Iterable[AttSpec | Attempt], *, t: int = T0
) -> tuple[Attempt, ...]:
    """(st, ec, ms) 목록 → Attempt들. n은 1부터, t0는 앞 시도 끝 뒤로 이어 붙인다."""
    out: list[Attempt] = []
    cur = t
    for i, s in enumerate(specs, start=1):
        if isinstance(s, Attempt):
            out.append(s)
            cur = s.t1
            continue
        st, ec, ms = tuple(s) + _ATT_DEFAULTS[len(s) :]
        t0 = cur
        t1 = t0 + int(ms)
        out.append(Attempt(n=i, t0=t0, t1=t1, st=st, ec=ec, ms=float(ms)))
        cur = t1 + 1000  # 재시도 간격 흉내
    return tuple(out)


def make_lookup(
    st: int = 200,
    raw: str | None = None,
    *,
    atts: Sequence[AttSpec | Attempt] | None = None,
    t: int = T0 + 20_000,
) -> Lookup:
    """멱등키 조회. raw 기본값: 200이면 response_raw(), 그 밖은 None."""
    if raw is None and st == 200:
        raw = response_raw()
    a = make_attempts(atts if atts is not None else [(st, None, 8)], t=t)
    return Lookup(atts=a, st=st, raw=raw)


def make_resend(
    atts: Sequence[AttSpec | Attempt] = ((200, None, 5),),
    raw: str | None = None,
    *,
    t: int = T0 + 30_000,
) -> Resend:
    """재송신. ack는 첫 2xx 시도(raw 기본 response_raw()). 2xx가 없으면 None."""
    a = make_attempts(atts, t=t)
    first = next((x for x in a if x.is_ack), None)
    ack = (
        None
        if first is None
        else Ack(st=first.st, raw=raw if raw is not None else response_raw())
    )
    return Resend(atts=a, ack=ack)


def make_result(
    key: str,
    atts: Sequence[AttSpec | Attempt] = ((201, None, 10),),
    *,
    lookup: Lookup | int | None = None,
    resend: Resend | Sequence[AttSpec] | None = None,
    ack: Ack | None = _AUTO,
    raw: str | None = None,
    t: int | None = None,
    t_start: int = T0,
) -> Result:
    """result 한 줄.

    atts: (st, ec, ms) 목록. ack를 안 주면 첫 200·201 시도로 Ack(st, raw or response_raw()).
    lookup: Lookup 또는 상태 코드 정수(make_lookup(st)).
    resend: Resend 또는 (st, ec, ms) 목록(make_resend(...)).
    """
    a = make_attempts(atts, t=t_start)
    if ack is _AUTO:
        first = next((x for x in a if x.is_ack), None)
        ack = (
            None
            if first is None
            else Ack(st=first.st, raw=raw if raw is not None else response_raw())
        )
    lk = make_lookup(lookup) if isinstance(lookup, int) else lookup
    rs = (
        resend
        if (resend is None or isinstance(resend, Resend))
        else make_resend(resend)
    )
    end = t if t is not None else (a[-1].t1 if a else t_start) + 100
    return Result(key=key, atts=a, lookup=lk, ack=ack, resend=rs, t=end)


def make_log(
    intents: Iterable[Intent],
    results: Iterable[Result],
    accounts: Sequence[str | AccountRec] | None = None,
    *,
    conn_reuse: bool | None = False,
    run_id: str = RUN_ID,
    problems: Sequence[str] = (),
    has_token_key: bool = False,
    initial_cash: int | None = INITIAL_CASH,
    profile: str = "steady",
    symbols: Sequence[str] = (SYMBOL,),
    with_run_start: bool = True,
) -> K6Log:
    """K6Log. accounts를 안 주면 intent 계좌를 처음 나온 순서로 idx 0..n-1에 둔다."""
    its = list(intents)
    rss = list(results)
    if accounts is None:
        seen: list[str] = []
        for it in its:
            if it.acct not in seen:
                seen.append(it.acct)
        accounts = seen
    recs = tuple(
        a
        if isinstance(a, AccountRec)
        else AccountRec(idx=i, acct=a, nick=f"k6-{run_id}-{i}")
        for i, a in enumerate(accounts)
    )
    rstart = (
        RunStart(
            v=1,
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
            t=T0 - 60_000,
        )
        if with_run_start
        else None
    )
    kind_counts = {
        "run_start": 1 if with_run_start else 0,
        "account": len(recs),
        "setup_done": 1,
        "intent": len(its),
        "result": len(rss),
    }
    payload = canonical_json({"intents": its, "results": rss, "accounts": recs})
    return K6Log(
        run_id=run_id,
        run_start=rstart,
        accounts=recs,
        setup_done=SetupDone(accounts=len(recs), t=T0 - 1000),
        intents={it.key: it for it in its},
        results={r.key: r for r in rss},
        problems=tuple(problems),
        line_count=sum(kind_counts.values()),
        has_token_key=has_token_key,
        sha256=sha256_text(payload),
        kind_counts=kind_counts,
    )


# ---------------------------------------------------------------- DB 스냅샷


def make_account(
    account_id: str,
    *,
    nickname: str | None = None,
    mode: str = "replay",
    initial_cash: int = INITIAL_CASH,
    cash: int | None = None,
    version: int = 0,
    k6: bool = True,
    idx: int = 0,
) -> DbAccount:
    return DbAccount(
        id=account_id,
        nickname=nickname if nickname is not None else f"k6-{RUN_ID}-{idx}",
        mode=mode,
        initial_cash=initial_cash,
        cash=initial_cash if cash is None else cash,
        version=version,
        k6=k6,
    )


def make_order(
    account_id: str,
    idem_key: str,
    *,
    body: Mapping[str, Any] | None = None,
    status: str = "FILLED",
    reject_reason: str | None = None,
    order_id: str | None = None,
    n: int = 0,
    ref_price: str | None = PRICE,
    ref_age_ms: int | None = 500,
    received_ms: int | None = T0 + 5,
    ref_source: str | None = "replay",
    body_hash: str | None = None,
    **body_kw: Any,
) -> DbOrder:
    """orders 행. body를 안 주면 default_body(**body_kw). id는 (계좌, 키, n)의 uuid5."""
    b = dict(body) if body is not None else default_body(**body_kw)
    oid = order_id or str(uuid.uuid5(_NS, f"{account_id}/{idem_key}/{n}"))
    ev = None if received_ms is None or ref_age_ms is None else received_ms - ref_age_ms
    return DbOrder(
        id=oid,
        account_id=account_id,
        idem_key=idem_key,
        body_hash=body_hash,
        symbol=b.get("symbol"),
        side=b.get("side"),
        qty=dec(b.get("qty")),
        limit_price=dec(b.get("limit_price")),
        status=status,
        reject_reason=reject_reason,
        ref_source=ref_source,
        ref_seq="1",
        ref_event_ts=ev,
        ref_recv_ts=ev,
        ref_price=dec(ref_price),
        ref_age_ms=ref_age_ms,
        received_at=None,
        received_ms=received_ms,
    )


def _amount(qty: Decimal, price: Decimal) -> int:
    return int((qty * price).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _fee(amount: int, rate: Decimal) -> int:
    return int((Decimal(amount) * rate).to_integral_value(rounding=ROUND_CEILING))


def make_fill(
    order: DbOrder,
    *,
    price: str | Decimal | None = None,
    fee: int | None = None,
    amount: int | None = None,
    fee_rate: str = FEE_RATE,
    realized_pnl: int | None = None,
    fill_id: int = 0,
    qty: str | Decimal | None = None,
) -> DbFill:
    """fills 행. 기본값: 가격 = order.ref_price, 금액 = round(qty×price), 수수료 = ceil(금액×요율).

    매수 실현손익 기본값은 −수수료, 매도는 금액 − 수수료(원가 0 가정, 필요하면 직접 준다).
    fill_id 0은 make_db가 1부터 차례로 번호를 매긴다.
    """
    p = dec(price) if price is not None else order.ref_price
    q = dec(qty) if qty is not None else order.qty
    amt = amount if amount is not None else _amount(q, p)
    f = fee if fee is not None else _fee(amt, dec(fee_rate))
    if realized_pnl is None:
        realized_pnl = -f if order.side == "buy" else amt - f
    return DbFill(
        id=fill_id,
        order_id=order.id,
        account_id=order.account_id,
        symbol=order.symbol,
        side=order.side,
        qty=q,
        price=p,
        amount=amt,
        fee=f,
        fee_rate=dec(fee_rate),
        realized_pnl=realized_pnl,
        created_ms=order.received_ms,
    )


def make_db(
    accounts: Sequence[str | DbAccount] | None = None,
    orders: Sequence[DbOrder] = (),
    fills: Sequence[DbFill] = (),
    positions: Sequence[DbPosition] = (),
    *,
    extra_accounts: Sequence[str] = (),
    fingerprint2: tuple[int, int, int] | None | Any = _AUTO,
    sections: Iterable[str] | None = None,
    invariant_rows: Mapping[str, Sequence[Mapping[str, str]]] | None = None,
) -> DbSnapshot:
    """DbSnapshot. accounts를 안 주면 주문 계좌를 처음 나온 순서로 k6 계좌로 만든다.

    extra_accounts: k6=False인 extra 계좌 id.
    fingerprint1은 (주문 수, 체결 수, version 합)으로 계산하고 fingerprint2 기본값도 같다.
    sections 기본값: SNAPSHOT_SECTIONS 전부 + 모든 "inv_<name>"(INVARIANT_SQL).
    invariant_rows 기본값은 비어 있어 모든 불변식이 위반 0(PASS)이다.
    """
    if accounts is None:
        ids: list[str] = []
        for o in orders:
            if o.account_id not in ids and o.account_id not in extra_accounts:
                ids.append(o.account_id)
        accounts = ids
    accs = [
        a if isinstance(a, DbAccount) else make_account(a, idx=i)
        for i, a in enumerate(accounts)
    ]
    accs += [make_account(a, k6=False, nickname="presenter") for a in extra_accounts]

    numbered: list[DbFill] = []
    next_id = 1
    for f in fills:
        if f.id == 0:
            f = replace(f, id=next_id)
        next_id = max(next_id, f.id) + 1
        numbered.append(f)

    fp1 = (len(orders), len(numbered), sum(a.version or 0 for a in accs))
    fp2 = fp1 if fingerprint2 is _AUTO else fingerprint2
    rows = {k: tuple(v) for k, v in (invariant_rows or {}).items()}
    secs = (
        frozenset(sections)
        if sections is not None
        else frozenset(SNAPSHOT_SECTIONS) | {f"inv_{k}" for k in INVARIANT_SQL}
    )
    return DbSnapshot(
        accounts=tuple(accs),
        orders=tuple(orders),
        fills=tuple(numbered),
        positions=tuple(positions),
        fingerprint1=fp1,
        fingerprint2=fp2,
        sections=secs,
        invariant_rows=rows,
    )

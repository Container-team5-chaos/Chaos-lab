"""포지션 조회: 보유 수량·평균 매수가에 Redis 현재가를 붙여 평가금액과 손익을 계산한다.

잔고·예약은 get_account()의 같은 DB 스냅샷을 그대로 쓴다.
시세가 없거나 오래되면 그 마켓의 평가 필드는 null로 두고 price_status에 이유를 적는다.
(주문 체결과 같은 STALE_MS 기준. 오래된 가격으로 손익을 보여주지 않는다.)
"""

from decimal import ROUND_DOWN, Decimal

from redis.exceptions import RedisError

from .orders import Rejected, get_account, get_price
from .schemas import dec

KRW = Decimal("0.0001")  # 금액 소수 자릿수 (accounts.krw, orders.amount와 같음)
RATE = Decimal("0.01")  # 수익률 % 소수 자릿수


async def current_price(redis, market: str) -> tuple[Decimal | None, str]:
    """(가격, 상태). 상태는 ok / stale_price / price_unavailable."""
    try:
        return await get_price(redis, market), "ok"
    except Rejected as r:
        return None, r.reason
    except RedisError:
        # Redis 장애여도 잔고·보유 수량은 DB에서 보여준다
        return None, "price_unavailable"


def evaluate(volume: Decimal, avg_price: Decimal, price: Decimal | None) -> dict:
    if price is None:
        return {"valuation": None, "pnl": None, "pnl_rate": None}
    valuation = (price * volume).quantize(KRW, ROUND_DOWN)
    pnl = ((price - avg_price) * volume).quantize(KRW, ROUND_DOWN)
    rate = (
        ((price / avg_price - 1) * 100).quantize(RATE, ROUND_DOWN)
        if avg_price > 0
        else None
    )
    return {"valuation": valuation, "pnl": pnl, "pnl_rate": rate}


async def get_positions(pool, redis, user_id: str) -> dict:
    acc, positions = await get_account(pool, user_id)  # 없는 사용자는 Rejected(404)

    items = []
    total_valuation = Decimal(0)
    priced_all = True
    for p in positions:
        price, status = await current_price(redis, p["market"])
        ev = evaluate(p["volume"], p["avg_price"], price)
        if ev["valuation"] is None:
            priced_all = False
        else:
            total_valuation += ev["valuation"]
        items.append(
            {
                "market": p["market"],
                "volume": dec(p["volume"]),
                "avg_price": dec(p["avg_price"]),
                "reserved_volume": dec(p["reserved_volume"]),
                "available_volume": dec(p["volume"] - p["reserved_volume"]),
                "current_price": dec(price),
                "price_status": status,
                "valuation": dec(ev["valuation"]),
                "pnl": dec(ev["pnl"]),
                "pnl_rate": dec(ev["pnl_rate"]),
            }
        )

    krw = acc["krw"]
    return {
        "user_id": user_id,
        "krw": dec(krw),
        "reserved_krw": dec(acc["reserved_krw"]),
        "available_krw": dec(krw - acc["reserved_krw"]),
        "positions": items,
        # 한 마켓이라도 가격을 못 읽으면 합계는 null (일부만 더한 값을 총자산처럼 보이지 않게)
        "total_valuation": dec(total_valuation) if priced_all else None,
        "total_assets": dec(krw + total_valuation) if priced_all else None,
    }

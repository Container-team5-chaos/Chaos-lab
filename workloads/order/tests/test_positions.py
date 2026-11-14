"""포지션 조회 API: 평가금액·손익 계산과 시세 지연 처리."""

import pytest

from .conftest import delete_price, set_price

pytestmark = pytest.mark.asyncio


def order(cid, side="bid", volume="0.001", user="user-0001"):
    return {
        "client_order_id": cid,
        "user_id": user,
        "market": "KRW-BTC",
        "side": side,
        "volume": volume,
    }


async def test_보유가_없으면_빈_목록이고_총자산은_원화(client):
    r = await client.get("/accounts/user-0001/positions")
    assert r.status_code == 200
    body = r.json()
    assert body["positions"] == []
    assert body["krw"] == "10000000.0000"
    assert body["total_valuation"] == "0"
    assert body["total_assets"] == "10000000.0000"


async def test_가격이_오르면_평가금액과_손익이_늘어난다(client):
    await client.post("/orders", json=order("p-1", volume="0.01"))  # 98,000,000에 매수
    await set_price("99000000")

    body = (await client.get("/accounts/user-0001/positions")).json()
    p = body["positions"][0]
    assert p["market"] == "KRW-BTC"
    assert p["volume"] == "0.01000000"
    assert p["avg_price"] == "98000000.00000000"
    assert p["current_price"] == "99000000"
    assert p["price_status"] == "ok"
    assert p["valuation"] == "990000.0000"
    assert p["pnl"] == "10000.0000"
    assert p["pnl_rate"] == "1.02"
    assert p["available_volume"] == "0.01000000"
    # 원화 9,020,000 + 평가 990,000
    assert body["total_assets"] == "10010000.0000"


async def test_가격이_내리면_손익은_음수(client):
    await client.post("/orders", json=order("p-2", volume="0.01"))
    await set_price("97000000")

    p = (await client.get("/accounts/user-0001/positions")).json()["positions"][0]
    assert p["pnl"] == "-10000.0000"
    assert p["pnl_rate"] == "-1.02"


async def test_시세가_오래되면_평가는_null_보유량은_그대로(client):
    await client.post("/orders", json=order("p-3"))
    await set_price(age_ms=60_000)

    r = await client.get("/accounts/user-0001/positions")
    assert r.status_code == 200
    body = r.json()
    p = body["positions"][0]
    assert p["volume"] == "0.00100000"
    assert p["price_status"] == "stale_price"
    assert p["current_price"] is None and p["valuation"] is None and p["pnl"] is None
    assert body["total_assets"] is None
    assert body["krw"] == "9902000.0000"


async def test_시세가_없으면_price_unavailable(client):
    await client.post("/orders", json=order("p-4"))
    await delete_price()

    p = (await client.get("/accounts/user-0001/positions")).json()["positions"][0]
    assert p["price_status"] == "price_unavailable"
    assert p["valuation"] is None


async def test_전량_매도하면_포지션이_사라진다(client):
    await client.post("/orders", json=order("p-5"))
    await client.post("/orders", json=order("p-6", side="ask"))

    body = (await client.get("/accounts/user-0001/positions")).json()
    assert body["positions"] == []


async def test_없는_사용자는_404(client):
    r = await client.get("/accounts/nobody/positions")
    assert r.status_code == 404
    assert r.json()["reason"] == "user_not_found"

"""API와 정합성 규칙을 확인하는 테스트."""

import asyncio
from decimal import Decimal

import pytest

from .conftest import delete_price, set_price

pytestmark = pytest.mark.asyncio


def order(cid="t-1", user="user-0001", side="bid", volume="0.001"):
    return {
        "client_order_id": cid,
        "user_id": user,
        "market": "KRW-BTC",
        "side": side,
        "volume": volume,
    }


async def krw(db, user="user-0001") -> Decimal:
    return await db.fetchval("SELECT krw FROM accounts WHERE user_id = $1", user)


async def test_매수하면_잔고가_줄고_보유량이_생긴다(client, db):
    r = await client.post("/orders", json=order())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "filled" and body["replayed"] is False
    assert body["amount"] == "98000.0000"
    assert await krw(db) == Decimal("9902000")

    acc = (await client.get("/accounts/user-0001")).json()
    assert acc["positions"][0]["volume"] == "0.00100000"


async def test_같은_주문을_다시_보내면_처음_결과를_돌려주고_잔고는_그대로(client, db):
    first = (await client.post("/orders", json=order())).json()
    second = await client.post("/orders", json=order())
    assert second.status_code == 200
    assert second.json()["replayed"] is True
    assert second.json()["order_id"] == first["order_id"]
    assert await krw(db) == Decimal("9902000")


async def test_같은_번호인데_내용이_다르면_409(client):
    await client.post("/orders", json=order())
    r = await client.post("/orders", json=order(volume="0.002"))
    assert r.status_code == 409
    assert r.json()["reason"] == "idempotency_conflict"


async def test_같은_주문이_동시에_20번_와도_1건만_체결(client, db):
    results = await asyncio.gather(
        *[client.post("/orders", json=order()) for _ in range(20)]
    )
    assert all(r.status_code == 200 for r in results)
    assert await db.fetchval("SELECT count(*) FROM orders") == 1
    assert await krw(db) == Decimal("9902000")


async def test_동시_주문이_몰려도_잔고는_음수가_안_되고_합계가_맞는다(client, db):
    # 0.02 BTC = 196만 원씩 10건 → 잔고 1,000만 원으로는 5건만 가능
    reqs = [order(cid=f"c-{i}", volume="0.02") for i in range(10)]
    results = await asyncio.gather(*[client.post("/orders", json=r) for r in reqs])
    filled = [r for r in results if r.status_code == 200]
    rejected = [r for r in results if r.status_code == 422]
    assert len(filled) == 5 and len(rejected) == 5

    balance = await krw(db)
    spent = await db.fetchval(
        "SELECT sum(amount) FROM orders WHERE user_id = 'user-0001'"
    )
    assert balance >= 0
    assert balance + spent == Decimal("10000000")  # 잔고 불변식


async def test_보유량보다_많이_팔면_422(client):
    await client.post("/orders", json=order())
    r = await client.post("/orders", json=order(cid="t-2", side="ask", volume="0.002"))
    assert r.status_code == 422
    assert r.json()["reason"] == "insufficient_volume"


async def test_매도하면_원화가_늘고_평균가는_그대로(client, db):
    await client.post("/orders", json=order(volume="0.002"))
    await set_price("100000000")
    r = await client.post("/orders", json=order(cid="t-2", side="ask", volume="0.001"))
    assert r.status_code == 200
    assert await krw(db) == Decimal("10000000") - Decimal("196000") + Decimal("100000")
    acc = (await client.get("/accounts/user-0001")).json()
    assert acc["positions"][0]["avg_price"] == "98000000.00000000"


async def test_오래된_시세면_503_stale_price(client):
    await set_price(age_ms=10_000)
    r = await client.post("/orders", json=order())
    assert r.status_code == 503
    assert r.json()["reason"] == "stale_price"


async def test_시세가_없으면_503_price_unavailable(client):
    await delete_price()
    r = await client.post("/orders", json=order())
    assert r.status_code == 503
    assert r.json()["reason"] == "price_unavailable"


async def test_시세_지연으로_거절된_주문은_같은_번호로_재시도하면_체결(client):
    await set_price(age_ms=10_000)
    assert (await client.post("/orders", json=order())).status_code == 503
    await set_price()
    r = await client.post("/orders", json=order())
    assert r.status_code == 200 and r.json()["replayed"] is False


async def test_잘못된_요청은_400(client):
    r = await client.post("/orders", json=order(volume="-1"))
    assert r.status_code == 400
    assert r.json()["reason"] == "invalid_request"


async def test_없는_사용자는_404(client):
    r = await client.post("/orders", json=order(user="nobody"))
    assert r.status_code == 404


async def test_상태_확인과_지표(client):
    assert (await client.get("/healthz")).status_code == 200
    assert (await client.get("/readyz")).status_code == 200
    await client.post("/orders", json=order())
    metrics = (await client.get("/metrics/")).text
    assert 'orders_total{reason="",status="filled"}' in metrics

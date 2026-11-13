from decimal import Decimal

import httpx
import pytest

from app.adapter_client import AdapterClient, Uncertain, validate_receipt


def request_payload():
    return {
        "client_order_id": "external-1",
        "user_id": "user-0001",
        "market": "KRW-BTC",
        "side": "bid",
        "volume": "0.00100000",
        "max_amount": "98000.0000",
    }


def receipt(payload=None):
    payload = payload or request_payload()
    return {
        **payload,
        "status": "filled",
        "result": {
            **payload,
            "status": "filled",
            "broker_order_id": "broker-1",
            "fill_price": "97000000.00000000",
            "amount": "97000.0000",
            "filled_at": "2026-11-13T09:00:00Z",
        },
    }


def test_exact_fill_and_budget_validation():
    result = validate_receipt(receipt(), request_payload())
    assert result["amount"] == Decimal("97000.0000")
    assert result["filled_at"].utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    "change",
    [
        {"fill_price": "NaN"},
        {"amount": "98001.0000"},
        {"volume": "0.002"},
        {"amount": "97000.0001"},
        {"filled_at": "2026-11-13T09:00:00"},
        {"broker_order_id": ""},
        {"status": "rejected"},
        {"user_id": "other"},
    ],
)
def test_invalid_or_over_budget_fill_is_uncertain(change):
    body = receipt()
    body["result"].update(change)
    with pytest.raises(Uncertain):
        validate_receipt(body, request_payload())


async def test_adapter_timeout_retries_by_lookup_not_new_key():
    calls = []

    async def upstream(request):
        calls.append((request.method, request.url.path))
        if len(calls) == 1:
            return httpx.Response(404)
        if request.method == "POST":
            raise httpx.ReadTimeout("response lost")
        return httpx.Response(200, json=receipt())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://adapter"
    ) as client:
        adapter = AdapterClient(client)
        assert await adapter.reconcile(request_payload()) is None
        assert (await adapter.reconcile(request_payload()))["status"] == "filled"
    assert [method for method, _ in calls] == ["GET", "POST", "GET"]


async def test_adapter_conflict_is_not_a_business_rejection():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(409)),
        base_url="http://adapter",
    ) as client:
        with pytest.raises(Uncertain, match="adapter_contract_error"):
            await AdapterClient(client).reconcile(request_payload())

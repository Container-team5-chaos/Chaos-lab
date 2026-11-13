import httpx
import pytest

from adapter.broker import Broker, RateLimiter, Retry
from adapter.models import OrderRequest

from .conftest import filled


@pytest.mark.parametrize("budget", ["NaN", "-1", "1e20", "0.00001", 98000])
def test_invalid_budget(payload, budget):
    with pytest.raises(ValueError):
        OrderRequest.model_validate({**payload, "max_amount": budget})


async def test_budget_is_forwarded_and_echoed(payload):
    payload = {**payload, "max_amount": "98000.0000"}
    calls = []

    def upstream(request):
        calls.append(request)
        return (
            httpx.Response(404)
            if request.method == "GET"
            else httpx.Response(200, json=filled(payload))
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://broker.test"
    ) as client:
        result = await Broker(client, RateLimiter(10000)).reconcile(payload)
    assert result.max_amount == "98000.0000"
    assert b'"max_amount"' in calls[1].content


@pytest.mark.parametrize(
    "change", [{"max_amount": None}, {"max_amount": "99000"}, {"amount": "99000"}]
)
async def test_missing_or_violated_budget_is_not_confirmed(payload, change):
    payload = {**payload, "max_amount": "98000.0000"}
    result = {**filled(payload), **change}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=result)),
        base_url="http://broker.test",
    ) as client:
        with pytest.raises(Retry):
            await Broker(client, RateLimiter(10000)).reconcile(payload)

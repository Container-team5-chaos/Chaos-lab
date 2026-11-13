import asyncio
from datetime import datetime
from decimal import ROUND_DOWN, Decimal, InvalidOperation, localcontext
from urllib.parse import quote

import httpx


class Uncertain(Exception):
    """확정 거절로 처리하면 안 되는 외부 상태/계약 오류."""


def decimal_value(value, *, positive=False, scale=8, bound="1e16"):
    if not isinstance(value, str) or len(value) > 128:
        raise ValueError("invalid decimal string")
    number = Decimal(value)
    if (
        not number.is_finite()
        or number < 0
        or (positive and number == 0)
        or number >= Decimal(bound)
        or number.as_tuple().exponent < -scale
    ):
        raise ValueError("invalid decimal range")
    return number


def validate_receipt(body, payload):
    try:
        for key in ("client_order_id", "user_id", "market", "side"):
            if body[key] != payload[key]:
                raise ValueError("order mismatch")
        if decimal_value(body["volume"], positive=True, bound="1e22") != Decimal(
            payload["volume"]
        ):
            raise ValueError("volume mismatch")
        state = body["status"]
        if state not in {"pending", "filled", "rejected", "unknown"}:
            raise ValueError("unsupported status")
        if payload.get("max_amount") is not None and Decimal(
            body["max_amount"]
        ) != Decimal(payload["max_amount"]):
            raise ValueError("budget mismatch")
        if state in {"pending", "unknown"}:
            return {
                "status": state,
                "reason": "broker_unknown" if state == "unknown" else None,
            }
        result = body["result"]
        for key in ("client_order_id", "user_id", "market", "side"):
            if result[key] != payload[key]:
                raise ValueError("result mismatch")
        if decimal_value(result["volume"], positive=True, bound="1e22") != Decimal(
            payload["volume"]
        ):
            raise ValueError("result volume mismatch")
        if result["status"] != state:
            raise ValueError("result status mismatch")
        if state == "rejected":
            reason = result["reason"]
            if not isinstance(reason, str) or not 1 <= len(reason) <= 128:
                raise ValueError("missing rejection reason")
            return {"status": state, "reason": reason}
        price = decimal_value(result["fill_price"], positive=True)
        amount = decimal_value(result["amount"], scale=4, bound="1e20")
        with localcontext() as ctx:
            ctx.prec = 64
            expected = (price * Decimal(payload["volume"])).quantize(
                Decimal("0.0001"), rounding=ROUND_DOWN
            )
        if amount != expected or (
            payload.get("max_amount") is not None
            and amount > Decimal(payload["max_amount"])
        ):
            raise ValueError("invalid fill amount")
        filled_at = datetime.fromisoformat(result["filled_at"].replace("Z", "+00:00"))
        if (
            filled_at.tzinfo is None
            or not isinstance(result["broker_order_id"], str)
            or not result["broker_order_id"]
        ):
            raise ValueError("missing fill evidence")
        return {
            "status": state,
            "price": price,
            "amount": amount,
            "filled_at": filled_at,
        }
    except (KeyError, ValueError, TypeError, InvalidOperation, AttributeError) as exc:
        raise Uncertain("adapter_invalid_response") from exc


class AdapterClient:
    def __init__(self, client, timeout=5):
        self.client = client
        self.timeout = timeout

    async def reconcile(self, payload):
        try:
            result = await asyncio.wait_for(
                self.client.get(
                    "/orders/" + quote(payload["client_order_id"], safe="")
                ),
                self.timeout,
            )
            if result.status_code == 404:
                result = await asyncio.wait_for(
                    self.client.post("/orders", json=payload), self.timeout
                )
            if result.status_code >= 500 or result.status_code == 429:
                return None  # 다음 시도에서 같은 키로 조회부터 재개함.
            if result.status_code not in {200, 202}:
                raise Uncertain("adapter_contract_error")
            return validate_receipt(result.json(), payload)
        except (httpx.HTTPError, TimeoutError):
            return None
        except ValueError as exc:
            raise Uncertain("adapter_invalid_response") from exc

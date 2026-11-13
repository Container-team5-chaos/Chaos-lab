from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


def decimal_string(value, *, positive=False):
    if not isinstance(value, str):
        raise ValueError("decimal values must be strings")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not number.is_finite() or number < 0 or (positive and number == 0):
        raise ValueError("invalid decimal range")
    return number


class OrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    client_order_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
    user_id: str = Field(min_length=1, max_length=64)
    market: Literal["KRW-BTC"]
    side: Literal["bid", "ask"]
    volume: str = Field(min_length=1, max_length=64)
    max_amount: str | None = Field(default=None, min_length=1, max_length=64)

    @field_validator("max_amount")
    @classmethod
    def budget_valid(cls, value):
        if value is None:
            return None
        amount = decimal_string(value)
        if amount >= Decimal("1e20") or amount.as_tuple().exponent < -4:
            raise ValueError(
                "max_amount allows at most 20 integer and 4 fractional digits"
            )
        return f"{amount:.4f}"

    @field_validator("volume")
    @classmethod
    def volume_valid(cls, value):
        volume = decimal_string(value, positive=True)
        if volume.as_tuple().exponent < -8 or volume >= Decimal("1e22"):
            raise ValueError("volume allows at most 22 integer and 8 fractional digits")
        return f"{volume:.8f}"


class BrokerResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    client_order_id: str
    user_id: str
    market: str
    side: str
    volume: str
    max_amount: str | None = None
    status: Literal["pending", "filled", "rejected"]
    broker_order_id: str | None = Field(default=None, max_length=128)
    fill_price: str | None = Field(default=None, max_length=128)
    amount: str | None = Field(default=None, max_length=128)
    filled_at: AwareDatetime | None = None
    reason: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def valid_result(self):
        decimal_string(self.volume, positive=True)
        if self.status == "filled":
            decimal_string(self.fill_price, positive=True)
            decimal_string(self.amount)
            if not self.broker_order_id or self.filled_at is None:
                raise ValueError("filled result requires broker id and fill time")
        if self.status == "rejected" and not self.reason:
            raise ValueError("rejected result requires reason")
        return self

    def matches(self, payload):
        if payload.get("max_amount") is not None:
            if self.max_amount is None:
                return False
            try:
                budget = decimal_string(self.max_amount)
            except ValueError:
                return False
            if budget != Decimal(payload["max_amount"]):
                return False
            if self.status == "filled" and Decimal(self.amount) > budget:
                return False
        return (
            self.client_order_id == payload["client_order_id"]
            and self.user_id == payload["user_id"]
            and self.market == payload["market"]
            and self.side == payload["side"]
            and Decimal(self.volume) == Decimal(payload["volume"])
        )


class OrderResponse(OrderRequest):
    model_config = ConfigDict(extra="forbid", strict=False)

    status: Literal["pending", "filled", "rejected", "unknown"]
    attempts: int
    result: BrokerResult | None
    last_error: str | None
    created_at: AwareDatetime
    updated_at: AwareDatetime
    replayed: bool


class ErrorResponse(BaseModel):
    status: Literal["rejected"] = "rejected"
    reason: str
    client_order_id: str | None


def response(row, *, replayed=False):
    return {
        **row["payload"],
        "status": row["status"],
        "attempts": row["attempts"],
        "result": row["broker_result"],
        "last_error": row["last_error"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
        "replayed": replayed,
    }

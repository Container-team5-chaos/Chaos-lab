"""요청 형식과 응답 변환."""

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from .config import SUPPORTED_MARKETS


class OrderRequest(BaseModel):
    client_order_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=64)
    market: str
    side: Literal["bid", "ask"]
    volume: Decimal

    @field_validator("market")
    @classmethod
    def market_supported(cls, v: str) -> str:
        if v not in SUPPORTED_MARKETS:
            raise ValueError("지원하지 않는 마켓")
        return v

    @field_validator("volume")
    @classmethod
    def volume_valid(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("수량은 0보다 커야 함")
        if v.as_tuple().exponent < -8:
            raise ValueError("수량은 소수점 8자리까지")
        return v


def dec(d: Decimal | None) -> str | None:
    """Decimal을 지수 표기 없는 문자열로."""
    return None if d is None else f"{d:f}"


def order_to_dict(row, replayed: bool) -> dict:
    return {
        "order_id": row["id"],
        "client_order_id": row["client_order_id"],
        "user_id": row["user_id"],
        "market": row["market"],
        "side": row["side"],
        "volume": dec(row["volume"]),
        "status": row["status"],
        "fill_price": dec(row["fill_price"]),
        "amount": dec(row["amount"]),
        "filled_at": row["filled_at"].isoformat() if row["filled_at"] else None,
        "replayed": replayed,
    }

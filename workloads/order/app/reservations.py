from decimal import Decimal


async def reserved(conn, user_id, market=None):
    # 기존 로컬 전용 DB도 마이그레이션 없이 읽을 수 있음.
    if await conn.fetchval("SELECT to_regclass('order_external')") is None:
        return Decimal(0), Decimal(0)
    row = await conn.fetchrow(
        """SELECT coalesce(sum(e.reserved_krw), 0) AS krw,
                  coalesce(sum(e.reserved_volume) FILTER (WHERE o.market = $2), 0) AS volume
           FROM order_external e JOIN orders o USING (client_order_id)
           WHERE o.user_id = $1 AND o.status IN ('pending', 'unknown')""",
        user_id,
        market,
    )
    return row["krw"], row["volume"]

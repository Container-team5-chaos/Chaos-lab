"""임시 푸시 서버 (E, 데모 화면용) — C의 푸시 서버가 나오면 교체한다.

Redis 채널 price:<market>을 구독해서, 연결된 브라우저에 WebSocket으로 그대로 전달한다.
  - 연결 직후 Redis의 최신 값(GET price:<market>)을 먼저 보낸다 → 재연결 후 최신 시세부터 받음
  - 메시지 형식: {"market": "KRW-BTC", "price": "<문자열>", "ts": <epoch ms>}
  - GET /healthz → 200 (Redis 연결이 살아 있을 때)
Redis가 끊기면 1초 뒤 다시 구독한다. 브라우저 쪽 재연결은 화면이 담당한다.
"""

import asyncio
import json
import logging
import os
from http import HTTPStatus

import redis.asyncio as aioredis
from websockets.asyncio.server import broadcast, serve

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
MARKET = os.environ.get("MARKET", "KRW-BTC")
PORT = int(os.environ.get("PORT", "8080"))
CHANNEL = f"price:{MARKET}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("dev-push")
# healthz probe마다 "connection rejected" 로그가 쌓이지 않게
logging.getLogger("websockets.server").setLevel(logging.WARNING)

clients = set()
state = {"redis_ok": False}


def to_message(raw) -> str | None:
    """Redis 값({"price", "ts"})을 브라우저용 메시지로 바꾼다. 형식이 틀리면 None."""
    try:
        data = json.loads(raw)
        return json.dumps(
            {"market": MARKET, "price": str(data["price"]), "ts": int(data["ts"])}
        )
    except (ValueError, KeyError, TypeError):
        LOG.warning("형식이 맞지 않는 시세 무시: %r", raw)
        return None


async def handler(ws, r):
    clients.add(ws)
    LOG.info("브라우저 연결 (현재 %d개)", len(clients))
    try:
        latest = await r.get(CHANNEL)
        if latest and (msg := to_message(latest)):
            await ws.send(msg)
        await ws.wait_closed()
    finally:
        clients.discard(ws)
        LOG.info("브라우저 연결 종료 (현재 %d개)", len(clients))


async def relay(r):
    """Redis 채널을 구독해서 모든 브라우저에 전달한다. 끊기면 1초 뒤 재구독."""
    while True:
        try:
            async with r.pubsub() as ps:
                await ps.subscribe(CHANNEL)
                state["redis_ok"] = True
                LOG.info("Redis 구독 시작: %s", CHANNEL)
                async for item in ps.listen():
                    if item["type"] == "message" and (msg := to_message(item["data"])):
                        broadcast(clients, msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # Redis 단절 등
            state["redis_ok"] = False
            LOG.warning("Redis 구독 중단(%s). 1초 뒤 재시도", e)
            await asyncio.sleep(1)


def health(connection, request):
    if request.path == "/healthz":
        status = HTTPStatus.OK if state["redis_ok"] else HTTPStatus.SERVICE_UNAVAILABLE
        return connection.respond(
            status, "ok\n" if state["redis_ok"] else "redis down\n"
        )
    return None


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    relay_task = asyncio.create_task(relay(r))
    async with serve(
        lambda ws: handler(ws, r), "0.0.0.0", PORT, process_request=health
    ) as server:
        LOG.info("임시 푸시 서버 시작 :%d (market=%s)", PORT, MARKET)
        await server.serve_forever()
    relay_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())

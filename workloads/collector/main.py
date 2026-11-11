"""업비트 ticker 수집 → Redis 최신가/알림 → PostgreSQL 배치 저장.

Python 3.11 이상이 필요함. 환경변수와 연동 계약은 README.md 참조.
DB 장애 시 현재 배치를 유지해 재시도함. 메모리 대기열은 영구 저장소가
아니므로 강제 종료 또는 큐 포화 시 미저장 시세 손실 가능성을 로그로 알림.
"""

import asyncio
import json
import logging
import math
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo
from redis.asyncio import Redis
from redis.exceptions import RedisError
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

LOG = logging.getLogger("collector")
READY_FILE = Path(os.getenv("READY_FILE", "/tmp/collector-ready"))


def database_url_from_env():
    """Chaos-lab의 기존 postgres Secret을 재사용하고 비밀번호를 안전하게 인코딩함."""
    if os.getenv("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    return make_conninfo(
        host=os.environ["PGHOST"],
        port=os.getenv("PGPORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


@dataclass(frozen=True)
class Settings:
    redis_url: str
    database_url: str
    ws_url: str = "wss://api.upbit.com/websocket/v1"
    market: str = "KRW-BTC"
    batch_size: int = 100
    flush_seconds: float = 1.0
    queue_size: int = 10000
    receive_timeout: float = 60.0
    max_source_age_ms: int = 5000
    shutdown_seconds: float = 20.0

    @classmethod
    def from_env(cls):
        settings = cls(
            redis_url=os.environ["REDIS_URL"],
            database_url=database_url_from_env(),
            ws_url=os.getenv("UPBIT_WS_URL", cls.ws_url),
            market=os.getenv("MARKET", cls.market).upper(),
            batch_size=int(os.getenv("BATCH_SIZE", "100")),
            flush_seconds=float(os.getenv("FLUSH_SECONDS", "1")),
            queue_size=int(os.getenv("QUEUE_SIZE", "10000")),
            receive_timeout=float(os.getenv("RECEIVE_TIMEOUT", "60")),
            max_source_age_ms=int(os.getenv("MAX_SOURCE_AGE_MS", "5000")),
            shutdown_seconds=float(os.getenv("SHUTDOWN_SECONDS", "20")),
        )
        for value in (
            settings.batch_size,
            settings.flush_seconds,
            settings.queue_size,
            settings.receive_timeout,
            settings.max_source_age_ms,
            settings.shutdown_seconds,
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("시간 제한, 배치 크기, 큐 크기는 양수여야 함")
        if settings.market != "KRW-BTC":
            raise ValueError("이번 주 지원 마켓은 KRW-BTC임")
        return settings


@dataclass(frozen=True)
class Tick:
    event_id: uuid.UUID
    market: str
    price: Decimal
    ts: int
    source_ts: int

    def payload(self):
        # JSON number의 부동소수점 변환을 피하도록 가격은 십진 문자열로 전달함.
        return json.dumps({"price": format(self.price, "f"), "ts": self.ts})

    def row(self):
        received_at = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            milliseconds=self.ts
        )
        return (self.event_id, self.market, self.price, received_at, self.source_ts)


def parse_tick(raw, settings, received_ms):
    """DEFAULT 형식만 허용하며 잘못된/오래된 메시지는 최신가로 사용하지 않음."""
    message = json.loads(raw, parse_float=Decimal)
    if not isinstance(message, dict):
        raise ValueError("ticker object가 필요함")
    if message.get("type") != "ticker" or message.get("code") != settings.market:
        raise ValueError("구독한 market의 ticker가 아님")
    price = Decimal(str(message["trade_price"]))
    source_ts = message["timestamp"]
    if not price.is_finite() or price <= 0:
        raise ValueError("유효하지 않은 가격")
    if type(source_ts) is not int:
        raise ValueError("timestamp는 Unix 밀리초 정수여야 함")
    if abs(received_ms - source_ts) > settings.max_source_age_ms:
        raise ValueError("오래된 시세 또는 시스템 시각 불일치")
    return Tick(uuid.uuid4(), settings.market, price, received_ms, source_ts)


async def publish_tick(redis, tick):
    """SET과 PUBLISH를 Redis transaction으로 함께 실행함.

    키는 유지하므로 D는 반드시 ts를 검사해야 함. Pub/Sub 알림은 영속성이
    없으므로 푸시 서버는 재연결 시 GET으로 최신값을 먼저 확인해야 함.
    """
    key = f"price:{tick.market}"
    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(key, tick.payload())
        pipe.publish(key, tick.payload())
        await pipe.execute()


async def collect(settings, redis, queue):
    delay = 1.0
    last_source_ts = 0
    while True:
        try:
            # ping은 프로토콜 Ping frame임. 매 연결마다 구독 메시지를 재전송함.
            async with connect(
                settings.ws_url,
                ping_interval=60,
                ping_timeout=20,
                open_timeout=10,
                close_timeout=5,
                max_queue=16,
                proxy=None,
            ) as ws:
                await ws.send(
                    json.dumps(
                        [
                            {"ticket": str(uuid.uuid4())},
                            {"type": "ticker", "codes": [settings.market]},
                            {"format": "DEFAULT"},
                        ]
                    )
                )
                LOG.info("WebSocket 연결 및 구독 완료 market=%s", settings.market)
                while True:
                    raw = await asyncio.wait_for(ws.recv(), settings.receive_timeout)
                    received_ms = time.time_ns() // 1_000_000
                    try:
                        tick = parse_tick(raw, settings, received_ms)
                    except (ValueError, KeyError, InvalidOperation, TypeError):
                        LOG.warning("잘못되거나 오래된 ticker 무시")
                        continue
                    if tick.source_ts < last_source_ts:
                        continue  # 재연결 후 이전 시세로 최신가를 되돌리지 않음.
                    last_source_ts = tick.source_ts
                    delay = 1.0
                    # 큐가 가득 차면 조용히 버리지 않고 프로세스를 실패시킴.
                    # 메모리 무한 증가를 막고 DB 장애를 운영자가 인지하게 함.
                    queue.put_nowait(tick)
                    try:
                        await publish_tick(redis, tick)
                    except (RedisError, OSError):
                        READY_FILE.unlink(missing_ok=True)
                        LOG.warning(
                            "Redis 저장 실패. 이력은 저장하고 다음 시세에서 재시도함"
                        )
                        continue
                    READY_FILE.write_text(str(tick.ts), encoding="utf-8")
                    LOG.info(
                        "시세 저장 market=%s price=%s ts=%s",
                        tick.market,
                        tick.price,
                        tick.ts,
                    )
        except (WebSocketException, OSError, TimeoutError):
            # 예외 문자열에 연결 정보/인증정보가 섞이지 않도록 출력하지 않음.
            wait = random.uniform(delay / 2, delay)
            LOG.warning("WebSocket 연결 중단. %.1f초 후 재연결함", wait)
            await asyncio.sleep(wait)
            delay = min(delay * 2, 30.0)


INSERT = """INSERT INTO ticks (event_id, market, price, received_at, source_ts)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING"""


async def store_batch(settings, batch):
    """연결/commit 결과가 불명확해도 같은 event_id로 배치 전체를 재시도함."""
    delay = 1.0
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(
                settings.database_url,
                connect_timeout=5,
                options="-c statement_timeout=5000 -c lock_timeout=5000",
            ) as conn:
                async with conn.cursor() as cursor:
                    await cursor.executemany(INSERT, [tick.row() for tick in batch])
                # connection context가 성공적으로 끝나면 commit함.
            LOG.info("PostgreSQL 저장 완료 count=%s", len(batch))
            return
        except (
            psycopg.OperationalError,
            psycopg.InterfaceError,
            psycopg.errors.QueryCanceled,
            psycopg.errors.LockNotAvailable,
        ):
            LOG.warning("PostgreSQL 일시 장애. 동일 배치 %s건 재시도함", len(batch))
            await asyncio.sleep(random.uniform(delay / 2, delay))
            delay = min(delay * 2, 30.0)


async def write_history(settings, queue, store=store_batch):
    while True:
        batch = [await queue.get()]
        deadline = asyncio.get_running_loop().time() + settings.flush_seconds
        while len(batch) < settings.batch_size:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(queue.get(), remaining))
            except TimeoutError:
                break
        await store(settings, batch)
        # 실제 commit 성공 후에만 join 대기자에게 완료를 알림.
        for _ in batch:
            queue.task_done()


async def run(settings):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop():
        READY_FILE.unlink(missing_ok=True)
        stop.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, request_stop)
        except NotImplementedError:
            # Windows 직접 실행도 지원함. Docker/WSL은 위 경로를 사용함.
            signal.signal(signum, lambda *_: loop.call_soon_threadsafe(request_stop))

    READY_FILE.parent.mkdir(parents=True, exist_ok=True)
    READY_FILE.unlink(missing_ok=True)
    redis = Redis.from_url(
        settings.redis_url,
        socket_timeout=3,
        socket_connect_timeout=3,
        decode_responses=True,
    )
    queue = asyncio.Queue(maxsize=settings.queue_size)
    receiver = asyncio.create_task(collect(settings, redis, queue))
    writer = asyncio.create_task(write_history(settings, queue))
    stopper = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            [receiver, writer, stopper], return_when=asyncio.FIRST_COMPLETED
        )
        # 예상하지 못한 worker 종료는 성공으로 취급하지 않음.
        for task in (receiver, writer):
            if task in done:
                task.result()
                raise RuntimeError("worker가 예상치 않게 종료됨")
    finally:
        incomplete = False
        receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
        READY_FILE.unlink(missing_ok=True)
        if not writer.done():
            drain = asyncio.create_task(queue.join())
            finished, _ = await asyncio.wait(
                [drain, writer],
                timeout=settings.shutdown_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if drain not in finished:
                incomplete = True
                LOG.error("종료 제한 내 이력 저장 실패. 미저장 시세가 남아 있음")
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
        writer.cancel()
        stopper.cancel()
        results = await asyncio.gather(writer, stopper, return_exceptions=True)
        READY_FILE.unlink(missing_ok=True)
        await redis.aclose()
        if isinstance(results[0], Exception):
            raise results[0]
        if incomplete:
            raise RuntimeError("종료 전 시세 이력 저장 미완료")


async def initialize_database(settings):
    """스키마 변경은 명시적인 init-db 실행으로만 수행함."""
    async with await psycopg.AsyncConnection.connect(
        settings.database_url,
        connect_timeout=5,
        options="-c statement_timeout=10000 -c lock_timeout=5000",
    ) as conn:
        await conn.execute(Path(__file__).with_name("schema.sql").read_text("utf-8"))


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    # psycopg async는 Windows에서 SelectorEventLoop를 요구함.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        settings = Settings.from_env()
        if sys.argv[1:] == ["init-db"]:
            asyncio.run(initialize_database(settings))
        elif not sys.argv[1:]:
            asyncio.run(run(settings))
        else:
            raise ValueError("사용법: python main.py [init-db]")
    except KeyboardInterrupt:
        pass
    except Exception as error:
        LOG.error(
            "수집기 종료 error_type=%s. 환경변수/스키마/저장소 상태 확인 필요함",
            type(error).__name__,
        )
        sys.exit(1)

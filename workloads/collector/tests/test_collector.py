"""외부 API/DB 없이 실행하는 회귀 테스트. 실제 DB 통합 검증은 README 참조."""

import asyncio
import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main
from websockets.asyncio.server import serve


class CollectorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = main.Settings(
            "redis://localhost", "postgresql://localhost", flush_seconds=0.01
        )
        self.now = time.time_ns() // 1_000_000
        self.message = {
            "type": "ticker",
            "code": "KRW-BTC",
            "trade_price": "100000000.12345678",
            "timestamp": self.now,
        }

    def tick(self):
        return main.parse_tick(json.dumps(self.message), self.settings, self.now)

    def test_price_precision_and_contract(self):
        tick = self.tick()
        self.assertEqual(
            json.loads(tick.payload()), {"price": "100000000.12345678", "ts": self.now}
        )
        self.assertEqual(str(tick.row()[2]), "100000000.12345678")

    def test_reject_invalid_and_stale_ticks(self):
        for field, value in [
            ("trade_price", "NaN"),
            ("trade_price", "-1"),
            ("code", "KRW-ETH"),
            ("timestamp", self.now - 31000),
            ("timestamp", True),
        ]:
            with self.subTest(field=field, value=value):
                message = dict(self.message, **{field: value})
                with self.assertRaises(ValueError):
                    main.parse_tick(json.dumps(message), self.settings, self.now)

    async def test_set_and_publish_use_same_payload(self):
        class Pipeline:
            def __init__(self):
                self.operations = []
                self.executed = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

            def set(self, *args):
                self.operations.append(("set", args))

            def publish(self, *args):
                self.operations.append(("publish", args))

            async def execute(self):
                self.executed = True

        class Redis:
            def pipeline(self, transaction):
                self.transaction = transaction
                return pipeline

        pipeline = Pipeline()
        redis = Redis()
        await main.publish_tick(redis, self.tick())
        self.assertTrue(redis.transaction)
        self.assertTrue(pipeline.executed)
        self.assertEqual(pipeline.operations[0][1], pipeline.operations[1][1])

    async def test_small_batch_flush_and_join_after_commit(self):
        queue = asyncio.Queue()
        await queue.put(self.tick())
        committed = asyncio.Event()

        async def store(settings, batch):
            self.assertEqual(len(batch), 1)
            await committed.wait()

        writer = asyncio.create_task(main.write_history(self.settings, queue, store))
        join = asyncio.create_task(queue.join())
        await asyncio.sleep(0.03)
        self.assertFalse(join.done())
        committed.set()
        await asyncio.wait_for(join, 1)
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)

    async def test_database_retry_preserves_event_ids(self):
        connection = AsyncMock()
        connection.__aenter__.return_value = connection
        cursor = AsyncMock()
        cursor.__aenter__.return_value = cursor
        # cursor()는 await하지 않는 context manager factory임.
        from unittest.mock import Mock

        connection.cursor = Mock(return_value=cursor)
        tick = self.tick()
        with patch.object(
            main.psycopg.AsyncConnection, "connect", new_callable=AsyncMock
        ) as connect:
            connect.side_effect = [main.psycopg.OperationalError(), connection]
            with patch.object(main.random, "uniform", return_value=0):
                await main.store_batch(self.settings, [tick])
        self.assertEqual(connect.await_count, 2)
        cursor.executemany.assert_awaited_once_with(main.INSERT, [tick.row()])

    async def test_silent_websocket_times_out_and_resubscribes(self):
        """서버가 연결을 유지하며 침묵해도 수신 timeout으로 재접속해야 한다."""
        subscriptions = []
        silent_duration = []
        loop = asyncio.get_running_loop()

        async def upstream(ws):
            subscriptions.append(json.loads(await ws.recv()))
            if len(subscriptions) == 1:
                started = loop.time()
                # 서버는 close도 데이터 전송도 하지 않고 클라이언트 종료를 기다린다.
                await ws.wait_closed()
                silent_duration.append(loop.time() - started)
            else:
                message = dict(self.message, timestamp=time.time_ns() // 1_000_000)
                await ws.send(json.dumps(message).encode())
                await ws.wait_closed()

        async with serve(upstream, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            settings = main.Settings(
                "redis://localhost",
                "postgresql://localhost",
                ws_url=f"ws://127.0.0.1:{port}",
                receive_timeout=0.2,  # 테스트만 단축. 운영 기본값은 60초.
            )
            queue = asyncio.Queue(100)
            with (
                patch.object(main, "publish_tick", new_callable=AsyncMock),
                patch.object(Path, "write_text"),
                patch.object(main.random, "uniform", return_value=0.01),
            ):
                task = asyncio.create_task(main.collect(settings, None, queue))
                try:
                    tick = await asyncio.wait_for(queue.get(), 3)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            self.assertGreaterEqual(len(subscriptions), 2)
            self.assertGreaterEqual(silent_duration[0], settings.receive_timeout * 0.75)
            self.assertEqual(subscriptions[1][1]["codes"], ["KRW-BTC"])
            self.assertNotEqual(subscriptions[0][0], subscriptions[1][0])
            self.assertEqual(tick.market, "KRW-BTC")
            self.assertEqual(str(tick.price), self.message["trade_price"])

    async def test_websocket_reconnect_and_resubscribe(self):
        subscriptions = []
        enough = asyncio.Event()

        async def upstream(ws):
            subscriptions.append(json.loads(await ws.recv()))
            message = dict(self.message, timestamp=time.time_ns() // 1_000_000)
            await ws.send(json.dumps(message).encode())
            if len(subscriptions) >= 2:
                enough.set()
            await ws.close()  # 실제 로컬 WebSocket 서버가 연결을 종료함.

        async with serve(upstream, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            settings = main.Settings(
                "redis://localhost",
                "postgresql://localhost",
                ws_url=f"ws://127.0.0.1:{port}",
            )
            queue = asyncio.Queue(100)
            with (
                patch.object(main, "publish_tick", new_callable=AsyncMock),
                patch.object(Path, "write_text"),
                patch.object(main.random, "uniform", return_value=0.01),
            ):
                task = asyncio.create_task(main.collect(settings, None, queue))
                try:
                    await asyncio.wait_for(enough.wait(), 3)
                    await asyncio.sleep(0.02)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            self.assertGreaterEqual(queue.qsize(), 2)
            self.assertEqual(subscriptions[1][1]["codes"], ["KRW-BTC"])
            self.assertNotEqual(subscriptions[0][0], subscriptions[1][0])


if __name__ == "__main__":
    unittest.main()

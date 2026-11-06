"""가격 계약, 인프라 설정 및 장애/종료 경로 회귀 테스트."""

import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import healthcheck
import main
from psycopg.conninfo import conninfo_to_dict


class SettingsTests(unittest.TestCase):
    def test_existing_postgres_secret_with_special_password(self):
        env = {
            "REDIS_URL": "redis://redis.exchange.svc.cluster.local:6379/0",
            "PGHOST": "postgres.exchange.svc.cluster.local",
            "POSTGRES_USER": "exchange",
            "POSTGRES_DB": "exchange",
            "POSTGRES_PASSWORD": "a b'\\@:/?#",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = main.Settings.from_env()
        actual = conninfo_to_dict(settings.database_url)
        self.assertEqual(actual["password"], env["POSTGRES_PASSWORD"])
        self.assertEqual(actual["host"], env["PGHOST"])
        self.assertEqual(actual["dbname"], "exchange")
        self.assertEqual(settings.max_source_age_ms, 5000)

    def test_database_url_override(self):
        with patch.dict(
            os.environ, {"DATABASE_URL": "postgresql://localhost/exchange"}, clear=True
        ):
            self.assertEqual(main.database_url_from_env(), os.environ["DATABASE_URL"])

    def test_invalid_configuration(self):
        for name, value in [
            ("BATCH_SIZE", "0"),
            ("QUEUE_SIZE", "-1"),
            ("FLUSH_SECONDS", "nan"),
            ("SHUTDOWN_SECONDS", "inf"),
            ("MARKET", "KRW-ETH"),
        ]:
            with (
                self.subTest(name=name),
                patch.dict(
                    os.environ,
                    {
                        "REDIS_URL": "redis://localhost",
                        "DATABASE_URL": "postgresql://localhost",
                        name: value,
                    },
                    clear=True,
                ),
                self.assertRaises(ValueError),
            ):
                main.Settings.from_env()

    def test_readiness_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ready"
            self.assertFalse(healthcheck.is_ready(path, 10000, 5000))
            for timestamp, expected in [(5000, True), (4999, False), (10001, False)]:
                path.write_text(str(timestamp), encoding="utf-8")
                self.assertEqual(healthcheck.is_ready(path, 10000, 5000), expected)
            path.write_text("invalid", encoding="utf-8")
            self.assertFalse(healthcheck.is_ready(path, 10000, 5000))


class ResilienceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = main.Settings(
            "redis://localhost", "postgresql://localhost", flush_seconds=0.01
        )
        self.now = time.time_ns() // 1_000_000
        self.message = {
            "type": "ticker",
            "code": "KRW-BTC",
            "trade_price": "98000000",
            "timestamp": self.now,
        }

    def tick(self):
        return main.parse_tick(json.dumps(self.message), self.settings, self.now)

    def test_json_numeric_price_keeps_decimal_precision_and_receive_time(self):
        raw = '{"type":"ticker","code":"KRW-BTC","trade_price":98000000.12345678,"timestamp":10000}'
        tick = main.parse_tick(raw, self.settings, 10123)
        self.assertEqual(
            json.loads(tick.payload()), {"price": "98000000.12345678", "ts": 10123}
        )
        self.assertEqual(tick.source_ts, 10000)

    def test_source_age_matches_five_second_default(self):
        for timestamp in [self.now - 5001, self.now + 5001]:
            with self.assertRaises(ValueError):
                main.parse_tick(
                    json.dumps(dict(self.message, timestamp=timestamp)),
                    self.settings,
                    self.now,
                )

    async def receive(self, messages, publisher, queue=None):
        """유한 메시지 이후 취소하여 실제 네트워크 없이 수신 루프를 검사함."""
        ws = AsyncMock()
        ws.recv.side_effect = [json.dumps(message) for message in messages] + [
            asyncio.CancelledError()
        ]
        connection = AsyncMock()
        connection.__aenter__.return_value = ws
        queue = queue if queue is not None else asyncio.Queue()
        with (
            patch.object(main, "connect", return_value=connection),
            patch.object(main, "publish_tick", publisher),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await main.collect(self.settings, None, queue)
        return queue

    async def test_redis_failure_preserves_history_and_clears_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ready"
            path.write_text(str(self.now), encoding="utf-8")
            with patch.object(main, "READY_FILE", path):
                queue = await self.receive(
                    [self.message], AsyncMock(side_effect=main.RedisError())
                )
            self.assertEqual(queue.qsize(), 1)
            self.assertFalse(path.exists())

    async def test_redis_recovery_and_out_of_order_price(self):
        publisher = AsyncMock(side_effect=[main.RedisError(), None])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ready"
            with patch.object(main, "READY_FILE", path):
                queue = await self.receive(
                    [
                        dict(self.message, timestamp=self.now - 2),
                        self.message,
                        dict(self.message, timestamp=self.now - 1),
                    ],
                    publisher,
                )
            self.assertTrue(path.exists())
            self.assertEqual(queue.qsize(), 2)
            self.assertEqual(publisher.await_count, 2)

    async def test_queue_overflow_is_fatal(self):
        queue = asyncio.Queue(maxsize=1)
        queue.put_nowait(self.tick())
        with self.assertRaises(asyncio.QueueFull):
            await self.receive([self.message], AsyncMock(), queue)

    async def test_commit_failure_retries_identical_batch(self):
        connection = AsyncMock()
        connection.__aenter__.return_value = connection
        connection.__aexit__.side_effect = [main.psycopg.OperationalError(), None]
        cursor = AsyncMock()
        cursor.__aenter__.return_value = cursor
        connection.cursor = Mock(return_value=cursor)
        with (
            patch.object(
                main.psycopg.AsyncConnection,
                "connect",
                AsyncMock(return_value=connection),
            ),
            patch.object(main.random, "uniform", return_value=0),
        ):
            await main.store_batch(self.settings, [self.tick()])
        self.assertEqual(cursor.executemany.await_count, 2)
        self.assertEqual(
            cursor.executemany.await_args_list[0], cursor.executemany.await_args_list[1]
        )

    async def test_batch_size_triggers_flush(self):
        queue = asyncio.Queue()
        for _ in range(3):
            queue.put_nowait(self.tick())
        store = AsyncMock()
        task = asyncio.create_task(
            main.write_history(replace(self.settings, batch_size=2), queue, store)
        )
        try:
            await asyncio.wait_for(queue.join(), 1)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual([len(call.args[1]) for call in store.await_args_list], [2, 1])

    async def shutdown_case(self, fail_to_drain):
        callbacks = {}
        queued = asyncio.Event()
        store_started = asyncio.Event()
        allow_commit = asyncio.Event()
        committed = asyncio.Event()
        redis = AsyncMock()

        async def receiver(settings, redis, queue):
            queue.put_nowait(self.tick())
            main.READY_FILE.write_text(str(self.now), encoding="utf-8")
            queued.set()
            await asyncio.Event().wait()

        async def store(settings, batch):
            store_started.set()
            await allow_commit.wait()
            committed.set()

        original_writer = main.write_history

        async def writer(settings, queue):
            await original_writer(settings, queue, store)

        def register(signum, callback):
            callbacks[signum] = callback

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(main, "READY_FILE", Path(directory) / "ready"),
            patch.object(
                asyncio.get_running_loop(), "add_signal_handler", side_effect=register
            ),
            patch.object(main.Redis, "from_url", return_value=redis),
            patch.object(main, "collect", receiver),
            patch.object(main, "write_history", writer),
        ):
            task = asyncio.create_task(
                main.run(replace(self.settings, shutdown_seconds=0.1))
            )
            try:
                await asyncio.wait_for(queued.wait(), 1)
                await asyncio.wait_for(store_started.wait(), 1)
                callbacks[main.signal.SIGTERM]()
                self.assertFalse(main.READY_FILE.exists())
                if fail_to_drain:
                    with self.assertRaisesRegex(RuntimeError, "미완료"):
                        await asyncio.wait_for(task, 1)
                else:
                    allow_commit.set()
                    await asyncio.wait_for(task, 1)
                    self.assertTrue(committed.is_set())
                redis.aclose.assert_awaited_once()
                self.assertFalse(main.READY_FILE.exists())
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_sigterm_drains_history_after_readiness_cleared(self):
        await self.shutdown_case(False)

    async def test_shutdown_timeout_reports_failure(self):
        await self.shutdown_case(True)

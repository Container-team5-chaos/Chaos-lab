import asyncio
import logging
import os
import sys

import uvicorn

from .config import Settings
from .main import app
from .store import Store


async def init_db():
    store = await Store.connect(Settings.from_env())
    try:
        await store.initialize()
    finally:
        await store.close()


class Server(uvicorn.Server):
    def handle_exit(self, sig, frame):
        app.state.stopping.set()
        super().handle_exit(sig, frame)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if sys.argv[1:] == ["init-db"]:
        asyncio.run(init_db())
    elif not sys.argv[1:]:
        Server(
            uvicorn.Config(
                app,
                host="0.0.0.0",
                port=int(os.getenv("PORT", "8000")),
                timeout_graceful_shutdown=20,
            )
        ).run()
    else:
        raise SystemExit("usage: python -m adapter [init-db]")

import logging
import os

import uvicorn

from .main import create_app


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    app = create_app()

    class Server(uvicorn.Server):
        def handle_exit(self, sig, frame):
            app.state.hub.stopping = True
            super().handle_exit(sig, frame)

    Server(
        uvicorn.Config(
            app,
            host="0.0.0.0",
            port=int(os.getenv("PORT", "8080")),
            ws="websockets",
            ws_max_size=4096,
            ws_max_queue=4,
            ws_ping_interval=20,
            ws_ping_timeout=20,
            timeout_graceful_shutdown=10,
        )
    ).run()


if __name__ == "__main__":
    main()

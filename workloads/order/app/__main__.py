"""`python -m app`으로 실행. SIGTERM을 받으면 /readyz를 먼저 503으로 바꾼 뒤 정상 종료한다."""

import os

import uvicorn

from .main import app, state


class Server(uvicorn.Server):
    def handle_exit(self, sig, frame):
        state.shutting_down = True
        super().handle_exit(sig, frame)


if __name__ == "__main__":
    cfg = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        timeout_graceful_shutdown=20,  # k8s terminationGracePeriodSeconds(30)보다 짧게
        log_level="info",
    )
    Server(cfg).run()

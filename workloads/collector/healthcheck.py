"""Redis에 성공적으로 저장한 시세의 나이를 검사하는 exec readiness probe."""

import os
import sys
import time
from pathlib import Path


def is_ready(path=None, now_ms=None, max_age_ms=None):
    try:
        path = Path(path or os.getenv("READY_FILE", "/tmp/collector-ready"))
        now_ms = time.time_ns() // 1_000_000 if now_ms is None else now_ms
        max_age_ms = (
            int(os.getenv("READY_MAX_AGE_MS", "5000"))
            if max_age_ms is None
            else max_age_ms
        )
        age = now_ms - int(path.read_text(encoding="utf-8"))
        return 0 <= age <= max_age_ms
    except (OSError, ValueError):
        return False


if __name__ == "__main__":
    sys.exit(0 if is_ready() else 1)

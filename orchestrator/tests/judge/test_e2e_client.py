"""실서비스 e2e 도우미(e2e_client) 단위 테스트. docker·주문 서비스 없이 돈다(CI 포함)."""

from __future__ import annotations

import re

import pytest

from orchestrator.judge.model import CLIENT_ORDER_ID_MAX, is_run_key

from .e2e_client import APP_ENV_PASS, Recorder, app_env, env_with, run_key


def test_app_env_passes_only_runtime_values(monkeypatch):
    """셸의 서비스 설정·libpq 변수가 앱에 새지 않는다(STALE_MS가 크면 stale 단정이 깨진다)."""
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("LANG", "C.UTF-8")
    for k, v in {
        "STALE_MS": "60000",
        "DB_POOL_SIZE": "1",
        "PGSSLMODE": "require",
        "PGPASSWORD": "x",
        "PYTHONPATH": "/elsewhere",
        "DATABASE_URL": "postgresql://shell",
    }.items():
        monkeypatch.setenv(k, v)
    env = app_env(DATABASE_URL="postgresql://test", PORT="1234")
    assert env["DATABASE_URL"] == "postgresql://test"  # overrides가 이긴다
    assert env["PORT"] == "1234"
    assert env["PATH"] == "/usr/bin" and env["LANG"] == "C.UTF-8"
    for k in ("STALE_MS", "DB_POOL_SIZE", "PGSSLMODE", "PGPASSWORD", "PYTHONPATH"):
        assert k not in env, k
    assert set(env) <= set(APP_ENV_PASS) | {"DATABASE_URL", "PORT"}


def test_env_with_keeps_shell_for_docker_cli(monkeypatch):
    """docker CLI는 DOCKER_HOST 등 셸 값이 필요하다. PYTHONPATH만 뺀다."""
    monkeypatch.setenv("DOCKER_HOST", "unix:///tmp/docker.sock")
    monkeypatch.setenv("PYTHONPATH", "/elsewhere")
    env = env_with(POSTGRES_PASSWORD="p")
    assert env["DOCKER_HOST"] == "unix:///tmp/docker.sock"
    assert env["POSTGRES_PASSWORD"] == "p"
    assert "PYTHONPATH" not in env


@pytest.mark.parametrize(
    "run_id, vu, seq, want",
    [
        ("demo-1106", 1, 42, "k6-demo-1106-0001-000042"),  # 명세 11 예
        ("r-e2e-order", 5, 1, "k6-r-e2e-order-0005-000001"),
    ],
)
def test_run_key_matches_k6_format(run_id, vu, seq, want):
    assert run_key(run_id, vu, seq) == want
    assert is_run_key(want, run_id)


def test_run_key_fits_service_limit_for_longest_run_id():
    """run_id는 최대 40자(run.json 규칙)다. 그래도 64자 이하다(명세 11)."""
    run_id = "r" * 40
    assert len(run_key(run_id, 9999, 999999)) <= CLIENT_ORDER_ID_MAX


def test_recorder_intent_uses_k6_key_format():
    rec = Recorder(port=0, run_id="r-e2e", plan={}, vus=2)
    k1, b1 = rec.intent(2, "bid", "0.0001")
    k2, _ = rec.intent(2, "ask", "0.0001")
    assert (k1, k2) == ("k6-r-e2e-0002-000001", "k6-r-e2e-0002-000002")
    assert b1["client_order_id"] == k1 and b1["user_id"] == "user-0002"
    assert re.fullmatch(r"k6-r-e2e-[0-9]{4}-[0-9]{6}", rec.lines[-1]["key"])

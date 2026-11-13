from dataclasses import replace
from urllib.parse import unquote, urlsplit

import pytest

from adapter.config import database_url_from_env


@pytest.mark.parametrize(
    "change",
    [
        {"broker_rps": 0},
        {"poll_seconds": float("nan")},
        {"request_timeout": float("inf")},
        {"max_attempts": 0},
        {"lease_seconds": 10},
        {"retry_base": 31},
        {"broker_url": "file:///etc/passwd"},
        {"broker_url": "http://broker.test/api"},
    ],
)
def test_invalid_configuration(settings, change):
    with pytest.raises(ValueError):
        replace(settings, **change)


def test_existing_secret_password_is_url_encoded(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    for name, value in {
        "PGHOST": "postgres",
        "POSTGRES_USER": "exchange",
        "POSTGRES_PASSWORD": "a b'@:/?#",
        "POSTGRES_DB": "exchange",
    }.items():
        monkeypatch.setenv(name, value)
    parsed = urlsplit(database_url_from_env())
    assert parsed.hostname == "postgres"
    assert unquote(parsed.password) == "a b'@:/?#"

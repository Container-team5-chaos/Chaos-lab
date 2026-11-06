"""-m pg: postgres:16에 실제 서비스 스키마·seed를 넣고 bundle.sql 전체를 psql로 돌린다(계약 정렬 명세 6, 7, 10).

- 스키마·seed는 workloads/order/db/schema.sql, seed.sql을 그대로 쓴다(저장소 루트 기준으로 찾는다).
- 이미지는 서비스(docker-compose.dev.yml)와 같은 postgres:16, 컨테이너는 pgtest-judge다. 끝나면 docker rm -f로 지운다.
- 접속은 운영과 같은 경로다: 컨테이너 안 Unix 소켓, `psql -U exchange -d exchange -f -`(DbTarget 기본값).
  비밀번호는 쓰지 않는다(POSTGRES_HOST_AUTH_METHOD=trust. 포트를 열지 않는다).
- 정상 데이터: seed 계좌 1,000개에 주문 몇 건을 서비스와 같은 산식(_Exchange = orders.place_order를 옮김)으로
  넣는다. 모든 불변식이 0건이어야 한다. 위반을 하나씩 심어 그 불변식만 잡히는지 본다.
- `-m pg`를 명시했을 때만 돈다. docker가 없거나 데몬이 꺼져 있으면 skip한다.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, Context, Decimal, localcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.judge import db as D
from orchestrator.judge import reasons as R
from orchestrator.judge.criteria import load_criteria
from orchestrator.judge.invariants import (
    check_invariants,
    item_results,
    run_key_counts,
    snapshot_consistent,
)
from orchestrator.judge.model import (
    ACCOUNT_COLUMNS,
    FINGERPRINT_COLUMNS,
    ORDER_COLUMNS,
    POSITION_COLUMNS,
    SNAPSHOT_SECTIONS,
    TICK_COLUMNS,
    DbTarget,
    Outcome,
    compute_fingerprint,
)
from orchestrator.judge.transport import Transports

from .factories import default_schema

pytestmark = pytest.mark.pg

CONTAINER = "pgtest-judge"
IMAGE = "postgres:16"
ROOT = Path(__file__).resolve().parents[3]
SERVICE_DB = ROOT / "workloads" / "order" / "db"
DEFAULT_YAML = ROOT / "experiments" / "criteria" / "default.yaml"
TARGET = DbTarget()  # postgres-0, 컨테이너 없음, exchange/exchange(서비스 로컬 기본값)

RUN = "r1117-crash"
TWIN = "r1117-crash-2"  # 접두어가 겹치는 다른 run
EXTRA = "user-0999"  # 발표자
MARKET = "KRW-BTC"
CTX = SimpleNamespace(run_id=RUN, extra_accounts=(EXTRA,))
T0 = datetime(2026, 11, 6, 1, 0, tzinfo=timezone.utc)
SEED_KRW = Decimal("10000000")
Q4 = Decimal("0.0001")
Q8 = Decimal("0.00000001")


def _ts(off_ms: int) -> str:
    return (T0 + timedelta(milliseconds=off_ms)).isoformat()


def _q(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, Decimal):
        return format(v, "f")
    return "'" + str(v).replace("'", "''") + "'"


# ---------------------------------------------------------------- 서비스 산식(orders.place_order)


@dataclass
class _Exchange:
    """orders.place_order의 체결 산식을 그대로 옮긴 것. 같은 SQL 변경을 만든다.

    서비스처럼 기본 context(28자리, ROUND_HALF_EVEN)로 계산하고, DB에 저장되는 값(krw 4자리,
    volume·avg 8자리)을 그대로 상태로 들고 간다.
    """

    krw: dict[str, Decimal] = field(default_factory=dict)
    pos: dict[str, tuple[Decimal, Decimal]] = field(default_factory=dict)

    def place(
        self,
        user: str,
        coid: str,
        side: str,
        volume: str,
        price: str,
        at_ms: int,
        *,
        overdraft: bool = False,
    ) -> list[str]:
        v, p = Decimal(volume), Decimal(price)
        with localcontext(Context()):
            amount = (p * v).quantize(Q4, rounding=ROUND_DOWN)
            held, avg = self.pos.get(user, (Decimal(0), Decimal(0)))
            krw = self.krw.get(user, SEED_KRW)
            if side == "bid":
                assert overdraft or krw >= amount, "insufficient_balance"
                new_volume = held + v
                new_avg = ((held * avg + v * p) / new_volume).quantize(Q8)
                delta = -amount
            else:
                assert held >= v, "insufficient_volume"
                new_volume = held - v
                new_avg = avg
                delta = amount
            self.krw[user] = krw + delta
        self.pos[user] = (new_volume, new_avg)
        at = _q(_ts(at_ms))
        return [
            f"UPDATE accounts SET krw = krw + {_q(delta)} WHERE user_id = {_q(user)};",
            "INSERT INTO positions (user_id, market, volume, avg_price) VALUES "
            f"({_q(user)}, {_q(MARKET)}, {_q(new_volume)}, {_q(new_avg)}) "
            "ON CONFLICT (user_id, market) DO UPDATE SET volume = EXCLUDED.volume, "
            "avg_price = EXCLUDED.avg_price, updated_at = now();",
            "INSERT INTO orders (client_order_id, user_id, market, side, volume, status, "
            "fill_price, amount, created_at, filled_at) VALUES ("
            f"{_q(coid)}, {_q(user)}, {_q(MARKET)}, {_q(side)}, {_q(v)}, 'filled', "
            f"{_q(p)}, {_q(amount)}, {at}, {at});",
        ]


def _rk(vu: int, seq: int, run: str = RUN) -> str:
    return f"k6-{run}-{vu}-{seq}"


# (user, client_order_id, side, volume, price, 체결 시각 ms). 사용자별로 시간순 = id 순.
ORDERS = [
    ("user-0001", _rk(1, 1), "bid", "0.0001", "100000000", 1000),
    (
        "user-0001",
        _rk(1, 2),
        "bid",
        "0.0003",
        "101234567.12345678",
        2000,
    ),  # avg HALF_EVEN 동률
    ("user-0001", _rk(1, 3), "ask", "0.0002", "102000000", 3000),
    ("user-0002", _rk(2, 1), "bid", "0.00012345", "99999999", 4000),  # amount 내림 자리
    ("user-0002", _rk(2, 2), "bid", "0.00000001", "100000001", 5000),  # 28자리 몫
    ("user-0003", _rk(3, 1), "bid", "0.0005", "100000000", 6000),
    ("user-0003", _rk(3, 2), "ask", "0.0005", "100500000", 7000),  # 전량 매도, avg 유지
    (EXTRA, "demo-1", "bid", "0.001", "100000000", 8000),
    ("user-0500", _rk(500, 1, TWIN), "bid", "0.0002", "100000000", 9000),
    ("user-0500", _rk(500, 2, TWIN), "ask", "0.0001", "100000000.5", 9500),
]
SCOPED = 8  # run 주문 7건 + 발표자 1건(arith·fresh 대상)


def _clean_data_sql() -> str:
    ex = _Exchange()
    out: list[str] = []
    for user, coid, side, volume, price, at in ORDERS:
        out += ex.place(user, coid, side, volume, price, at)
    # 체결되지 않은 run 주문(돈이 움직이지 않았다). 불변식은 filled만 센다
    out.append(
        "INSERT INTO orders (client_order_id, user_id, market, side, volume, status, created_at) "
        f"VALUES ({_q(_rk(4, 1))}, 'user-0004', {_q(MARKET)}, 'bid', 0.1, 'pending', {_q(_ts(500))});"
    )
    # ticks: 대상 주문마다 체결 0.5초 전 같은 가격. 창 밖·다른 종목·다른 run 쪽 tick은 구획에 안 들어온다
    for _user, coid, _side, _v, price, at in ORDERS[:SCOPED]:
        out.append(
            "INSERT INTO ticks (market, price, received_at) "
            f"VALUES ({_q(MARKET)}, {price}, {_q(_ts(at - 500))});"
        )
    out.append(
        f"INSERT INTO ticks (market, price, received_at) VALUES ({_q(MARKET)}, 1, {_q(_ts(7_200_000))});"
    )
    out.append(
        f"INSERT INTO ticks (market, price, received_at) VALUES ('KRW-ETH', 1, {_q(_ts(1000))});"
    )
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- docker


def _docker(*args: str, input: bytes | None = None, timeout: float = 120):
    return subprocess.run(
        ["docker", *args], input=input, capture_output=True, timeout=timeout
    )


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return _docker("info", timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _psql(sql: str, dbname: str = "exchange", *, check: bool = True):
    cp = _docker(
        "exec",
        "-i",
        CONTAINER,
        "psql",
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "exchange",
        "-d",
        dbname,
        "-f",
        "-",
        input=sql.encode("utf-8"),
    )
    if check:
        assert cp.returncode == 0, cp.stderr.decode("utf-8", "replace")
    return cp


def _wait_ready(deadline_s: float = 90) -> None:
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        # TCP로 확인한다. 초기화 중 임시 서버는 Unix 소켓만 연다
        cp = _docker(
            "exec", CONTAINER, "pg_isready", "-h", "127.0.0.1", "-U", "exchange"
        )
        if cp.returncode == 0:
            return
        time.sleep(1)
    logs = _docker("logs", "--tail", "30", CONTAINER).stderr.decode("utf-8", "replace")
    raise RuntimeError(f"postgres가 {deadline_s}s 안에 준비되지 않았다\n{logs}")


@pytest.fixture(scope="module")
def pg_container(request):
    # 명시적으로 -m pg(또는 pg를 포함한 식)를 줬을 때만 돈다. 그냥 pytest -q에서는 컨테이너를 띄우지 않는다
    if "pg" not in (request.config.getoption("markexpr") or ""):
        pytest.skip("-m pg로 실행할 때만 돈다")
    if not _docker_available():
        pytest.skip("docker를 쓸 수 없다")
    _docker("rm", "-f", CONTAINER)
    try:
        cp = _docker(
            "run",
            "-d",
            "--name",
            CONTAINER,
            "-e",
            "POSTGRES_USER=exchange",
            "-e",
            "POSTGRES_DB=exchange",
            "-e",
            # 비밀번호를 만들지 않는다. 포트를 열지 않으므로 컨테이너 안에서만 접속한다
            "POSTGRES_HOST_AUTH_METHOD=trust",
            IMAGE,
            timeout=600,
        )
        if cp.returncode != 0:
            pytest.skip(
                f"{IMAGE} 컨테이너를 띄우지 못했다: {cp.stderr.decode('utf-8', 'replace')}"
            )
        _wait_ready()
        yield IMAGE
    finally:
        _docker("rm", "-f", CONTAINER)


@pytest.fixture
def fresh_db(pg_container):
    """매 테스트마다 exchange DB를 새로 만들고 서비스 schema.sql·seed.sql과 정상 주문을 넣는다."""
    _psql(
        "DROP DATABASE IF EXISTS exchange WITH (FORCE);\nCREATE DATABASE exchange;\n",
        dbname="postgres",
    )
    _psql((SERVICE_DB / "schema.sql").read_text(encoding="utf-8"))
    _psql((SERVICE_DB / "seed.sql").read_text(encoding="utf-8"))
    _psql(_clean_data_sql())
    return pg_container


def _snapshot():
    bundle = D.build_bundle(CTX, stale_ms=5000)
    run = D.exec_bundle(
        Transports.for_local_pg(CONTAINER), bundle, target=TARGET, timeout_s=60
    )
    return run, D.parse_bundle_output(run.stdout)


def _flagged(results) -> set[str]:
    return {r.id for r in results if r.violations != 0}


# ---------------------------------------------------------------- 정상 데이터


def test_clean_snapshot_has_all_sections_and_no_violations(fresh_db):
    run, snap = _snapshot()
    assert run.returncode == 0, run.stderr
    assert run.stderr == ""
    assert snap.sections == set(SNAPSHOT_SECTIONS) and snap.errors == ()
    assert snap.quiescent and snapshot_consistent(snap) is True
    assert snap.fingerprint1 == compute_fingerprint(
        snap.accounts, snap.positions, snap.orders
    )
    assert len(snap.accounts) == 1000 and len(snap.orders) == len(ORDERS) + 1
    assert len(snap.ticks) == SCOPED  # 창 밖·다른 종목 tick은 빠진다

    res = check_invariants(snap, CTX, load_criteria(DEFAULT_YAML))
    assert _flagged(res) == set(), [
        (r.id, r.violations, r.reasons, r.examples) for r in res
    ]
    by = {r.id: r for r in res}
    assert by["arith"].detail["orders"] == SCOPED
    assert by["fresh"].detail["orders"] == SCOPED
    assert by["cash"].detail["accounts"] == 1000


def test_snapshot_values_keep_db_precision_and_service_formula(fresh_db):
    _, snap = _snapshot()
    a = snap.position("user-0001", MARKET)
    assert str(a.volume) == "0.00020000"
    assert a.avg_price == Decimal("100925925.34259258")  # HALF_EVEN(HALF_UP이면 …59)
    assert snap.position("user-0002", MARKET).avg_price == Decimal("99999999.00016200")
    c = snap.position("user-0003", MARKET)
    assert c.volume == 0 and c.avg_price == Decimal("100000000")
    o = next(o for o in snap.orders if o.client_order_id == _rk(2, 1))
    assert str(o.amount) == "12344.9998" and str(o.fill_price) == "99999999.00000000"
    assert (
        o.filled_us == int((T0 + timedelta(milliseconds=4000)).timestamp()) * 1_000_000
    )
    assert str(snap.account("user-0002").krw) == "9987654.0002"


def test_section_headers_match_model_columns(fresh_db):
    run, _ = _snapshot()
    headers = dict(re.findall(r"^--@ (\S+)\n([^\n]*)\n", run.stdout, re.M))
    assert headers["accounts"] == ",".join(ACCOUNT_COLUMNS)
    assert headers["positions"] == ",".join(POSITION_COLUMNS)
    assert headers["orders"] == ",".join(ORDER_COLUMNS)
    assert headers["ticks"] == ",".join(TICK_COLUMNS)
    assert headers["schema"] == ",".join(D.SCHEMA_COLUMNS)
    assert (
        headers["fingerprint1"]
        == headers["fingerprint2"]
        == ",".join(FINGERPRINT_COLUMNS)
    )


def test_schema_section_matches_factories_default_schema(fresh_db):
    """factories.default_schema(오프라인 테스트의 기대 스키마)가 실제 postgres:16 출력과 같다."""
    _, snap = _snapshot()
    key = lambda s: (s.kind, s.table, s.name)  # noqa: E731
    assert sorted(snap.schema, key=key) == sorted(default_schema(), key=key)


def test_run_key_counts_use_full_key_format(fresh_db):
    _, snap = _snapshot()
    assert run_key_counts(snap, RUN) == {"orders": 8, "keys": 8, "filled": 7}
    assert run_key_counts(snap, TWIN) == {"orders": 2, "keys": 2, "filled": 2}


def test_default_yaml_items_pass(fresh_db):
    _, snap = _snapshot()
    crit = load_criteria(DEFAULT_YAML)
    items = {r.id: r for r in item_results(check_invariants(snap, CTX, crit), crit)}
    assert items["invariants"].outcome is Outcome.PASS
    assert items["stale_fill"].outcome is Outcome.PASS


# ---------------------------------------------------------------- 위반 하나씩


def _overspend_sql() -> str:
    # 1,000만 원 계좌가 2,000만 원어치를 샀다(서비스 산식 그대로, 잔고 검사만 건너뜀)
    ex = _Exchange()
    return "\n".join(
        ex.place(
            "user-0005", "manual-big", "bid", "0.2", "100000000", 10_000, overdraft=True
        )
    )


VIOLATIONS = [
    pytest.param(
        "UPDATE accounts SET krw = krw + 0.0001 WHERE user_id = 'user-0002';",
        {"cash"},
        id="cash-run-account",
    ),
    pytest.param(
        "UPDATE accounts SET krw = krw - 1 WHERE user_id = 'user-0700';",
        {"cash"},
        id="cash-any-seed-account",
    ),
    pytest.param(
        "UPDATE positions SET volume = volume + 0.00000001 WHERE user_id = 'user-0002';",
        {"qty"},
        id="qty-volume",
    ),
    pytest.param(
        "DELETE FROM positions WHERE user_id = 'user-0001';",
        {"qty", "avg"},
        id="qty-avg-position-row-gone",
    ),
    pytest.param(
        "UPDATE positions SET avg_price = 100925925.34259259 WHERE user_id = 'user-0001';",
        {"avg"},
        id="avg-half-up",
    ),
    pytest.param(
        "UPDATE positions SET avg_price = 0 WHERE user_id = 'user-0003';",
        {"avg"},
        id="avg-reset-after-full-sell",
    ),
    pytest.param(
        # 서비스가 HALF_UP으로 금액을 냈다면. 잔고는 그 금액으로 맞춰 cash는 맞다
        f"UPDATE orders SET amount = 12344.9999 WHERE client_order_id = '{_rk(2, 1)}';\n"
        "UPDATE accounts SET krw = krw - 0.0001 WHERE user_id = 'user-0002';",
        {"arith"},
        id="arith-half-up",
    ),
    pytest.param(
        # 다른 run의 주문은 arith 대상이 아니다(잔고는 맞춰 둔다)
        f"UPDATE orders SET amount = amount + 1 WHERE client_order_id = '{_rk(500, 1, TWIN)}';\n"
        "UPDATE accounts SET krw = krw - 1 WHERE user_id = 'user-0500';",
        set(),
        id="arith-other-run-ignored",
    ),
    pytest.param(
        # 음수 잔고는 CHECK가 막는다. 제약을 내렸다가 NOT VALID로 다시 걸면 constraints도 잡는다
        "ALTER TABLE accounts DROP CONSTRAINT accounts_krw_check;\n"
        + _overspend_sql()
        + "\nALTER TABLE accounts ADD CONSTRAINT accounts_krw_check CHECK (krw >= 0) NOT VALID;",
        {"nonneg", "constraints"},
        id="nonneg-consistent-overspend",
    ),
    pytest.param(
        "ALTER TABLE orders DROP CONSTRAINT orders_client_order_id_key;",
        {"constraints"},
        id="constraints-unique-dropped",
    ),
    pytest.param(
        "ALTER TABLE accounts ALTER COLUMN krw TYPE numeric(30,4);",
        {"constraints"},
        id="constraints-column-type",
    ),
    pytest.param(
        "UPDATE ticks SET price = price + 1 WHERE price = 102000000;",
        {"fresh"},
        id="fresh-price",
    ),
    pytest.param(
        "DELETE FROM ticks;",
        {"fresh"},
        id="fresh-no-ticks",
    ),
]


@pytest.mark.parametrize(("plant", "expected"), VIOLATIONS)
def test_each_violation_is_caught_by_its_invariant_only(fresh_db, plant, expected):
    _psql(plant)
    run, snap = _snapshot()
    assert run.returncode == 0, run.stderr
    res = {r.id: r for r in check_invariants(snap, CTX, load_criteria(DEFAULT_YAML))}
    assert _flagged(res.values()) == expected, {
        i: (r.violations, r.reasons, r.examples) for i, r in res.items()
    }
    for i in expected:
        r = res[i]
        if i == "constraints":
            assert r.violations is None and r.reasons == (R.SCHEMA_DRIFT,)
        elif plant == "DELETE FROM ticks;":
            assert r.violations is None and r.reasons == (R.NOT_EVALUATED,)
        else:
            assert r.violations >= 1 and r.examples


def test_duplicate_key_is_impossible_until_unique_is_dropped(fresh_db):
    dup = (
        "INSERT INTO orders (client_order_id, user_id, market, side, volume, status) "
        f"VALUES ('{_rk(1, 1)}', 'user-0001', 'KRW-BTC', 'bid', 1, 'pending');"
    )
    cp = _psql(dup, check=False)
    assert cp.returncode != 0
    assert "orders_client_order_id_key" in cp.stderr.decode("utf-8", "replace")

    _psql("ALTER TABLE orders DROP CONSTRAINT orders_client_order_id_key;\n" + dup)
    _, snap = _snapshot()
    assert run_key_counts(snap, RUN) == {"orders": 9, "keys": 8, "filled": 7}
    res = {r.id: r for r in check_invariants(snap, CTX)}
    assert res["constraints"].reasons == (R.SCHEMA_DRIFT,)
    assert res["cash"].violations == 0  # pending 행은 돈을 옮기지 않는다


def test_missing_ticks_table_gives_empty_ticks_section(fresh_db):
    _psql("DROP TABLE ticks;")
    run, snap = _snapshot()
    assert run.returncode == 0 and run.stderr == ""
    assert "ticks" in snap.sections and snap.ticks == ()
    res = {r.id: r for r in check_invariants(snap, CTX)}
    assert res["fresh"].violations is None and res["fresh"].reasons == (
        R.NOT_EVALUATED,
    )
    assert res["constraints"].violations == 0  # ticks는 constraints 대상이 아니다


def test_missing_column_stops_psql_but_keeps_schema_evidence(fresh_db):
    _psql("ALTER TABLE orders DROP COLUMN filled_at CASCADE;")
    run, snap = _snapshot()
    assert run.returncode == 3  # ON_ERROR_STOP
    assert snap.sections == {"fingerprint1", "schema", "accounts", "positions"}
    assert any("orders" in e for e in snap.errors)
    res = {r.id: r for r in check_invariants(snap, CTX)}
    drift = {(v.subject, v.detail["problem"]) for v in res["constraints"].examples}
    assert ("orders.filled_at", "column_missing") in drift
    assert res["constraints"].reasons == (R.SCHEMA_DRIFT,)
    for i in ("cash", "qty", "avg", "arith", "fresh"):
        assert res[i].violations is None and res[i].reasons == (R.DB_ERROR,), i
    assert res["nonneg"].violations == 0

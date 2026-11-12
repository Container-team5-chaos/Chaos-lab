"""실행 진입점.

python -m orchestrator.main run experiments/scenarios/pod-kill.yaml --mock
python -m orchestrator.main recover        # 재시작 후 정리만
"""

import argparse
import signal
import sys
from pathlib import Path

import yaml

from .cleanup import recover
from .gates import (
    AlwaysOpen,
    CompositeGate,
    http_ready,
    no_leftover_chaos,
    observability_up,
    previous_cleanup_ok,
    price_fresh,
    redis_price_getter,
)
from .inject import MockInjector
from .judging import ChaosJudge, MockJudge
from .k6load import K6JobLoad
from .observe import MockLoad
from .report import summary
from .runner import Runner
from .store import DEFAULT_PATH, StateStore
from .watch import AlwaysHealthy, PrometheusWatch


def _build(mock: bool, namespace: str):
    if mock:
        return MockInjector(), MockLoad(), MockJudge()
    from .inject import ChaosMeshInjector

    return ChaosMeshInjector(namespace), K6JobLoad(namespace), ChaosJudge()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="orchestrator")
    p.add_argument("--state", default=DEFAULT_PATH)
    p.add_argument(
        "--mock", action="store_true", help="클러스터 없이 가짜 구현으로 실행"
    )
    p.add_argument("--namespace", default="exchange")
    p.add_argument("--runs-dir", default="runs")
    p.add_argument(
        "--prometheus", default=None, help="기본: 클러스터 안 kube-prometheus-stack"
    )
    p.add_argument(
        "--no-gate", action="store_true", help="시작 게이트 생략 (로컬 디버깅용)"
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("scenario")
    r.add_argument("--run-id")
    sub.add_parser("recover")
    args = p.parse_args(argv)

    store = StateStore(args.state)
    injector, load, judge = _build(args.mock, args.namespace)

    if args.cmd == "recover":
        state = recover(store, injector)
        print(summary(state) if state else "정리할 실험 없음")
        return 0

    # SIGTERM(Pod 종료)도 예외로 바꿔서 Runner가 정리 후 ABORTED를 남기게 한다
    def _term(signum, frame):
        raise SystemExit(f"signal {signum}")

    signal.signal(signal.SIGTERM, _term)

    scenario = yaml.safe_load(Path(args.scenario).read_text(encoding="utf-8")) or {}
    if args.mock:
        watch, gate = AlwaysHealthy(), AlwaysOpen()
    else:
        watch = (
            PrometheusWatch(args.prometheus) if args.prometheus else PrometheusWatch()
        )
        gate = CompositeGate(
            {
                "previous_cleanup": previous_cleanup_ok(store),
                "no_leftover_chaos": no_leftover_chaos(injector),
                "order_ready": http_ready(),
                "price_fresh": price_fresh(redis_price_getter()),
                "prometheus": observability_up(watch),
            }
        )
    if args.no_gate:
        gate = AlwaysOpen()
    runner = Runner(
        store, injector, load, judge, gate, watch=watch, runs_dir=args.runs_dir
    )
    try:
        state = runner.run(scenario, run_id=args.run_id, scenario_path=args.scenario)
    except BaseException:
        print(summary(store.load()), file=sys.stderr)
        raise
    print(summary(state))
    return {"PASSED": 0, "FAILED": 10, "INCONCLUSIVE": 11}.get(state.get("result"), 1)


if __name__ == "__main__":
    sys.exit(main())

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
from .gates import AlwaysOpen
from .inject import MockInjector
from .judge import MockJudge
from .observe import MockLoad
from .report import summary
from .runner import Runner
from .store import DEFAULT_PATH, StateStore


def _build(mock: bool, namespace: str):
    if mock:
        return MockInjector(), MockLoad(), MockJudge()
    from .inject import ChaosMeshInjector

    # TODO(B): k6·판정 실제 구현이 들어오면 교체
    return ChaosMeshInjector(namespace), MockLoad(), MockJudge()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="orchestrator")
    p.add_argument("--state", default=DEFAULT_PATH)
    p.add_argument(
        "--mock", action="store_true", help="클러스터 없이 가짜 구현으로 실행"
    )
    p.add_argument("--namespace", default="chaos-lab")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("scenario")
    r.add_argument("--exp-id")
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
    runner = Runner(store, injector, load, judge, AlwaysOpen())
    try:
        state = runner.run(scenario, exp_id=args.exp_id)
    except BaseException:
        print(summary(store.load()), file=sys.stderr)
        raise
    print(summary(state))
    return 0 if state["phase"] == "DONE" and state["verdict"]["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

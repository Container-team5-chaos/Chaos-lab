# orchestrator (A 파트)

실험 하나를 phase 단위로 진행하고, 매 phase를 `state.json`에 먼저 기록한 뒤 동작한다.
Pod가 중간에 죽어도 새 Pod가 기록을 보고 **주입 없이 정리 → ABORTED**로 끝낸다.

## phase

```
PRECHECK → WARMUP → INJECTING → OBSERVING → RECOVERING → JUDGING → DONE
   │ 게이트 실패 → GATE_FAILED
   └ 어디서든 예외·SIGTERM·재시작 → ABORTED (aborted_from에 끊긴 phase 기록)
```

| phase | 하는 일 | 호출하는 쪽 |
|---|---|---|
| PRECHECK | 시작 게이트 확인 | `Gate.check(scenario)` |
| WARMUP | k6 출발, warmup 대기 | B: `LoadGenerator.start(exp_id, scenario)` |
| INJECTING | 시나리오의 faults 순서대로 주입 | E: `Injector.apply(exp_id, step, fault)` |
| OBSERVING | observe 시간 대기 | - |
| RECOVERING | 장애 리소스 제거, k6 정지 | E: `Injector.cleanup(exp_id)`, B: `LoadGenerator.stop(exp_id)` |
| JUDGING | 판정 | B: `Judge.evaluate(exp_id, started_at, ended_at)` → `Verdict(passed, details)` |

## 팀에 요청하는 약속

- **E (injector)**: Chaos 리소스 이름은 `chaos_name(exp_id, step)`으로 고정, 라벨 `chaos-lab/exp-id=<exp_id>` 필수. 같은 이름 재생성 시 409는 무시.
- **B (k6·판정)**: 위 세 함수 시그니처로 구현. 판정은 `started_at`~`ended_at` 구간 기준.
- **state.json 위치**: `ORCH_STATE_PATH` 환경변수 (기본 `/var/lib/orchestrator/state.json`) → **PVC로 마운트해야 재시작 후에도 남음.**

## state.json 예시

```json
{
  "exp_id": "exp-1a2b3c4d",
  "scenario_name": "pod-kill",
  "phase": "ABORTED",
  "aborted_from": "OBSERVING",
  "chaos_resources": ["exp-1a2b3c4d-s0"],
  "cleaned_resources": ["exp-1a2b3c4d-s0"],
  "history": [{"phase": "PRECHECK", "at": 1730870000.1}]
}
```

## 실행

```bash
cd orchestrator && pip install -e '.[dev]'
pytest                                               # mock으로 복구 로직 검증
python -m orchestrator.main --mock run ../experiments/scenarios/pod-kill.yaml
pip install -e '.[k8s]'                              # 실제 클러스터 (E 환경 준비 후)
python -m orchestrator.main run ../experiments/scenarios/pod-kill.yaml
python -m orchestrator.main recover                  # 정리만
```

## 미정 (회의에서 합의)

- 오케스트레이터를 Deployment로 띄울지 Job으로 띄울지
- 시작 게이트가 확인할 항목 (Pod Ready, 수집기 정상 등)
- 시나리오 YAML 형식 (`experiments/scenarios/pod-kill.yaml`이 초안)

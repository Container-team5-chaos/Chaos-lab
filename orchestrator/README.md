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
| WARMUP | k6 출발, warmup 대기 | B: `LoadGenerator.start(run_id, scenario)` |
| INJECTING | 시나리오의 faults 순서대로 주입 | E: `Injector.apply(run_id, step, fault)` |
| OBSERVING | observe 시간 대기 | - |
| RECOVERING | 장애 리소스 제거, k6 정지 | E: `Injector.cleanup(run_id)`, B: `LoadGenerator.stop(run_id)` |
| JUDGING | 판정 | B: `python -m orchestrator.judge run --run runs/<run_id>/run.json` → 수용 조건 4개 확인 |

## B·E 브랜치와 맞춘 것

- **run_id**: B 규칙 `^[a-z0-9-]{3,40}$`, 기본 `r-xxxxxxxx`.
- **run.json (chaos.run/v1)**: runner가 `runs/<run_id>/run.json`에 쓴다. `k6.job = k6-<run_id>`, 주입 시작·종료 시 `marks.inject_start/inject_end`(epoch ms), 주입마다 `faults[{kind, target, start_ms}]`.
- **판정 수용**: 종료 코드 0, `verdict == PASS`, `run_id` 일치, `run_json_sha256`·`criteria_sha256` 일치 4개 모두 만족할 때만 PASS (`judging.accept`).
- **judge.py 삭제**: B의 `orchestrator/judge/` 패키지와 이름이 겹쳐서. 연결부는 `judging.py`.
- **Namespace**: E 환경 기준 `exchange`.

## 팀에 요청하는 약속

- **E (injector)**: Chaos 리소스 이름은 `chaos_name(run_id, step)`으로 고정, 라벨 `chaos/run-id=<run_id>` 필수. 장애 선택자에서 k6 Pod(`app.kubernetes.io/name: k6`)는 제외. 같은 이름 재생성 시 409는 무시.
- **B (k6)**: k6 Job 적용(`judge plan` → `kubectl apply`)을 `LoadGenerator.start`에 연결하는 방식 합의 필요. Job 재적용 금지, fetch 전 Job 삭제 금지는 지킨다.
- **state.json 위치**: `ORCH_STATE_PATH` 환경변수 (기본 `/var/lib/orchestrator/state.json`) → **PVC로 마운트해야 재시작 후에도 남음.**

## state.json 예시

```json
{
  "run_id": "r-1a2b3c4d",
  "scenario_name": "pod-kill",
  "phase": "ABORTED",
  "aborted_from": "OBSERVING",
  "chaos_resources": ["r-1a2b3c4d-s0"],
  "cleaned_resources": ["r-1a2b3c4d-s0"],
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

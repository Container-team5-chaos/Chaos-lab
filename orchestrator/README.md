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

## 최종 결과 (`result`)

| result | 언제 | 종료 코드 |
|---|---|---|
| PASSED | B 판정 PASS + 수용 조건 4개 + `cleanup=OK` | 0 |
| FAILED | B 판정 FAIL (같은 run의 판정일 때만) | 10 |
| INCONCLUSIVE | B 판정 INCONCLUSIVE, 판정 신뢰 불가, 관측 소실(5-a), 정리 실패 | 11 |
| ABORTED | 오케스트레이터 예외·SIGTERM·재시작(5-b) | 1 |

판단할 수 없으면 PASSED를 내지 않는다(fail-closed). `cleanup`은 `OK`/`INCOMPLETE`.

## 시나리오 5: 실험 도구 장애

- **5-a 관측 소실**: WARMUP·OBSERVING·RECOVERING 동안 5초마다 Prometheus `/-/ready` 확인.
  정상 응답이 15초 넘게 없으면 장애·부하를 멈추고 판정 없이 `INCONCLUSIVE (observability_lost)`.
- **5-b 오케스트레이터 재시작**: Job이 같은 `RUN_ID`로 대체 Pod를 띄우면, state.json을 보고
  k6에 멈춤 신호(SIGINT)를 보내고 Chaos 리소스를 지운 뒤 `ABORTED (orchestrator_restart)`로 끝난다.
  새 주입·새 부하는 만들지 않는다. k6 멈춤 성공 여부는 `load_stopped`에 남는다.

## 시나리오 6: 대기열이 쌓였다 빠지는가

시나리오 YAML에 `drain` 블록이 있으면 adapter 대기열(`adapter_queue_depth`)을 지켜본다.

```yaml
drain:
  query: sum(adapter_queue_depth)   # 기본값
  timeout_s: 300                    # 장애 해제 후 이 시간 안에 빠져야 한다
  threshold: 0                      # 최소 기준선
```

- WARMUP 동안의 최대값을 평소 수준으로 잡는다(부하 중에는 정상이어도 pending이 몇 건 있다).
- 장애 중 최대값(`drain.peak`)을 기록하고, 회복 단계에서 부하를 유지한 채 평소 수준 이하로 내려올 때까지 기다린다.
- 결과 반영 (`drain_outcome`)

| drain_outcome | 상황 | result |
|---|---|---|
| drained | 기한 안에 평소 수준으로 내려옴 | 판정기 결과 유지 |
| queue_not_drained | 기한 안에 안 내려옴 | FAILED |
| queue_not_built | 장애 중 대기열이 평소 수준을 넘은 적 없음(장애 효과 없음) | INCONCLUSIVE |
| drain_unobserved | 지표를 읽지 못함(빈 값은 0이 아니라 '모름') | INCONCLUSIVE |

- 빠진 시각은 run.json `marks.queue_drained`에 남는다.

## 시작 게이트

하나라도 실패하면 시작하지 않고(`GATE_FAILED`) 이전 run 기록도 덮어쓰지 않는다. 확인 중 오류도 실패로 본다.

| 검사 | 내용 |
|---|---|
| previous_cleanup | 직전 run이 `cleanup=INCOMPLETE`면 차단 (기동 시 정리를 한 번 더 시도한 뒤 확인) |
| no_leftover_chaos | `chaos/run-id` 라벨이 붙은 Chaos 리소스가 남아 있으면 차단 |
| order_ready | 주문 서비스 `/readyz` 200 |
| price_fresh | Redis `price:<market>`의 `ts`가 5초 이내 |
| prometheus | Prometheus `/-/ready` 200 |

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
  "result": "ABORTED",
  "reason": "orchestrator_restart",
  "aborted_from": "OBSERVING",
  "cleanup": "OK",
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

## 클러스터에서 실행 (E 로컬 환경 기준)

```bash
# 레포 루트에서
docker build -f orchestrator/Dockerfile -t chaos-orchestrator:dev .
kind load docker-image chaos-orchestrator:dev --name chaos-lab
kubectl --context kind-chaos-lab apply -f orchestrator/k8s/rbac.yaml -f orchestrator/k8s/pvc.yaml
RUN_ID=r-$(date +%m%d%H%M)
sed "s/__RUN_ID__/$RUN_ID/" orchestrator/k8s/job.yaml | kubectl --context kind-chaos-lab apply -f -
kubectl --context kind-chaos-lab -n exchange logs -f job/chaos-run-$RUN_ID
```

5-b 확인: OBSERVING 중에 Pod를 지우고 대체 Pod 로그에 `result=ABORTED  cleanup=OK`가 나오는지,
`kubectl -n exchange get podchaos`가 비어 있는지 본다.

## 미정

- k6 Job 출발을 `LoadGenerator.start`에 연결 (B `judge plan` 방식과 합의)
- 시나리오 1의 상태 인지형 주입 (HPA desired > Ready 일 때 주입)
- 시나리오 YAML 형식 (`experiments/scenarios/pod-kill.yaml`이 초안)

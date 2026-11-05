# B. 측정과 판정 — 코드 설계

- 이 문서는 Part B(측정과 판정)가 만들 k6 부하 스크립트와 판정기(judge)의 코드 설계다.
- 근거: 피실험 시스템 명세 `workbook-week1/reference/design-spec.md`. `L123`은 그 명세의 줄 번호다.
- 상태: 초안(2026-11-05). 인터페이스는 11/8 팀 합의 전이다.
- 경로 전제: 분담표는 `chaos/k6/`, `chaos/orchestrator/judge/`다. 현재 저장소는 `load/k6/`, `orchestrator/src/orchestrator/`다. 본문은 현재 경로로 쓰고, 대응은 2절에 둔다.

## 0. 한눈에 보기

**완료 기준이 어떤 코드로 충족되는가**

> 실험이 끝나면 중복·유실 주문 수와 잔고 불일치를 자동 계산해 판정에 반영

| 단계 | 코드 | 결과 위치 |
|---|---|---|
| 보낸 주문 기록 | `load/k6/lib_orders.js`가 보내기 전 `intent`, 끝난 뒤 `result` 줄을 쓴다 | k6 Pod `/out/orders.jsonl` |
| 실험 후 자동 수집 | `judge run` → `fetch.py`(exec로 기록 회수) → `db.py`(exec psql 스냅샷 1회) | `runs/<run_id>/` |
| 유실·중복 주문 수 | `classify.py`(클라이언트 상태 × DB 행렬), `sql/inv_i4.sql`(교차 확인) | `verdict.json` `integrity.classes` |
| 잔고 불일치 | `sql/inv_i2.sql`, `inv_i5_*.sql`, `invariants.py` | `integrity.invariants` |
| 판정 반영 | `verdict.py` 규칙 1: 돈 불변식 FAIL이 다른 결과보다 앞선다 | `verdict`, 종료 코드 0/10/11 |

**산출물과 마감**

| 산출물 | 파일(현재 경로) | 완료 기준 | 마감 |
|---|---|---|---|
| k6 주문 부하 | `load/k6/orders.js`, `lib_*.js` | smoke·steady·accept·demo가 가짜 API와 실제 order-api에서 돈다 | **1차 11/17** |
| k6 WS 부하(최소형) | `load/k6/ws.js` | VU당 10연결, 구독, 재연결(S7, C 측정용) | **1차 11/17** |
| Job과 기록 회수 | `load/k6/k8s/job.yaml`, `hold.sh` | k6가 OOM이어도 기록을 노트북으로 가져온다 | **1차 11/17** |
| judge 핵심 | `orchestrator/src/orchestrator/judge/` | 위 완료 기준. 같은 증거면 판정이 같다 | **1차 11/17** |
| 판정 기준 | `experiments/criteria/*.yaml` | default, order-api-pod-kill, order-api-crash-after-commit, demo-min | **1차 11/17** |
| PromQL, precheck, k6 회복 시간 | `judge/promql.py`, `client.py` | 서버 지표 기준, 주입 전 게이트 | 2차 11/22 |
| 한계 찾기, 거래량 재생, WS 판정, gateway-pod-kill | `judge/limit.py`, `judge/ws.py`, `tools/make_profile.py` | `limit.json`, 재연결·신선도 판정 | 2차 11/22 |
| 여유분 | — | PromQL range 회복, 재송신 분할, 원가 재생, I6 원천 대조 | 3차 11/25 |

## 1. 범위와 경계

**1.1 B가 소유하는 것**: `orders.js`, `ws.js`, 프로파일, Job 템플릿, judge 패키지, 판정 기준 YAML, 로컬 검증용 가짜 order-api(`load/k6/tools/fake_order_api.py`).

**1.2 받는다고 전제하는 것**

| 받는 것 | 주는 쪽(추정) | 없을 때 |
|---|---|---|
| run_id, Job 적용, 장애 시각, 정리 | runner, inject | 운영자가 런북과 `judge mark`로 한다 |
| 주문 API 계약, 응답 필드 이름(L249–261) | 앱 팀 | 응답 원문 보존, 매핑은 기준 파일 한 곳 |
| ingest-replay 가동, `REPLAY:UPBIT:*` 종목(L383, L1187) | 인프라·앱 | 모두 `MARKET_CLOSED`라 INCONCLUSIVE. fallback 파일(L339)로 11/14까지 가동 요청 |
| `pt_ro`, postgres local trust | 앱 팀 | 7.2 |
| 서버 지표, PodMonitor(L788–799) | 앱·인프라 | PromQL 기준은 `required: false` |
| `mirror/k6:2.3.0`(우리 소유, L958) | 팀 인프라 담당 | 11/12까지 push 요청 |

**1.3 만들지 않는 것**: runner·gates·inject·cleanup 코드, 클러스터 안 judge Pod, SA용 Role, NetworkPolicy 변경, DB 드라이버, toolbox, Indexed Job(L630), Grafana 패널, S3 자동 업로드, 판정 결과의 Prometheus 내보내기(L639).

**1.4 B가 주는 계약(3개)**
1. **k6 Job 템플릿과 env 목록**(4.5, 4.8). runner는 `${…}`만 채운다.
2. **JSONL v1**(4.6).
3. **judge 입출력**: 입력 `run.json`, 출력 `verdict.json` v1과 종료 코드.

- `run.json` 키: `schema`(`chaos.run/v1`), `run_id`(`^[a-z0-9-]{3,40}$`), `experiment`, `criteria`, `scenario`(상대 경로는 `run.json` 위치 기준), `k6.job`, `marks{inject_start, inject_end}`, `faults[{kind, target, start_ms}]`, `replay_speed[{from_ms, speed}]`, `extra_accounts`. 시각은 노트북 시계 epoch ms(UTC)다.
- runner가 쓰고 judge는 읽기만 한다. `judge mark`는 `marks.local.json`에 쓰고, judge는 `run.json` 값을 우선해 빈 칸만 채운다.
- 장애 정의(대상, 방법)는 `scenario` 파일 몫이다. 기준 파일에는 판정에 쓸 기대 효과만 둔다.

| 종료 코드(우리 설정값) | 0 | 10 | 11 | 12 | 그 밖 |
|---|---|---|---|---|---|
| `run`, `evaluate` | PASS | FAIL | INCONCLUSIVE | 입력 오류 | judge 오류(예외 1, argparse 2) |
| `doctor`, `precheck`(2차) | 준비됨 | — | 미비 | 입력 오류 | judge 오류 |

- runner는 넷을 모두 만족할 때만 PASS로 받는다: 종료 코드 0, `verdict == "PASS"`, `run_id` 일치, `inputs.criteria_sha256`·`run_json_sha256` 일치.
- **임시 제공**: `judge plan`(Job 렌더), `judge accounts`(대상 계좌 조회), `judge precheck`(2차). runner·gates·observe 담당이 정해지면 옮긴다.
- 스텁 연결 제안: `judge.judge()` → `judge(run_json) -> Verdict`, `report.summary()` ← `report.fragment(verdict)`, `gates.check()` → `precheck()`.

## 2. 디렉터리

| 분담표 경로 | 현재 경로 | 내용 |
|---|---|---|
| `chaos/k6/` | `load/k6/` | ConfigMap에 그대로 넣는 평면 구조. 빈 `script.js`는 `orders.js`로 바꾼다. `k8s/`, `tools/`는 넣지 않는다 |
| `chaos/orchestrator/judge/` | `orchestrator/src/orchestrator/judge/` | 패키지. 기존 `judge.py`는 지운다(같은 이름 파일과 패키지는 공존 불가) |
| `chaos/orchestrator/judge/criteria/` | `experiments/criteria/` | 빈 `default.yaml`을 채운다 |
| `chaos/orchestrator/tests/judge/` | `orchestrator/tests/judge/` | 골든 케이스, 픽스처 |
| (산출물) | `runs/` | `.gitignore`에 추가 |

```text
load/k6/  orders.js ws.js lib_config.js lib_orders.js lib_record.js lib_prng.js lib_ws.js hold.sh
          k8s/job.yaml  tools/fake_order_api.py tools/make_profile.py     # 표준 라이브러리만
judge/    __main__.py model.py context.py criteria.py transport.py fetch.py k6log.py db.py
          classify.py invariants.py client.py verdict.py report.py sql/*.sql  promql.py ws.py limit.py(2차)
```
- **ConfigMap**: kustomize `configMapGenerator`는 루트 밖 파일을 거부한다. 대신 `kubectl -n papertrade create configmap k6-scripts --from-file=load/k6/ --dry-run=client -o yaml | kubectl apply -f -`를 쓴다(최상위 일반 파일만 들어간다).
- **pyproject 제안**: `dependencies = ["PyYAML>=6"]`, `[project.scripts] chaos-judge = "orchestrator.judge.__main__:main"`, pytest `pythonpath = ["src"]`, markers `pg`, `k6`.
- **재구성 대비**: 상대 import만 쓰고 실행 이름을 고정한다. 재구성 PR은 import, pyproject, CI 경로, ConfigMap 명령을 함께 고친다(`git mv`만으로 끝나지 않는다).
- ruff 기본 규칙(E4, E7, E9, F)을 지킨다. 한 줄에 필드 여러 개를 쓰면 E701/E702로 CI가 실패한다.

## 3. 실행 흐름

```text
1 run_id 발급, run.json 작성 → ConfigMap 갱신 → judge plan(job.yaml 렌더) → kubectl apply
2 judge accounts --wait        k6 setup_done 확인, idx0 계좌 id 출력
3 baseline 대기(≥150s)          [2차: precheck가 11이면 주입하지 않는다]
4 장애 주입 → judge mark inject_start
5 k6 종료 또는 judge stop      exec kill -INT 1 → handleSummary 후 종료, holder는 대기
6 judge run                     fetch → 정지 확인 → psql 1회 → Pod 관측 → RELEASE → evaluate
7 수용 조건 확인 → 장애 원복, Job은 TTL로 정리
```
- fetch 전에 `kubectl delete job`을 하면 emptyDir와 함께 기록이 사라진다. 런북에 금지로 적는다.
- **시연**: S7에서 `PROFILE=demo` Job 시작 → S8 두 조작 → S10에서 `judge stop` → `judge run --criteria experiments/criteria/demo-min.yaml`.

## 4. k6

### 4.1 안전장치
init에서 검사하고 어기면 `throw`한다(k6 107 → judge INCONCLUSIVE(LOAD_INVALID)).
```text
ORDER_API, WS_URL ⊨ ^(http|ws)://(order-api|gateway)(\.papertrade(\.svc(\.cluster\.local)?)?)?:8080(/.*)?$
K6_TARGET=local일 때만 ^(http|ws)://127\.0\.0\.1:[0-9]+ 허용(가짜 서버 전용)
```
- 8080 공개 리스너만 허용한다. 8081, 제3자 호스트, 공개 ALB는 막힌다(L631, I13).
- setup은 `GET /api/symbols`에서 `REPLAY:UPBIT:`만 남기고 0개면 abort한다. 첫 계좌 생성 10건이 모두 status 0이어도 abort한다(NetworkPolicy 의심).
- 계좌는 setup이 `mode:"replay"`로 만든 것만 쓴다. 토큰은 VU 메모리에만 둔다.
- `noConnectionReuse: true`를 강제하고 `run_start`에 남긴다(4.4).
- `orders.js`는 시나리오가 하나이고 `ACCOUNTS == preAllocatedVUs == maxVUs`인지 검사한다.
- judge도 다시 검사한다. 위반이면 INCONCLUSIVE(SAFETY_VIOLATION)와 보고서 맨 위 경고를 낸다.

### 4.2 계좌 준비(setup)
- `POST /api/accounts {nickname:"k6-<run_id>-<idx>", mode:"replay"}`를 `ACCOUNTS`번(기본 300, L604) 보낸다. `http.batch` 20개씩, `setupTimeout` 180s(우리 설정값, 기본값 60s).
- 계좌 생성은 멱등하지 않다. 실패하면 새 idx로 만든다. 시도 상한 N × 1.1(우리 설정값), 모자라면 abort(108).
- 초기 현금은 `GET /api/portfolio`로 읽는다(replay 금액이 명세에 없다, L845). 시작가는 `GET /api/series/{sym}?interval=1s&n=1`(L258).
- **1 VU = 1 계좌**: `accts[exec.vu.idInTest − 1]`. `idInTest`는 테스트 전체 번호라 프로세스에 시나리오가 하나일 때만 성립한다. 그래서 WS는 별도 프로세스다(4.8).

### 4.3 주문 생성 규칙(우리 설정값)

| 항목 | 규칙 |
|---|---|
| 난수 | VU마다 mulberry32(seed = `RUN_SEED ^ vu`) |
| 종목·side | `REPLAY:UPBIT:*` 균등. 보유가 있으면 0.4 확률로 sell |
| qty | buy `floor8(10,000원 ÷ P)`. sell은 0.7 확률로 보유 절반, 0.3 확률로 전량(절반이 0이면 전량) |
| limit_price | buy P × 1.05, sell P × 0.95. 체결가는 기준가라(L245, L393) 금액에 영향이 없다 |
| 가격·보유 | 응답 근거가와 거래 후 보유(L272)로 갱신. `LIMIT_NOT_MARKETABLE`이거나 60s가 지나면 series를 다시 읽는다 |
| 수량 표현 | BigInt(1e-8 단위) → 10진 문자열. 본문 문자열은 한 번 만들어 모든 시도에 쓴다 |

### 4.4 연결, 재시도, 재송신

**연결 재사용을 끈다(필수).**
- Go `net/http`는 `Idempotency-Key`가 있는 POST를 재전송 가능하다고 본다. k6는 본문에 `GetBody`를 둔다. 그래서 재사용 연결에서 응답 전 EOF가 나면 Go가 스크립트 모르게 다시 보낸다.
- crash-after-commit에서 이 재전송이 다른 Pod의 저장 결과(200)를 받으면 k6에는 "첫 시도 200"만 남아 거짓 FAIL이 된다.
- `noConnectionReuse: true`면 k6가 `DisableKeepAlives`를 켜 이 경로가 없어진다. 연결마다 kube-proxy가 백엔드를 고르므로 확장된 Pod에도 부하가 퍼진다. 포트·CPU 비용은 스모크로 잰다(L640).
- `run_start.conn_reuse`가 false가 아니면 judge는 FIRST_200을 INCONCLUSIVE(TRANSPORT_REPLAY_POSSIBLE)로 낮춘다.

| 응답 | 분류 | 동작 |
|---|---|---|
| 200, 201 | ACK | 멈춘다 |
| 0(전송 오류·타임아웃), 502, 503, 504 | 결과 불명 또는 롤백 | 같은 키로 1s·2s·4s 뒤 최대 3회(L291) |
| 500 | 결과 불명(명세 밖) | 재시도 없이 조회 |
| 401, 409, 422 등 4xx | PROTOCOL | 멈춘다(L293) |

- 타임아웃 5s(L290, k6 기본값 60s). ACK 없이 4xx도 아니면 `GET /api/orders?idem_key=`를 1s 간격 최대 3회(L292).
- **재송신**: 201을 받은 의도의 10%는 같은 키·같은 본문으로 즉시 다시 보낸다(L604). 재송신에도 위 재시도 규칙을 쓴다.
  - OK: 최종 200, 의미 필드(status, reject_reason, price, fee) 같음.
  - FAIL: 201(DOUBLE_CREATE), 또는 200인데 의미 필드가 다름(RESEND_DIFF).
  - RESEND_UNRESOLVED: 끝까지 2xx 없음(503 LEDGER_*, 장애 중 전송 오류). 보고만 한다.
- 모든 요청에 `tags.name`을 단다(`order_create`, `order_lookup`, `order_resend`, `series`, `account_create`). 기본 `name`은 URL이라 멱등키가 든 조회 URL이 시계열을 늘린다. `systemTags`에서 `url`도 뺀다.
- 의도 하나의 최악 길이는 44s다(계산). `gracefulStop`은 60s다(우리 설정값).

```js
export function placeIntent(a, o) {                    // lib_orders.js 핵심
  const key = crypto.randomUUID(), body = orderBody(o);  // 문자열 한 번만 만든다
  rec({ kind: 'intent', run: RUN, key, acct: a.id, vu: exec.vu.idInTest, t: Date.now(), body }); intents.add(1);
  const p = params(a.token, key, 'order_create');        // Idempotency-Key, timeout 5s, tags.name
  const atts = sendWithRetry(body, p), ack = firstAck(atts);
  const lookup = (!ack && !is4xx(last(atts))) ? lookupByKey(key, a.token) : null;
  const resend = (ack && ack.st === 201 && rnd() < RESEND_RATIO) ? sendWithRetry(body, params(a.token, key, 'order_resend')) : null;
  rec({ kind: 'result', run: RUN, key, atts, lookup, ack: ack && raw(ack), resend, t: Date.now() }); results.add(1);
}
```

### 4.5 프로파일
- 주문 프로파일은 모두 `ramping-arrival-rate`(열린 모델)다. `timeUnit: '1m'`, target = `round(rps × 60)`, 구간마다 `{0s 점프}` + `{유지}` stage. `run_start.plan`으로 judge가 계획 도착 수를 적분한다.

| PROFILE | 구간(우리 설정값) | 용도 |
|---|---|---|
| `smoke` | `--once` 또는 1/s × 10s | 경로 확인 |
| `steady` | 30/s × 600s. 0–60s warmup, 240s 주입, 480–600s tail | 장애 run |
| `accept` | 30/s 300s → 300/s 10s → 30/s 180s(L604) | 11/18 주문 축 |
| `demo` | 30/s를 `DURATION_S`(기본 900s)까지, `SPIKE_AT_S`(기본 60s)에 300/s × 10s | 시연. `judge stop`으로 끝낸다 |
| `step`, `replay` | 4.10, 4.11 | 2차 |

- demo를 고정 3.5분으로 두지 않는다. S8 수동 조작이 부하 창을 넘기면 `fault_in_load` 가드로 INCONCLUSIVE가 되고, 슬롯 길이도 미정이다(L614).
- `--paused` + `k6 resume`은 쓰지 않는다. k6 v2.0.0에서 `k6 pause/resume/scale/status`가 제거됐다.
- **env**: `RUN_ID`, `PROFILE`, `ORDER_API`, `ACCOUNTS`(300), `RATE`(30), `DURATION_S`, `SPIKE_AT_S`, `THRESHOLDS`(`fault`|`slo`|`step`), `RUN_SEED`, `RESEND_RATIO`(0.1), `WS_URL`, `WS_TARGET`(괄호는 우리 설정값).

### 4.6 주문별 기록 JSONL v1
- `console.log(JSON.stringify(x))` 한 줄, 인자 `--log-format=raw --console-output=/out/orders.jsonl`. raw면 메시지가 그대로 한 줄이다(소스 확인).

| kind | 언제 | 필드 |
|---|---|---|
| `run_start` | setup 시작 | `v, run, script_ver, seed, profile, plan, symbols, initial_cash, conn_reuse, t` |
| `account` | 계좌마다 | `run, idx, acct, nick`(토큰 없음) |
| `setup_done` | setup 끝 | `run, accounts, t` |
| `intent` | **보내기 전** | `run, key, acct, vu, t, body` |
| `result` | 끝난 뒤 | `run, key, atts[{n,t0,t1,st,ec,ms}], lookup, ack{st, raw(≤2KB)}, resend, t` |

- 응답 원문을 남겨 필드 이름 확정 뒤 다시 판정한다. 의도 1건 약 1KB(추정)다.

### 4.7 지표, threshold, handleSummary
- 지표: `order_intents`·`order_results`(Counter, 완전성 대조), `order_sys_err`(Rate, 첫 시도 0·5xx), `order_ack_ms`(Trend), `order_protocol_errors`·`resend_mismatch`(Counter). 앞 두 지표에 태그 `step`.
- k6 threshold는 전체 누적이고 `p(95)`는 선형 보간이다. judge는 구간별 nearest-rank다. 그래서 **SLO 판정은 judge 하나로만 한다.** threshold는 계약 위반 지표와 클러스터 보호에만 쓴다. `THRESHOLDS=slo`의 SLO 실패(99)는 경고 `K6_THRESHOLD_DISAGREES`다.
- v2.3.0 소스로 확인한 사실: `abortOnFail` 중단도 99다(108은 `exec.test.abort()` 전용). `--new-machine-readable-summary` 기본값은 false라 handleSummary는 기존 모양(`metrics`, `state`, `setup_data`)을 받는다. Job env에 `K6_NEW_MACHINE_READABLE_SUMMARY`를 두지 않는다.
```js
export function handleSummary(data) {
  delete data.setup_data;                               // 필수: 계좌 토큰이 들어 있다
  return { '/out/summary.json': JSON.stringify(data), stdout: oneLine(data) + '\n' };
}
```

### 4.8 Job과 기록 회수
- 한 Pod에 `k6`(orders.js), `k6-ws`(ws.js, WS run만), `holder`를 둔다. 자원은 명세 1 CPU / 2Gi, limit 4Gi(L763, L834) 안에서 나눈다. 시작값 k6 300m/512Mi, k6-ws 700m/1.5Gi(우리 설정값, 100 VU 실측 후 조정, L632). 쿼터 계산(L826–837)이 바뀌지 않는다.

```yaml
spec:                                              # load/k6/k8s/job.yaml 핵심
  backoffLimit: 0                                  # L528
  podFailurePolicy: {rules: [{action: FailJob, onPodConditions: [{type: DisruptionTarget}]}]}
  activeDeadlineSeconds: ${DEADLINE}               # HOLD_S + 120 (우리 설정값)
  template:
    metadata:
      labels: {app.kubernetes.io/name: k6, chaos/run-id: "${RUN_ID}"}   # part-of=papertrade는 달지 않는다
      annotations: ${DISRUPT_ANNOTATION}           # 기본 {}. 리허설·시연만 do-not-disrupt
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      affinity: ${ANTI_AFFINITY}                   # preferred, name In [gateway, order-api] (L637)
      containers:                                  # 셋 다 ${K6_IMAGE}, /scripts·/out 마운트
      - {name: k6, args: ["run", "--log-format=raw", "--console-output=/out/orders.jsonl", "/scripts/orders.js"]}
      - {name: k6-ws, args: ["run", "--log-format=raw", "--console-output=/out/ws.jsonl", "/scripts/ws.js"]}
      - {name: holder, command: ["sh", "/scripts/hold.sh"]}   # /out/RELEASE 또는 HOLD_S까지 대기
      volumes: [{name: scripts, configMap: {name: k6-scripts}}, {name: out, emptyDir: {sizeLimit: 2Gi}}]
```
- `HOLD_S = setupTimeout + 프로파일 길이 + gracefulStop + 수집 예산 600s`(우리 설정값).
- **재실행 차단**: Pod가 다시 뜨면 계좌와 주문이 또 생긴다. `podFailurePolicy`와 `backoffLimit: 0`으로 막고, 섞이면 ACCOUNT_SET_MISMATCH로 잡는다.
- **holder**: cgroup v2에서 kubelet 기본값(`singleProcessOOMKill: false`)은 `memory.oom.group`을 건다. OOM이면 컨테이너 전체가 죽으므로 같은 컨테이너 안 래퍼로는 회수할 수 없다.
- **do-not-disrupt**: 자발적 중단과 graceful eviction만 막는다. expiration, interruption, 수동 노드 삭제는 막지 못한다. 명세는 11/25–11/26에만 쓴다(L579).
- **라벨**: `app.kubernetes.io/name: k6`가 `allow-vpc-http`의 k6 podSelector와 맞아야 한다(L893).

**`judge fetch`**(`pods/exec`, `pods/log`, L995)
1. `-l batch.kubernetes.io/job-name=k6-<run_id>`로 Pod를 찾고 k6 컨테이너가 모두 terminated가 될 때까지 기다린다. `exitCode`, `reason`을 `k6/pod-status.json`에 쓴다.
2. `exec -c holder -- sha256sum /out/*`, `exec -c holder -- gzip -c /out/orders.jsonl > k6/orders.jsonl.gz`. `kubectl cp`(tar 의존)는 피한다.
3. sha256을 비교한다. 3회 실패하면 SENT_LOG_MISSING이다.
4. `exec -c holder -- date -u +%s`를 RTT로 감싸 노드 시계 차이를 `derived.json`에 남긴다.
5. DB 수집 뒤 `exec -c holder -- touch /out/RELEASE`.

**`judge stop`**: `kubectl exec -c k6 -- kill -INT 1`(WS run은 `-c k6-ws`도). k6는 SIGINT에 테스트를 멈추고 teardown과 handleSummary를 돈다(스모크로 확인).

### 4.9 WS 부하 [1차 최소형]
- S7의 연결 수·replica 장면(L102–107)과 C 측정(L595, L1192)에는 WS 부하가 필요하다. k6는 컷 대상이 아니다(L1174). 그래서 **부하 생성은 1차**, 판정(5.11)은 2차다.
- `k6/websockets`(v2.3.0, 재연결 루프 버그 수정 포함), `ramping-vus`, VU당 연결 10개(L627).
- 연결마다 `{"op":"sub","symbols":[REPLAY:UPBIT 5종목]}`를 `ws://gateway.papertrade.svc:8080/ws`로 보낸다(L298–299, L601).
- 재연결은 1s → 최대 10s 지수 백오프 + full jitter다(L512). `run_start`에 `ws_reconnect: true`를 남긴다.
- demo 모양: 0 → `WS_TARGET` 연결을 90s 램프하고 유지한다(L611).
- 기록은 상태 전이(`ws_open`, `ws_snap`, `ws_down{cause, code}`, `ws_regress`)와 VU별 10초 집계 `wsagg{n, bins}`(경계 50–5000ms, 우리 설정값)만이다.
- 측정 정의: 신선도 = `수신 Date.now() − recv_ts`(L619). `t_down` = onclose·onerror·`status` 10s 무수신 중 이른 시각(L513). 재연결 시간 = `t_down` → 첫 `snapshot`(L307). I10 역행 = 연결 안 `event_ts` 감소(L473).
- `onerror` 뒤 `onclose`가 반드시 오는지 문서에 없다(확인 필요). 두 핸들러 모두에서 중복을 막는다.

### 4.10 한계 찾기 [2차]
- 계단 1 run: `R_k = 30 × 1.25^k`, `R_max = 360`(관리자 합의). 계단 60s 중 앞 15s를 버리고 45s를 잰다. 표본 `n ≥ 20/(1−q)`(p95면 400).
- 계단 판정 순서:
  1. p95 ≥ 300ms 또는 첫 시도 오류율 ≥ 1% → SYSTEM_LIMIT. R*는 직전 계단이다.
  2. 1이 통과하고 달성률 < 0.98만 깨짐 → GENERATOR_LIMITED(INCONCLUSIVE).
  3. 체결 비율 < 0.8(우리 설정값) → CHEAP_PATH(INCONCLUSIVE). 거절은 orders 1행만 써서(L271) R*를 부풀린다.
- 실패 의도는 VU를 최대 44s 붙잡는다. step은 VU·계좌를 `R_max × 5s + 여유`(예: 1,800)로 늘린다.
- 보호 장치는 계단마다 건다: `'order_sys_err{step:"k"}': [{threshold: 'rate<0.2', abortOnFail: true, delayAbortEval: '15s'}]`.
- HPA min=max, KEDA `paused-replicas` 고정을 요청한다. 못 하면 "배포 설정 기준 한계"로 적고 Pod별 요청 분포를 남긴다. `REPLAY_SPEED=1`로 리플레이 위상 영향을 줄인다.
- `limit.json`: 계단별 p95, 오류율, 달성률, 체결 비율, 거절 사유, dropped_iterations, R*.

### 4.11 거래량 재생 [2차]
- 정의(명세 밖): 녹화된 업비트 체결 **건수**의 초별 모양을 주문 도착률 모양으로 옮긴다. volume은 쓰지 않는다(L315).
- `make_profile.py`: `type=="trade"`, `(source, seq)` 중복 제거(L340), 칸 = `floor((ts − 첫 ts) ÷ 1000 ÷ 10)`, 평균 30/s로 배율, `--cap 300`, `--spike 300:10s@t`. 35분 녹화(L616)면 210점이다.
- `source != "upbit"`이거나 리플레이 symbol이 `REPLAY:UPBIT:`가 아니면 중단한다.
- 프로파일 JSON은 커밋하지 않는다(공개 저장소, 업비트 파생 데이터, L1170). 생성 명령과 sha256만 커밋한다.
- 원천: `REPLAY_SOURCE` 파일 → `kubectl exec <ingest-replay> -- cat /app/replay/fallback.jsonl.gz`(L339) → 노트북 S3 읽기(요청).

## 5. judge

### 5.1 모듈과 시그니처
```python
def collect(ctx: RunContext, crit: Criteria, tr: Transports) -> Path      # 클러스터를 만지는 유일한 함수
def evaluate(ctx: RunContext, crit: Criteria, evidence: Path) -> Verdict  # 순수 함수(네트워크·현재 시각 없음)
def judge(run_json: Path, *, offline: bool = False) -> Verdict
def classify(log: K6Log, db: DbSnapshot, cfg: IntegrityCfg) -> list[Classified]
def check_invariants(db: DbSnapshot, ctx: RunContext, cfg: IntegrityCfg) -> list[CheckResult]
def combine(checks: Sequence[CheckResult], guards: Sequence[CheckResult]) -> tuple[Outcome, tuple[str, ...]]
CMD_PREFIX = ("kubectl", "-n", "papertrade")   # 테스트는 ("docker", "exec", "-i", "pgtest")
```
- `model.py`의 dataclass(RunContext, K6Log, DbSnapshot, CheckResult, Verdict 등)는 frozen이다.
- CLI(1차): `chaos-judge {plan|accounts|doctor|fetch|collect|evaluate|run|mark|stop} --run runs/<id>/run.json [--criteria x.yaml] [--offline]`. 2차: `precheck`, `limit`.
- `mark`는 시각 외에 `--speed 10`, `--extra-account <uuid>`, `--obs key=value`를 받는다.

### 5.2 collect 순서
1. **fetch**(4.8).
2. **정지 확인**: k6 종료 뒤 10s 기다린다(우리 설정값). 서버 deadline 3s(L265), idle 트랜잭션 5s(L515)보다 길다.
3. **DB**: psql을 한 번 exec한다(5.4). 지문 F1·F2가 다르면 30s 뒤 한 번 더, 그래도 다르면 INCONCLUSIVE(DB_NOT_QUIESCENT).
4. **Pod 관측**: `kubectl get pods -l 'app.kubernetes.io/name in (order-api,gateway,ingest-replay)' -o json`(L995) 한 번으로 만든다.
   - `pod_replaced`: `creationTimestamp > inject_start`인 Ready Pod.
   - `crash_restart`: `lastState.terminated.exitCode == 1`이고 `finishedAt`이 fault 구간 안.
   - 배속: ingest-replay `REPLAY_SPEED`와 `startTime`. Pod가 k6보다 먼저 떴으면 run 내내 그 배속이다. 아니면 `mark --speed`가 필요하다.
   - run.json에 inject 관측값이 있으면 그것을 우선한다.
5. RELEASE, `evidence.json`(파일별 sha256).
6. **evaluate**: 증거, run.json, `criteria.resolved.yaml`만 읽는다. 분위수는 nearest-rank `x[ceil(q·n)−1]`이다. `verdict_digest`는 `generated_at`과 `run_info`(실행 시간, 전송 경로)를 뺀 canonical JSON의 sha256이다.
- 산출물은 `runs/<run_id>/` 아래 `k6/`, `raw/db/`, `raw/k8s/`, `verdict.json`, `report.md`, `classified.csv`, `judge.log.jsonl`(단계별 소요 시간)이다.

### 5.3 주문 분류

**이번 run 계좌**
- 1차 키는 JSONL `account` 줄의 계좌 UUID 목록이다.
- 보조로 `mode='replay' AND nickname ~ '^k6-<run_id>-[0-9]+$'` 계좌를 찾는다(setup 응답을 잃어 서버에만 생긴 계좌). 정확한 정규식이라 `r1117-crash`와 `r1117-crash-2`가 섞이지 않는다.
- JSONL 계좌가 DB에 없거나 보조 계좌가 N × 0.1을 넘으면 INCONCLUSIVE(ACCOUNT_SET_MISMATCH)다. 이때 LOST·ORPHAN은 판정하지 않는다.
- `extra_accounts`(시연 발표자 replay 계좌)는 DB 전용 검사(I2, I4, I5, I8, DUP_*)만 받는다.
- 키는 `(account_id, idem_key)`다. 응답에 주문 id가 없다(L272).

**1단계: 클라이언트 상태 K**

| K | 조건 |
|---|---|
| K_ACK | 원 시도 중 200 또는 201 |
| K_FOUND / K_404 | ACK 없음, 조회 200 / 404 |
| K_UNKNOWN | ACK 없음, 조회 실패 또는 없음 |
| K_4XX | 마지막 원 시도가 4xx |
| INTERRUPTED | `intent`만 있고 `result`가 없다(k6가 중간에 죽음). DB 행 유무만 보고한다 |

**2단계: DB와 상관없는 시도 이상**(IDEMPOTENCY_VIOLATION, FAIL)
- DOUBLE_CREATE: 한 키에 201이 2번 이상. FIRST_200: 새 키의 첫 시도가 200(`conn_reuse == false`일 때만 FAIL). RESEND_DIFF. 409_SAME_BODY: k6는 늘 같은 바이트를 보낸다.

**3단계: K × DB 행렬**(D1 = 비교 항목 일치, 2건 이상은 DUP_ORDER)

| K \ DB | D1 일치 | D1 불일치 | D0(행 없음) |
|---|---|---|---|
| K_ACK | OK | MISMATCH, FAIL | LOST, FAIL(I12, L475) |
| K_FOUND | OK(lookup) | MISMATCH, FAIL | PHANTOM_ACK, FAIL |
| K_404 | FALSE_NOT_FOUND, FAIL | FALSE_NOT_FOUND, FAIL | NOT_ENTERED |
| K_UNKNOWN | UNRESOLVED_COMMITTED | BODY_MISMATCH, FAIL | UNRESOLVED_ABSENT |
| K_4XX | PROTOCOL, INCONCLUSIVE | PROTOCOL | PROTOCOL |

- 보낸 본문 대 DB(`acct, symbol, side, qty, limit_price`, Decimal) 불일치는 늘 FAIL이다.
- 응답 대 DB(status, reject_reason, fill의 price·fee)는 `response_fields` 매핑을 따른다. `response_fields_confirmed: false`면 RESPONSE_SCHEMA_UNCONFIRMED(INCONCLUSIVE)다.
- **OK 세부**: `clean`(첫 시도 201), `after_commit`(0·502·504 뒤 200, crash 효과), `not_committed`(결과 불명 뒤 201), `lookup`, `after_503`(503 뒤 200). 명세상 503은 롤백이라(L254) `after_503`은 경고 ROLLBACK_REPLY_COMMITTED이고 장애 효과 증거에서 뺀다.

**4단계: DB 전용**: DUP_ORDER, DUP_FILL, FILL_STATE(FILLED인데 fill≠1, REJECTED인데 fill 있음, 필드 불일치), ORPHAN(k6 계좌 주문인데 키가 `intent`에 없음).

**완료 기준 용어 대응**
- 유실 = LOST + PHANTOM_ACK + FALSE_NOT_FOUND. 중복 = DUP_ORDER + DUP_FILL + FILL_STATE + IDEMPOTENCY_VIOLATION. 내용 불일치 = MISMATCH + BODY_MISMATCH. 고아 = ORPHAN.
- UNRESOLVED_*, NOT_ENTERED, INTERRUPTED는 DB 사실로 확정했으므로 돈 문제가 아니다. 사용자 영향(`intent_unresolved_ratio`)으로 보고한다.

**기록 완전성(`log_complete`)**: 모든 줄 파싱, `run` 값 하나, `run_start` 1개, `intent`·`result` 줄 수 ≥ 각 Counter, sha256 일치, k6 종료 코드 0 또는 99. 아니면 ORPHAN과 unresolved 기준이 INCONCLUSIVE(SENT_LOG_INCOMPLETE)다. LOST·MISMATCH·중복은 있는 줄이 근거라 계속 판정한다.

### 5.4 DB 스냅샷과 잔고 불변식
```bash
kubectl -n papertrade exec -i postgres-0 -c postgres -- psql -X -q -v ON_ERROR_STOP=1 -U pt_ro -d papertrade -f - < bundle.sql
```
`db.py`가 만드는 `bundle.sql` 순서:
1. `\set ids '{uuid,…}'`, `\set extra '{…}'`, `\set nick_re '^k6-<run_id>-[0-9]+$'`.
2. 지문 F1: 대상 계좌의 `(count(orders), count(fills), sum(accounts.version))`.
3. `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` → 스냅샷 COPY 4개 → 필수 불변식 → `COMMIT`.
4. 지문 F2.
5. `\set ON_ERROR_STOP off` → 선택 검사(arith, privileges). 선택 검사 오류가 필수 증거를 지우지 않는다.

- 결과마다 `\qecho '--@ <name>'`을 둔다. 구획이 빠진 기준만 INCONCLUSIVE(DB_ERROR)다.
- `COPY … TO STDOUT`은 SELECT 권한만 있으면 된다. numeric은 텍스트로 받아 `Decimal`로 바꾼다.
- 스냅샷에 `k6` 열을 붙인다. 분류기는 k6 계좌만, 불변식은 `ra` 전체를 본다.
- 비교는 모두 `IS DISTINCT FROM`이다. 명세 스키마에 NOT NULL이 없고 CHECK는 NULL을 만족으로 본다.

```sql
WITH ra AS (SELECT id FROM accounts WHERE id = ANY (:'ids'::uuid[]) OR id = ANY (:'extra'::uuid[])
                                       OR (mode = 'replay' AND nickname ~ :'nick_re'))
-- inv_i5_cash.sql 본문
, f AS (SELECT account_id, sum(CASE WHEN side = 'buy' THEN -(amount + fee) ELSE amount - fee END) AS d,
               sum(realized_pnl) AS pnl FROM fills WHERE account_id IN (SELECT id FROM ra) GROUP BY 1)
, p AS (SELECT account_id, sum(cost_basis) AS cb FROM positions WHERE account_id IN (SELECT id FROM ra) GROUP BY 1)
SELECT a.id, a.cash, a.initial_cash + coalesce(f.d, 0) AS expect_cash
FROM accounts a JOIN ra USING (id) LEFT JOIN f ON f.account_id = a.id LEFT JOIN p ON p.account_id = a.id
WHERE a.cash IS DISTINCT FROM a.initial_cash + coalesce(f.d, 0)
   OR a.cash + coalesce(p.cb, 0) IS DISTINCT FROM a.initial_cash + coalesce(f.pnl, 0);
```

| 파일(빈 결과 = PASS) | 위반 행 | 판정 |
|---|---|---|
| `inv_i4` | 키 중복, fill 중복, 상태-체결 불일치, 필드 불일치 | 분류기와 다르면 JUDGE_SELF_CHECK |
| `inv_i2`, `inv_nulls` | 음수 cash·qty·cost_basis. 핵심 열과 FILLED ref 필드의 NULL | required |
| `inv_constraints` | CHECK 3개, UNIQUE 2개 부재(`to_regclass`로 조회) | CONSTRAINT_MISSING |
| `inv_i5_cash`, `inv_i5_qty` | 현금식·보존식(L468), (계좌, 종목)별 순수량 ≠ positions.qty | required |
| `inv_i5_flat` | `qty = 0 AND cost_basis <> 0`(전량 매도면 원가 전부, L398) | required |
| `inv_i6_db` | `price IS DISTINCT FROM ref_price`, ref NULL / REPLAY인데 `ref_source <> 'replay'`(추론) | 앞 둘 required |
| `inv_i8` | replay 계좌가 `REPLAY:` 아닌 종목 체결, live는 반대 | required |
| `inv_arith` | `abs(amount − qty×price) > 0.5`, `fee <> ceil(amount×fee_rate)`, 매수 `realized_pnl <> −fee` | 선택(반올림 확정 후 required) |
| `inv_privileges` | `to_regrole('pt_app')`의 orders·fills UPDATE·DELETE 권한(I9) | 선택 |

**I7: 신선도 위반 체결(Python)**
- FILLED이고 `ref_age_ms > 60000 ÷ speed(received_ms)`면 위반이다(L388). `ref_age_ms` NULL도 위반이다.
- 배속은 Pod 관측 → `marks.local.json` → `run.json` 순이다. 전환 전후 15s(우리 설정값)는 느슨한 값을 쓴다. 모르면 INCONCLUSIVE(NO_SPEED_TIMELINE).
- DB 시계 기준 나이 `received_ms − ref_event_ts`도 보고한다.
- 앱이 쓴 상한을 추정한다(STALE_PRICE 거절의 최소 나이, FILLED의 최대 나이). judge 상한과 다르면 경고 APP_MAX_AGE_DIFFERS다. 앱이 배속을 아는 방법이 명세에 없다(L348, L845).
- 체결 순간 hb 나이는 저장되지 않아 피드 생존 부분은 "검증 불가"로 보고한다.
- 2차: order-api·ingest-replay Pod 시계를 exec `date`로 재서 차이가 상한의 10%를 넘으면 CLOCK_SKEW.

### 5.5 k6 기록 기반 구간 SLO
- `ack_p95_ms`: ACK 받은 첫 시도의 `ms`(구간 배정은 `t0`). `sys_err_ratio`: 첫 시도 중 0·5xx 비율(4xx 제외, 업무 거절은 성공, L255). `achieved_ratio`: intent 수 ÷ `plan` 적분. `filled`, `sell_filled`: 분류 결과의 FILLED 수.
- 구간(우리 설정값): warmup 60s, baseline `[inject_start − 150s, inject_start − 2s]`, fault `[inject_start, inject_end + 60s]`, recovery(fault 이후 끝까지), tail 마지막 120s, steady(장애 없는 run의 warmup 이후).
- 노드·노트북 시계 차이(4.8)로 보정한다. 2s(우리 설정값)를 넘으면 시간 기준은 CLOCK_SKEW다. 표본은 `n ≥ 20/(1−q)`, 모자라면 LOW_SAMPLES다.

### 5.6 가드(공허한 PASS 방지, 미충족이면 INCONCLUSIVE)

| 가드 | 조건 | 사유 |
|---|---|---|
| `safety` | 모든 intent가 `REPLAY:UPBIT:*`·run 계좌, JSONL·summary에 `token` 키 없음 | SAFETY_VIOLATION + 경고 |
| `account_set` | 5.3 계좌 집합 일치 | ACCOUNT_SET_MISMATCH |
| `fill_coverage`, `sell_coverage` | 구간별 FILLED·매도 체결 ≥ 기준(전부 거절이면 불변식이 저절로 성립) | LOW_FILL_COVERAGE |
| `load_coverage` | baseline·tail·steady `achieved_ratio ≥ 0.98` | GENERATOR_LIMITED |
| `fault_in_load` | 모든 장애가 부하 창 안 | FAULT_OUTSIDE_LOAD |
| `fault_effect` | 장애별 기대 효과 관측(6절) | FAULT_NOT_OBSERVED |
| `db_quiescent` | F1 == F2 | DB_NOT_QUIESCENT |

### 5.7 집계 규칙
1. **돈 불변식**(kind `integrity`: 유실, 내용 불일치, 중복, 고아, I2–I8)이 required FAIL → **FAIL(INTEGRITY_VIOLATION)**. 가드보다 앞선다. ACCOUNT_SET_MISMATCH면 LOST·ORPHAN은 평가하지 않는다.
2. 가드 미충족 → INCONCLUSIVE.
3. 나머지 required(kind `unresolved`, `client`, `prom`, `k6`) FAIL → FAIL.
4. required가 INCONCLUSIVE이거나 평가하지 못함 → INCONCLUSIVE. 조용히 건너뛰지 않는다.
5. 그 밖은 PASS. `required: false`는 보고만, `requires:`가 맞지 않으면 N/A.

- k6 99: `state.testRunDurationMs`가 계획보다 짧으면 abortOnFail 중단(LOAD_ABORTED). 끝까지 돌았으면 실패한 threshold로 매핑한다(`resend_mismatch`는 분류기 결과, protocol은 PROTOCOL, SLO는 경고).
- 108·104·107은 LOAD_ABORTED·LOAD_INVALID, OOM·기록 잘림은 기록 의존 기준만 INCONCLUSIVE, psql 실패는 DB_ERROR다. DB만으로 되는 불변식은 계속 판정한다.

### 5.8 verdict와 보고서
- 최상위 키: `schema`(`papertrade.verdict/v1`), `run_id`, `experiment`, `verdict`, `reasons`, `checks`, `guards`, `integrity{intents, log_complete, classes, subs, invariants, examples}`, `client`, `load`, `inputs{criteria_sha256, run_json_sha256, evidence_manifest_sha256, judge_version, response_fields_confirmed}`, `warnings`, `verdict_digest`, `run_info`, `generated_at`.
- `examples`는 클래스별 키 20개까지, 전체는 `classified.csv`다. Prometheus 주소는 넣지 않는다(공개 저장소).
- `report.md` 조각: 의도·ACK 수, 유실·불일치·중복·고아, 잔고 불일치와 신선도 위반 체결, 커밋 후 회수 건수, 구간 p95, INCONCLUSIVE 사유와 해소 방법, 증거 해시.

### 5.9 PromQL 판정 [2차]
- 구간 끝 시각에 창 = 구간 길이로 instant 쿼리를 부른다. gauge reduce는 `min_over_time((Q)[$win:$res])`로 감싸고 `$res`(scrape 간격)를 늘 명시한다.
- 창 하한: rate는 `$win ≥ 4 × scrape_interval`, gauge subquery는 점 3개 이상. `judge plan`이 구간 길이를 검사해 모자라면 입력 오류(12)다. baseline이 150s인 이유다(118s는 30s × 4에 못 미친다).
- 조회는 구간 끝 + 2 × scrape 이후다. NaN·±Inf·빈 결과·`status:error`는 INCONCLUSIVE이고, 비교 전에 `math.isfinite`를 검사한다.
- 오류율 분자는 `(… or vector(0))`. 주문 p95는 `sum(rate(…_bucket{$sel, le="0.3"}[$win])) / sum(rate(…_count{$sel}[$win])) ≥ 0.95`로 본다. 경계가 없으면 BUCKET_BOUNDARY_MISSING이다.
- 스크레이프가 한 번 실패하면 stale marker로 `sum`이 순간적으로 준다. gauge 합계는 `sum(last_over_time(x{$sel}[2×scrape]))`, reduce는 `quantile_over_time(0.1, …)`를 쓴다. `up==0`으로 Pod 다운을 판정하지 않는다.
- 전송: direct 우선, 없으면 relay(`kubectl exec <k6-pod> -c holder -- wget -q -O - -T 15 "<URL>"`). busybox wget은 오류 응답 본문을 버린다. 상태 코드는 stderr `server returned error: HTTP/1.x NNN`에서 읽는다(400/422 → CRITERIA_ERROR, 503 → 재시도 후 PROM_ERROR).
- 서버 지표는 교차 확인(`required: false`)이다. 죽은 프로세스의 요청은 히스토그램에 없다(L794).

### 5.10 회복 시간 [2차: k6 기록, 3차: PromQL]
```text
입력: 첫 시도 (t0, st, ms), ref = inject_start, W=10s, min_n=50, hold=30s, deadline=120s (우리 설정값)
1 초 칸 b마다 창 [b−W, b)의 n, bad, p95.  ok(b) = n ≥ min_n ∧ bad/n < 0.01 ∧ p95 < 300
2 [ref, ref+deadline+hold]에서 n < min_n 칸이 10% 초과 → INCONCLUSIVE(LOW_TRAFFIC)
3 t_imp = ref 이후 첫 위반 칸. 없으면 PASS(rt=0)
4 s = t_imp 이후 ok가 hold 동안 이어지기 시작하는 첫 칸
5 s − ref ≤ deadline → PASS(rt, time_to_impact, peak) / 늦으면 FAIL(RECOVERED_LATE)
  관찰 부족 → INCONCLUSIVE(OBSERVATION_TOO_SHORT) / 그 밖 FAIL(NOT_RECOVERED_BY_DEADLINE)
```
- 3차 PromQL 판은 히스테리시스, max_gap, coverage를 더한다.

### 5.11 WS 판정 [2차]
- `never_reconnected == 0`, `reconnect_p95_ms ≤ 15,000`(L512 + 여유), `event_ts_regress == 0`(한 연결 안), 신선도는 상대 기준(tail p95 ≤ baseline p95 × 1.5)이 required이고 절대 기준(p95 < 1000ms, L619)은 시계 측정 전 선택이다.
- `ws_restored`(tail 연결 수 ≥ baseline 중앙값 × 0.95)는 부하 쪽이 `ws_reconnect: true`를 선언했을 때만 required다. 아니면 N/A다.

## 6. 판정 기준 YAML

**규칙**
- `schema_version: 1`. 모르는 키는 입력 오류(12)다.
- `extends`는 패치 병합이다. 맵은 깊은 병합, `checks`·`guards`·`fault_effect`는 같은 id면 필드 단위 덮어쓰기, 새 id면 추가한다. 통째 교체는 `replace: true`, 끄기는 `disable: [id]`다.
- 병합 뒤 필수 키를 검증하고 `criteria.resolved.yaml`로 저장한다. 그 해시가 `criteria_sha256`이다.
- 모든 기준에 `required`(기본 true), `on_violation`(fail|inconclusive, 기본 fail)이 있다.

```yaml
# experiments/criteria/default.yaml (가드는 5.6, recovery·steady 구간은 5.5 기본값)
schema_version: 1
safety: {symbol_prefix: "REPLAY:UPBIT:", account_mode: replay}
phases: {warmup: 60s, baseline: {before: inject_start, duration: 150s, guard: 2s},
         fault: {from: inject_start, to: inject_end, extra: 60s}, tail: {before: k6_end, duration: 120s}}
coverage: {min_filled: {baseline: 100, tail: 100, steady: 300}, min_sell_filled: 20, min_achieved_ratio: 0.98}
integrity: {response_fields: {status: status, reject_reason: reason, price: price, fee: fee},  # (확인 필요: 앱 팀)
            response_fields_confirmed: false, price_max_age_ms: 60000}                     # L388
prometheus: {enabled: false}            # 2차. scrape_interval 30s (확인 필요: 관리자)
checks:
  - {id: lost, kind: integrity, class: [LOST, PHANTOM_ACK, FALSE_NOT_FOUND], op: '==', value: 0}
  - {id: mismatch, kind: integrity, class: [MISMATCH, BODY_MISMATCH], op: '==', value: 0}
  - {id: duplicates, kind: integrity, class: [DUP_ORDER, DUP_FILL, FILL_STATE, IDEMPOTENCY_VIOLATION], op: '==', value: 0}
  - {id: orphans, kind: integrity, class: [ORPHAN], op: '==', value: 0}
  - {id: invariants, kind: integrity, sql: [i2, constraints, nulls, i4, i5_cash, i5_qty, i5_flat, i6_db, i8], op: '==', value: 0}
  - {id: i7_stale_fill, kind: integrity, check: i7, op: '==', value: 0}     # L501, L623
  - {id: arithmetic, kind: integrity, sql: [arith], op: '==', value: 0, required: false}
  - {id: protocol, kind: unresolved, class: [PROTOCOL], op: '==', value: 0, on_violation: inconclusive}
  - {id: order_p95_k6, kind: client, stat: ack_p95_ms, op: '<', value: 300, phases: [baseline, tail, steady]}
  - {id: order_err_k6, kind: client, stat: sys_err_ratio, op: '<', value: 0.01, phases: [baseline, tail, steady]}
  - {id: k6_exit, kind: k6, check: exit_code}
```

```yaml
# order-api-pod-kill.yaml: kubectl delete pod(정상 종료, L514). 응답 손실이 없어야 한다
extends: default.yaml
fault_effect: [{id: pod_replaced, observe: pod_replaced, target: order-api, within_s: 120}]
checks:
  - {id: unresolved_zero, kind: unresolved, class: [UNRESOLVED_COMMITTED, UNRESOLVED_ABSENT], phases: [fault, recovery], op: '==', value: 0}
  - {id: order_recovery_k6, kind: client, stat: recovery, required: false}   # 2차에 required
---
# order-api-crash-after-commit.yaml(리허설): 장애 키는 k6 계좌 idx0 하나에만. 계좌당 약 0.1건/s라 TTL 300s 안에 주문이 온다(계산)
extends: order-api-pod-kill.yaml
disable: [unresolved_zero, pod_replaced]       # os._exit(1)은 Pod 교체가 아니라 컨테이너 재시작이다
fault_effect:
  - {id: crash_restart, observe: crash_restart, target: order-api, min: 1}
  - {id: crash_seen_by_k6, class_sub: [after_commit, lookup], account: idx0, op: '>=', value: 1}
checks: [{id: unresolved_committed, kind: unresolved, class: [UNRESOLVED_COMMITTED], op: '==', value: 0}]
---
# demo-min.yaml(시연): 장애 키는 발표자 replay 계좌다(L112, L281). Prometheus 없이 낸다
extends: default.yaml
require_extra_accounts: true                   # run.json 또는 judge mark --extra-account
fault_effect: [{id: presenter_crash, observe: crash_restart, target: order-api, min: 1}]
checks: [{id: order_p95_k6, required: false}, {id: order_err_k6, required: false}]
```

- **gateway-pod-kill [2차]**: 가드 `ws_load_present`(baseline 연결 수 ≥ 100, 우리 설정값), `orders_unaffected`(fault `sys_err_ratio < 0.01`, 주문은 대조군), `ws_restored`(5.11), 선택 `tick_p95 < 1s`(L619), `ws_*`(`requires: ws_scenario`).

## 7. 실행 위치·접근·비밀값

**7.1 위치**: k6는 papertrade 단일 Pod Job이다(L628). runner·judge는 운영자 노트북이다. 우리 SA는 K8s API를 부르지 않고(L750), 장애 조작도 노트북에서 한다(L281). 클러스터 안 judge는 5432 차단(L895), Secret 마운트(L848), SA Role 부재(L995) 때문에 고르지 않는다.

**7.2 Postgres 접근**
- 선택: `kubectl exec postgres-0 -- psql -U pt_ro`. 정책·Secret 변경이 없고 pg_dump와 같은 경로다(L780). 공식 이미지는 컨테이너 안 Unix 소켓에 trust 인증을 쓴다.
- 고르지 않은 것: judge Pod → 5432(정책·Secret 변경, 연결 예산 L658 밖), port-forward(비밀번호가 노트북으로 옴, L899 미확인), order-api HTTP(토큰 필요, 자기 검증 순환).
- exec 셸은 슈퍼유저로도 접속할 수 있다. judge는 `-U pt_ro`와 READ ONLY 트랜잭션만 쓴다.
- local trust가 없어도 슈퍼유저 경로는 쓰지 않는다. pt_ro 비밀번호를 postgres Pod에 마운트하는 변경을 앱·인프라와 합의한 뒤에만 쓴다(L848).

**7.3 Prometheus 접근 [2차]**: direct(노트북) → relay(holder wget, Prometheus namespace ingress 허용 필요, 확인 필요: 관리자) → 자체 Prometheus(L997). 인증이 있으면 direct만 쓰고 토큰은 노트북 `.env`에만 둔다. 모든 쿼리에 `namespace="papertrade"`를 강제한다.

**7.4 비밀값**
- pt_ro 비밀번호: 노트북, git, 로그 어디에도 없다.
- 계좌 토큰: VU 메모리에만 둔다. `setup_data` 삭제, judge의 `token` 키 검사.
- `PROM_URL`, Prometheus 토큰: 노트북 `.env`(이미 gitignore). 공개 저장소라 내부 주소를 커밋하지 않는다.
- 증권사 키: Part B는 다루지 않는다.

**7.5 doctor(1차, 얇게)**: `kubectl auth can-i create pods/exec`·`get pods`·`get networkpolicy`, `allow-vpc-http`의 `from[].podSelector`와 Job 라벨 일치, psql `SELECT 1`과 제약 조회, 같은 run_id 계좌 존재(있으면 거부). 시연 17:00 점검(L85–88)에서도 돌린다.

**7.6 산출물**: 노트북 `runs/<run_id>/`(gitignore). 공유는 S3 `results/`(L422)에 손으로 올린다(쓰기 권한 미요청, L1001). 대표 `verdict.json`, `report.md`만 팀 합의 뒤 `docs/results/`에 커밋한다.

## 8. 테스트
- **CI(pytest)**: k6log(깨진 줄, `run_start` 2개, Counter와 줄 수 차이, `token` 키), classify 골든, criteria 병합 체인 스냅샷, invariants CSV 픽스처(I7 배속 경계 포함), combine(돈 FAIL 우선, N/A, INTERRUPTED), 결정성(2회·줄 섞기 → 같은 digest), CLI 종료 코드.
- **로컬 `-m pg`**: 명세 DDL(L436–457)을 docker `postgres:18`에 넣고 위반을 하나씩 심는다. 접두어만 `docker exec -i pgtest`로 바꿔 같은 경로로 돈다.
- **로컬 `-m k6`**: 가짜 API 스위치마다 30s run → 기대 클래스 수 일치. **클러스터 스모크**: `PROFILE=smoke`로 전 경로.
- **골든(최소 22건)**: 행렬 칸 12개, DOUBLE_CREATE, FIRST_200(conn_reuse 켜짐·꺼짐), RESEND_DIFF, RESEND_UNRESOLVED, 409_SAME_BODY, DUP_ORDER, DUP_FILL, FILL_STATE, ORPHAN(완전·불완전), INTERRUPTED, `after_503`, 응답 스키마 미확정.
- **가짜 order-api**: 표준 라이브러리 `ThreadingHTTPServer`. 스위치 `close_after_commit`, `commit_then_503`, `http503`, `dup_fill`, `resend_diff`, `delay_ms`. 스냅샷을 psql과 같은 `--@` + CSV로 낸다.
- **CI 잡 제안**: `test`(python 3.11, `pytest -q orchestrator/tests -m "not pg and not k6"`). fork의 `ci/` 브랜치로 PR을 연다. 첫 기여자 fork PR 워크플로는 승인이 필요하다.

## 9. 일정

| 기간 | 산출물 | 확인·중단점 |
|---|---|---|
| 11/5–11/8 | 10절 요청, 계약 합의안. judge 골격(model, context, criteria, k6log, classify), 골든 22건 | `pytest` 통과 |
| 11/9–11/13 | `orders.js`, `ws.js`, 가짜 API, 로컬 e2e. `bundle.sql`, invariants, client, verdict, report. Job, fetch, stop, plan | **11/10 응답 필드.** 11/13에 e2e가 안 되면 doctor는 체크리스트, plan은 envsubst |
| 11/14–11/17 | 클러스터 스모크, WS 100 VU 메모리. ① accept ② pod-kill ③ crash-after-commit | **1차 마감**: verdict 3개 |
| **11/18** | go/no-go 근거(L1014): 주문 축(30/s·300/s × 10s, p95, 오류율, 정합성 0)과 WS 축(가능한 최대 연결 수의 서버 tick p95). 못 채운 축은 INCONCLUSIVE | |
| 11/18–11/22 | 2차, C 측정 지원(L1192) | |
| 11/23–11/25 | 리허설(L1193): demo + S8 → `judge run` 소요 시간, 기준값 확정, `response_fields_confirmed: true`. 11/25 동결 | 리허설 verdict 백업 |
| 11/26 | 17:00 `judge doctor`, 리허설 증거 `judge evaluate --offline`. 18시 이후 시연 | |

- **일정 모순**: 11/18 게이트 조건은 "k6 합격 기준 통과"인데(L1014), k6로 C를 재는 일은 11/18 이후 칸이다(L1192). Part B 결과물이 게이트의 선행 조건이다.
- 11/17 기능 동결(L1191)이 k6·judge에 적용되는지 확인한다. 적용되면 2차는 측정 도구 예외로 요청한다.
- **시연 최소선(자르지 않는 것)**: 송신 전 기록, WS 부하 최소형, fetch, `judge stop`, DB 스냅샷, 분류기, I2·I4·I5, 기록 완전성, `demo-min` 판정과 `report.md`. 화면에는 "유실 0 · 불일치 0 · 중복 0 · 고아 0 · 잔고 불일치 0 · 신선도 위반 체결 0 → PASS"와 커밋 후 회수 건수가 나온다.
- **늦어지면 자르는 순서**: WS 판정 → 한계 찾기 → 거래량 재생 → PromQL → k6 회복 시간 → precheck.

## 10. 다른 파트·앱·관리자 요청

| 대상 | 요청 | 기한 |
|---|---|---|
| 팀장·저장소 소유자 | 쓰기 권한 또는 fork PR 수락과 워크플로 승인. `judge.py` 패키지 전환, `chaos/` 재구성 시점, CI `test` 잡 | 11/8 |
| 팀(분담 리드) | WS 부하 생성이 B 1차 범위인지. 11/17 동결의 k6 적용 여부. runner·gates·observe 담당. "k6 합격 기준"을 주문·WS 두 축으로 정의(11/12) | 11/8 |
| runner·inject | run.json v1, epoch ms, Job 재적용 금지, fetch 전 Job 삭제 금지, 장애 선택자에서 k6 Pod 제외. 장애 키는 run당 계좌 하나(리허설 idx0, 시연 발표자). 두 계좌에 넣으면 order-api Pod 2개가 죽는다 | 11/8 |
| 앱 팀 | ① 응답 필드(status, 사유, 체결가, 수수료, 근거가, 거래 후 보유), 가능하면 `order_id` ② 오류 본문 ③ `GET ?idem_key=` 인증 ④ qty 문자열 수용 ⑤ 재요청 200의 가변 필드 ⑥ nickname·생성 속도 제한 ⑦ replay 초기 현금 ⑧ `round` 방식 ⑨ 버킷 0.3·1.0 경계, `status="error"` 정의 ⑩ `INVALID_QTY` 저장(L266 대 L376) ⑪ 앱이 배속을 아는 방법 ⑫ local trust, `pt_ro`, `postgres-init` 사본 ⑬ 리플레이 원천, 11/14 ingest-replay 가동 ⑭ 커밋 뒤 503 가능성 | ①–⑨ 11/10 |
| 팀 인프라 담당 | `mirror/k6:2.3.0` push와 이미지 도구(sh, gzip, sha256sum, wget, date, kill) 확인. k6 podSelector = `app.kubernetes.io/name: k6`(L893). 한계 run의 replica 고정 | 11/12 |
| 관리자(확인 필요: 관리자) | Prometheus 주소·인증·scrape 간격·보존, papertrade ingress 허용. 팀 Role 실제 내용(L995). ephemeral-storage 쿼터. `do-not-disrupt`(k6 Pod, 11/24–11/26). 노드 시간 동기화, cgroup 버전. 노트북 S3 `results/` 쓰기. 부하 시험 시간대(L1004). 시연장 IP | 11/12 |

## 11. 위험과 확인 필요

| 위험 | 대응 |
|---|---|
| `pods/exec` 권한 없음(기록 회수·DB 판정 불가) | 요청 1순위, doctor로 먼저 확인 |
| order-api가 11/17 직전에 준비됨(L1186) | 11/13까지 가짜 API로 파이프라인 완성 |
| 응답 필드·오류 형식이 추정과 다름 | 원문 보존, `response_fields_confirmed`, 재판정 |
| k6 Pod 축출·재생성, OOM | `podFailurePolicy`, holder, `account_set` 가드 |
| local trust 꺼짐 | 슈퍼유저 금지, pt_ro 마운트 합의 |
| Prometheus 경로 막힘, scrape 간격 미확인 | PromQL은 선택 기준, `judge plan` 창 검사 |
| 장애 중 VU 고갈, 10배속 거절 증가(L388) | fault 달성률은 보고만, coverage 가드 |
| 반올림 미정(L398), 시계 차이(L803) | ±0.5 허용식, CLOCK_SKEW, 상대 신선도 |
| console-output 줄 단위 보장이 소스 근거뿐 | 엄격 파서, Counter 대조, 스모크 |
| 시연장 IP가 EKS API 허용 목록에 없음(L999) | 17:00 doctor, 허용된 다른 PC, 리허설 verdict 백업 |
| NetworkPolicy 라벨 불일치 | doctor, 첫 요청이 모두 status 0이면 abort |
| 공용 클러스터 부하, WS 메모리 | 계단별 abort, `R_max`, 시간대 협의(L1004), 100 VU 실측(L632) |

- **스모크로 확인**: busybox 도구, raw 줄 모양, `0s` stage, SIGINT 뒤 handleSummary, exec 바이너리 전송, 기록 크기, VU당 메모리.
- **미확인(브리프)**: `onerror` 뒤 `onclose` 보장, WS 메시지 봉투·snapshot 필드, `RESTRICTED_SOURCE` 거절 형식.

## 12. 참고 자료
조사·검증 단계에서 직접 확인한 URL만 적는다.
- https://github.com/grafana/k6/releases/tag/v2.0.0 (pause/resume 제거, `--address`)
- https://github.com/grafana/k6/blob/v2.3.0/internal/metrics/engine/engine.go (abortOnFail → 99)
- https://github.com/grafana/k6/blob/v2.3.0/internal/js/summary.go (`setup_data`)
- https://grafana.com/docs/k6/latest/using-k6/k6-options/reference/ (`noConnectionReuse`, `--console-output`)
- https://grafana.com/docs/k6/latest/javascript-api/k6-execution/ (`vu.idInTest`)
- https://github.com/golang/go/blob/go1.25.0/src/net/http/request.go (`isReplayable`, Idempotency-Key)
- https://prometheus.io/docs/prometheus/latest/querying/basics/ (staleness, subquery)
- https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/ (`singleProcessOOMKill`)
- https://karpenter.sh/docs/concepts/disruption/ (do-not-disrupt 범위)
- https://www.postgresql.org/docs/current/sql-copy.html , https://www.postgresql.org/docs/current/ddl-constraints.html

## 부록: 검증에서 기각한 지적과 사유
- **재사용 연결 run의 FIRST_200을 `OK(transport_replay)`로 받기**: 재사용을 강제로 끄므로 생기지 않는다. 켜진 run은 INCONCLUSIVE로 낮춘다.
- **`--paused` + `k6 resume`으로 시연 시작 맞추기**: v2.0.0에서 `k6 resume`이 제거됐다. demo 연장과 `judge stop`으로 대신한다.
- **WS와 주문을 한 k6 프로세스로 합치기**: `vu.idInTest`가 테스트 전체 번호라 1 VU = 1 계좌가 깨진다. 한 Pod 안 별도 컨테이너로 두고 자원을 Pod 예산 안에서 나눈다(쿼터 지적은 반영).
- **1차 축소안 중 `extends` 제거와 doctor 2차 이동**: 패치 병합은 작고 중복을 막는다. doctor는 시연 점검과 라벨 확인에 1차로 필요하다. 나머지 축소안은 반영했다.

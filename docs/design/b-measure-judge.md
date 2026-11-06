# B. 측정과 판정 — 코드 설계

- 이 문서는 Part B(측정과 판정)가 만들 k6 부하 스크립트와 판정기(judge)의 코드 설계다.
- 근거: 주문 계약은 실제 주문 서비스 `workloads/order/`(main 6e8f38b)다. 파일 참조는 그 폴더 기준 경로다(예: `app/orders.py`). 클러스터·일정·장애·WS는 피실험 시스템 명세 `workbook-week1/reference/design-spec.md`를 따르고, `L123`은 그 명세의 줄 번호다.
- 상태: 초안(2026-11-05). 인터페이스는 11/8 팀 합의 전이다.
- **2026-11-06 계약 정렬**: 주문 계약의 세부(행렬, 불변식, 가드, runner 연동)는 [b-judge-contract.md](b-judge-contract.md)에 따로 둔다. 코드 주석의 "계약 명세 N"은 그 문서의 N절이다.
- **2026-11-06 계약 정렬**: 근거를 `workloads/order`(main 6e8f38b)로 바꿨다. 옛 설계 명세의 주문 API·스키마 가정은 버렸다. 서비스 코드와 다르면 서비스 코드가 우선이다. 서비스는 체결만 저장하는 최소 버전이라 바뀔 수 있다(10절).
- 경로 전제: 분담표는 `chaos/k6/`, `chaos/orchestrator/judge/`다. 현재 저장소는 `load/k6/`, `orchestrator/src/orchestrator/`다. 본문은 현재 경로로 쓰고, 대응은 2절에 둔다.

**계약 정렬 요약(옛 가정 → 실제)**

| 항목 | 옛 가정(설계 명세) | 실제(`workloads/order`) |
|---|---|---|
| 주문 API | `POST /api/orders`, 새 주문 201 | `POST /orders`, 성공은 늘 200(`app/main.py`) |
| 멱등 키 | `Idempotency-Key` 헤더 | 본문 `client_order_id`(UNIQUE). 같은 본문이면 200 `replayed: true`, 다르면 409(`app/orders.py`) |
| 주문 조회 | `GET /api/orders?idem_key=` | 없다. `GET /accounts/{user_id}`만 있다 |
| 계좌 | k6가 `POST /api/accounts`로 만들고 토큰을 쓴다 | seed의 `user-0001`..`user-1000`, 각 10,000,000원. 인증 없음(`db/seed.sql`) |
| 종목·본문 | `REPLAY:UPBIT:*`, buy/sell, qty, limit_price | `KRW-BTC` 하나, `bid`/`ask`, volume(소수 8자리 이하, `app/config.py`, `app/schemas.py`) |
| 응답 | status, reject_reason, price, fee | order_id, status, fill_price, amount, filled_at, replayed. 거절은 `{status:"rejected", reason}` |
| 테이블·잔고 | fills, reconcile_runs, 수수료, cost_basis | accounts(krw), positions(volume, avg_price), orders, ticks. 거절은 저장 안 함(`db/schema.sql`) |
| 503 | 롤백 보장 | price_unavailable·stale_price는 트랜잭션 전. not_ready는 연결 오류면 커밋 뒤일 수 있다 |
| DB 계정 | `pt_ro` 읽기 전용, DB `papertrade` | `exchange` 계정·DB, namespace `exchange`, StatefulSet `postgres`(Pod `postgres-0`). 읽기 전용 역할 없음(`infra/k8s/data/postgres.yaml`, PR #4) |

- judge 쪽: JSONL v1 → v2(4.6). K는 K_ACK·K_REJECT·K_UNKNOWN·INTERRUPTED다(옛 K_FOUND·K_404·K_4XX 삭제). 옛 PHANTOM_ACK·FALSE_NOT_FOUND·NOT_ENTERED·DUP_FILL 대신 REJECT_COMMITTED다(5.3).
- 불변식은 옛 I2–I8 대신 cash·qty·avg·nonneg·arith·constraints·fresh다(5.4). `noConnectionReuse` 강제는 없앴다(4.4).

## 0. 한눈에 보기

**완료 기준이 어떤 코드로 충족되는가**

> 실험이 끝나면 중복·유실 주문 수와 잔고 불일치를 자동 계산해 판정에 반영

| 단계 | 코드 | 결과 위치 |
|---|---|---|
| 보낸 주문 기록 | `load/k6/lib_orders.js`가 보내기 전 `intent`, 끝난 뒤 `result` 줄을 쓴다(JSONL v2) | k6 Pod `/out/orders.jsonl` |
| 실험 후 자동 수집 | `judge run` → `fetch.py`(exec로 기록 회수) → `db.py`(exec psql 스냅샷 1회) | `runs/<run_id>/` |
| 유실·중복 주문 수 | `classify.py`(클라이언트 상태 K × DB 행렬), 교차 확인(run 키 주문 수 = D1 수) | `verdict.json` `integrity.classes` |
| 잔고 불일치 | `invariants.py`(스냅샷 행으로 cash, qty, avg, nonneg, arith, constraints 계산), `sql/snapshot_*.sql` | `integrity.invariants` |
| 판정 반영 | `verdict.py` 규칙 1: 돈 불변식 FAIL이 다른 결과보다 앞선다 | `verdict`, 종료 코드 0/10/11 |

**산출물과 마감**

| 산출물 | 파일(현재 경로) | 완료 기준 | 마감 |
|---|---|---|---|
| k6 주문 부하 | `load/k6/orders.js`, `lib_*.js` | smoke·steady·accept·demo가 가짜 API와 실제 주문 서비스(`workloads/order`)에서 돈다 | **1차 11/17** |
| k6 WS 부하(최소형) | `load/k6/ws.js` | VU당 10연결, 구독, 재연결(S7, C 측정용) | **1차 11/17** |
| Job과 기록 회수 | `load/k6/k8s/job.yaml`, `hold.sh` | k6가 OOM이어도 기록을 노트북으로 가져온다 | **1차 11/17** |
| judge 핵심 | `orchestrator/src/orchestrator/judge/` | 위 완료 기준. 같은 증거면 판정이 같다 | **1차 11/17** |
| 판정 기준 | `experiments/criteria/*.yaml` | default, order-api-pod-kill, order-api-crash-after-commit(앱 FAULT 훅 전까지 쓸 수 없음), demo-min | **1차 11/17** |
| PromQL, precheck, k6 회복 시간 | `judge/promql.py`, `client.py` | 서버 지표 기준, 주입 전 게이트 | 2차 11/22 |
| 한계 찾기, 거래량 재생, WS 판정, gateway-pod-kill | `judge/limit.py`, `judge/ws.py`, `tools/make_profile.py` | `limit.json`, 재연결·신선도 판정 | 2차 11/22 |
| 여유분 | — | PromQL range 회복, `fresh` required 전환(ticks 기록 주체 확정 뒤) | 3차 11/25 |

## 1. 범위와 경계

**1.1 B가 소유하는 것**: `orders.js`, `ws.js`, 프로파일, Job 템플릿, judge 패키지, 판정 기준 YAML, 로컬 검증용 가짜 order-api(`load/k6/tools/fake_order_api.py`).

**1.2 받는다고 전제하는 것**

| 받는 것 | 주는 쪽(추정) | 없을 때 |
|---|---|---|
| run_id, Job 적용, 장애 시각, 정리 | runner, inject | 운영자가 런북과 `judge mark`로 한다 |
| 주문 API·스키마(`workloads/order`, main 6e8f38b) | 앱 팀(주문 서비스 담당) | 바뀌면 judge를 맞춘다. 응답 원문 보존, 매핑은 기준 파일 한 곳 |
| 시세(Redis `price:KRW-BTC`), ticks 기록 | 수집기 담당(다른 파트) | 시세가 없으면 모두 503 거절이라 coverage 가드로 INCONCLUSIVE. ticks가 비면 `fresh`는 NOT_EVALUATED |
| seed 계좌(`db/seed.sql`) | 앱 팀 | k6 setup abort, judge `account_set` 가드 |
| postgres local trust, 클러스터 DB 계정·DB 이름 | 앱·인프라 | 7.2 |
| 서버 지표(`app/metrics.py`), PodMonitor(L788–799) | 앱·인프라 | PromQL 기준은 `required: false` |
| `mirror/k6:2.3.0`(우리 소유, L958) | 팀 인프라 담당 | 11/12까지 push 요청 |

**1.3 만들지 않는 것**: runner·gates·inject·cleanup 코드, 클러스터 안 judge Pod, SA용 Role, NetworkPolicy 변경, DB 드라이버, toolbox, Indexed Job(L630), Grafana 패널, S3 자동 업로드, 판정 결과의 Prometheus 내보내기(L639), 주문 서비스 코드 수정(팀원 소유, 읽기만 한다).

**1.4 B가 주는 계약(3개)**
1. **k6 Job 템플릿과 env 목록**(4.5, 4.8). runner는 `${…}`만 채운다.
2. **JSONL v2**(4.6).
3. **judge 입출력**: 입력 `run.json`, 출력 `verdict.json` v1과 종료 코드.

- `run.json` 키: `schema`(`chaos.run/v1`), `run_id`(`^[a-z0-9-]{3,40}$`), `experiment`, `criteria`, `scenario`(상대 경로는 `run.json` 위치 기준), `k6.job`, `marks{inject_start, inject_end}`, `faults[{kind, target, start_ms}]`, `extra_accounts`(발표자 user_id), `obs`, `db{pod, container, user, database}`(7.2, 비밀번호 없음). `replay_speed`는 받기만 한다. 시각은 노트북 시계 epoch ms(UTC)다.
- runner가 쓰고 judge는 읽기만 한다. `judge mark`는 `marks.local.json`에 쓰고, judge는 `run.json` 값을 우선해 빈 칸만 채운다.
- 장애 정의(대상, 방법)는 `scenario` 파일 몫이다. 기준 파일에는 판정에 쓸 기대 효과만 둔다.

| 종료 코드(우리 설정값) | 0 | 10 | 11 | 12 | 그 밖 |
|---|---|---|---|---|---|
| `run`, `evaluate` | PASS | FAIL | INCONCLUSIVE | 입력 오류 | judge 오류(예외 1, argparse 2) |
| `doctor`, `precheck`(2차) | 준비됨 | — | 미비 | 입력 오류 | judge 오류 |

- runner는 넷을 모두 만족할 때만 PASS로 받는다: 종료 코드 0, `verdict == "PASS"`, `run_id` 일치, `inputs.criteria_sha256`·`run_json_sha256` 일치(A 구현: `orchestrator/judging.py` `accept`).
- `criteria_sha256`은 run.json `criteria`(또는 `--criteria`)가 가리키는 **기준 파일 원본 바이트**의 sha256이다. A의 `accept`가 그 파일과 대조한다. extends까지 병합한 결과의 해시는 `criteria_resolved_sha256`에 따로 둔다.
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
- **ConfigMap**: kustomize `configMapGenerator`는 루트 밖 파일을 거부한다. 대신 `kubectl -n exchange create configmap k6-scripts --from-file=load/k6/ --dry-run=client -o yaml | kubectl apply -f -`를 쓴다(최상위 일반 파일만 들어간다). namespace는 거래소 서비스와 같은 `exchange`다(PR #4·#5).
- **pyproject 제안**: `dependencies = ["PyYAML>=6"]`, `[project.scripts] chaos-judge = "orchestrator.judge.__main__:main"`, pytest `pythonpath = ["src"]`, markers `pg`, `k6`, `e2e`.
- **재구성 대비**: 상대 import만 쓰고 실행 이름을 고정한다. 재구성 PR은 import, pyproject, CI 경로, ConfigMap 명령을 함께 고친다(`git mv`만으로 끝나지 않는다).
- ruff 기본 규칙(E4, E7, E9, F)을 지킨다. 한 줄에 필드 여러 개를 쓰면 E701/E702로 CI가 실패한다.

## 3. 실행 흐름

```text
1 run_id 발급, run.json 작성 → ConfigMap 갱신 → judge plan(job.yaml 렌더) → kubectl apply
2 judge accounts --wait        k6 run_start 확인, idx0 user_id 출력
3 baseline 대기(≥150s)          [2차: precheck가 11이면 주입하지 않는다]
4 장애 주입 → judge mark inject_start
5 k6 종료 또는 judge stop      exec kill -INT 1 → handleSummary 후 종료, holder는 대기
6 judge run                     fetch → 정지 확인 → psql 1회 → Pod 관측 → RELEASE → evaluate
7 수용 조건 확인 → 장애 원복, Job은 TTL로 정리
```
- fetch 전에 `kubectl delete job`을 하면 emptyDir와 함께 기록이 사라진다. 런북에 금지로 적는다.
- `db/seed.sql`은 `TRUNCATE`로 시작한다. run 중이나 DB 수집 전에 돌리면 증거가 사라진다. 런북에 금지로 적는다.
- **시연**: S7에서 `PROFILE=demo` Job 시작 → S8 두 조작 → S10에서 `judge stop` → `judge run --criteria experiments/criteria/demo-min.yaml`.

## 4. k6

### 4.1 안전장치
init에서 검사하고 어기면 `throw`한다(k6 107 → judge INCONCLUSIVE(LOAD_INVALID)).
```text
ORDER_API ⊨ ^http://order(\.<ns>(\.svc(\.cluster\.local)?)?)?:8000(/.*)?$        # 주문 서비스 8000 포트뿐
WS_URL    ⊨ ^ws://gateway(\.<ns>(\.svc(\.cluster\.local)?)?)?(:[0-9]+)?(/.*)?$  # WS 계약이 정해지면 좁힌다
K6_TARGET=local일 때만 ^(http|ws)://(127\.0\.0\.1|localhost):[0-9]+ 허용(가짜 서버·로컬 주문 서비스 전용)
```
- 클러스터 안 Service 주소만 허용한다. 제3자 호스트, 공개 ALB는 막힌다(L631). 주문 서비스 주소는 `http://order.<namespace>.svc.cluster.local:8000`이다(통일본, 로컬 `http://localhost:8000`, 앱 포트 8000은 `Dockerfile`). k6 주문 대상은 이 8000 포트뿐이다. `<ns>`(namespace)는 확인 필요다(10절).
- 종목은 `KRW-BTC` 하나, 계좌는 seed의 `user-0001`..`user-1000`만, 키는 `k6-<run_id>-<vu 4자리>-<seq 6자리>`(예 `k6-demo-1106-0001-000042`)만 쓴다. 어기면 `throw`한다. 인증이 없어 토큰도 없다.
- setup의 첫 요청 10건이 모두 status 0이면 abort한다(NetworkPolicy 의심).
- `orders.js`는 시나리오가 하나이고 `ACCOUNTS == preAllocatedVUs == maxVUs`인지 검사한다.
- judge도 다시 검사한다(5.6 `safety`). 위반이면 INCONCLUSIVE(SAFETY_VIOLATION)와 보고서 맨 위 경고를 낸다.

### 4.2 계좌 준비(setup)
- 계좌는 seed로 미리 있다(`user-0001`..`user-1000`, 각 krw 10,000,000, `db/seed.sql`). k6는 만들지 않는다.
- **1 VU = 1 user**: VU v(`exec.vu.idInTest`)는 `user-` + 4자리(`USER_BASE + v`)를 쓴다. `USER_BASE`(0), `ACCOUNTS`(300, L604)는 우리 설정값이고 `USER_BASE + ACCOUNTS ≤ 1000`을 검사한다. `idInTest`는 시나리오가 하나일 때만 성립해 WS는 별도 프로세스다(4.8).
- setup은 VU 계좌마다 `GET /accounts/{user_id}`로 krw와 보유(앞 run 보유 포함)를 읽어 넘긴다. 404면 abort(108)한다(seed 미적용). `http.batch` 20개씩, `setupTimeout` 180s(우리 설정값, 기본값 60s).
- 발표자 계좌는 k6 범위 밖 user_id(예: `user-1000`)를 run.json `extra_accounts`에 적는다.
- 시세 조회 API가 없다. 첫 매수 수량은 `PRICE_HINT`(env, 우리 설정값)로, 그 뒤는 VU가 받은 마지막 `fill_price`로 정한다.

### 4.3 주문 생성 규칙(우리 설정값)

| 항목 | 규칙 |
|---|---|
| 난수 | VU마다 mulberry32(seed = `RUN_SEED ^ vu`) |
| 종목·side | `KRW-BTC` 하나. 보유가 있으면 0.4 확률로 `ask`, 아니면 `bid` |
| volume | bid `floor8(BUY_KRW ÷ P)`(`BUY_KRW` 10,000원). ask는 0.7 확률로 보유 절반, 0.3 확률로 전량(절반이 0이면 전량) |
| 가격 | 시장가다. 체결가는 Redis 현재가이고(`app/orders.py` `get_price`) 본문에 가격이 없다 |
| 가격·보유 갱신 | 200 응답의 `fill_price`·`volume`·`side`로 갱신한다(응답에 거래 후 보유가 없다). 422이거나 결과 불명이면 `GET /accounts/{user_id}`로 다시 읽는다 |
| 수량 표현 | BigInt(1e-8 단위) → 10진 문자열. 본문 문자열은 한 번 만들어 모든 시도에 쓴다 |
| client_order_id | `k6-<run_id>-<vu 4자리>-<seq 6자리>`(예 `k6-demo-1106-0001-000042`). 통일본 예시 `k6-0001-000042`에 run_id를 더해 run 사이 충돌을 막는다. run_id ≤ 40자라 최대 55자로 64자 제한 안에 든다. judge는 자릿수를 강제하지 않고(`[0-9]+`, 5.3) 64자 상한은 `safety`가 본다(5.6) |

### 4.4 연결, 재시도, 재요청

**멱등 키 헤더를 보내지 않는다.**
- 멱등 키는 본문 `client_order_id`다(`app/orders.py` `place_order`). k6는 `Idempotency-Key`·`X-Idempotency-Key` 헤더를 보내지 않는다. 서비스가 쓰지 않고, Go `net/http`의 POST 자동 재전송 조건이라서다.
- 헤더가 없으면 Go는 재사용 연결의 EOF 뒤에도 POST를 몰래 다시 보내지 않는다. 그래서 `noConnectionReuse` 강제는 필요 없다. `run_start.conn_reuse`는 기록만 한다. 부하 분산이 필요한 성능 run에서만 켠다(판정과 무관).

| 응답 | 시도 성격(5.3) | 동작 |
|---|---|---|
| 200, 본문에 `order_id` | ACK | 멈춘다 |
| 400, 404, 409, 422 | 확정 거절 | 멈춘다(400은 고쳐서 보낼 일이지만 k6는 재시도하지 않는다) |
| 503 price_unavailable·stale_price | 확정 거절(그 시도는 저장 안 함) | 같은 본문·같은 키로 1s·2s·4s 뒤 최대 `RETRY_MAX`(3)회(통일본 재시도 규칙) |
| 0(전송 오류·타임아웃), 502, 504, 503 not_ready·모르는 reason, 그 밖 5xx | 결과 불명 | 같은 본문·같은 키로 1s·2s·4s 뒤 최대 `RETRY_MAX`(3)회 |
| 201, 200인데 `order_id` 없음, 그 밖 | PROTOCOL | 멈춘다 |

- 재시도 조건은 `st == 0 || st >= 500`이다(503은 reason과 무관하게 재시도). 두 경우는 재시도 수 `RETRY_MAX`를 함께 쓴다. 키의 K는 ACK가 있으면 K_ACK, 없으면 마지막 시도로 정한다(5.3). 503 stale_price 뒤 재시도가 체결되면 OK(`not_committed`)다.
- 타임아웃 5s(우리 설정값, k6 기본값 60s). 서비스에 요청 deadline이 없어 k6가 포기한 뒤 커밋될 수 있다. 주문 조회 API도 없어 결과 불명은 judge가 DB 행으로 가른다(UNRESOLVED_COMMITTED / UNRESOLVED_ABSENT).
- **같은 키 경합**: 서비스는 `find_order`를 잠금 밖에서 부른다(`app/orders.py`). 타임아웃 난 앞 시도가 아직 accounts 잠금을 쥐고 있으면 재전송은 행을 못 찾고 잠금을 기다린다. 앞 시도가 커밋한 뒤 재전송은 바뀐 잔고로 422를 받거나(전량 매도 등), 그 사이 시세가 낡아 503 stale_price를 받는다. 422에서 멈추면 그 키는 K_REJECT × D1이라 REJECT_COMMITTED(유실 기준)다. 분류 정책은 미정이다(5.3, 11절).
- **재요청 검사**: ACK(`replayed: false`)를 받은 의도의 `RESEND_RATIO`(0.1, L604)는 같은 키·같은 본문으로 한 번 더 보낸다. 기대는 200 `replayed: true`와 같은 의미 필드다. 재시도 규칙도 같고, 시도는 같은 `atts`에 이어 붙인다. 위반(DOUBLE_CREATE, RESEND_DIFF)은 judge가 판정한다(5.3).
- 모든 요청에 `tags.name`을 단다(`order_create`, `order_resend`, `account_read`). 기본 `name`은 URL이라 user_id가 든 조회 URL이 시계열을 늘린다. `systemTags`에서 `url`도 뺀다.
- 의도 하나의 최악 길이는 54s다(계산). 전송 하나는 첫 전송 + 재시도 3회가 모두 타임아웃일 때 5s × 4 + 백오프 7s = 27s이고, 재요청까지 두 번이다. 503 stale_price·price_unavailable 재시도가 늘어도 같은 `RETRY_MAX`를 쓰고 503은 타임아웃보다 빨리 오므로 최악 길이는 그대로다. `gracefulStop`은 60s다(우리 설정값).

```js
const retryable = (a) => a.st === 0 || a.st >= 500;      // 결과 불명 + 503 stale_price·price_unavailable(통일본)
export function placeIntent(u, o) {                       // lib_orders.js 핵심
  const key = `k6-${RUN}-${pad4(VU)}-${pad6(++seq)}`;     // k6-<run_id>-<vu 4자리>-<seq 6자리>
  const b = { client_order_id: key, user_id: u.id, market: MARKET, side: o.side, volume: o.volume };
  const body = JSON.stringify(b);                          // 문자열 한 번만 만든다. 헤더에 키를 넣지 않는다
  rec({ type: 'intent', key, acct: u.id, vu: VU, t: Date.now(), body: b }); intents.add(1);
  const atts = sendWithRetry(body, params('order_create'), [], retryable), ack = firstAck(atts);  // timeout 5s, tags.name
  if (ack && ack.rp === false && rnd() < RESEND_RATIO) sendWithRetry(body, params('order_resend'), atts, retryable);
  rec({ type: 'result', key, atts }); results.add(1);
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
- **env**: `RUN_ID`, `PROFILE`, `ORDER_API`, `ACCOUNTS`(300), `USER_BASE`(0), `RATE`(30), `DURATION_S`, `SPIKE_AT_S`, `THRESHOLDS`(`fault`|`slo`|`step`), `RUN_SEED`, `RESEND_RATIO`(0.1), `RETRY_MAX`(3), `BUY_KRW`(10000), `PRICE_HINT`, `WS_URL`, `WS_TARGET`(괄호는 우리 설정값).

### 4.6 주문별 기록 JSONL v2
- `console.log(JSON.stringify(x))` 한 줄, 인자 `--log-format=raw --console-output=/out/orders.jsonl`. raw면 메시지가 그대로 한 줄이다(소스 확인).
- 줄 종류는 `type` 키로 가르고 세 종류만 쓴다. 계좌를 만들지 않아 v1의 `account`·`setup_done` 줄은 없다. 계좌 목록은 intent의 user_id로 만든다(VU 오름차순 idx).

| type | 언제 | 필드 |
|---|---|---|
| `run_start` | setup 시작 | `v`(2), `run, script_ver, seed, profile, plan, vus, symbols`(`["KRW-BTC"]`), `initial_cash`(참고용), `conn_reuse`(기록만), `t` |
| `intent` | **보내기 전** | `key`(client_order_id), `acct`(user_id), `vu, t, body{client_order_id, user_id, market, side, volume(문자열)}` |
| `result` | 끝난 뒤 | `key, atts[{n, t0, t1, st, ec, ms, rp, raw}]` |

- `key == body.client_order_id`, `acct == body.user_id`여야 한다. 아니면 PROTOCOL이고 safety 위반이다.
- `atts`는 같은 키의 모든 전송(첫 전송, 재시도, 재요청)을 시간순으로 담는다. `st`는 전송 오류면 0, `ec`는 거절 `reason` 또는 k6 error_code, `rp`는 200 본문의 `replayed`(그 밖 null), `raw`는 200·409·4xx·503 본문(2048자 이하)이다.
- 옛 `lookup`·`ack`·`resend` 하위 객체는 없다. 원문을 남겨 계약이 바뀌어도 다시 판정한다. 의도 1건 약 1KB(추정)다.
- `run_end{run, t}`는 써도 되고 안 써도 된다(받기만 한다).

**파서 규칙(`k6log.py`, k6 스크립트가 지킬 것)**
- v1 흔적(최상위 `kind`, `type`이 `account`·`setup_done`, result의 `lookup`·`ack`·`resend`, `run_start.v ≠ 2`)은 형식 오류다. `run`은 `run_start`에만 필수다.
- `run_start` 필수 키: `v, run, script_ver, seed, profile, plan, symbols, initial_cash, vus, t`. `conn_reuse`는 없어도 된다.
- 시도는 8개 키(`n, t0, t1, st, ec, ms, rp, raw`)를 모두 쓴다(`rp`·`raw`는 null 가능). `n`은 1부터 차례로, `rp`는 200에서만 쓰고 원문 `replayed`와 같아야 하며, `raw`는 2048자 이하다. 어기면 줄은 살리고 기록 완전성(lines_parsed)을 깬다.
- 503의 `ec`에는 서비스 `reason`을 그대로 쓴다. `reason`이 없는 503은 결과 불명이다(5.3).
- intent 형식 위반(키가 `k6-<run_id>-<vu>-<seq>`가 아님, 64자 초과, 키의 vu ≠ `intent.vu`, body 필드가 비어 있지 않은 문자열이 아님)은 `intent_format`으로 표시만 한다. 판정은 분류(PROTOCOL)와 safety 가드가 한다.
- 어느 줄이든 `token`·`authorization`으로 끝나는 키(대소문자 무시)가 있으면 safety 위반이다.

### 4.7 지표, threshold, handleSummary
- 지표: `order_intents`·`order_results`(Counter, 완전성 대조), `order_sys_err`(Rate, 첫 시도 0·5xx), `order_ack_ms`(Trend), `order_protocol_errors`·`resend_mismatch`(Counter). 앞 두 지표에 태그 `step`.
- k6 threshold는 전체 누적이고 `p(95)`는 선형 보간이다. judge는 구간별 nearest-rank다. 그래서 **SLO 판정은 judge 하나로만 한다.** threshold는 계약 위반 지표와 클러스터 보호에만 쓴다. `THRESHOLDS=slo`의 SLO 실패(99)는 경고 `K6_THRESHOLD_DISAGREES`다.
- v2.3.0 소스로 확인한 사실: `abortOnFail` 중단도 99다(108은 `exec.test.abort()` 전용). `--new-machine-readable-summary` 기본값은 false라 handleSummary는 기존 모양(`metrics`, `state`, `setup_data`)을 받는다. Job env에 `K6_NEW_MACHINE_READABLE_SUMMARY`를 두지 않는다.
```js
export function handleSummary(data) {
  delete data.setup_data;                               // VU 계좌 상태가 크고 판정에 쓰지 않는다
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
      affinity: ${ANTI_AFFINITY}                   # preferred, 주문 서비스 Pod와 떨어뜨린다(L637)
      containers:                                  # 셋 다 ${K6_IMAGE}, /scripts·/out 마운트
      - {name: k6, args: ["run", "--log-format=raw", "--console-output=/out/orders.jsonl", "/scripts/orders.js"]}
      - {name: k6-ws, args: ["run", "--log-format=raw", "--console-output=/out/ws.jsonl", "/scripts/ws.js"]}
      - {name: holder, command: ["sh", "/scripts/hold.sh"]}   # /out/RELEASE 또는 HOLD_S까지 대기
      volumes: [{name: scripts, configMap: {name: k6-scripts}}, {name: out, emptyDir: {sizeLimit: 2Gi}}]
```
- `HOLD_S = setupTimeout + 프로파일 길이 + gracefulStop + 수집 예산 600s`(우리 설정값).
- **재실행 차단**: Pod가 다시 뜨면 새 emptyDir라 앞 Pod 기록이 없고, 같은 키가 다시 나가 replayed나 409가 된다. `podFailurePolicy`와 `backoffLimit: 0`으로 막고, 섞이면 앞 Pod 주문이 ORPHAN으로, 겹친 키가 FIRST_REPLAYED·409로 드러난다.
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
- 연결마다 `KRW-BTC` 시세 구독 메시지를 `WS_URL`로 보낸다. WS 계약(주소, 메시지 모양)은 아직 없다(확인 필요: 시세 담당).
- 재연결은 1s → 최대 10s 지수 백오프 + full jitter다(L512). `run_start`에 `ws_reconnect: true`를 남긴다.
- demo 모양: 0 → `WS_TARGET` 연결을 90s 램프하고 유지한다(L611).
- 기록은 상태 전이(`ws_open`, `ws_snap`, `ws_down{cause, code}`, `ws_regress`)와 VU별 10초 집계 `wsagg{n, bins}`(경계 50–5000ms, 우리 설정값)만이다.
- 측정 정의: 신선도 = `수신 Date.now() − recv_ts`(L619). `t_down` = onclose·onerror·`status` 10s 무수신 중 이른 시각(L513). 재연결 시간 = `t_down` → 첫 `snapshot`(L307). I10 역행 = 연결 안 `event_ts` 감소(L473). 필드 이름은 WS 계약이 정해지면 맞춘다.
- `onerror` 뒤 `onclose`가 반드시 오는지 문서에 없다(확인 필요). 두 핸들러 모두에서 중복을 막는다.

### 4.10 한계 찾기 [2차]
- 계단 1 run: `R_k = 30 × 1.25^k`, `R_max = 360`(관리자 합의). 계단 60s 중 앞 15s를 버리고 45s를 잰다. 표본 `n ≥ 20/(1−q)`(p95면 400).
- 계단 판정 순서:
  1. p95 ≥ 300ms 또는 첫 시도 오류율 ≥ 1% → SYSTEM_LIMIT. R*는 직전 계단이다.
  2. 1이 통과하고 달성률 < 0.98만 깨짐 → GENERATOR_LIMITED(INCONCLUSIVE).
  3. 체결 비율 < 0.8(우리 설정값) → CHEAP_PATH(INCONCLUSIVE). 거절은 DB에 쓰지 않아(`db/schema.sql` 주석) R*를 부풀린다.
- 실패 의도는 VU를 최대 27s 붙잡는다(4.4). step은 VU를 `R_max × 5s + 여유`로 늘려야 하는데, 1 VU = 1 user라 seed 계좌 1,000개가 상한이다. 모자라면 `R_max`를 낮추거나 seed 확대를 요청한다.
- 보호 장치는 계단마다 건다: `'order_sys_err{step:"k"}': [{threshold: 'rate<0.2', abortOnFail: true, delayAbortEval: '15s'}]`.
- HPA min=max, KEDA `paused-replicas` 고정을 요청한다. 못 하면 "배포 설정 기준 한계"로 적고 Pod별 요청 분포를 남긴다.
- `limit.json`: 계단별 p95, 오류율, 달성률, 체결 비율, 거절 사유, dropped_iterations, R*.

### 4.11 거래량 재생 [2차]
- 정의(명세 밖): 녹화된 업비트 체결 **건수**의 초별 모양을 주문 도착률 모양으로 옮긴다. volume은 쓰지 않는다(L315).
- `make_profile.py`: `type=="trade"`, `(source, seq)` 중복 제거(L340), 칸 = `floor((ts − 첫 ts) ÷ 1000 ÷ 10)`, 평균 30/s로 배율, `--cap 300`, `--spike 300:10s@t`. 35분 녹화(L616)면 210점이다.
- `source != "upbit"`이거나 녹화 종목이 `KRW-BTC`가 아니면 중단한다.
- 프로파일 JSON은 커밋하지 않는다(공개 저장소, 업비트 파생 데이터, L1170). 생성 명령과 sha256만 커밋한다.
- 원천: `REPLAY_SOURCE` 파일(노트북). 클러스터 원천은 수집기 담당과 정한다(확인 필요).

## 5. judge

### 5.1 모듈과 시그니처
```python
def collect(ctx: RunContext, crit: Criteria, tr: Transports) -> Path      # 클러스터를 만지는 유일한 함수
def evaluate(ctx: RunContext, crit: Criteria, evidence: Path) -> Verdict  # 순수 함수(네트워크·현재 시각 없음)
def judge(run_json: Path, *, offline: bool = False) -> Verdict
def classify(log: K6Log, db: DbSnapshot, cfg: IntegrityCfg, *,
             account_ids=None, extra_account_ids=None) -> ClassifyResult  # classes_of, classified.csv 유지
def check_invariants(db: DbSnapshot, ctx: RunContext, crit: Criteria) -> list[CheckResult]  # cash … fresh
def combine(checks: Sequence[CheckResult], guards: Sequence[CheckResult]) -> tuple[Outcome, tuple[str, ...]]
CMD_PREFIX = ("kubectl", "-n", "exchange")   # 테스트는 ("docker", "exec", "-i", "pgtest")
```
- `model.py`의 dataclass(RunContext, DbTarget, K6Log, Attempt, DbSnapshot, CheckResult, Verdict 등)는 frozen이다. 상수(`DEFAULT_RESPONSE_FIELDS`, `INVARIANTS` 등)와 Enum(`KState`, `AttemptKind`, `OrderClass`, `OkSub`, `IdemViolation`, 값은 5.3과 같다)도 여기 둔다.
- CLI(1차): `chaos-judge {plan|accounts|doctor|fetch|collect|evaluate|run|mark|stop} --run runs/<id>/run.json [--criteria x.yaml] [--offline]`. 2차: `precheck`, `limit`.
- `mark`는 시각 외에 `--extra-account <user_id>`, `--obs key=value`를 받는다. `--speed`는 옛 I7 배속용이라 판정에 쓰지 않는다(지울 후보).

### 5.2 collect 순서
1. **fetch**(4.8).
2. **정지 확인**: k6 종료 뒤 10s 기다린다(우리 설정값). 주문 트랜잭션은 요청 안에서 끝난다(`app/orders.py`). 서비스에 요청 deadline이 없으므로 늦은 커밋은 지문(F1 ≠ F2)으로 잡는다.
3. **DB**: psql을 한 번 exec한다(5.4). 지문 F1·F2가 다르면 30s 뒤 한 번 더, 그래도 다르면 INCONCLUSIVE(DB_NOT_QUIESCENT).
4. **Pod 관측**: 주문 서비스 Pod 선택자로 `kubectl get pods -l <선택자> -o json`(L995)을 한 번 부른다. 라벨은 매니페스트가 없어 미정이다(10절).
   - `pod_replaced`: `creationTimestamp > inject_start`인 Ready Pod.
   - `crash_restart`: `lastState.terminated.exitCode ≠ 0`이고 `finishedAt`이 fault 구간 안.
   - run.json에 inject 관측값(`obs`)이 있으면 그것을 우선한다.
5. RELEASE, `evidence.json`(파일별 sha256).
6. **evaluate**: 증거, run.json, `criteria.resolved.yaml`만 읽는다. 분위수는 nearest-rank `x[ceil(q·n)−1]`이다. `verdict_digest`는 `generated_at`과 `run_info`(실행 시간, 전송 경로)를 뺀 canonical JSON의 sha256이다.
- 산출물은 `runs/<run_id>/` 아래 `k6/`, `raw/db/`, `raw/k8s/`, `verdict.json`, `report.md`, `classified.csv`, `judge.log.jsonl`(단계별 소요 시간)이다.

### 5.3 주문 분류

**키와 이번 run 계좌**
- 키는 `client_order_id` 하나다(DB UNIQUE). 계좌와 묶지 않는다.
- run 키 형식은 `^k6-<run_id>-[0-9]+-[0-9]+$`다. 접두어 `k6-<run_id>-`만 보면 `r1117-crash`가 `r1117-crash-2`의 키를 제 것으로 센다.
- run 계좌는 intent의 user_id 집합이다. DB accounts에 없는 user_id가 있으면 INCONCLUSIVE(ACCOUNT_SET_MISMATCH)이고 LOST·ORPHAN은 판정하지 않는다.
- `extra_accounts`(발표자 user_id)는 DB 전용 검사와 arith·fresh만 받는다. cash·qty·avg·nonneg는 모든 계좌를 본다(5.4).

**1단계: 시도 성격**(`model.attempt_kind`, 상수 집합만 본다)

| 성격 | 조건 |
|---|---|
| ACK | `st == 200`이고 `raw`가 JSON 객체이며 `order_id`가 있다 |
| 확정 거절(REJECT) | `st ∈ {400, 404, 409, 422}`, 또는 503이고 `ec ∈ {price_unavailable, stale_price}`. INSERT 전에 거절돼(422·404는 잠금 안에서 롤백) **그 시도는** 저장하지 않는다(`app/orders.py`). 같은 키의 처리 중인 앞 시도는 나중에 커밋될 수 있다(4.4 같은 키 경합) |
| 결과 불명(UNKNOWN) | `st == 0`, 502, 504, 503 not_ready·모르는 `ec`, 그 밖 5xx. not_ready는 커밋 뒤 연결 오류일 수 있다(`app/main.py`) |
| PROTOCOL | 그 밖(201, 200인데 `order_id`가 없거나 원문이 깨짐) |

**2단계: 클라이언트 상태 K**

| K | 조건 |
|---|---|
| K_ACK | ACK 시도가 하나라도 있다. 첫 ACK 본문이 원 응답이다 |
| K_REJECT | ACK 없음, 마지막 시도가 확정 거절 |
| K_UNKNOWN | ACK 없음, 마지막 시도가 결과 불명 |
| INTERRUPTED | `intent`만 있고 `result`가 없다(k6가 중간에 죽음). DB 행 유무만 보고한다 |

- ACK가 없고 마지막 시도가 PROTOCOL이거나 시도가 없으면 K_UNKNOWN으로 두고 PROTOCOL을 함께 붙인다(명세가 정하지 않은 칸, 클라이언트가 결과를 해석하지 못했다).

**3단계: DB와 상관없는 멱등 위반**(IDEMPOTENCY_VIOLATION, FAIL)
- DOUBLE_CREATE: 한 키에서 rp=false ACK가 2번 이상이고 order_id가 다르다.
- FIRST_REPLAYED: 첫 시도(n=1)가 ACK인데 rp=true다(키 충돌·서버 결함). Go 자동 재전송이 없으므로(4.4) `conn_reuse`와 무관하게 FAIL이다.
- RESEND_DIFF: 두 번째 이후 ACK의 의미 필드(order_id, user_id, market, side, volume, status, fill_price, amount)가 원 응답과 다르다.
- 409_SAME_BODY: 409를 받았는데 그 키의 전송 본문이 모두 같다. k6는 늘 같은 바이트를 보낸다.
- 원문을 못 읽어 비교하지 못하면 PASS만 막는다(`detail.uncertain_rows`).

**4단계: K × DB 행렬**(D1 = 그 client_order_id 행이 있다. 일치 = 원 응답 필드가 DB 행과 같다)

| K \ DB | D1 일치 | D1 불일치 | D0(행 없음) |
|---|---|---|---|
| K_ACK | OK | MISMATCH, FAIL | LOST, FAIL |
| K_REJECT | REJECT_COMMITTED, FAIL | REJECT_COMMITTED, FAIL | OK(rejected) |
| K_UNKNOWN | UNRESOLVED_COMMITTED | UNRESOLVED_COMMITTED | UNRESOLVED_ABSENT |
| INTERRUPTED | INTERRUPTED(detail committed) | INTERRUPTED | INTERRUPTED |

- REJECT_COMMITTED: 확정 거절을 받았는데 DB에 주문이 있다. 돈이 움직였는데 거절로 안내됐다. 앞에 결과 불명 시도가 있으면 서비스 결함이 아니어도 같은 키 경합(4.4)으로 생긴다(실서비스에서 `[0, 422] × D1` 재현). 지금은 이 경우도 REJECT_COMMITTED(유실 기준)로 센다. 따로 셀지(예: 앞 시도가 모두 확정 거절인 K_REJECT × D1만 REJECT_COMMITTED로 두기), 서비스에 `find_order`를 잠금 안으로 옮기도록 요청할지는 미정이다(확인 필요: 리드·앱 팀). 골든 `reject_after_inflight_commit`이 지금 동작을 고정한다.
- BODY_MISMATCH: D1 행의 (user_id, market, side, volume)이 intent.body와 다르다(Decimal). K와 무관하게 also로 붙고 늘 FAIL이다.
- 응답 대 DB 비교는 `integrity.response_fields`(키 = 응답 필드, 값 = orders 열, 기본값 `model.DEFAULT_RESPONSE_FIELDS`, 근거 `app/schemas.py` `order_to_dict`)를 따른다. 숫자는 Decimal로 비교한다. `filled_at`은 두 형식(통일본 밀리초 `…:00.123Z`, 코드 `isoformat()` 마이크로초 `+00:00`)을 모두 읽고, **응답에 적힌 소수 자릿수까지만** DB 값을 내림해 시각으로 비교한다(밀리초 응답이면 밀리초까지). 형식을 못 읽으면 불일치가 아니라 `response_unparsed`(PASS만 막음)다. `replayed`·`reason`은 분류 입력이다. `response_fields_confirmed: false`인데 다르면 RESPONSE_SCHEMA_UNCONFIRMED(INCONCLUSIVE)다.
- **OK 세부(OkSub)**: `clean`(첫 시도 ACK, rp=false), `after_commit`(앞에 결과 불명, 첫 ACK rp=true. 앞 시도가 커밋됐다. crash 효과), `not_committed`(앞에 결과 불명 또는 확정 거절, 첫 ACK rp=false. 503 stale_price 뒤 재시도 체결 포함), `after_503`(앞에 503 not_ready, 첫 ACK rp=true), `rejected`(K_REJECT × D0).
- 앞에 503 아닌 결과 불명(0·502·504)이 있으면 `after_commit`이 `after_503`보다 앞선다. `after_503`은 경고 ROLLBACK_REPLY_COMMITTED를 낸다.
- **주 클래스**(`classified.csv` `cls`): DUP_ORDER → PROTOCOL → 행렬 칸(OK 아님) → BODY_MISMATCH → FILL_STATE → IDEMPOTENCY_VIOLATION 중 처음 것이다. 나머지는 `detail.also`에 둔다. 행렬 칸이 OK여도 멱등 위반이 있으면 `cls`는 IDEMPOTENCY_VIOLATION이라 OK 수에는 문제없는 행만 들어간다.
- 명세 문구보다 넓힌 곳(확인 필요: 리드): 앞 시도가 모두 확정 거절이고 첫 ACK가 rp=true면 FIRST_REPLAYED로 본다(확정 거절은 그 시도 자체를 저장하지 않고 앞에 결과 불명 시도도 없어 n=1과 같다). 첫 ACK 뒤에 확정 거절이 오면 RESEND_DIFF다(이미 있는 키라 서비스는 replay해야 한다).
- PASS만 막는 표시(`marks`, 판정 몫): `response_unparsed`(200 원문 또는 응답 `filled_at`을 못 읽었고 D1 → 내용 불일치), `lost_unknown`(200 원문을 못 읽었고 D0 → 유실), `resend_uncompared`(원문·replayed를 못 읽어 멱등 검사를 다 못 함 → 중복), `body_missing`(D1인데 보낸 본문이 없음 → 내용 불일치).

**5단계: DB 전용**(행 단위): DUP_ORDER(같은 client_order_id 행 2개 이상. UNIQUE라 정상이면 0), FILL_STATE(status='filled'인데 fill_price·amount·filled_at 중 NULL, 또는 run 주문인데 status가 filled가 아님), ORPHAN(run 키 형식인데 `intent`가 없다).

**완료 기준 용어 대응**
- 유실 = LOST + REJECT_COMMITTED. 중복 = DUP_ORDER + FILL_STATE + IDEMPOTENCY_VIOLATION. 내용 불일치 = MISMATCH + BODY_MISMATCH. 고아 = ORPHAN.
- UNRESOLVED_*, INTERRUPTED는 DB 사실로 확정했으므로 돈 문제가 아니다. 사용자 영향(`intent_unresolved_ratio`)으로 보고한다.
- 교차 확인(`judge_self_check`, `required: false`): 스냅샷의 run 키 형식 주문 행 수·키 수(`invariants.run_key_counts`)와 분류기가 run 키 행에 매칭한 orders.id 수·행 수(D1 + ORPHAN, `classify.run_order_count`)가 다르면 JUDGE_SELF_CHECK다.

**기록 완전성(`log_complete`)**: 모든 줄 파싱, `run_start` 1개(`v == 2`), `run` 값 하나, `intent`·`result` 줄 수 ≥ 각 Counter, sha256 일치, k6 종료 코드 0 또는 99. 아니면 ORPHAN과 unresolved 기준이 INCONCLUSIVE(SENT_LOG_INCOMPLETE)다. LOST·MISMATCH·중복은 있는 줄이 근거라 계속 판정한다.

### 5.4 DB 스냅샷과 잔고 불변식
```bash
kubectl -n <ns> exec -i <pod> [-c <container>] -- psql -X -q -v ON_ERROR_STOP=1 -U <user> -d <database> -f - < bundle.sql
```
- 대상은 run.json `db{pod, container, user, database}`이고, 기본값은 `postgres-0`, 없음(`-c` 생략), `exchange`, `exchange`다(로컬 개발 기본값). 클러스터 값은 확인 필요다(10절). 비밀번호는 넘기지 않는다(7.2).

`db.py`가 만드는 `bundle.sql` 순서:
1. `\set` 변수(run 키 정규식 `^k6-<run_id>-[0-9]+-[0-9]+$`, extra user_id, stale_ms)와 세션 설정(`TIME ZONE 'UTC'`, 읽기 전용, 시간 제한).
2. 지문 F1: `(count(orders), max(orders.id), sum(accounts.krw), count(positions), sum(positions.volume))`.
3. `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` → schema → accounts → positions → orders → `COMMIT`. schema가 첫 구획이라 열이 없어 멈춰도 드리프트 근거가 남는다.
4. 지문 F2(F1과 같은 식).
5. `\set ON_ERROR_STOP off` → ticks(선택 구획). 다른 파트가 쓰는 표의 오류가 필수 증거를 지우지 않는다. ticks 표가 없으면 빈 구획이다.

- F1은 BEGIN 앞, F2는 COMMIT 뒤에 잰다. 같은 REPEATABLE READ 트랜잭션 안에서 재면 늘 같다.
- 구획: `accounts(user_id, krw)`, `positions(user_id, market, volume, avg_price)`, `orders`(전체 열), `ticks(market, price, received_at)`(run 종목, run 창 ± stale_ms만), `schema`(constraints용 메타).
- orders는 전체를 가져온다(시연 규모). 100만 행을 넘으면 경고 SNAPSHOT_LARGE만 낸다.
- 구획은 `--@ <name>` … `--@end <name>`으로 감싸고 `COPY (…) TO STDOUT (FORMAT csv, HEADER, FORCE_QUOTE *)`로 받는다(SELECT 권한이면 된다). 끝 표지가 없는 구획은 버린다. 구획이 빠진 기준만 INCONCLUSIVE(DB_ERROR)이고 빠진 구획 이름은 `integrity.db_errors`에 남는다. numeric은 `Decimal`, timestamptz는 epoch µs로 바꾼다.
- 불변식은 모두 `invariants.py`가 스냅샷 행으로 계산한다(SQL 불변식 구획 `inv_<id>`는 내지 않는다). 수집 때의 `initial_krw`가 증거에 묶이지 않고, 한 스냅샷으로 계산해 두 결과가 갈릴 일이 없다. NULL은 위반으로 센다(스키마가 NOT NULL이라 정상이면 없다).
- 정지 상태(F1 == F2)인데 파싱한 행의 지문이 F1과 다르면 행 기반 불변식은 평가 불가(JUDGE_SELF_CHECK)다.

**불변식**(모두 `status='filled'` 주문만 센다. id는 기준 YAML `sql:`·`check:` 값)

| id | 내용 | 계산 위치 |
|---|---|---|
| `cash` | 모든 계좌: `krw + Σ(bid amount) − Σ(ask amount) = initial_krw` | Python(스냅샷) |
| `qty` | 모든 (user, market) ∈ positions ∪ orders: `positions.volume`(없으면 0) `= Σ(bid volume) − Σ(ask volume)` | Python(스냅샷) |
| `avg` | 모든 (user, market): 그 사용자 주문을 id 순으로 서비스 공식대로 재생한 avg_price `=` positions.avg_price(정확히 같음) | Python(Decimal 기본 context) |
| `nonneg` | krw ≥ 0, volume ≥ 0, avg_price ≥ 0 | Python(스냅샷) |
| `arith` | filled 주문: `amount = ROUND_DOWN(fill_price × volume, 4)` | Python |
| `constraints` | 필요한 테이블·열 존재, `orders.client_order_id` UNIQUE, CHECK 제약 존재(이름이 아니라 정의로 비교). 다르면 INCONCLUSIVE(SCHEMA_DRIFT) | schema 구획(information_schema, pg_constraint) |
| `fresh` | (선택, `required: false`) run의 filled 주문마다 같은 market tick 중 `price = fill_price`, `received_at ∈ [filled_at − stale_ms − 1000ms, filled_at + 1000ms]`인 것이 있다. ticks가 비면 NOT_EVALUATED | Python |

- 서비스 공식(`app/orders.py`): bid는 `avg' = ((held × avg + v × price) ÷ (held + v)).quantize(1e-8)`(ROUND_HALF_EVEN, 정밀도 28), ask는 avg 유지(volume 0이어도). `amount = (price × volume).quantize(0.0001, ROUND_DOWN)`.
- 같은 사용자 주문은 accounts `FOR UPDATE`로 직렬화되고 INSERT가 잠금 안이라, 사용자별 orders.id 순서가 처리 순서다. 그래서 avg 재생이 성립한다.
- `initial_krw`는 `integrity.initial_krw`(문자열 Decimal, 기본 `"10000000"`, `db/seed.sql`), `stale_ms`는 `integrity.stale_ms`(기본 5000, `app/config.py` STALE_MS)다.
- 대상 범위: cash·qty·avg·nonneg는 **모든 계좌**다(seed 기준 전체 보존식, 위반 user_id 예시를 detail에 남긴다). arith·fresh는 run 주문과 extra 계좌 주문이다.
- `fresh`는 체결가가 수집기 ticks에 남았는지 본다. ticks 기록 주체가 정해지기 전까지 보고만 한다(10절).

### 5.5 k6 기록 기반 구간 SLO
- `ack_p95_ms`: ACK 받은 첫 시도의 `ms`(구간 배정은 `t0`). `sys_err_ratio`: 첫 시도 중 0·5xx 비율(4xx 제외, 업무 거절은 성공). `achieved_ratio`: intent 수 ÷ `plan` 적분. `filled`, `sell_filled`: 분류 결과의 filled 수, 그중 `side == 'ask'` 수.
- 구간(우리 설정값): warmup 60s, baseline `[inject_start − 150s, inject_start − 2s]`, fault `[inject_start, inject_end + 60s]`, recovery(fault 이후 끝까지), tail 마지막 120s, steady(장애 없는 run의 warmup 이후).
- 노드·노트북 시계 차이(4.8)로 보정한다. 2s(우리 설정값)를 넘으면 시간 기준은 CLOCK_SKEW다. 표본은 `n ≥ 20/(1−q)`, 모자라면 LOW_SAMPLES다.

### 5.6 가드(공허한 PASS 방지, 미충족이면 INCONCLUSIVE)

| 가드 | 조건 | 사유 |
|---|---|---|
| `safety` | 모든 intent: `market ∈ safety.markets`, `acct`·`body.user_id` ⊨ `safety.user_pattern`(전체 일치)이고 끝 번호가 1..`safety.user_max`(기본 1000, seed), `key`·`body.client_order_id`가 run 키 전체 형식(5.3)이고 64자 이하(서비스 `app/schemas.py` 상한), `key == body.client_order_id`, `acct == body.user_id`. 1 VU = 1 user(두 방향). 기록(JSONL·summary)에 `token`·`authorization` 키 없음 | SAFETY_VIOLATION + 경고 |
| `account_set` | JSONL user_id가 모두 DB accounts에 있다(seed 계좌) | ACCOUNT_SET_MISMATCH |
| `fill_coverage`, `sell_coverage` | 구간별 filled·ask filled ≥ 기준(전부 거절이면 불변식이 저절로 성립) | LOW_FILL_COVERAGE |
| `load_coverage` | baseline·tail·steady `achieved_ratio ≥ 0.98` | GENERATOR_LIMITED |
| `fault_in_load` | 모든 장애가 부하 창 안 | FAULT_OUTSIDE_LOAD |
| `fault_effect` | 장애별 기대 효과 관측(6절) | FAULT_NOT_OBSERVED |
| `db_quiescent` | F1 == F2 | DB_NOT_QUIESCENT |

### 5.7 집계 규칙
1. **돈 불변식**(kind `integrity`: 유실, 내용 불일치, 중복, 고아, cash·qty·avg·nonneg·arith·constraints)이 required FAIL → **FAIL(INTEGRITY_VIOLATION)**. 가드보다 앞선다. ACCOUNT_SET_MISMATCH면 LOST·ORPHAN은 평가하지 않는다.
2. 가드 미충족 → INCONCLUSIVE.
3. 나머지 required(kind `unresolved`, `client`, `prom`, `k6`) FAIL → FAIL.
4. required가 INCONCLUSIVE이거나 평가하지 못함 → INCONCLUSIVE. 조용히 건너뛰지 않는다.
5. 그 밖은 PASS. `required: false`는 보고만, `requires:`가 맞지 않으면 N/A.

- k6 99: `state.testRunDurationMs`가 계획보다 짧으면 abortOnFail 중단(LOAD_ABORTED). 끝까지 돌았으면 실패한 threshold로 매핑한다(`resend_mismatch`는 분류기 결과, protocol은 PROTOCOL, SLO는 경고).
- 108·104·107은 LOAD_ABORTED·LOAD_INVALID, OOM·기록 잘림은 기록 의존 기준만 INCONCLUSIVE, psql 실패는 DB_ERROR다. DB만으로 되는 불변식은 계속 판정한다.

### 5.8 verdict와 보고서
- 최상위 키: `schema`(`papertrade.verdict/v1`), `run_id`, `experiment`, `verdict`, `reasons`, `checks`, `guards`, `integrity{intents, log_complete, classes, subs, invariants, examples}`, `client`, `load`, `inputs{criteria_sha256, criteria_resolved_sha256, run_json_sha256, evidence_manifest_sha256, judge_version, response_fields_confirmed}`, `warnings`, `verdict_digest`, `run_info`, `generated_at`.
- `examples`는 클래스별 키 20개까지, 전체는 `classified.csv`다(열은 그대로이고 `k_state` 값은 5.3의 K다. `resend`는 옛 재송신 결과 대신 재전송 수(시도 수 − 1)다). Prometheus 주소는 넣지 않는다(공개 저장소).
- 경고: ROLLBACK_REPLY_COMMITTED(after_503), K6_THRESHOLD_DISAGREES, SNAPSHOT_LARGE(orders 100만 행 초과), SAFETY_VIOLATION.
- `report.md` 조각: 의도·ACK 수, 유실·불일치·중복·고아, 잔고 불일치와 신선도 위반 체결, 커밋 후 회수 건수, 구간 p95, INCONCLUSIVE 사유와 해소 방법, 증거 해시.

### 5.9 PromQL 판정 [2차]
- 구간 끝 시각에 창 = 구간 길이로 instant 쿼리를 부른다. gauge reduce는 `min_over_time((Q)[$win:$res])`로 감싸고 `$res`(scrape 간격)를 늘 명시한다.
- 창 하한: rate는 `$win ≥ 4 × scrape_interval`, gauge subquery는 점 3개 이상. `judge plan`이 구간 길이를 검사해 모자라면 입력 오류(12)다. baseline이 150s인 이유다(118s는 30s × 4에 못 미친다).
- 조회는 구간 끝 + 2 × scrape 이후다. NaN·±Inf·빈 결과·`status:error`는 INCONCLUSIVE이고, 비교 전에 `math.isfinite`를 검사한다.
- 지표는 `app/metrics.py`의 `orders_total{status, reason}`, `orders_replayed_total`, `order_duration_seconds{status}`, `price_age_seconds{market}`다. 오류율 분자는 `(… or vector(0))`다.
- 주문 p95는 `sum(rate(order_duration_seconds_bucket{$sel, le="0.3"}[$win])) / sum(rate(…_count{$sel}[$win])) ≥ 0.95`로 본다. 지금 버킷에 0.3이 없어(0.25, 0.5) BUCKET_BOUNDARY_MISSING이다(10절).
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
- `schema_version: 1`. 모르는 키는 입력 오류(12)다. 옛 키(`safety.symbol_prefix`, `safety.account_mode`, `integrity.price_max_age_ms`)는 새 키를 안내하며 거부한다.
- `extends`는 패치 병합이다. 맵은 깊은 병합, `checks`·`guards`·`fault_effect`는 같은 id면 필드 단위 덮어쓰기, 새 id면 추가한다. 통째 교체는 `replace: true`, 끄기는 `disable: [id]`다.
- 병합 뒤 필수 키를 검증하고(빠진 `safety.user_max`·`initial_krw`·`stale_ms`는 기본값으로 채운다) `criteria.resolved.yaml`로 저장한다. 그 해시가 `criteria_resolved_sha256`이고, 원본 기준 파일 바이트의 해시가 `criteria_sha256`이다(1.4).
- 모든 기준에 `required`(기본 true), `on_violation`(fail|inconclusive, 기본 fail)이 있다.

```yaml
# experiments/criteria/default.yaml (가드는 5.6, recovery·steady 구간은 5.5 기본값)
# 계약 근거: workloads/order(6e8f38b) app/config.py, app/schemas.py, db/schema.sql, db/seed.sql
schema_version: 1
safety: {markets: [KRW-BTC], user_pattern: '^user-[0-9]{4}$', user_max: 1000}   # SUPPORTED_MARKETS, seed user-0001..1000
phases: {warmup: 60s, baseline: {before: inject_start, duration: 150s, guard: 2s},
         fault: {from: inject_start, to: inject_end, extra: 60s}, tail: {before: k6_end, duration: 120s}}
coverage: {min_filled: {baseline: 100, tail: 100, steady: 300}, min_sell_filled: 20, min_achieved_ratio: 0.98}
integrity:
  initial_krw: "10000000"               # seed.sql
  stale_ms: 5000                        # config.py STALE_MS(클러스터 값 확인 필요: 앱 팀)
  response_fields: {order_id: id, client_order_id: client_order_id, user_id: user_id, market: market,
                    side: side, volume: volume, status: status, fill_price: fill_price, amount: amount,
                    filled_at: filled_at}  # 응답 필드 → orders 열
  response_fields_confirmed: true       # schemas.py order_to_dict
prometheus: {enabled: false}            # 2차. scrape_interval 30s (확인 필요: 관리자)
guards: [{id: safety}, {id: account_set}, …, {id: db_quiescent}]   # 5.6 전부
checks:
  - {id: lost, kind: integrity, class: [LOST, REJECT_COMMITTED], op: '==', value: 0}
  - {id: mismatch, kind: integrity, class: [MISMATCH, BODY_MISMATCH], op: '==', value: 0}
  - {id: duplicates, kind: integrity, class: [DUP_ORDER, FILL_STATE, IDEMPOTENCY_VIOLATION], op: '==', value: 0}
  - {id: orphans, kind: integrity, class: [ORPHAN], op: '==', value: 0}
  - {id: invariants, kind: integrity, sql: [cash, qty, avg, nonneg, arith, constraints], op: '==', value: 0}
  - {id: stale_fill, kind: integrity, check: fresh, op: '==', value: 0, required: false}
  - {id: protocol, kind: unresolved, class: [PROTOCOL], op: '==', value: 0, on_violation: inconclusive}
  - {id: order_p95_k6, kind: client, stat: ack_p95_ms, op: '<', value: 300, phases: [baseline, tail, steady]}
  - {id: order_err_k6, kind: client, stat: sys_err_ratio, op: '<', value: 0.01, phases: [baseline, tail, steady]}
  - {id: k6_exit, kind: k6, check: exit_code}
```

```yaml
# order-api-pod-kill.yaml: kubectl delete pod(SIGTERM 정상 종료, app/__main__.py). 종료 중 주문은 503 not_ready → 재전송
# 응답 손실이 없어야 한다
extends: default.yaml
fault_effect:
  - {id: pod_replaced, observe: pod_replaced, target: order-api, within_s: 120}
  - {id: shutdown_seen_by_k6, class_sub: [not_committed, after_503], account: any, op: '>=', value: 1, required: false}
checks:
  - {id: unresolved_zero, kind: unresolved, class: [UNRESOLVED_COMMITTED, UNRESOLVED_ABSENT], phases: [fault, recovery], op: '==', value: 0}
  - {id: order_recovery_k6, kind: client, stat: recovery, required: false}   # 2차에 required
---
# order-api-crash-after-commit.yaml(리허설): 앱에 커밋 뒤 FAULT 훅이 생기기 전까지 쓸 수 없다(확인 필요: 앱 팀).
# 장애 키는 k6 계좌 idx0 하나에만. 기대 효과는 after_commit
extends: order-api-pod-kill.yaml
disable: [unresolved_zero, pod_replaced, shutdown_seen_by_k6]   # os._exit(1)은 Pod 교체가 아니라 컨테이너 재시작이다
fault_effect:
  - {id: crash_restart, observe: crash_restart, target: order-api, min: 1}
  - {id: crash_seen_by_k6, class_sub: [after_commit], account: idx0, op: '>=', value: 1}
checks: [{id: unresolved_committed, kind: unresolved, class: [UNRESOLVED_COMMITTED], op: '==', value: 0}]
---
# demo-min.yaml(시연): 발표자 계좌는 run.json extra_accounts의 user_id다(L112, L281). Prometheus 없이 낸다
extends: default.yaml
require_extra_accounts: true                   # run.json 또는 judge mark --extra-account <user_id>
fault_effect: [{id: presenter_crash, observe: crash_restart, target: order-api, min: 1}]   # 앱 FAULT 훅 필요
checks: [{id: order_p95_k6, required: false}, {id: order_err_k6, required: false}]
```

- `target: order-api`는 주문 서비스 Pod 라벨이 정해지면 맞춘다(10절).
- **gateway-pod-kill [2차]**: 가드 `ws_load_present`(baseline 연결 수 ≥ 100, 우리 설정값), `orders_unaffected`(fault `sys_err_ratio < 0.01`, 주문은 대조군), `ws_restored`(5.11), 선택 `tick_p95 < 1s`(L619), `ws_*`(`requires: ws_scenario`).

## 7. 실행 위치·접근·비밀값

**7.1 위치**: k6는 `exchange` namespace의 단일 Pod Job이다(L628, PR #4·#5). runner·judge는 운영자 노트북이다. 우리 SA는 K8s API를 부르지 않고(L750), 장애 조작도 노트북에서 한다(L281). 클러스터 안 judge는 5432 차단(L895), Secret 마운트(L848), SA Role 부재(L995) 때문에 고르지 않는다.

**7.2 Postgres 접근**
- 선택: `kubectl exec <postgres Pod> -- psql -U <user> -d <database>`. 정책·Secret 변경이 없다. 공식 이미지(서비스와 같은 `postgres:16`)는 컨테이너 안 Unix 소켓에 trust 인증을 쓴다.
- 계정: 읽기 전용 역할이 없다. 로컬 기본값은 `exchange` 계정·DB이고(`docker-compose.dev.yml`, `app/config.py`), `POSTGRES_USER`라 슈퍼유저다. 그래서 judge가 쓰기를 스스로 막는다: `READ ONLY` 트랜잭션, 번들에 DML·DDL 없음. 읽기 전용 역할이 생기면 run.json `db.user`만 바꾼다.
- 클러스터의 Pod·컨테이너·계정·DB 이름은 확인 필요다(10절). run.json `db`로 넘기고 코드 기본값은 로컬 값으로 둔다.
- 고르지 않은 것: judge Pod → 5432(정책·Secret 변경, 연결 예산 L658 밖), port-forward(비밀번호가 노트북으로 옴), 주문 서비스 HTTP(조회 API가 없고 자기 검증 순환).
- local trust가 꺼져 있으면 DB_ERROR로 끝낸다. 비밀번호는 어떤 경로로도 받지 않는다.

**7.3 Prometheus 접근 [2차]**: direct(노트북) → relay(holder wget, Prometheus namespace ingress 허용 필요, 확인 필요: 관리자) → 자체 Prometheus(L997). 인증이 있으면 direct만 쓰고 토큰은 노트북 `.env`에만 둔다. 모든 쿼리에 `namespace="exchange"`를 강제한다(PR #4). 로컬 kind의 Prometheus는 `monitoring` namespace, 수집 주기 5초다(`infra/local/README.md`).

**7.4 비밀값**
- DB 비밀번호: judge는 받지 않는다(run.json `db`에 password 키가 있으면 입력 오류). 서비스가 커밋한 로컬 기본 비밀번호도 judge 코드·문서·테스트에 옮기지 않는다.
- 계좌 토큰: 주문 서비스에 인증이 없어 토큰이 없다. judge의 `token`·`authorization` 키 검사는 유지한다.
- `PROM_URL`, Prometheus 토큰: 노트북 `.env`(이미 gitignore). 공개 저장소라 내부 주소를 커밋하지 않는다.
- 증권사 키: Part B는 다루지 않는다.

**7.5 doctor(1차, 얇게)**: `kubectl auth can-i create pods/exec`·`get pods`·`get networkpolicy`, `allow-vpc-http`의 `from[].podSelector`와 Job 라벨 일치, psql `SELECT 1`과 제약 조회(`constraints`), seed 계좌 수, 같은 run_id 키 형식 주문 존재(있으면 거부). 시연 17:00 점검(L85–88)에서도 돌린다.

**7.6 산출물**: 노트북 `runs/<run_id>/`(gitignore). 공유는 S3 `results/`(L422)에 손으로 올린다(쓰기 권한 미요청, L1001). 대표 `verdict.json`, `report.md`만 팀 합의 뒤 `docs/results/`에 커밋한다.

## 8. 테스트
- **CI(pytest)**: k6log(JSONL v2: 깨진 줄, `run_start` 2개, Counter와 줄 수 차이, `token` 키, key·acct ≠ body), classify 골든, criteria 병합 체인 스냅샷(옛 키 거부), invariants 스냅샷 픽스처(cash·qty·avg·arith 경계), combine(돈 FAIL 우선, N/A, INTERRUPTED), 결정성(2회·줄 섞기 → 같은 digest), CLI 종료 코드.
- **로컬 `-m pg`**: 실제 스키마 `workloads/order/db/schema.sql`·`seed.sql`을 docker `postgres:16`(서비스와 같은 이미지)에 그대로 넣고 위반을 하나씩 심는다(krw, positions volume, avg_price, amount 조작, 중복 INSERT 불가). 우리 픽스처 스키마는 두지 않는다. 접두어만 `docker exec -i pgtest`로 바꾼다.
- **로컬 `-m k6`**: 가짜 API 스위치마다 30s run → 기대 클래스 수 일치. **클러스터 스모크**: `PROFILE=smoke`로 전 경로.
- **실서비스 e2e(`-m e2e`, CI 제외)**: docker `postgres:16` + `redis:7`, `workloads/order` 앱을 별도 venv uvicorn으로 띄운다(코드는 고치지 않는다). 파이썬 클라이언트가 JSONL v2를 쓰며 정상, 재요청, 422, stale_price 뒤 재시도, 동시 같은 키를 보낸다 → `docker exec psql` 번들 → evaluate PASS. krw를 바꾸면 FAIL(INTEGRITY_VIOLATION).
- **골든(최소 22건)**: 행렬 칸 전부, OK 세부 5종, BODY_MISMATCH, 멱등 위반 4종, DUP_ORDER, FILL_STATE, ORPHAN, INTERRUPTED(커밋·미커밋), PROTOCOL, 503 stale_price 뒤 재시도 체결, 422 거절 D0, 응답 원문 못 읽음. 오프라인 픽스처(`fixtures/run_ok/`)도 v2로 다시 만든다.
- **가짜 order-api**: 표준 라이브러리 `ThreadingHTTPServer`로 `POST /orders`, `GET /accounts/{user_id}`를 흉내 낸다. 스위치 `close_after_commit`, `commit_then_not_ready`, `not_ready`, `stale_price`, `resend_diff`, `delay_ms`. 스냅샷을 psql과 같은 `--@` + CSV로 낸다.
- **CI 잡**: `.github/workflows/ci.yml` `test`(python 3.11, `pytest -q -m "not pg and not k6 and not e2e"`), pyproject markers에 `e2e`.

## 9. 일정

| 기간 | 산출물 | 확인·중단점 |
|---|---|---|
| 11/5–11/8 | 10절 요청, 계약 합의안. judge 골격(model, context, criteria, k6log, classify), 골든 22건. 11/6 계약 정렬 | `pytest` 통과 |
| 11/9–11/13 | `orders.js`, `ws.js`, 가짜 API, 로컬·실서비스 e2e. `bundle.sql`, invariants, client, verdict, report. Job, fetch, stop, plan | **주문 서비스 계약 변경 추적.** 11/13에 e2e가 안 되면 doctor는 체크리스트, plan은 envsubst |
| 11/14–11/17 | 클러스터 스모크, WS 100 VU 메모리. ① accept ② pod-kill ③ crash-after-commit(앱 FAULT 훅이 있을 때만) | **1차 마감**: verdict 3개(훅이 없으면 2개) |
| **11/18** | go/no-go 근거(L1014): 주문 축(30/s·300/s × 10s, p95, 오류율, 정합성 0)과 WS 축(가능한 최대 연결 수의 서버 tick p95). 못 채운 축은 INCONCLUSIVE | |
| 11/18–11/22 | 2차, C 측정 지원(L1192) | |
| 11/23–11/25 | 리허설(L1193): demo + S8 → `judge run` 소요 시간, 기준값 확정(`stale_ms`, `initial_krw` 클러스터 값). 11/25 동결 | 리허설 verdict 백업 |
| 11/26 | 17:00 `judge doctor`, 리허설 증거 `judge evaluate --offline`. 18시 이후 시연 | |

- **일정 모순**: 11/18 게이트 조건은 "k6 합격 기준 통과"인데(L1014), k6로 C를 재는 일은 11/18 이후 칸이다(L1192). Part B 결과물이 게이트의 선행 조건이다.
- 11/17 기능 동결(L1191)이 k6·judge에 적용되는지 확인한다. 적용되면 2차는 측정 도구 예외로 요청한다.
- **시연 최소선(자르지 않는 것)**: 송신 전 기록, WS 부하 최소형, fetch, `judge stop`, DB 스냅샷, 분류기, cash·qty·avg 불변식, 기록 완전성, `demo-min` 판정과 `report.md`. 화면에는 "유실 0 · 불일치 0 · 중복 0 · 고아 0 · 잔고 불일치 0 → PASS"와 커밋 후 회수 건수가 나온다.
- **늦어지면 자르는 순서**: WS 판정 → 한계 찾기 → 거래량 재생 → PromQL → k6 회복 시간 → precheck.

## 10. 다른 파트·앱·관리자 요청

| 대상 | 요청 | 기한 |
|---|---|---|
| 팀장·저장소 소유자 | `judge.py` 패키지 전환, `chaos/` 재구성 시점, CI `test` 잡(`e2e` 마커 제외) | 11/8 |
| 팀(분담 리드) | WS 부하 생성이 B 1차 범위인지. 11/17 동결의 k6 적용 여부. runner·gates·observe 담당. "k6 합격 기준"을 주문·WS 두 축으로 정의(11/12) | 11/8 |
| runner·inject | run.json v1(`db`, `extra_accounts`는 user_id), epoch ms, Job 재적용 금지, fetch 전 Job 삭제 금지, run 중·수집 전 seed 금지, 장애 선택자에서 k6 Pod 제외. 장애 키는 run당 계좌 하나(리허설 idx0, 시연 발표자) | 11/8 |
| 앱 팀(주문 서비스 담당) | ① 클러스터 DB 계정·DB 이름, postgres Pod·컨테이너, local trust 유지(가능하면 읽기 전용 역할) ② 커밋 뒤 장애 훅 부재: crash-after-commit·시연용 FAULT 훅 계획 ③ 주문 조회 API 부재: `GET /orders/{client_order_id}` 계획(없어도 judge는 DB로 확정) ④ 거절 미저장 확인: 저장하게 되면 FILL_STATE·REJECT_COMMITTED 규칙을 바꾼다 ⑤ `initial_krw`: 클러스터 seed도 계좌마다 10,000,000인지 ⑥ 클러스터 `STALE_MS` ⑦ `order_duration_seconds` 0.3 버킷 ⑧ 계약 변경 알림 ⑨ 통일본에 반올림 명시: 코드는 amount를 소수 4자리 ROUND_DOWN, avg_price를 8자리 quantize(ROUND_HALF_EVEN)한다. judge는 코드를 따른다 ⑩ `filled_at` 형식 통일: 통일본 예시는 밀리초 `Z`, 코드(`isoformat()`)는 마이크로초 `+00:00`다. judge는 둘 다 읽고 응답 자릿수까지 비교한다(5.3) | ①⑤ 11/10 |
| 수집기 담당 | ticks 기록 주체와 시점(`received_at` 시계), Redis `price:{market}` 갱신 주기. `fresh`가 기댄다 | 11/12 |
| 팀 인프라 담당 | `mirror/k6:2.3.0` push와 이미지 도구(sh, gzip, sha256sum, wget, date, kill) 확인. k6 podSelector = `app.kubernetes.io/name: k6`(L893). 주문 서비스·postgres namespace, Service 이름·포트(앱 8000), Pod 라벨. 한계 run의 replica 고정 | 11/12 |
| 관리자(확인 필요: 관리자) | Prometheus 주소·인증·scrape 간격·보존, exchange → monitoring ingress 허용. 팀 Role 실제 내용(L995). ephemeral-storage 쿼터. `do-not-disrupt`(k6 Pod, 11/24–11/26). 노드 시간 동기화, cgroup 버전. 노트북 S3 `results/` 쓰기. 부하 시험 시간대(L1004). 시연장 IP | 11/12 |

## 11. 위험과 확인 필요

| 위험 | 대응 |
|---|---|
| `pods/exec` 권한 없음(기록 회수·DB 판정 불가) | 요청 1순위, doctor로 먼저 확인 |
| 주문 서비스가 최소 버전이라 계약·응답 필드가 바뀜 | 근거 커밋(6e8f38b) 명시, 원문 보존, 매핑은 기준 파일 한 곳, 실서비스 e2e로 변경 감지 |
| k6 Pod 축출·재생성, OOM | `podFailurePolicy`, holder, ORPHAN·`account_set` 가드 |
| local trust 꺼짐, 클러스터 DB 계정 미정, 계정에 쓰기 권한 | run.json `db`, 비밀번호 없이 DB_ERROR, READ ONLY 트랜잭션, 읽기 전용 역할 요청 |
| 커밋 뒤 장애 훅이 없음 | crash-after-commit은 기준 파일만 둔다. pod-kill(not_ready)로 결과 불명 경로를 본다 |
| seed `TRUNCATE`가 run 중·수집 전에 돎 | 런북 금지, `db_quiescent`, doctor의 run 키 주문 확인 |
| seed 계좌 1,000개(1 VU = 1 user) | 한계 찾기 `R_max`를 낮추거나 seed 확대 요청 |
| avg 재생 정밀도(서비스는 요청·Redis 값, judge는 DB 소수 8자리 값) | Decimal 기본 context, 위반이면 입력값을 detail에 남긴다 |
| ticks가 비거나 기록 주체 미정 | `fresh`는 `required: false`, NOT_EVALUATED |
| Prometheus 경로 막힘, scrape 간격 미확인, 0.3 버킷 없음 | PromQL은 선택 기준, `judge plan` 창 검사, BUCKET_BOUNDARY_MISSING |
| not_ready는 `order_duration_seconds`를 기록하지 않는다(연결 오류 경로, `app/main.py`). 종료 중 503은 `orders_total`도 세지 않는다 | 서버 지표 지연·오류율은 그 요청을 빠뜨린다. 주 판정은 k6 기록(5.5)이고 PromQL은 교차 확인(`required: false`)이다 |
| 같은 키 재전송이 처리 중인 앞 시도를 기다린 뒤 422·503 stale_price를 받음(4.4 같은 키 경합) | 지금은 REJECT_COMMITTED(유실)로 센다. DB 지연 장애 run에서 k6 재전송 때문에 생길 수 있다. 분류 정책 미정(확인 필요: 리드·앱 팀, 5.3) |
| 장애 중 VU 고갈, stale_price 거절 증가 | fault 달성률은 보고만, coverage 가드 |
| 시계 차이(L803) | CLOCK_SKEW, 상대 신선도 |
| console-output 줄 단위 보장이 소스 근거뿐 | 엄격 파서, Counter 대조, 스모크 |
| 시연장 IP가 EKS API 허용 목록에 없음(L999) | 17:00 doctor, 허용된 다른 PC, 리허설 verdict 백업 |
| NetworkPolicy 라벨 불일치 | doctor, 첫 요청이 모두 status 0이면 abort |
| 공용 클러스터 부하, WS 메모리 | 계단별 abort, `R_max`, 시간대 협의(L1004), 100 VU 실측(L632) |

- **스모크로 확인**: busybox 도구, raw 줄 모양, `0s` stage, SIGINT 뒤 handleSummary, exec 바이너리 전송, 기록 크기, VU당 메모리.
- **미확인(브리프)**: `onerror` 뒤 `onclose` 보장, WS 계약(주소, 메시지 봉투, snapshot 필드).

## 12. 참고 자료
조사·검증 단계에서 직접 확인한 URL만 적는다.
- https://github.com/grafana/k6/releases/tag/v2.0.0 (pause/resume 제거, `--address`)
- https://github.com/grafana/k6/blob/v2.3.0/internal/metrics/engine/engine.go (abortOnFail → 99)
- https://github.com/grafana/k6/blob/v2.3.0/internal/js/summary.go (`setup_data`)
- https://grafana.com/docs/k6/latest/using-k6/k6-options/reference/ (`noConnectionReuse`, `--console-output`)
- https://grafana.com/docs/k6/latest/javascript-api/k6-execution/ (`vu.idInTest`)
- https://github.com/golang/go/blob/go1.25.0/src/net/http/request.go (`isReplayable`, 멱등 키 헤더)
- https://prometheus.io/docs/prometheus/latest/querying/basics/ (staleness, subquery)
- https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/ (`singleProcessOOMKill`)
- https://karpenter.sh/docs/concepts/disruption/ (do-not-disrupt 범위)
- https://www.postgresql.org/docs/current/sql-copy.html , https://www.postgresql.org/docs/current/ddl-constraints.html

## 부록: 검증에서 기각한 지적과 사유
- **재사용 연결 run의 첫 시도 replayed를 OK로 받기**: k6가 멱등 키 헤더를 보내지 않아 Go 자동 재전송이 없다. FIRST_REPLAYED는 `conn_reuse`와 무관하게 FAIL이다.
- **`--paused` + `k6 resume`으로 시연 시작 맞추기**: v2.0.0에서 `k6 resume`이 제거됐다. demo 연장과 `judge stop`으로 대신한다.
- **WS와 주문을 한 k6 프로세스로 합치기**: `vu.idInTest`가 테스트 전체 번호라 1 VU = 1 user가 깨진다. 한 Pod 안 별도 컨테이너로 두고 자원을 Pod 예산 안에서 나눈다(쿼터 지적은 반영).
- **1차 축소안 중 `extends` 제거와 doctor 2차 이동**: 패치 병합은 작고 중복을 막는다. doctor는 시연 점검과 라벨 확인에 1차로 필요하다. 나머지 축소안은 반영했다.

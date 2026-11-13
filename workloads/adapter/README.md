# 외부 증권사 adapter

주문을 PostgreSQL `adapter_orders`에 보관하고 worker가 외부 증권사로 전달한다.
Redis와 주문 서비스의 테이블은 사용하지 않는다. API와 mock-broker 계약은 [API.md](API.md)를 참고한다.
주문 서비스는 `ADAPTER_URL`을 설정하면 adapter에 연결된다. [D 연동/배포 방법](../order/README.md)을 참고한다.

## 전달 흐름과 장애 처리

1. POST 요청을 검증·정규화한 뒤 DB 저장 성공을 확인하고 202를 반환한다.
2. worker가 처리할 pending 주문에 lease를 걸고 attempts를 증가시킨다.
3. 같은 주문 키로 증권사에 GET 조회한다. 404인 경우에만 같은 키로 POST한다.
4. filled/rejected 응답은 원본 주문과 일치하는지 검사하고 DB에 저장한다.
5. pending·429·5xx·timeout·응답 오류는 지수 backoff와 jitter로 재조회한다.
   Retry-After는 해당 주문의 최소 재시도 대기시간으로 반영한다.
6. 최대 시도 횟수까지 결과를 확정하지 못하면 unknown으로 남기고 자동 처리를 멈춘다.

Pod가 죽으면 DB 기록은 남는다. 만료된 lease는 다음 worker가 회수하고 증권사부터 조회한다.
lease token이 바뀌면 이전 worker는 처리 결과를 덮어쓸 수 없다.
SIGTERM 시 신규 접수를 막고 종료 과정에서 worker를 취소한다. 처리 중 lease는 만료 후 복구한다.
DB 장애 시에도 기존 lease/주문을 유지하고 재연결한다.

외부 호출은 최소 간격을 두며 조회와 전송이 모두 호출 제한에 포함된다.
제한은 프로세스당 적용되므로 Helm은 단일 replica와 Recreate로 배포한다.
여러 replica로 확장하려면 공용 속도 제한기가 필요하다.
외부 증권사의 같은 키 중복 방지/조회 보장 없이는 응답 유실에 대한 중복 체결 방지를 보증할 수 없다.
unknown 주문의 외부 확인·수동 복구는 별도 운영 절차가 필요하다.
D는 pending/unknown 주문의 금액·수량을 예약하고 filled 확인 후 한 번만 잔고에 반영한다.
매수 max_amount 한도를 지원하는 증권사 계약이 필요하다. partial fill/수수료는 이번 계약에 포함하지 않는다.

## 로컬 실행 — Bash/WSL

레포 루트에서 다음을 실행한다. 전용 PostgreSQL은 localhost:15432로 열려 기존 kind DB와 분리된다.

```bash
cd workloads/adapter
docker compose -f docker-compose.dev.yml up -d --wait
python3 -m venv .venv-wsl
source .venv-wsl/bin/activate
python -m pip install -r requirements-dev.txt
export DATABASE_URL='postgresql://adapter:adapter@localhost:15432/adapter'
export BROKER_URL='http://localhost:18080'
python -m adapter init-db
PORT=8001 python -m adapter
```

mock-broker가 없는 동안 주문은 pending으로 접수되고 호출 실패 후 최종 unknown이 된다.
정상 체결 확인은 아래 자동 테스트의 외부 HTTP 대역으로 먼저 검증한다.
브로커가 준비되면 BROKER_URL을 해당 주소로 지정한다.

```bash
curl -X POST localhost:8001/orders -H 'Content-Type: application/json' \
  -d '{"client_order_id":"adapter-test-1","user_id":"user-0001","market":"KRW-BTC","side":"bid","volume":"0.001"}'
curl localhost:8001/orders/adapter-test-1
```

PowerShell에서는 `python -m venv .venv`, `.\.venv\Scripts\Activate.ps1`을 사용하고,
export 대신 `$env:DATABASE_URL='...'`, `$env:BROKER_URL='...'`, `$env:PORT='8001'`로 설정한다.
API를 실행하는 Python에 패키지를 설치해야 하며 WSL/Windows 가상환경은 공유하지 않는다.

## kind 배포

기존 `infra/local/up.sh`로 exchange의 DB와 Secret을 먼저 준비한다.

```bash
bash workloads/adapter/deploy-local.sh
# 브로커 주소 변경 예시
bash workloads/adapter/deploy-local.sh --set config.brokerUrl=http://mock-broker:8000
kubectl --context kind-chaos-lab -n exchange port-forward svc/adapter 8001:8000
```

init container가 전용 adapter_orders와 인덱스를 준비하며 기존 거래/시세 테이블은 수정하지 않는다.
동일 Secret `postgres`의 접속 정보를 사용하고 비밀번호의 특수문자를 URL 인코딩한다.
Pod 보안 설정은 non-root, 읽기 전용 파일시스템, API 토큰 자동 마운트 해제다.
ServiceMonitor가 /metrics/를 5초마다 수집한다. Prometheus Operator가 없으면
배포 인자에 `--set serviceMonitor.enabled=false`를 추가한다.
배포 스크립트는 이미지를 빌드·kind에 로드한 뒤 Helm으로 적용하고 같은 태그도 재시작한다.
이미지 빌드는 Dockerfile·requirements.txt·adapter 소스만 tar로 전달한다.
WSL의 /mnt/c에서 Windows가 만든 pytest 캐시의 xattr 권한 오류가 발생해도 캐시를 탐색하지 않는다.

## 설정

| 환경변수 | 기본값 | 의미 |
| --- | --- | --- |
| DATABASE_URL | 없음 | 로컬 DB URL. 없으면 아래 Secret/PGHOST 조합 사용 |
| PGHOST / PGPORT | 필수 / 5432 | 클러스터 DB 주소 |
| POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB | 필수 | 기존 postgres Secret |
| BROKER_URL | http://mock-broker.exchange.svc.cluster.local:8000 | 증권사 기본 주소, 경로 접두어 없음 |
| BROKER_RPS | 5 | 프로세스당 GET/POST 최대 호출률 |
| BROKER_TIMEOUT_SECONDS | 5 | 외부 HTTP 요청당 전체 timeout |
| MAX_ATTEMPTS | 8 | 조회/필요 시 전송으로 구성한 처리 시도 횟수 |
| RETRY_BASE_SECONDS / RETRY_MAX_SECONDS | 1 / 30 | backoff 상한은 min(max, base × 2^(attempts−1)), jitter는 그 절반~전체 |
| LEASE_SECONDS | 30 | 처리 중 장애 후 회수까지 대기시간 |
| POLL_SECONDS | 0.2 | worker 루프 대기시간 |
| DB_POOL_SIZE | 5 | Pod당 DB 연결 수 |
| PORT | 8000 | HTTP 포트 |

lease는 두 번의 외부 HTTP timeout + 두 번의 호출 간격 + 2초보다 길어야 한다.
최대 시도에서 죽었다가 lease를 회수한 경우에는 추가 전송 없이 마지막 조회만 한다.
pending에는 증권사 비동기 처리 대기도 포함되므로 장기 체결에는 시도/대기 설정 조정이 필요하다.

## 테스트

```bash
cd workloads/adapter
source .venv-wsl/bin/activate
python -m pytest -q
```

기본 테스트는 실제 DB/증권사 없이 API, 중복 요청, 정밀도, 제한, 재시도, 응답 유실 복구를 검사한다.
실제 PostgreSQL 테스트는 위 전용 개발 DB를 띄우고 다음처럼 실행한다.

```bash
export ADAPTER_TEST_DATABASE_URL='postgresql://adapter:adapter@localhost:15432/adapter'
python -m pytest -q
```

통합 테스트는 매번 고유한 adapter_test_* schema를 만들고 그 schema만 정리한다.
기존 주문/계좌 데이터를 초기화하지 않는다. 환경변수가 없으면 DB 테스트는 skip한다.
설치한 환경에서 `python -m ruff check adapter tests`와 `python -m ruff format --check adapter tests`로 검사한다.

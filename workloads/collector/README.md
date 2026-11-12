# Collector

WebSocket ticker → Redis 현재가 저장/Pub/Sub → PostgreSQL 시세 이력 배치 저장을 수행한다.
Python 3.11 이상을 지원하며 컨테이너는 Python 3.12를 사용한다.

## order API

- Redis key와 Pub/Sub channel: `price:KRW-BTC`
- 값과 발행 메시지: `{"price":"98000000","ts":1793923200123}`
- `price`는 십진 문자열, `ts`는 **수집기가 수신한 시각**의 Unix 밀리초 정수다.
- 업비트의 `timestamp`는 별도 `source_ts`로 DB에 기록한다. JSON 숫자는 `Decimal`로 읽는다.
- Redis 키에 TTL을 걸지 않는다. 수집 중단 후에도 주문 서비스가 `ts`로 `stale_price`를 판단한다.
- 동일 source timestamp의 snapshot은 별도 수신 건으로 취급하고, 이전 timestamp로 역행한 메시지는 버린다.
- 주문 API, 주문 테이블, 계좌, seed는 변경하지 않는다. `/healthz`, `/readyz`, `/metrics`는 주문 서비스의 HTTP 경로다. 수집기는 외부 HTTP 요청을 받지 않고 exec readiness를 사용한다.

## Chaos-lab 인프라에서 실행

아래 명령은 **Chaos-lab 루트의 Bash/WSL**에서 실행한다. Windows에서는 Docker Desktop의 Linux containers를 사용한다.

```bash
# 이미 실행했다면 생략. exchange namespace와 postgres/redis를 준비한다.
bash infra/local/up.sh

docker build -t chaos-lab-collector:dev workloads/collector
kind load docker-image chaos-lab-collector:dev --name chaos-lab
kubectl --context kind-chaos-lab apply -f infra/k8s/collector/
kubectl --context kind-chaos-lab -n exchange rollout status deployment/collector --timeout=180s
kubectl --context kind-chaos-lab -n exchange logs -f deployment/collector -c collector
```

`infra/local/up.sh`는 기존처럼 인프라만 준비한다. 수집기 배포는 위의 별도 명령으로 수행한다.
기존 `exchange` namespace의 Secret `postgres`에서 `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`를 읽는다.
DB는 `postgres.exchange.svc.cluster.local:5432`, Redis는 `redis.exchange.svc.cluster.local:6379/0`에 연결한다.
연결 문자열은 psycopg로 구성하므로 Secret 비밀번호의 공백이나 특수문자를 직접 URL에 삽입하지 않는다.
별도 Redis/PostgreSQL 또는 복제한 접속 Secret은 만들지 않는다.

init container는 `python main.py init-db`로 공용 `ticks` 시세 테이블과 인덱스를 준비한다.
기존 `ticks`에는 `source_ts BIGINT`, `event_id UUID`와 UUID 유일 인덱스를 추가하고,
`price`를 자릿수 제한 없는 `NUMERIC`으로 확장한다. 기존 행의 두 추가 필드는 NULL로 유지한다.
종목은 길이 제한 없는 `TEXT`, 수신 시각은 기존 계약의 `received_at TIMESTAMPTZ`를 사용한다.
수집기 내부/Redis의 `ts`(Unix 밀리초)는 DB 저장 시 UTC datetime으로 정확히 변환한다.
`source_ts`는 업비트 원본 Unix 밀리초를 64비트 정수로 저장한다.
`event_id`는 동일 배치 재시도 시 그대로 사용하며 `ON CONFLICT`로 중복 저장을 막는다.
주문/계좌 스키마와 seed는 실행하지 않는다. 기존 `price_history` 이력은 이동하거나 삭제하지 않는다.
기존 DB에는 갱신된 이미지의 init container 또는 명시적인 `python main.py init-db`로 적용한다.

Pod는 `app: collector`, 단일 replica, `Recreate` 전략을 사용한다. 업데이트 동안 수집이 잠시 중단된다.
카오스 주입 대상으로 사용할 때 namespace는 `exchange`, label selector는 `app: collector`를 지정한다.
DB/Redis의 기존 `chaos.exchange/protected` 라벨은 유지하고 수집기에는 붙이지 않는다.

같은 `dev` 태그로 이미지를 다시 빌드했다면 kind에 다시 로드한 뒤 재시작한다.

```bash
kubectl --context kind-chaos-lab -n exchange rollout restart deployment/collector
```

커스텀 kind 이름을 사용했다면 위 명령의 `chaos-lab`과 `kind-chaos-lab`을 해당 이름/context로 바꾼다.
공유 클러스터에서는 manifest의 두 이미지(init/main)를 같은 레지스트리 이미지의 고정 태그로 바꾼다.

## 수신 확인

```bash
kubectl --context kind-chaos-lab -n exchange exec deployment/redis -- redis-cli GET price:KRW-BTC
kubectl --context kind-chaos-lab -n exchange exec deployment/redis -- redis-cli SUBSCRIBE price:KRW-BTC
kubectl --context kind-chaos-lab -n exchange exec statefulset/postgres -- \
  psql -U exchange -d exchange -c 'SELECT market, price, received_at, source_ts FROM ticks ORDER BY received_at DESC LIMIT 5;'
```

GET을 몇 초 간격으로 반복해 `ts`가 증가하는지 확인한다. 주문 서비스가 실행 중이면 기존 `/orders` 요청을 보내 같은 Redis 현재가로 체결되는지 확인할 수 있다.
Pub/Sub는 영속 메시지 큐가 아니다. 구독자는 재연결 시 최신값을 GET하고 `ts`로 메시지 순서를 검사해야 한다.

## 로컬 Python 실행

기존 클러스터 저장소에 각각 별도 터미널로 port-forward한다.

```bash
kubectl --context kind-chaos-lab -n exchange port-forward svc/postgres 5432
kubectl --context kind-chaos-lab -n exchange port-forward svc/redis 6379
```

로컬 디버깅 중에는 클러스터 수집기를 `replicas=0`으로 내려 중복 수집을 방지한다.

```bash
kubectl --context kind-chaos-lab -n exchange scale deployment/collector --replicas=0
cd workloads/collector
python3 -m venv .venv-wsl
source .venv-wsl/bin/activate
python -m pip install -r requirements.txt
export REDIS_URL='redis://localhost:6379/0'
export DATABASE_URL='postgresql://exchange:exchange@localhost:5432/exchange'
python main.py init-db
python main.py
```

접속 정보는 `infra/local` 기본값 기준이다. PowerShell에서는 `python -m venv .venv`와 `.\.venv\Scripts\Activate.ps1`로 Windows용 환경을 준비하고, `export NAME='value'` 대신 `$env:NAME='value'`를 사용한다.
직접 실행은 Ctrl+C로 종료한다. 디버깅을 마치면 로컬 프로세스를 종료한 후 `scale deployment/collector --replicas=1`로 복원한다.
Kubernetes와 달리 직접 실행할 때 스키마 초기화는 명시적으로 수행한다.

## 설정

| 환경변수 | 기본값 | 의미 |
| --- | --- | --- |
| REDIS_URL | 필수 | Redis 연결 URL |
| DATABASE_URL | 없음 | 설정 시 DB 연결에 우선 사용 |
| PGHOST | DATABASE_URL이 없으면 필수 | 기존 PostgreSQL 서비스 주소 |
| PGPORT | 5432 | PostgreSQL 포트 |
| POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB | DATABASE_URL이 없으면 필수 | 기존 postgres Secret의 키 |
| MARKET | KRW-BTC | 이번 주 지원 마켓 |
| UPBIT_WS_URL | wss://api.upbit.com/websocket/v1 | mock 서버로 교체 가능 |
| BATCH_SIZE | 100 | 배치당 최대 건수 |
| FLUSH_SECONDS | 1 | 첫 시세부터 배치를 모으는 최대 초 |
| QUEUE_SIZE | 10000 | 메모리 대기열 상한; 처리 중인 배치는 별도 |
| RECEIVE_TIMEOUT | 60 | 메시지 미수신 시 재연결까지의 초 |
| MAX_SOURCE_AGE_MS | 5000 | 수신 시각과 업비트 timestamp의 허용 차이 |
| READY_MAX_AGE_MS | 5000 | 마지막 Redis 저장 시세의 readiness 허용 나이 |
| SHUTDOWN_SECONDS | 20 | 수신 중단 후 DB 저장 대기 제한(초) |
| READY_FILE | /tmp/collector-ready | readiness 기록 파일; 직접 실행 시 경로 변경 가능 |

기본 5초는 주문 서비스의 `STALE_MS=5000`에 맞췄다. 이 세 설정은 독립적이므로 시세 유효시간 정책을 바꿀 때 함께 검토한다.
readiness는 마지막 Redis 저장 성공과 가격 나이를 검사하며 DB 상태를 보증하지 않는다.
외부 장애를 이유로 컨테이너가 반복 재시작되지 않도록 freshness 검사를 liveness로 사용하지 않는다.
Redis 오류 시 readiness 파일을 지우며 새 시세의 저장 성공 시 복구한다.

## 장애 처리와 검증

- WebSocket 단절/수신 timeout: 최대 30초의 지수 backoff와 jitter로 재연결·재구독한다.
- Redis 장애: 해당 시세의 DB 이력을 유지하고 다음 시세에서 Redis 쓰기를 다시 시도한다.
- DB 일시 장애: 현재 배치를 보관하고 동일 UUID로 재시도한다. commit 결과가 불명확해도 `ON CONFLICT`로 중복 저장을 막는다.
- SIGTERM/Ctrl+C: readiness를 해제하고 수신을 취소한 뒤 최대 20초 동안 이력을 저장한다. Pod 종료 유예는 30초다.
- 큐 포화/worker 오류/종료 내 저장 실패: 오류 종료한다. SIGKILL, 큐 포화, 종료 제한 초과 시 메모리 이력은 유실될 수 있다.

Redis와 PostgreSQL은 하나의 트랜잭션이 아니며, 단절 기간의 모든 과거 시세를 복원하지 않는다.
수집기와 주문 서비스 노드의 시각 동기화가 필요하다.

테스트를 PC에서 실행하려면 **그 터미널에서 사용하는 Python에** 의존성을 설치해야 한다.
Docker 이미지 안에 설치된 패키지는 호스트나 WSL의 Python에 설치되지 않는다.
Windows의 `.venv`와 WSL의 가상환경은 공유할 수 없으므로 WSL에서는 `.venv-wsl`을 사용한다.
DB/Redis 실행, port-forward, 접속 환경변수 설정은 이 단위 테스트에 필요하지 않다.

WSL/Bash에서 Chaos-lab 루트 기준:

```bash
cd workloads/collector
python3 -m venv .venv-wsl  # 최초 1회
source .venv-wsl/bin/activate  # 새 터미널마다 실행
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

PowerShell에서 Chaos-lab 루트 기준(가상환경 활성화 없이 직접 실행):

```powershell
cd workloads/collector
python -m venv .venv  # 최초 1회
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

`ModuleNotFoundError: No module named 'psycopg'`가 나오면 해당 Python에 패키지가 설치되지 않은 것이다.
`python -c "import sys; print(sys.executable)"`로 위 가상환경의 Python을 사용하는지 확인한다.

테스트는 숫자 정밀도·Redis 계약·5초 경계·기존 Secret 설정·재연결/재구독·DB 재시도·Redis 장애/복구·역순 시세·배치 flush·큐 포화·종료 drain을 검사한다.
재연결 테스트는 로컬 WebSocket 서버를 사용한다. Redis/PostgreSQL 테스트는 대역을 사용하므로 실제 저장소 검증은 위 수신 확인 절차로 수행한다.
수집기 테스트는 위 명령으로 로컬에서 실행한다.

### 연결은 살아 있지만 시세가 오지 않는 경우 확인

가상환경을 활성화한 `workloads/collector` 디렉터리에서 다음 테스트만 실행할 수 있다.

```bash
python -m unittest discover -s tests -v -k silent_websocket
```

로컬 WebSocket 서버가 첫 구독을 받은 뒤 연결을 유지하면서 시세를 보내지 않는다.
수집기가 수신 timeout으로 연결을 닫고 다시 접속·구독하면 서버가 시세 한 건을 보낸다.
테스트는 timeout 경과, 재구독 및 새 시세 수신을 확인한다. Redis/DB는 필요하지 않다.
빠른 검증을 위해 테스트의 `receive_timeout`만 0.2초로 설정하며, 운영 기본값은 60초다.
운영에서는 마지막 메시지 수신 이후 60초의 대기와 연결 정리·backoff를 거쳐 재접속하므로
정확히 60초째에 재접속이 완료되는 것은 아니다.

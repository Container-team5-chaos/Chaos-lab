# Push server

collector가 Redis `price:KRW-BTC`에 저장·발행하는 현재가를 브라우저에 전달한다.
주문 서비스, adapter, mock-broker, PostgreSQL에 의존하지 않는다.
기존 E의 임시 `web/dev-push`를 대체하며 웹 nginx가 같은 주소의 `/ws`로 연결한다.

```text
Upbit → collector → Redis SET + PUBLISH → push-server → nginx /ws → web
                                  주문 서비스 → nginx /api → 계좌·평가손익
```

## 배포

저장소 루트에서 WSL/Bash로 실행한다.

```bash
# 이미 collector/Redis/order가 실행 중이면 아래 한 번으로 push + web 교체
bash web/deploy-local.sh
kubectl --context kind-chaos-lab -n exchange port-forward svc/web 8080:80
# http://localhost:8080

# 전체 환경을 처음 구성하는 경우 (실제 시세)
PRICE_STUB=0 bash infra/local/up.sh --with-apps
# 모니터링·Chaos Mesh 없이 앱만 확인: 앞에 LITE=1 추가
```

푸시 서버만 재배포할 때는 `bash workloads/push-server/deploy-local.sh`를 쓴다.
`CLUSTER_NAME`, `TAG`를 지정할 수 있다. 빌드 시 필요한 파일만 tar로 전달하여 WSL의 캐시 xattr 오류를 피한다.
서버 Pod 하나·프로세스 하나로 시작한다. 여러 Pod로 늘려도 각각 Redis를 구독하고 자신에게 연결된 화면에 전송한다.
클러스터 내부 주소는 `push-server.exchange.svc.cluster.local:8080`이다.
ServiceMonitor CRD가 있으면 5초 수집 설정을 함께 적용한다.

## API

| 경로 | 동작 |
| --- | --- |
| `WS /ws?market=KRW-BTC` | 연결 직후 최신가, 이후 실시간 가격. market 생략 시 KRW-BTC |
| `WS /ws/prices?market=KRW-BTC` | 같은 동작의 별칭 |
| `GET /healthz` | 프로세스가 응답하면 200. Redis 장애와 분리 |
| `GET /readyz` | Redis 구독·조회 및 worker가 준비되고 종료 중이 아니면 200, 아니면 503 |
| `GET /metrics` | Prometheus 텍스트 형식 |

가격 메시지:

```json
{"type":"price","market":"KRW-BTC","price":"98000000.12345678","ts":1762330000123}
```

`price`는 양의 십진 문자열, `ts`는 collector 수신 시각인 Unix 밀리초 정수다.
`source: "stub"`이면 그대로 전달하여 웹에서 가상 시세임을 표시한다.
오래된 가격도 원래 시각으로 전달하므로 화면에서 지연 상태를 판단할 수 있다.

가격이 아직 없거나 갱신이 멈춘 경우:

```json
{"type":"status","market":"KRW-BTC","reason":"price_unavailable"}
{"type":"status","market":"KRW-BTC","reason":"stale_price"}
```

`type=status`는 가격 데이터가 아니다. 기존 가격을 덮어쓰지 않는다.
지원하지 않는 market·서비스 준비 전·연결 수 초과는 WebSocket upgrade를 거부한다(HTTP 403).
연결 후 클라이언트 데이터 메시지는 받지 않으며 보내면 1008로 닫는다. WebSocket ping/pong은 자동 처리한다.
Redis 장애/느린 전송은 1013, 서버 재시작은 1012로 연결을 닫고 웹이 지수 지연과 jitter로 재연결한다.

## 전달과 복구

- Redis 구독 응답을 확인한 뒤 GET으로 최신가를 읽는다. 대기 중 들어온 더 오래된 메시지는 무시한다.
- 새 화면은 서버의 최신 스냅샷을 즉시 받는다. Redis 구독은 사용자 수와 관계없이 프로세스당 하나다.
- Redis 구독 ping/pong과 정기 GET(2초)으로 단절 및 자동 재구독 때 놓친 최신가를 복구한다.
- Redis 장애 시 readiness를 내리고 기존 연결을 닫는다. 재연결 후 구독 + GET을 다시 수행한다.
- 연결별 전송 대기열은 최신가 한 개로 제한한다. 느린 화면 때문에 다른 화면이 대기하지 않는다.
- Pub/Sub 단절 동안의 모든 tick을 재전송하는 서비스는 아니다. 현재가용이며 차트는 접속 중 받은 값만 표시한다.
- collector가 멈췄어도 Redis가 정상이라면 readiness는 유지한다. 시세 지연은 메시지·화면·지표로 구분한다.
- SIGTERM부터 신규 연결을 막고 readiness 503, WebSocket 종료, Redis 연결 정리 순으로 종료한다.

## 설정과 지표

| 환경 변수 | 기본값 |
| --- | --- |
| `REDIS_URL` | `redis://redis:6379/0` |
| `MARKET` | `KRW-BTC` (현재 지원 마켓) |
| `PORT` | `8080` |
| `STALE_MS` | `5000` |
| `MAX_CLIENTS` | `1000` (프로세스당) |
| `SEND_TIMEOUT_SECONDS` | `5` |

`push_connections`, `push_redis_up`, `push_price_age_seconds{market}`(-1은 시세 없음),
`push_messages_total{type}`, `push_coalesced_total`, `push_invalid_messages_total`,
`push_redis_reconnects_total`을 노출한다.

## 검증

```bash
cd workloads/push-server
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -q

# 실제 Redis + TCP WebSocket 테스트. 실험용 DB 대신 전용 Redis를 쓴다.
docker run --rm -d --name push-test-redis -p 127.0.0.1:16379:6379 redis:7-alpine
TEST_REDIS_URL=redis://localhost:16379/0 python -m pytest -q
docker stop push-test-redis

# 배포된 웹을 Chromium으로 확인 (읽기 전용: 주문 생성 안 함)
python -m playwright install --with-deps chromium  # WSL/Ubuntu: 시스템 라이브러리도 설치 (sudo 필요)
WEB_BASE_URL=http://localhost:8080 python -m pytest tests/test_web.py -q

# 선택: 로컬 데모 클러스터 장애 복구 테스트 (아래 주의 사항 확인)
RUN_CLUSTER_TESTS=1 WEB_BASE_URL=http://localhost:8080 \
  TEST_KUBE_CONTEXT=kind-chaos-lab python -m pytest tests/test_cluster_recovery.py -q
```

실시간 웹 테스트는 실제 collector 또는 시세 stub이 갱신 중이어야 한다.
Windows에서 직접 실행할 때는 `python -m playwright install chromium`으로 충분하다.
WSL에서 `BrowserType.launch: TargetClosedError`와 함께 `libnspr4.so` 등 shared library 누락이 나오면
`python -m playwright install-deps chromium`으로 시스템 의존성을 설치한 뒤 다시 실행한다.
Windows용 Chromium 설치와 WSL용 설치는 별개
단위 테스트는 잘못된 가격 차단·최신가 큐·전송 시간 제한·readiness를,
실제 Redis 테스트는 최초 스냅샷·여러 화면 전송·연결 정리·시세 없음/지연·구독 단절 복구를 확인한다.

`test_cluster_recovery.py`는 명시적으로 켰을 때만 실행된다. `kind-`로 시작하는 로컬 context에서
user-1000의 모의 매수 주문 한 건을 만들고 collector를 잠시 중단한 뒤 복원하며,
push-server·Redis Deployment를 재시작한다. Redis는 영속 저장소가 없어 재시작 때 캐시가 사라지고
collector가 다시 채운다. 진행 중인 실험이 없는 데모 클러스터에서만 실행한다.

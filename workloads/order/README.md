# 주문 서비스와 adapter 연결

`ADAPTER_URL`을 설정하면 주문 서비스가 adapter를 통해 외부 증권사로 주문을 전달한다.
미설정이면 기존 Redis 현재가 즉시 체결 API를 사용한다. 두 모드 모두 같은 주문 키의 중복 반영을 막는다.
실제 mock-broker 서비스는 별도이며, [adapter API](../adapter/API.md)의 멱등성·조회·max_amount 계약이 필요하다.

## 외부 주문의 흐름

1. POST /orders에서 사용자·시세·가용 잔고·가용 수량을 확인한다.
2. 같은 트랜잭션에서 orders.pending과 order_external 전달 기록/예약을 저장한다.
   저장 완료 후 202를 반환한다. 원화·보유 수량의 총액은 아직 바뀌지 않는다.
3. 각 주문 Pod의 worker가 lease를 얻은 주문을 adapter에 조회하고, 없으면 같은 키로 접수한다.
   HTTP 호출 중에는 계좌 행 잠금이나 DB 트랜잭션을 유지하지 않는다.
4. pending/일시 통신 오류는 DB에 다음 조회 시각을 기록하고 재시도한다.
5. filled이면 계좌 잠금 후 원화·포지션·체결 상태·예약 해제를 한 트랜잭션으로 반영한다.
   체결가는 adapter가 확인한 외부 체결가를 사용한다.
6. 명시적 rejected이면 예약을 해제한다. unknown/계약 불일치는 자금을 예약한 채 자동 처리를 멈춘다.

매수 예약액은 접수 시 Redis 가격 × 수량을 소수점 4자리로 버림한 값이다.
같은 값을 max_amount로 전달하여 외부 증권사가 예약액보다 비싸게 체결하지 못하도록 한다.
가격 상승으로 한도 안에서 전량 체결할 수 없으면 증권사가 체결 전에 거절해야 한다.
더 낮은 가격에 체결되면 실제 금액만 차감하고 예약 차액은 해제한다.
매도는 주문 수량을 예약한다. 부분 체결·수수료·주문 취소는 현재 계약에서 지원하지 않는다.

동시에 접수한 주문은 계좌 잠금과 예약 합계로 이중 사용을 막는다.
로컬 즉시 체결 경로도 외부 주문의 예약액/수량을 제외하고 검사한다.
전달 중 Pod가 죽으면 DB 기록과 예약은 남고 lease 만료 후 다른 worker가 같은 키부터 조회한다.
이미 정산한 주문과 이전 lease token의 worker는 다시 잔고를 바꿀 수 없다.
체결 결과를 DB에 저장하다 실패하면 다음 조회에서 같은 결과로 정산을 재시도한다.

## 외부 모드 API

요청은 기존 POST /orders와 동일하다. 금액/수량은 문자열이며, 외부 모드의 주문 키는
1–64자, 영문/숫자로 시작하고 영문·숫자·`.`·`_`·`:`·`-`를 허용한다.
지원 마켓은 KRW-BTC다. 클라이언트는 max_amount를 직접 지정하지 않고 주문 서비스가 예약액으로 설정한다.

```json
{"client_order_id":"external-1","user_id":"user-0001","market":"KRW-BTC","side":"bid","volume":"0.001"}
```

접수 성공은 `202 Accepted`다. 기존 주문 응답 필드에 `execution_mode="adapter"`, `reason`이 추가된다.

```json
{
  "order_id": 42,
  "client_order_id": "external-1",
  "user_id": "user-0001",
  "market": "KRW-BTC",
  "side": "bid",
  "volume": "0.00100000",
  "status": "pending",
  "fill_price": null,
  "amount": null,
  "filled_at": null,
  "replayed": false,
  "execution_mode": "adapter",
  "reason": null
}
```

`GET /orders/{client_order_id}`로 최종 상태를 조회한다. 로컬 주문도 이 경로로 조회할 수 있다.
없는 주문은 404/order_not_found, DB 연결 불가는 503/not_ready다.

| status | 의미 | POST 재요청 HTTP | GET HTTP |
| --- | --- | --- | --- |
| pending | 예약 후 전달/확인 중 | 202 | 200 |
| filled | 체결·잔고·포지션 정산 완료 | 200 | 200 |
| rejected | 증권사의 명시적 거절, 예약 해제 | 422 | 200 |
| unknown | 체결/정산 확인 필요, 예약 유지 | 200 | 200 |

같은 키·같은 내용은 현재 기록과 replayed=true를 반환한다. 다른 내용은 409다.
사용자/시세/잔고 검사에서 거절한 요청은 기존처럼 주문에 저장하지 않는다.
외부에 접수한 뒤 거절된 주문은 rejected 상태로 저장하고, 같은 키를 새 주문으로 재사용하지 않는다.
unknown을 미체결로 간주하거나 새 키로 자동 재주문하지 않는다. 현재 수동 복구/취소 API는 없다.
클라이언트는 POST의 202를 성공 체결로 계산하지 말고 GET에서 filled를 확인해야 한다.
기존 k6/B 판정은 즉시 체결 모드 기준이므로 외부 시나리오에는 별도의 202/조회 상태 처리가 필요하다.

GET /accounts/{user_id}의 krw/volume은 예약을 포함한 총 보유값이다.
외부 모드에서는 reserved_krw/available_krw와 포지션별 reserved_volume/available_volume이 추가된다.
예약이 남아 있으면 로컬 모드에서도 이 필드를 보여준다. 조회는 동일 DB 스냅샷을 사용한다.

## 포지션 조회 API

`GET /accounts/{user_id}/positions`는 보유 포지션에 Redis 현재가를 붙여 평가금액과 손익을 계산한다.
두 모드 모두 같은 형식이며, 계산은 서버에서 Decimal로 한다 (화면은 이 값을 그대로 표시).

```json
{
  "user_id": "user-0001",
  "krw": "9020000.0000",
  "reserved_krw": "0",
  "available_krw": "9020000.0000",
  "positions": [
    {
      "market": "KRW-BTC",
      "volume": "0.01000000",
      "avg_price": "98000000.00000000",
      "reserved_volume": "0",
      "available_volume": "0.01000000",
      "current_price": "99000000",
      "price_status": "ok",
      "valuation": "990000.0000",
      "pnl": "10000.0000",
      "pnl_rate": "1.02"
    }
  ],
  "total_valuation": "990000.0000",
  "total_assets": "10010000.0000"
}
```

| 필드 | 계산 |
| --- | --- |
| valuation | 현재가 × 보유 수량 (예약 수량 포함), 소수점 4자리 버림 |
| pnl | (현재가 − 평균 매수가) × 보유 수량, 소수점 4자리 버림 |
| pnl_rate | (현재가 / 평균 매수가 − 1) × 100, 소수점 2자리 버림 (%) |
| total_assets | krw + total_valuation |

- 시세가 STALE_MS보다 오래됐거나(`stale_price`) 없거나 Redis에 닿지 않으면(`price_unavailable`)
  그 포지션의 current_price/valuation/pnl/pnl_rate는 null이고, total_valuation/total_assets도 null이다.
  오래된 가격으로 손익을 보여주지 않으며, 이때도 200과 보유 수량·원화는 반환한다.
- 없는 사용자는 404/user_not_found, DB 연결 불가는 503/not_ready.
- 보유 수량이 0인 마켓은 목록에 없다. 기존 `GET /accounts/{user_id}` 응답은 바꾸지 않았다.

## 실행/배포

로컬은 기존 DB/Redis를 준비한 뒤 주문 서비스 가상환경에서 다음을 실행한다.
환경변수 설정은 테스트/수집기와 공유하지 말고 실행 터미널에 지정한다.

```bash
cd workloads/order
python -m pip install -r requirements-dev.txt
export DATABASE_URL='postgresql://exchange:exchange@localhost:5432/exchange'
export REDIS_URL='redis://localhost:6379/0'
export ADAPTER_URL='http://localhost:8001'
python -m app
```

외부 모드 기동 시 계좌/주문을 비우지 않고 order_external을 만들고 orders의 상태 제약에 unknown을 추가한다.
여러 Pod가 동시에 시작해도 PostgreSQL advisory lock으로 스키마 변경을 직렬화한다.
DB 사용자는 해당 테이블 생성/변경 권한이 필요하다. Redis 가격은 수집기 또는 stub이 공급해야 한다.

kind의 기존 주문 서비스가 예전 이미지라면 예약을 인식하도록 먼저 새 코드를 로컬 모드로 전체 배포한다.
그 뒤 budget 계약을 지원하는 adapter/mock-broker를 준비하고 외부 모드를 활성화한다.

```bash
# 레포 루트, Bash/WSL
bash workloads/order/deploy-local.sh
bash workloads/adapter/deploy-local.sh
# mock-broker 준비 후
bash workloads/order/deploy-local.sh -f workloads/order/chart/values-adapter.yaml
kubectl --context kind-chaos-lab -n exchange port-forward svc/order 8000:8000
```

ADAPTER_URL 미설정으로 전환해도 이전 pending/unknown 주문이 취소되거나 예약이 해제되지 않는다.
외부 정산 worker도 꺼지므로 미처리 주문은 외부 모드의 Pod를 유지해서 처리해야 한다.
안전한 전환은 접수 중지→pending/unknown 확인/해결→모드 전환 순서다.
infra/local/reset-db.sh도 pending/unknown 외부 주문이 남아 있으면 예약/전달 기록을 지우지 않고 중단한다.

| 환경변수 | 기본값 | 의미 |
| --- | --- | --- |
| ADAPTER_URL | 빈 문자열 | 설정 시 외부 모드, 클러스터 주소는 http://adapter.exchange.svc.cluster.local:8000 |
| ADAPTER_TIMEOUT_SECONDS | 5 | adapter HTTP 호출당 전체 timeout |
| ADAPTER_POLL_SECONDS | 0.5 | 정상 pending 조회 및 worker 대기 간격 |

외부 모드의 readiness는 DB/Redis와 정산 worker의 실행 상태를 검사한다.
adapter/증권사 장애 자체는 접수를 막지 않으며 예약과 전달 기록을 보존한다.
SIGTERM은 신규 접수를 막고 worker를 취소하며 처리 중 lease는 재시작 후 회수한다.

orders_total은 최종 filled/rejected를 집계한다. 외부 접수 수는 orders_external_accepted_total,
확인 필요 상태는 orders_external_unknown_total이다. 지표는 프로세스 재시작 시 초기화된다.
order_duration_seconds의 pending은 접수 응답 시간이며, 외부 체결 완료까지의 시간을 뜻하지 않는다.

## 테스트

기존과 같이 테스트 전용 DB/Redis를 띄우고 schema.sql로 빈 DB를 준비한다.
테스트는 seed.sql로 주문·계좌·포지션을 초기화하므로 실행 중 실험 DB에 연결하지 않는다.

```bash
cd workloads/order
export ADAPTER_URL=''
python -m pytest -q
```

test_external_orders.py는 실제 PostgreSQL/Redis, 실제 order/adapter API와 worker,
증권사 HTTP 대역으로 예약·체결·거절·응답 유실·unknown·중복/lease 복구를 검사한다.
이 테스트는 adapter 패키지를 저장소에서 import한다. 외부 증권사 서비스는 필요하지 않다.

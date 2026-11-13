# Adapter API v0.1 — D·C 연동용 초안

adapter는 주문 전달 상태만 관리한다. 사용자 존재·잔고·포지션은 D의 주문 서비스가 검사·예약·정산한다.
주문 서비스에 `ADAPTER_URL`을 설정하면 접수/조회 API로 연결된다. (연결하지 않으면 redis로 바로 연결됨)
외부 모드의 202/조회/예약 계약은 [주문 서비스 문서](../order/README.md)에 정의한다.

기본 주소: `http://adapter.exchange.svc.cluster.local:8000`

JSON UTF-8. 금액·수량은 십진 문자열. 지원 마켓은 `KRW-BTC`.
내부 서비스용 API이며 사용자 인증 기능은 없다. 클러스터 외부 공개용으로 구성하지 않는다.
OpenAPI는 `/openapi.json`, 대화형 문서는 `/docs`에서 확인한다.

## POST /orders — 전달 요청 접수

```json
{
  "client_order_id": "order-0001",
  "user_id": "user-0001",
  "market": "KRW-BTC",
  "side": "bid",
  "volume": "0.001"
}
```

- `client_order_id`: 전역 고유 키, 1–64자. 영문/숫자로 시작하고 영문·숫자·`.`·`_`·`:`·`-` 허용.
- `user_id`: 문자열 1–64자. adapter에서 계좌 존재를 검사하지 않는다.
- `side`: `bid` 또는 `ask`. (매수 또는 매도)
- `volume`: 양수 문자열, 정수 최대 22자리/소수 최대 8자리. 8자리로 정규화하여 비교한다.
- `max_amount`: 선택적 비음수 금액 문자열, 소수 최대 4자리/정수 최대 20자리 미만.
  D는 매수 시 예약한 원화를 이 필드로 전달한다. `98000`과 `98000.0000`은 동일하다.
  한도를 전달한 요청은 증권사 응답에서도 같은 한도를 되돌려줘야 한다.
- 정의하지 않은 추가 필드는 400이다.

DB에 저장이 완료된 후 `202 Accepted`를 반환한다. 202는 체결 완료를 의미하지 않는다. (주문 접수만, 체결은 mock-broker)

```json
{
  "client_order_id": "order-0001",
  "user_id": "user-0001",
  "market": "KRW-BTC",
  "side": "bid",
  "volume": "0.00100000",
  "status": "pending",
  "attempts": 0,
  "result": null,
  "last_error": null,
  "created_at": "2026-11-11T09:00:00+00:00",
  "updated_at": "2026-11-11T09:00:00+00:00",
  "replayed": false
}
```

같은 키·같은 내용의 재요청은 현재 기록을 반환한다. pending은 202, 그 외 상태는 200이며 replayed는 true다.
`volume="0.001"`과 `"0.00100000"`은 같은 내용으로 취급한다.
같은 키에 사용자·마켓·방향·수량이 다르면 409를 반환하고 최초 주문은 변경하지 않는다.

## GET /orders/{client_order_id} — 전달/체결 상태 조회

동일한 응답 구조를 200으로 반환한다. GET의 `replayed`는 false이다.
worker 처리 중인 주문은 POST 직후 조회에서 이미 완료 상태로 보일 수 있다.

| status | 의미 | 호출자가 해야 할 일 |
| --- | --- | --- |
| pending | DB 접수 완료, 전달/조회/재시도 중 | 같은 주문 키로 조회 |
| filled | 외부 증권사가 체결 확인 | result를 읽고 D의 정합성 규칙으로 반영 |
| rejected | 외부 증권사가 명시적으로 거절 | result.reason 확인 |
| unknown | 시도 한도 내 확정 결과를 얻지 못함 | 외부 주문을 확인하기 전 새 키로 재주문하지 않음 |

filled/rejected의 `result`는 아래 증권사 응답 형식이다. unknown은 자동 재시도가 종료된 상태이며
조회만으로 추가 외부 호출이 일어나지 않는다. 수동 재처리 API는 이번 범위에 포함하지 않는다.

## 요청 오류

| HTTP | reason | 의미 |
| --- | --- | --- |
| 400 | invalid_request | 요청 형식/수량/마켓 오류 |
| 409 | idempotency_conflict | 기존 키와 다른 주문 내용 |
| 404 | order_not_found | GET에서 전달 기록 없음 |
| 503 | not_ready | DB 연결 오류, worker 중단, 종료 중 |

```json
{"status":"rejected","reason":"idempotency_conflict","client_order_id":"order-0001"}
```

요청 오류의 `rejected`는 HTTP 요청 거절이다. 이미 접수된 주문의 체결 취소나 미체결을 보증하지 않는다.
DB commit 후 응답이 유실될 수 있으므로 503/통신 오류 뒤에는 같은 키로 다시 요청한다.
형식 오류 응답의 client_order_id는 null이다.

## 외부 증권사 / mock-broker가 구현해야 하는 계약

이 계약을 사용하는 테스트 대역은 존재하지만 mock-broker 서비스 자체는 아직 없다.

- `GET /orders/{client_order_id}`: 없으면 404, 있으면 200과 현재 상태.
- `POST /orders`: 위 주문 요청 필드. `Idempotency-Key` 헤더도 client_order_id와 동일하게 전달한다.
- 같은 키에 대해 동시 요청·재시도해도 외부 주문을 한 번만 생성해야 한다.
- max_amount가 있으면 매수 체결 총액을 그 이하로 제한해야 한다. 초과할 경우 체결 전에
  `rejected`/`reason=price_limit_exceeded`로 거절한다. pending/filled/rejected 응답에 한도도 포함한다.
  adapter는 한도 누락·불일치·초과 체결 응답을 확정하지 않고 재조회 후 unknown으로 처리한다.
- 이미 접수한 주문은 GET으로 조회 가능해야 한다. 404 뒤의 재요청도 같은 키로 중복 방지해야 한다.
- 비동기 접수는 202와 `status=pending`, 체결은 200/201과 `status=filled`.
- 명시적 거절은 200 또는 4xx(429 제외)와 `status=rejected`, `reason` 및 원본 요청 필드.
- 429는 `Retry-After`(초 또는 HTTP 날짜) 지원. 5xx/단절/timeout은 미체결의 증거로 취급하지 않는다.

체결 응답:

```json
{
  "client_order_id": "order-0001",
  "user_id": "user-0001",
  "market": "KRW-BTC",
  "side": "bid",
  "volume": "0.00100000",
  "status": "filled",
  "broker_order_id": "broker-42",
  "fill_price": "98000000.00000000",
  "amount": "98000.0000",
  "filled_at": "2026-11-11T09:00:00Z"
}
```

pending/rejected도 원본 주문의 다섯 필드를 포함해야 한다. filled_at은 시간대가 있는 ISO 8601.
adapter는 요청 필드 일치, 금액·가격의 유효성, 필수 체결 정보를 검사한다.
필드 불일치/깨진 JSON/불완전한 응답은 체결이나 거절로 확정하지 않고 재조회한다.

## 상태 및 지표

- `GET /healthz`: 프로세스 응답 여부.
- `GET /readyz`: DB 테이블 조회 가능, worker 실행 중, 종료 중이 아니면 200. 아니면 503.
- 증권사 장애는 readiness를 내리지 않는다. DB에 접수하고 복구 후 전달하기 위해서다.
- `GET /metrics/`: Prometheus 형식.

| 지표 | 종류 | 라벨 |
| --- | --- | --- |
| adapter_orders_accepted_total | Counter | 없음 |
| adapter_orders_replayed_total | Counter | 없음 |
| adapter_retries_total | Counter | reason |
| adapter_orders_completed_total | Counter | status |
| adapter_queue_depth | Gauge | 없음 |
| adapter_broker_attempt_seconds | Histogram | 없음 |

지표는 프로세스 재시작 시 초기화되며 상태의 원본은 PostgreSQL이다.

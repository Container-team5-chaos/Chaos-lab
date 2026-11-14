# B 판정 계약 명세 — 실제 주문 서비스 기준

- 이 문서는 judge(`orchestrator/src/orchestrator/judge/`)가 실제 주문 서비스(`workloads/order/`)를 판정할 때 쓰는 계약이다. 코드 주석의 "계약 명세 N"은 이 문서의 N절이다.
- 근거: 주문 서비스 코드(main 6e8f38b)와 팀 공유 "주문 서비스 API" 통일본. 둘이 다르면 **코드가 실제 동작**이라 judge는 코드를 따르고, 차이는 앱 담당 확인 요청으로 남긴다(11절).
- 설계 전체(k6 Job, 수집, 일정, 다른 파트 요청)는 [b-measure-judge.md](b-measure-judge.md)를 따른다. 이 문서는 주문 계약에 기대는 부분만 정한다.
- 상태: 2026-11-06 초안. 주문 서비스는 체결만 저장하는 최소 버전이라 바뀔 수 있다.

## 1. 실제 서비스 계약 (`workloads/order/app/*.py`, `db/*.sql`)

**API**
- `POST /orders`, 본문 `{client_order_id(1..64), user_id(1..64), market, side: "bid"|"ask", volume}`. 금액·수량은 문자열로 주고받는다.
  `market`은 `KRW-BTC`만(`SUPPORTED_MARKETS`). 인증은 없다. **멱등 키는 본문의 `client_order_id`**다(헤더가 아니다).
- 성공은 **늘 HTTP 200**이다(새 체결도, 재요청도). 본문은 `order_to_dict(row, replayed)`다:
  `{order_id, client_order_id, user_id, market, side, volume, status("filled"), fill_price, amount, filled_at, replayed}`. Decimal은 지수 없는 문자열이다.
- 처리 순서(`orders.place_order`)
  1. `client_order_id`로 기존 주문을 찾는다. 있으면 본문(user_id, market, side, volume)이 같을 때 기존 행을 `replayed: true`로 200, 다르면 409 `idempotency_conflict`.
  2. Redis `price:{market}`을 읽는다. 없으면 503 `price_unavailable`, `STALE_MS`(기본 5000) 초과면 503 `stale_price`.
  3. 한 트랜잭션: accounts `FOR UPDATE`(없으면 404 `user_not_found`), positions `FOR UPDATE`.
     - bid: krw < amount면 422 `insufficient_balance`. 아니면 krw −= amount, volume += v, avg = ((held·avg + v·price) / new_volume)을 소수 8자리로 quantize(ROUND_HALF_EVEN, 정밀도 28).
     - ask: held < v면 422 `insufficient_volume`. 아니면 krw += amount, volume −= v, avg 그대로(0이 돼도 유지).
     - amount = (price × volume)을 소수 4자리 ROUND_DOWN. orders에 `status='filled'`, fill_price, amount, filled_at=now()로 INSERT.
  4. 같은 키가 동시에 오면 UniqueViolation → 롤백 → 기존 행을 `replayed: true`로 돌려준다.
- 1단계의 기존 주문 조회는 **잠금 밖**이다. 그래서 처리 중인 앞 요청이 잠금을 쥔 동안 온 재전송은 행을 못 찾고, 잠금을 기다린 뒤 바뀐 잔고로 422를 받을 수 있다(4절 REJECT_AFTER_INFLIGHT).
- 거절 본문은 `{status: "rejected", reason, client_order_id}`다. 400 `invalid_request`(검증 실패, client_order_id=null), 404, 409, 422, 503(`price_unavailable`, `stale_price`, `not_ready`).
  `not_ready`는 종료 중(처리 전)이거나 DB·Redis 연결 오류다. 연결 오류는 커밋 뒤일 수도 있어 **결과 불명**으로 본다.
- **거절된 주문은 저장하지 않는다.** 그래서 503 stale_price 뒤 같은 키로 다시 보내면 새로 체결된다.
- 조회는 `GET /accounts/{user_id}`뿐이다. **주문 조회 API는 없다.**
- 지표: `orders_total{status, reason}`, `orders_replayed_total`, `order_duration_seconds{status}`, `price_age_seconds{market}`.
- 종료: SIGTERM → 새 주문은 503 not_ready → uvicorn graceful 20s.

**DB** (`db/schema.sql`, `db/seed.sql`, `infra/k8s/data/postgres.yaml`)
- namespace `exchange`, StatefulSet `postgres`(Pod `postgres-0`), 계정·DB `exchange`.
- `accounts(user_id PK, krw NUMERIC(24,4) CHECK ≥ 0)`, `positions(user_id, market, volume NUMERIC(30,8) CHECK ≥ 0, avg_price NUMERIC(24,8) CHECK ≥ 0)`,
  `orders(id BIGSERIAL, client_order_id UNIQUE, user_id, market, side, volume CHECK > 0, status, fill_price, amount, created_at, filled_at, CHECK(filled면 체결 열 NOT NULL))`,
  `ticks(market, price, received_at)`(수집기가 쓴다. 비어 있을 수 있다).
- seed는 `TRUNCATE` 뒤 `user-0001`..`user-1000`에 krw 10,000,000을 넣는다.
- fills 테이블, 수수료, 읽기 전용 역할은 없다.
- 같은 사용자의 주문은 accounts 잠금으로 직렬화되고 INSERT가 잠금 안에서 일어나므로 **사용자별로는 orders.id 순서 = 처리 순서**다.

## 2. k6 주문 기록 JSONL v2

한 줄 = JSON 객체 하나. B가 소유하는 형식이다.
- `run_start`: `{type:"run_start", v:2, run, t, profile, vus, …}`.
- `intent`(보내기 전): `{type:"intent", key, acct, vu, t, body:{client_order_id, user_id, market, side, volume}}`. `key == body.client_order_id`, `acct == body.user_id`여야 한다.
- `client_order_id` 형식: `k6-<run_id>-<vu 4자리>-<seq 6자리>`(예 `k6-demo-1106-0001-000042`). run_id를 넣어 run끼리 겹치지 않게 한다. 64자 이하다(run_id 최대 40자면 최대 55자). judge는 자릿수와 상관없이 `k6-<run_id>-<숫자>-<숫자>`를 run 키로 본다.
- `result`(끝난 뒤): `{type:"result", key, atts:[Attempt…]}`. 같은 키로 보낸 모든 전송을 시간순으로 담는다.
  Attempt = `{n, t0, t1, st(전송 오류는 0), ec(reason 또는 k6 error_code), ms, rp(200 본문의 replayed), raw(본문 2048자 이하)}`.
- 재전송: 결과 불명 시도와 503 `stale_price`·`price_unavailable`에서 같은 본문·같은 키로 최대 R번(기본 3) 백오프 재전송한다. 200이나 재전송하지 않는 거절(400, 404, 409, 422)이 오면 멈춘다.
- k6는 `Idempotency-Key`·`X-Idempotency-Key` 헤더를 보내지 않는다(Go net/http가 POST를 자동 재시도하는 조건이다). 그래서 `noConnectionReuse` 강제는 필요 없다.

## 3. 시도 성격과 클라이언트 상태 K

시도 하나
- **ACK**: st == 200이고 원문이 `order_id`가 있는 JSON 객체다.
- **확정 거절(REJECT)**: st ∈ {400, 404, 409, 422}, 또는 503이고 reason ∈ {price_unavailable, stale_price}. **그 시도는** 저장되지 않는다. 같은 키의 처리 중인 앞 시도는 나중에 커밋될 수 있다(1절).
- **결과 불명(UNKNOWN)**: st == 0, 502, 504, 503 not_ready 또는 알 수 없는 reason, 그 밖 5xx.
- 그 밖(201, 200인데 주문 본문이 아님 등): PROTOCOL.

키별 K
- `K_ACK`: ACK가 하나라도 있다. 첫 ACK 본문이 원 응답이다.
- `K_REJECT`: ACK가 없고 마지막 시도가 확정 거절이다.
- `K_UNKNOWN`: ACK가 없고 마지막 시도가 결과 불명이다. 마지막 시도가 PROTOCOL이거나 시도가 없을 때도 K_UNKNOWN으로 두고 PROTOCOL을 함께 붙인다.
- `INTERRUPTED`: intent만 있고 result가 없다.

## 4. 주문 분류 행렬

DB 대조 키는 **본문의 `client_order_id`**다. 서버가 저장한 것이 그 값이기 때문이다. intent.key와 다르면 PROTOCOL(`intent_key`)과 safety 가드로 잡고, 서버 쪽 결과는 본문 키로 그대로 판정한다(클라이언트 기록 결함이 LOST·ORPHAN FAIL이 되지 않게).

D1 = 그 키의 orders 행이 있다. "일치" = 원 응답 필드가 DB 행과 같다(5절).

| K \ DB | D1 일치 | D1 불일치 | D0 |
|---|---|---|---|
| K_ACK | OK | MISMATCH | LOST |
| K_REJECT | REJECT_COMMITTED | REJECT_COMMITTED | OK(rejected) |
| K_UNKNOWN | UNRESOLVED_COMMITTED | UNRESOLVED_COMMITTED | UNRESOLVED_ABSENT |
| INTERRUPTED | INTERRUPTED | INTERRUPTED | INTERRUPTED |

- OK 하위: `clean`(첫 시도 ACK, replayed=false), `after_commit`(앞에 결과 불명, 첫 ACK replayed=true), `not_committed`(앞에 결과 불명이나 확정 거절, 첫 ACK replayed=false), `after_503`(앞이 503 not_ready뿐이고 첫 ACK replayed=true, 경고 ROLLBACK_REPLY_COMMITTED), `rejected`(K_REJECT × D0).
- `REJECT_COMMITTED`: 클라이언트는 확정 거절을 받았는데 DB에 주문이 있다. 돈이 움직였는데 거절로 안내됐으므로 유실 기준 FAIL이다.
  앞에 결과 불명 시도가 있고 마지막이 409가 아니면 서비스 경합(1절)이다. FAIL은 그대로 두고 `detail.after_inflight`와 경고 `REJECT_AFTER_INFLIGHT`로 원인을 표시한다.
- `BODY_MISMATCH`: D1 행의 (user_id, market, side, volume)이 보낸 본문과 다르다.
- DB 전용: `DUP_ORDER`(같은 키 행 2개 이상), `FILL_STATE`(filled인데 체결 열 NULL, 또는 filled가 아닌 run 주문), `ORPHAN`(run 키 형식인데 JSONL에 없다).
- 멱등 위반(IDEMPOTENCY_VIOLATION으로 센다): `DOUBLE_CREATE`(replayed=false ACK가 2번 이상, order_id가 다름), `FIRST_REPLAYED`(처음 보내는 키에 replayed=true), `RESEND_DIFF`(뒤 ACK의 의미 필드가 원 응답과 다름, 또는 ACK 뒤 확정 거절), `409_SAME_BODY`(k6는 늘 같은 본문을 보내므로 409는 서비스 결함).
- 원문을 못 읽은 경우는 PASS만 막는 불확실성으로 센다(설계 5.7 규칙 4).

## 5. 응답 필드 매핑 (`integrity.response_fields`)

`{order_id: id, client_order_id, user_id, market, side, volume, status, fill_price, amount, filled_at}`(키 = 응답 필드, 값 = orders 열).
- 숫자는 Decimal로 비교한다(문자열 표기 차이 무시).
- `filled_at`은 시각으로 비교하되 **응답에 적힌 소수 자릿수까지만** DB 값을 내림해 비교한다. 코드는 `+00:00`·마이크로초, 통일본 예시는 `Z`·밀리초라 둘 다 읽는다. 못 읽으면 불일치가 아니라 불확실성이다.
- `replayed`와 `reason`은 비교 필드가 아니라 분류 입력이다. `response_fields_confirmed: true`(근거 `app/schemas.py`).

## 6. DB 스냅샷 번들

- `kubectl -n exchange exec <pod> [-c <container>] -- psql -X -q -v ON_ERROR_STOP=1 -U <user> -d <database> -f -`. 비밀번호를 넘기지 않는다(공식 이미지 로컬 소켓 trust 전제).
- `<pod>`, `<container>`, `<user>`, `<database>`는 run.json 선택 키 `db`에서 읽고, 기본값은 `postgres-0`, 없음, `exchange`, `exchange`다.
- 번들: `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TIME ZONE 'UTC';` → 지문 F1 → 구획 → 지문 F2 → `COMMIT`. 구획 표지 `--@` + `COPY … TO STDOUT WITH CSV HEADER`.
- 구획: accounts, positions, orders(전체), ticks(run 종목, run 창 ± stale_ms), schema(제약 검사용), 지문 `(orders 수, max(id), sum(krw), positions 수, sum(volume))`.

## 7. 불변식 — `status='filled'` 주문만 센다

| id | 내용 |
|---|---|
| `cash` | 모든 계좌: `krw + Σ bid amount − Σ ask amount = initial_krw` |
| `qty` | 모든 (user, market): `positions.volume(없으면 0) = Σ bid volume − Σ ask volume` |
| `avg` | 모든 (user, market): 주문을 id 순으로 서비스 공식대로 재생한 avg = positions.avg_price |
| `nonneg` | krw, volume, avg_price ≥ 0 |
| `arith` | `amount = ROUND_DOWN(fill_price × volume, 4)` |
| `constraints` | 필요한 테이블·열, `client_order_id` UNIQUE, CHECK 제약이 있다. 다르면 SCHEMA_DRIFT(INCONCLUSIVE). status CHECK는 기본 4값과 adapter 모드의 5값(`unknown` 추가)을 모두 받는다 |
| `fresh` | (선택) 같은 종목 tick 중 price = fill_price이고 received_at ∈ [filled_at − stale_ms − 1s, filled_at + 1s]인 것이 있다. ticks가 비면 평가하지 않는다 |

- `initial_krw`(기본 `"10000000"`, seed)와 `stale_ms`(기본 5000)는 기준 파일 `integrity`에서 바꾼다.
- cash·qty·avg·nonneg는 모든 계좌를, arith·fresh는 run 주문(과 발표자 계좌 주문)을 본다.

## 8. 가드

- `safety`: market ∈ `safety.markets`(기본 KRW-BTC), user_id가 `safety.user_pattern`(기본 `^user-[0-9]{4}$`)이고 번호가 1..`safety.user_max`(기본 1000), 키가 run 키 형식이고 64자 이하, 1 VU = 1 user, 기록에 token·authorization 키 없음.
- `account_set`: JSONL의 user_id가 모두 DB accounts에 있다. 계좌는 seed로 미리 있고 k6가 만들지 않는다.
- coverage(fill, sell = ask 체결 수, load), fault_in_load, fault_effect, db_quiescent(F1 == F2)는 설계 5.6대로다.

## 9. 판정 기준 YAML (`experiments/criteria/`)

- `default.yaml`: safety, integrity(initial_krw, stale_ms, response_fields), 클래스 묶음(lost = LOST + REJECT_COMMITTED, mismatch, duplicates, orphans, protocol), invariants(cash, qty, avg, nonneg, arith, constraints), `stale_fill`(fresh, 선택).
- `order-api-pod-kill.yaml`: SIGTERM 정상 종료. 기대 효과는 not_committed·after_503 관측.
- `order-api-crash-after-commit.yaml`: 서비스에 커밋 뒤 죽이는 장애 훅이 없어 지금은 쓸 수 없다(앱 팀 확인 필요).
- `demo-min.yaml`: 발표자 계좌는 run.json `extra_accounts`(user_id)다.

## 10. 테스트

- CI: `pytest -q -m "not pg and not k6 and not e2e"`(`orchestrator/`에서).
- `-m pg`: docker `postgres:16`에 `workloads/order/db/schema.sql`·`seed.sql`을 그대로 넣고, 위반을 하나씩 심어 불변식이 잡는지 본다.
- `-m e2e`: docker postgres:16 + redis:7과 실제 주문 서비스를 띄우고, k6와 같은 JSONL v2를 쓰는 파이썬 클라이언트로 주문을 보낸 뒤 판정한다. 정상이면 PASS, krw를 1원 바꾸면 FAIL(cash). 서비스 파이썬은 환경변수 `ORDER_SERVICE_PYTHON`으로 준다.

## 11. API 통일본과 코드의 차이 (앱 담당 확인 요청)

- 재시도 표: 503 세 가지는 같은 키로 재시도, 404·409·422는 재시도 안 함. k6 재전송 정책(2절)은 이 표를 따른다.
- `filled_at`: 통일본 예시 `…00.123Z`, 코드 `+00:00`·마이크로초. 형식 통일 요청. judge는 둘 다 읽는다(5절).
- 반올림: 통일본은 amount와 평균가 식만 적었다. 코드는 amount를 4자리 ROUND_DOWN, 평균가를 8자리 ROUND_HALF_EVEN으로 자른다. 통일본에 명시 요청. judge는 코드를 따른다.
- 주소: `http://order.<namespace>.svc.cluster.local:8000`. k6 대상은 이 서비스 8000 포트뿐이다.
- 경합: 기존 주문 조회가 잠금 밖이라 재전송이 앞 시도의 커밋 뒤 거절을 받을 수 있다(4절). 조회를 잠금 안으로 옮기거나 INSERT를 먼저 하도록 검토 요청.
- 지표: 종료 중 503은 `orders_total`을 세지 않고, 연결 오류 not_ready는 `order_duration_seconds`를 기록하지 않는다.

## 12. runner(A) 연동

- runner(`orchestrator/runjson.py`)가 쓴 run.json(`chaos.run/v1`)을 judge가 그대로 읽는다. `db`·`extra_accounts`는 선택 키다.
- runner는 `python -m orchestrator.judge run --run runs/<run_id>/run.json [--offline]`을 부르고 `orchestrator/judging.py`의 `accept`로 수용 조건 4개를 본다.
- `inputs.criteria_sha256`은 run.json `criteria`(또는 `--criteria`)가 가리키는 **기준 파일 원본 바이트**의 sha256이다. 증거 판정이 `criteria.resolved.yaml`을 읽어도 이 값은 원본 파일 값이다. 병합 결과의 해시는 `inputs.criteria_resolved_sha256`이다.
- `run`(오프라인 아님)은 수집(`collect`)이 1차 스텁이라 지금은 종료 코드 3으로 끝난다. 증거가 있는 run은 `--offline`으로 판정한다.

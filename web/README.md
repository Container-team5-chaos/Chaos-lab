# 데모 화면

카오스 실험을 시연할 때 쓰는 모의거래 화면이다. 실시간 시세 차트, 주문, 내 평가손익을 보여 주고, 장애가 나면 화면에서도 드러나게 한다.

| 기능 | 내용 |
| --- | --- |
| 실시간 시세 | WebSocket으로 받은 KRW-BTC 현재가와 최근 5분 차트. 색은 접속 시점 대비(상승 빨강, 하락 파랑) |
| 시세 신선도 | "N초 전 시세" 표시. 5초(주문 서비스 `STALE_MS`)를 넘으면 경고로 바뀌고, 주문이 `stale_price`로 거절될 수 있다고 알려 줌 |
| 주문 | 시장가 매수·매도. 503(`stale_price`, `price_unavailable`, `not_ready`)이나 응답을 못 받으면 **같은 주문번호로 다시 보내기** 버튼 → 중복 없이 재시도(멱등성 시연) |
| 내 평가손익 | `GET /accounts/{user_id}/positions`를 3초마다 읽어 주문 서비스가 계산한 평가금액·손익·수익률·총자산을 그대로 표시. 시세가 오래되면 서버가 null을 주므로 "-"로 표시 |
| 연결 상태 | 시세가 끊기면 무작위 지연을 섞어 재연결하고, 다시 연결되면 최신 시세부터 받음 |

## 구성

```
브라우저 ── nginx(web) ─┬─ /        index.html
                        ├─ /api/*   order:8000      (주문 서비스, D)
                        └─ /ws      push-server:8080 (푸시 서버, C)
                                       └─ Redis SUBSCRIBE price:KRW-BTC
```

- 같은 주소에서 API와 WebSocket을 함께 내보내므로 주문 서비스에 CORS 설정이 필요 없다.
- `web/deploy-local.sh`는 `workloads/push-server/deploy-local.sh`를 먼저 실행한다. 새 서버와 웹이 준비되면 이전 `dev-push` Deployment·Service를 제거한다.
- 푸시 서버의 연결·복구·메시지 계약은 [push-server 문서](../workloads/push-server/README.md)를 참고한다.
- 화면과 푸시 서버에는 `chaos.exchange/protected` 라벨을 붙여 기본 장애 주입 대상에서 뺀다.

푸시 메시지 형식 (Redis 값 + market):

```json
{"type": "price", "market": "KRW-BTC", "price": "95000000", "ts": 1762330000123}
```

## 사용법

```bash
infra/local/up.sh                    # 로컬 환경
workloads/order/deploy-local.sh      # 주문 서비스 (D)
bash web/deploy-local.sh             # 화면 + 정식 푸시 서버
kubectl --context kind-chaos-lab -n exchange port-forward svc/web 8080:80
# 브라우저: http://localhost:8080
```

사용자 번호(1~1000)는 화면 오른쪽 위에서 바꾼다. seed 계좌 `user-0001`~`user-1000`을 쓴다.

실제 업비트 시세로 전체 앱을 처음 띄울 때는 `PRICE_STUB=0 bash infra/local/up.sh --with-apps`를 쓴다.
앱만 검증할 때는 앞에 `LITE=1`을 붙일 수 있다. 이미 인프라·collector·주문 서비스가 실행 중이면 `bash web/deploy-local.sh`만 실행한다.

WebSocket은 `/ws?market=KRW-BTC`로 연결한다. `type=status` 메시지는 시세 대기/지연 표시로 처리하고,
가격과 시각이 유효한 `price` 메시지만 화면에 반영한다. 오래된 시각의 메시지는 무시하고,
차트는 1초마다 최신 값 하나를 남겨 최근 5분을 표시한다. 같은 밀리초의 가격 변경도 현재가·손익에 반영한다.

## 시연 예시

| 장애 | 화면에서 보이는 것 |
| --- | --- |
| 시세 stub이나 수집기 중단 | 5초 뒤 현재가 영역이 경고로 바뀜 → 주문이 `stale_price`로 거절 → 시세가 돌아온 뒤 같은 주문번호로 다시 보내면 체결 |
| 푸시 서버 Pod 종료 | "시세 재연결 N초 후" → 새 Pod가 뜨면 자동으로 다시 연결 |
| 주문 서비스 Pod 종료 | 주문이 실패하거나 응답이 없으면 같은 주문번호로 재전송, 이미 처리된 주문은 "이미 처리된 주문"으로 표시(잔고 변화 없음) |

#!/usr/bin/env bash
# 앱까지 띄운 환경 검증: up.sh --with-apps 로 전부 띄우고 수집기·주문·화면을 확인
#
# 사용법 (Chaos-lab 루트, Git Bash 또는 WSL):
#   bash infra/local/check-apps.sh
# 옵션 (환경 변수):
#   SKIP_UP=1      이미 띄운 클러스터에서 확인만 다시 실행
#   SKIP_STALE=1   수집기를 잠깐 멈춰 stale_price 거절을 보는 단계 건너뜀
#   SKIP_SMOKE=1   infra/local/smoke-test.sh 건너뜀
#   PRICE_STUB=1   업비트 접속이 막힌 곳에서 stub 시세로 실행 (up.sh에 그대로 전달)
#   LITE=1         자원이 부족할 때: Prometheus·Chaos Mesh 없이 띄움 (up.sh에 전달, smoke-test 자동 생략)
# 결과: ~/chaos-test/<시각>/ 에 로그와 result.txt
# 클러스터는 끝나도 지우지 않음 → 화면을 직접 볼 수 있음. 정리: bash infra/local/down.sh
# 테스트 주문이 DB에 남음 → 실험 전 bash infra/local/reset-db.sh -y
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CTX="kind-${CLUSTER_NAME}"
OUT="$HOME/chaos-test/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
RESULT="$OUT/result.txt"
USER_ID="user-0999"
WEB_PORT=18080

k() { kubectl --context "$CTX" "$@"; }
pass=0; fail=0; skip=0
ok()   { echo "PASS  $*" | tee -a "$RESULT"; pass=$((pass + 1)); }
bad()  { echo "FAIL  $*" | tee -a "$RESULT"; fail=$((fail + 1)); }
note() { echo "SKIP  $*" | tee -a "$RESULT"; skip=$((skip + 1)); }
step() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; echo "-- $*" >> "$RESULT"; }

PF_PIDS=()
cleanup() { for p in "${PF_PIDS[@]+"${PF_PIDS[@]}"}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT

port_forward() { # 서비스 로컬포트:원격포트
  k -n exchange port-forward "svc/$1" "$2" >"$OUT/pf-$1.log" 2>&1 &
  PF_PIDS+=($!)
  local port="${2%%:*}" _
  for _ in $(seq 1 20); do
    curl -s -o /dev/null --max-time 1 "http://localhost:$port/" && return 0
    sleep 1
  done
  return 1
}

redis_age_ms() {
  local s us cur ts
  read -r s us <<<"$(k -n exchange exec deploy/redis -- redis-cli TIME 2>/dev/null | tr -d '\r' | tr '\n' ' ')"
  cur="$(k -n exchange exec deploy/redis -- redis-cli GET price:KRW-BTC 2>/dev/null | tr -d '\r')"
  ts="$(printf '%s' "$cur" | sed -nE 's/.*"ts": *([0-9]+).*/\1/p')"
  [ -n "${s:-}" ] && [ -n "${us:-}" ] && [ -n "$ts" ] || return 1
  echo $((s * 1000 + us / 1000 - ts))
}

ticks_count() {
  k -n exchange exec statefulset/postgres -- psql -U exchange -d exchange -tAc \
    "SELECT count(*) FROM ticks" 2>/dev/null | tr -d '\r '
}

order() { # client_order_id → 응답 본문 + 마지막 줄에 HTTP 코드
  curl -s --max-time 10 -w '\n%{http_code}' -H 'Content-Type: application/json' \
    -d "{\"client_order_id\":\"$1\",\"user_id\":\"$USER_ID\",\"market\":\"KRW-BTC\",\"side\":\"bid\",\"volume\":\"0.0001\"}" \
    "http://localhost:$WEB_PORT/api/orders"
}

echo "레포: $ROOT" | tee "$RESULT"
echo "결과: $OUT" | tee -a "$RESULT"
[ "${LITE:-0}" = "1" ] && echo "모드: LITE (Prometheus·Chaos Mesh 없음)" | tee -a "$RESULT"

# ---------------------------------------------------------------- 0. 준비
step "0. 준비물"
[ -f "$ROOT/infra/local/up.sh" ] || { bad "infra/local/up.sh 없음"; exit 1; }
grep -q -- "--with-apps" "$ROOT/infra/local/up.sh" && ok "up.sh에 --with-apps 있음" || { bad "예전 up.sh (--with-apps 없음)"; exit 1; }
[ -f "$ROOT/web/deploy-local.sh" ] && ok "web/ 있음" || bad "web/deploy-local.sh 없음"
missing=()
for b in docker kind kubectl helm curl; do command -v "$b" >/dev/null 2>&1 || missing+=("$b"); done
[ ${#missing[@]} -eq 0 ] && ok "도구 설치됨" || { bad "설치 필요: ${missing[*]}"; exit 1; }
docker info >/dev/null 2>&1 && ok "Docker 실행 중" || { bad "Docker Desktop을 켜세요"; exit 1; }

# ---------------------------------------------------------------- 1. 띄우기
step "1. up.sh --with-apps (로그: up.log, 처음엔 10~20분, LITE면 더 짧음)"
if [ "${SKIP_UP:-0}" = "1" ]; then
  note "SKIP_UP=1"
else
  start=$(date +%s)
  if bash "$ROOT/infra/local/up.sh" --with-apps 2>&1 | tee "$OUT/up.log"; then
    ok "up.sh --with-apps 성공 ($(( $(date +%s) - start ))초)"
  else
    bad "up.sh --with-apps 실패 — up.log 마지막 부분 확인"
    k get pods -A > "$OUT/pods.txt" 2>&1
    exit 1
  fi
  grep -q "ticks 최근 30초" "$OUT/up.log" && ok "up.sh가 ticks 기록 확인" || bad "up.sh가 ticks 경고를 냄"
fi
k get pods -A -o wide > "$OUT/pods.txt" 2>&1

# ---------------------------------------------------------------- 2. Pod
step "2. 앱 Pod"
for app in collector order dev-push web; do
  if k -n exchange wait --for=condition=Ready pod -l "app=$app" --timeout=60s >/dev/null 2>&1; then
    ok "$app Ready ($(k -n exchange get pods -l "app=$app" --no-headers 2>/dev/null | wc -l | tr -d ' ')개)"
  else
    bad "$app Ready 아님"
    k -n exchange logs -l "app=$app" --all-containers --tail=50 > "$OUT/log-$app.txt" 2>&1
  fi
done
k -n exchange get deploy price-stub >/dev/null 2>&1 && echo "      (price-stub 있음: PRICE_STUB=1 실행)" | tee -a "$RESULT"

# ---------------------------------------------------------------- 3. 시세
step "3. 시세 (수집기 → Redis, ticks)"
age="$(redis_age_ms)" && [ "$age" -lt 5000 ] && ok "Redis 시세 ${age}ms 전 갱신" || bad "Redis 시세가 5초보다 오래됨 (${age:-없음})"
t1="$(ticks_count)"; sleep 5; t2="$(ticks_count)"
if [ -n "$t1" ] && [ -n "$t2" ] && [ "$t2" -gt "$t1" ]; then
  ok "ticks 증가 ($t1 → $t2, 5초)"
else
  bad "ticks가 늘지 않음 ($t1 → $t2) — B의 fresh 판정이 NOT_EVALUATED가 됨"
  k -n exchange logs deploy/collector -c collector --tail=50 > "$OUT/log-collector.txt" 2>&1
fi

# ---------------------------------------------------------------- 4. 화면 (nginx 경유)
step "4. 화면 nginx (localhost:$WEB_PORT)"
if port_forward web "$WEB_PORT:80"; then
  [ "$(curl -s --max-time 3 "http://localhost:$WEB_PORT/healthz")" = "ok" ] && ok "/healthz" || bad "/healthz"
  curl -s --max-time 3 "http://localhost:$WEB_PORT/" | grep -qi "<html" && ok "/ 에서 index.html" || bad "/ 응답이 HTML 아님"
  acc="$(curl -s --max-time 5 "http://localhost:$WEB_PORT/api/accounts/$USER_ID")"
  echo "$acc" > "$OUT/account-before.json"
  echo "$acc" | grep -q '"user_id"' && ok "/api → 주문 서비스 계좌 조회" || bad "/api/accounts 실패: $acc"
  # WebSocket: 101 응답과 첫 시세 메시지
  ws="$(curl -s -i -N --http1.1 --max-time 4 \
    -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
    -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
    "http://localhost:$WEB_PORT/ws" 2>/dev/null | tr -d '\0' | head -c 2000)"
  echo "$ws" > "$OUT/ws.txt"
  echo "$ws" | grep -a -q " 101" && ok "/ws → 푸시 서버 WebSocket 연결 (101)" || bad "/ws 연결 실패 (ws.txt)"
  echo "$ws" | grep -a -q '"market": *"KRW-BTC"' && ok "/ws 첫 시세 메시지 수신" || bad "/ws 시세 메시지 없음"
else
  bad "web port-forward 실패"
fi

# ---------------------------------------------------------------- 5. 주문
step "5. 주문 (nginx 경유, $USER_ID, 0.0001 BTC 매수)"
cid="test-$(date +%s)"
r="$(order "$cid")"; code="${r##*$'\n'}"; body="${r%$'\n'*}"
echo "$body" > "$OUT/order.json"
[ "$code" = "200" ] && echo "$body" | grep -q '"filled"' && ok "주문 체결 (200)" || bad "주문 실패 ($code): $body"
r="$(order "$cid")"; code="${r##*$'\n'}"; body="${r%$'\n'*}"
echo "$body" | grep -q '"replayed": *true' && ok "같은 주문번호 재전송 → replayed" || bad "재전송 응답 ($code): $body"
acc="$(curl -s --max-time 5 "http://localhost:$WEB_PORT/api/accounts/$USER_ID")"
echo "$acc" > "$OUT/account-after.json"
echo "$acc" | grep -q 'KRW-BTC' && ok "계좌에 BTC 포지션" || bad "계좌에 포지션 없음: $acc"

# ---------------------------------------------------------------- 6. 시세 끊김
step "6. 시세 끊김 → stale_price 거절 → 복구"
if [ "${SKIP_STALE:-0}" = "1" ]; then
  note "SKIP_STALE=1"
elif k -n exchange get deploy price-stub >/dev/null 2>&1; then
  note "price-stub이 있어 수집기만 멈춰도 시세가 계속될 수 있음"
else
  k -n exchange scale deploy/collector --replicas=0 >/dev/null
  k -n exchange wait --for=delete pod -l app=collector --timeout=60s >/dev/null 2>&1
  sleep 6
  r="$(order "stale-$(date +%s)")"; code="${r##*$'\n'}"; body="${r%$'\n'*}"
  [ "$code" = "503" ] && echo "$body" | grep -q stale_price && ok "시세 6초 끊김 → 503 stale_price" || bad "stale 거절 안 됨 ($code): $body"
  k -n exchange scale deploy/collector --replicas=1 >/dev/null
  k -n exchange rollout status deploy/collector --timeout=180s >/dev/null 2>&1
  for _ in $(seq 1 30); do age="$(redis_age_ms)" && [ "$age" -lt 5000 ] && break; age=""; sleep 2; done
  [ -n "$age" ] && ok "수집기 복구 후 시세 다시 신선함" || bad "수집기 복구 후에도 시세 없음"
  r="$(order "after-$(date +%s)")"; code="${r##*$'\n'}"
  [ "$code" = "200" ] && ok "복구 후 주문 체결" || bad "복구 후 주문 실패 ($code)"
fi

# ---------------------------------------------------------------- 7. 기존 스모크 테스트
step "7. infra/local/smoke-test.sh (로그: smoke.log)"
if [ "${SKIP_SMOKE:-0}" = "1" ]; then
  note "SKIP_SMOKE=1"
elif [ "${LITE:-0}" = "1" ]; then
  note "LITE=1 — Prometheus·Chaos Mesh가 없어 smoke-test 생략"
elif bash "$ROOT/infra/local/smoke-test.sh" > "$OUT/smoke.log" 2>&1; then
  ok "smoke-test 통과"
else
  bad "smoke-test 실패 — smoke.log 확인"
fi

# ---------------------------------------------------------------- 결과
k get pods -A -o wide > "$OUT/pods-end.txt" 2>&1
{
  echo
  echo "합계: PASS $pass / FAIL $fail / SKIP $skip"
} | tee -a "$RESULT"
cat <<EOF

결과 폴더: $OUT   (result.txt를 보내 주세요. FAIL이 있으면 폴더 전체를 압축해서)

화면 직접 보기:
  kubectl --context $CTX -n exchange port-forward svc/web 8080:80
  브라우저 http://localhost:8080
정리: bash infra/local/down.sh
EOF
[ "$fail" -eq 0 ]

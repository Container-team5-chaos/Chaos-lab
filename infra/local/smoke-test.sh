#!/usr/bin/env bash
# 로컬 환경 점검 + 장애 주입 스모크 테스트
#
#   1. 환경: 노드, Prometheus, Chaos Mesh, metrics-server(HPA)
#   2. 데이터: DB 스키마·계좌, Redis 시세(형식·신선도·갱신·PUBLISH)
#   3. Prometheus: 쿼리 가능, k6 remote write receiver 켜짐
#   4. PodChaos: Pod 1개만 정확히 종료, 같은 이름 재생성 시 중복 0, 정리 멱등
#   5. 보호 라벨: chaos.exchange/protected Pod는 주입 대상에서 제외
#
# 사용법: infra/local/up.sh 후 infra/local/smoke-test.sh
#   SKIP_MONITORING=1  Prometheus·Grafana 검사 건너뜀 (자원이 부족해 설치하지 않은 경우)
#   SKIP_DB=1          PostgreSQL 검사 건너뜀
# 테스트용 리소스는 실행마다 새 Namespace(smoke-<시각>)에 만들고 끝나면 지운다.
set -uo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash가 /api/... 인자를 Windows 경로로 바꾸지 않게

CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
LABEL_KEY="chaos/run-id" # orchestrator(A)의 inject.LABEL_KEY와 같아야 함
RUN_ID="smoke-$(date +%s)"
SMOKE_NS="$RUN_ID" # 실행마다 새 Namespace → 이전 실행의 Terminating과 충돌하지 않음
SKIP_MONITORING="${SKIP_MONITORING:-0}"
SKIP_DB="${SKIP_DB:-0}"
PROM_SVC="kps-kube-prometheus-stack-prometheus:http-web"

k() { kubectl --context "$CONTEXT" "$@"; }

PASSED=0
FAILED=0
pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; PASSED=$((PASSED + 1)); }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAILED=$((FAILED + 1)); }
SKIPPED=0
skip() { printf '  \033[33mSKIP\033[0m %s\n' "$1"; SKIPPED=$((SKIPPED + 1)); }
section() { printf '\n\033[1m[%s]\033[0m\n' "$1"; }
check() { # check "설명" 명령...
  local desc="$1"
  shift
  if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}

cleanup_smoke() {
  k delete podchaos -A -l "$LABEL_KEY=$RUN_ID" --ignore-not-found >/dev/null 2>&1
  k delete namespace "$SMOKE_NS" --ignore-not-found --wait=false >/dev/null 2>&1
}
trap cleanup_smoke EXIT

# ---------------------------------------------------------------------------
section "1. 환경"
check "클러스터 접속 ($CONTEXT)" k get nodes
check "모든 노드 Ready" k wait --for=condition=Ready nodes --all --timeout=10s
if [ "$SKIP_MONITORING" = "1" ]; then
  skip "Prometheus·Grafana (SKIP_MONITORING=1)"
else
  check "Prometheus Ready" k -n monitoring wait --for=condition=Ready pod -l app.kubernetes.io/name=prometheus --timeout=10s
  check "Grafana Ready" k -n monitoring wait --for=condition=Ready pod -l app.kubernetes.io/name=grafana --timeout=10s
fi
check "Chaos Mesh controller Ready" k -n chaos-mesh wait --for=condition=Ready pod -l app.kubernetes.io/component=controller-manager --timeout=10s
check "chaos-daemon 전체 노드 배치" k -n chaos-mesh rollout status daemonset/chaos-daemon --timeout=10s
check "PodChaos·NetworkChaos CRD" k get crd podchaos.chaos-mesh.org networkchaos.chaos-mesh.org
check "metrics API Available (HPA가 CPU를 읽음)" k wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout=10s
check "kubectl top nodes 응답" k top nodes

# ---------------------------------------------------------------------------
section "2. 데이터"
psql_q() { k -n exchange exec statefulset/postgres -- psql -U exchange -d exchange -tAc "$1" 2>/dev/null | tr -d '\r[:space:]'; }

if [ "$SKIP_DB" = "1" ]; then
  skip "DB 테이블·계좌 (SKIP_DB=1)"
else
  tables="$(psql_q "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('accounts','positions','orders','ticks')")"
  if [ "$tables" = "4" ]; then pass "DB 테이블 4개 (accounts, positions, orders, ticks)"; else fail "DB 테이블 4개 (현재 ${tables:-0}개) → infra/local/reset-db.sh"; fi

  accounts="$(psql_q "SELECT count(*) FROM accounts")"
  if [ "$accounts" = "1000" ]; then pass "테스트 계좌 1,000개"; else fail "테스트 계좌 1,000개 (현재 ${accounts:-0}개) → infra/local/reset-db.sh"; fi
fi

redis_q() { k -n exchange exec deploy/redis -- redis-cli "$@" 2>/dev/null | tr -d '\r'; }
price_ts() { redis_q GET price:KRW-BTC | sed -nE 's/.*"ts": *([0-9]+).*/\1/p'; }

raw="$(redis_q GET price:KRW-BTC)"
if [ -z "$raw" ]; then
  fail "Redis price:KRW-BTC 존재 (없음 → 시세 stub 또는 수집기 확인)"
else
  pass "Redis price:KRW-BTC 존재: $raw"
  if printf '%s' "$raw" | grep -Eq '"price": *"[0-9.]+"'; then pass "price가 문자열 (API 문서 7절)"; else fail "price가 문자열 (API 문서 7절)"; fi
  if k -n exchange get deployment price-stub >/dev/null 2>&1; then
    printf '  \033[33mINFO\033[0m 시세 stub 배포됨 (수집기 배포 후에는 PRICE_STUB=0)\n'
  fi

  ts1="$(price_ts)"
  now_s="$(redis_q TIME | head -1)"
  if [ -n "$ts1" ] && [ -n "$now_s" ]; then
    age=$((now_s * 1000 - ts1))
    if [ "$age" -lt 5000 ]; then pass "시세 신선도 ${age}ms < STALE_MS 5000ms"; else fail "시세 신선도 ${age}ms ≥ 5000ms (주문이 stale_price로 거절됨)"; fi
  else
    fail "시세 신선도 계산 (ts 파싱 실패)"
  fi

  sleep 3
  ts2="$(price_ts)"
  if [ -n "$ts2" ] && [ -n "$ts1" ] && [ "$ts2" -gt "$ts1" ]; then pass "시세가 계속 갱신됨"; else fail "시세가 계속 갱신됨 (3초 동안 ts 변화 없음)"; fi

  # 푸시 서버가 구독하는 채널로도 나가는지 (3초 동안 구독)
  published="$(k -n exchange exec deploy/redis -- timeout 3 redis-cli SUBSCRIBE price:KRW-BTC 2>/dev/null | tr -d '\r' | grep -c '"ts"')"
  if [ "${published:-0}" -ge 1 ]; then pass "PUBLISH price:KRW-BTC 수신 (3초 동안 ${published}건)"; else fail "PUBLISH price:KRW-BTC 수신 (푸시 서버가 시세를 못 받음)"; fi
fi

# ---------------------------------------------------------------------------
section "3. Prometheus"
prom_get() { k get --raw "/api/v1/namespaces/monitoring/services/$PROM_SVC/proxy$1" 2>/dev/null; }

if [ "$SKIP_MONITORING" = "1" ]; then
  skip "PromQL 쿼리·remote write receiver (SKIP_MONITORING=1)"
else
  if prom_get "/api/v1/query?query=up" | grep -q '"status":"success"'; then pass "PromQL 쿼리 (up)"; else fail "PromQL 쿼리 (up)"; fi
  if prom_get "/api/v1/status/flags" | grep -q '"web.enable-remote-write-receiver":"true"'; then
    pass "k6 remote write receiver 켜짐"
  else
    fail "k6 remote write receiver 켜짐 (values의 enableRemoteWriteReceiver 확인)"
  fi
fi

# ---------------------------------------------------------------------------
section "4. PodChaos: Pod 1개만 종료 + 중복 0 + 정리 멱등"
k create namespace "$SMOKE_NS" >/dev/null 2>&1
k -n "$SMOKE_NS" apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: target
spec:
  replicas: 3
  selector:
    matchLabels: {app: smoke-target}
  template:
    metadata:
      labels: {app: smoke-target}
    spec:
      terminationGracePeriodSeconds: 0
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.10
---
apiVersion: v1
kind: Pod
metadata:
  name: protected
  labels: {app: smoke-target, chaos.exchange/protected: "true"}
spec:
  terminationGracePeriodSeconds: 0
  containers:
    - name: pause
      image: registry.k8s.io/pause:3.10
EOF

if ! k -n "$SMOKE_NS" rollout status deployment/target --timeout=120s >/dev/null 2>&1 ||
  ! k -n "$SMOKE_NS" wait --for=condition=Ready pod/protected --timeout=60s >/dev/null 2>&1; then
  fail "테스트 대상 Pod 준비 (이미지 pull 실패 가능)"
else
  target_uids() { k -n "$SMOKE_NS" get pods -l app=smoke-target,'!chaos.exchange/protected' -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | tr -d '\r' | sort; }
  before="$(target_uids)"
  protected_uid="$(k -n "$SMOKE_NS" get pod protected -o jsonpath='{.metadata.uid}')"

  # orchestrator의 chaos_name(run_id, 0)과 같은 규칙
  name="${RUN_ID}-s0"
  podchaos() {
    local chaos_name="$1" mode="$2"
    cat <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: $chaos_name
  namespace: $SMOKE_NS
  labels: {$LABEL_KEY: $RUN_ID}
spec:
  action: pod-kill
  mode: $mode
  gracePeriod: 0
  selector:
    namespaces: [$SMOKE_NS]
    labelSelectors: {app: smoke-target}
    expressionSelectors:
      - {key: chaos.exchange/protected, operator: DoesNotExist}
      - {key: app.kubernetes.io/name, operator: NotIn, values: [k6]}
EOF
  }

  podchaos "$name" one | k create -f - >/dev/null 2>&1
  second="$(podchaos "$name" one | k create -f - 2>&1)"
  count="$(k get podchaos -A -l "$LABEL_KEY=$RUN_ID" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "$count" = "1" ] && printf '%s' "$second" | grep -q AlreadyExists; then
    pass "같은 이름으로 두 번 생성 → AlreadyExists, PodChaos 1개"
  else
    fail "같은 이름으로 두 번 생성 → PodChaos ${count}개"
  fi

  # 원래 Pod 중 몇 개가 사라졌는지 최대 60초 관찰
  killed=0
  for _ in $(seq 1 30); do
    now="$(target_uids)"
    killed="$(comm -23 <(printf '%s\n' "$before") <(printf '%s\n' "$now") | grep -c .)"
    [ "$killed" -ge 1 ] && break
    sleep 2
  done
  sleep 5 # 추가로 죽는 Pod가 없는지 잠시 더 확인
  now="$(target_uids)"
  killed="$(comm -23 <(printf '%s\n' "$before") <(printf '%s\n' "$now") | grep -c .)"
  if [ "$killed" = "1" ]; then pass "mode: one → 정확히 Pod 1개 종료"; else fail "mode: one → 종료된 Pod ${killed}개 (기대 1개)"; fi

  # 라벨로 찾아 지우는 정리 경로 확인. orchestrator 주입기(ChaosMeshInjector.cleanup) 자체의 멱등성은 별도 테스트 대상
  check "라벨(chaos/run-id)로 찾아 정리" k delete podchaos -A -l "$LABEL_KEY=$RUN_ID"
  check "정리 재실행 시 오류 없음" k delete podchaos -A -l "$LABEL_KEY=$RUN_ID"
  left="$(k get podchaos -A -l "$LABEL_KEY=$RUN_ID" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "$left" = "0" ]; then pass "정리 후 남은 PodChaos 0개"; else fail "정리 후 남은 PodChaos ${left}개"; fi

  # -------------------------------------------------------------------------
  section "5. 보호 라벨 제외"
  k -n "$SMOKE_NS" rollout status deployment/target --timeout=60s >/dev/null 2>&1
  podchaos "${RUN_ID}-s1" all | k create -f - >/dev/null 2>&1
  sleep 10
  now_protected="$(k -n "$SMOKE_NS" get pod protected -o jsonpath='{.metadata.uid}' 2>/dev/null)"
  if [ -n "$protected_uid" ] && [ "$now_protected" = "$protected_uid" ]; then
    pass "mode: all이어도 protected Pod는 살아 있음"
  else
    fail "protected Pod가 종료됨 (선택자 확인)"
  fi
  k delete podchaos -A -l "$LABEL_KEY=$RUN_ID" --ignore-not-found >/dev/null 2>&1
fi

# ---------------------------------------------------------------------------
printf '\n결과: \033[32m%d PASS\033[0m, \033[31m%d FAIL\033[0m, \033[33m%d SKIP\033[0m\n' "$PASSED" "$FAILED" "$SKIPPED"
[ "$FAILED" -eq 0 ]

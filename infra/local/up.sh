#!/usr/bin/env bash
# 로컬 실험 환경을 한 번에 띄움: kind + Prometheus/Grafana + Chaos Mesh + PostgreSQL + Redis
# --with-apps 를 주면 수집기(C)·주문 서비스(D)·데모 화면(E)까지 빌드해서 배포
# 여러 번 실행해도 같은 결과가 되도록 작성 (이미 있는 것은 건너뛰거나 upgrade)
#
# 사용법:
#   infra/local/up.sh               # 인프라만 (시세는 stub)
#   infra/local/up.sh --with-apps   # 인프라 + 앱 (시세는 실제 수집기)
#   LITE=1 infra/local/up.sh --with-apps   # 자원이 부족할 때: Prometheus·Grafana·Chaos Mesh 없이 앱만 확인
# 환경 변수:
#   CLUSTER_NAME  기본 chaos-lab
#   LITE          1이면 Prometheus·Grafana·Chaos Mesh·metrics-server를 설치하지 않음 (장애 주입·지표 수집 불가)
#   PRICE_STUB    1이면 시세 stub 배포, 0이면 제거. 기본: 인프라만 1, --with-apps 0
#   ORDER_MODE    baseline 이면 주문 서비스를 개선 전 설정으로 배포 (--with-apps 일 때)
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# 버전 고정: 바꿀 때는 values 파일 상단 주석도 함께 수정
KPS_CHART_VERSION="92.21.1"
CHAOS_MESH_CHART_VERSION="2.8.7"
METRICS_SERVER_CHART_VERSION="3.14.0"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

WITH_APPS="${WITH_APPS:-0}"
for arg in "$@"; do
  case "$arg" in
    --with-apps) WITH_APPS=1 ;;
    -h | --help)
      sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) die "알 수 없는 옵션: $arg (--with-apps, --help)" ;;
  esac
done

# 앱을 띄우면 실제 수집기가 시세를 쓰므로 stub은 기본으로 끔
# (수집기가 업비트에 못 붙는 환경이면 PRICE_STUB=1 로 함께 띄움 → 수집기 시세가 오면 stub이 스스로 멈춤)
if [ "$WITH_APPS" = "1" ]; then
  PRICE_STUB="${PRICE_STUB:-0}"
else
  PRICE_STUB="${PRICE_STUB:-1}"
fi
ORDER_MODE="${ORDER_MODE:-}"
LITE="${LITE:-0}"

# kubectl/helm 이 다른 클러스터를 건드리지 않도록 context 명시
k() { kubectl --context "$CONTEXT" "$@"; }
h() { helm --kube-context "$CONTEXT" "$@"; }

check_deps() {
  log "필수 도구 확인"
  local missing=()
  for bin in docker kind kubectl helm; do
    command -v "$bin" >/dev/null 2>&1 || missing+=("$bin")
  done
  [ ${#missing[@]} -eq 0 ] || die "설치 필요: ${missing[*]}"
  docker info >/dev/null 2>&1 || die "Docker 데몬이 실행 중이 아닙니다"
}

create_cluster() {
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    log "kind 클러스터 '$CLUSTER_NAME' 이미 존재 — 생성 건너뜀"
  else
    log "kind 클러스터 '$CLUSTER_NAME' 생성"
    kind create cluster --name "$CLUSTER_NAME" --config "$ROOT/infra/local/kind-config.yaml" --wait 120s
  fi
  k wait --for=condition=Ready nodes --all --timeout=120s
}

add_repos() {
  log "Helm 저장소 등록"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update >/dev/null
  helm repo add chaos-mesh https://charts.chaos-mesh.org --force-update >/dev/null
  helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ --force-update >/dev/null
  helm repo update prometheus-community chaos-mesh metrics-server >/dev/null
}

# HPA가 CPU를 읽기 위한 metrics-server (없으면 HPA TARGETS가 <unknown>)
install_metrics_server() {
  log "metrics-server $METRICS_SERVER_CHART_VERSION 설치 (namespace: kube-system)"
  h upgrade --install metrics-server metrics-server/metrics-server \
    --namespace kube-system \
    --version "$METRICS_SERVER_CHART_VERSION" \
    --values "$ROOT/infra/helm/values/metrics-server.yaml" \
    --wait --timeout 5m
}

install_monitoring() {
  log "kube-prometheus-stack $KPS_CHART_VERSION 설치 (namespace: monitoring)"
  h upgrade --install kps prometheus-community/kube-prometheus-stack \
    --namespace monitoring --create-namespace \
    --version "$KPS_CHART_VERSION" \
    --values "$ROOT/infra/helm/values/kube-prometheus-stack.yaml" \
    --wait --timeout 10m
}

# LITE 모드: Prometheus 없이 ServiceMonitor CRD만 등록 (Pod 없음)
# 주문 서비스 차트(D)가 ServiceMonitor를 만들기 때문에 CRD가 없으면 설치가 실패함
# 같은 버전 차트의 CRD를 쓰므로 나중에 LITE 없이 다시 실행해도 충돌하지 않음
install_servicemonitor_crd() {
  log "LITE=1 — Prometheus·Grafana·Chaos Mesh·metrics-server 생략, ServiceMonitor CRD만 등록"
  local tmp
  tmp="$(mktemp -d)"
  helm pull prometheus-community/kube-prometheus-stack --version "$KPS_CHART_VERSION" --untar --untardir "$tmp"
  k apply --server-side -f "$tmp/kube-prometheus-stack/charts/crds/crds/crd-servicemonitors.yaml"
  rm -rf "$tmp"
  k wait --for=condition=Established crd/servicemonitors.monitoring.coreos.com --timeout=60s
}

install_chaos_mesh() {
  log "Chaos Mesh $CHAOS_MESH_CHART_VERSION 설치 (namespace: chaos-mesh)"
  h upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
    --namespace chaos-mesh --create-namespace \
    --version "$CHAOS_MESH_CHART_VERSION" \
    --values "$ROOT/infra/helm/values/chaos-mesh.yaml" \
    --wait --timeout 10m
}

install_data() {
  log "PostgreSQL, Redis 배포 (namespace: exchange)"
  # 주문 서비스 스키마와 시드를 PostgreSQL 첫 기동 때 적용 (workloads/order/db)
  log "주문 서비스 스키마 ConfigMap 생성"
  k apply -f "$ROOT/infra/k8s/data/00-namespace.yaml"
  k -n exchange create configmap postgres-init \
    --from-file=01-schema.sql="$ROOT/workloads/order/db/schema.sql" \
    --from-file=02-seed.sql="$ROOT/workloads/order/db/seed.sql" \
    --dry-run=client -o yaml | k apply -f -
  k apply -f "$ROOT/infra/k8s/data/"
  k -n exchange rollout status statefulset/postgres --timeout=180s
  k -n exchange rollout status deployment/redis --timeout=120s
}

install_price_stub() {
  if [ "$PRICE_STUB" = "1" ]; then
    log "시세 stub 배포 (price:KRW-BTC 1초마다 갱신)"
    k apply -f "$ROOT/infra/k8s/stubs/price-stub.yaml"
    k -n exchange rollout status deployment/price-stub --timeout=120s
  else
    log "PRICE_STUB=0 — 시세 stub 제거"
    k delete -f "$ROOT/infra/k8s/stubs/price-stub.yaml" --ignore-not-found
  fi
}

verify() {
  log "설치 확인"
  local failed=0
  k -n exchange exec statefulset/postgres -- pg_isready -U exchange -d exchange >/dev/null || failed=1
  [ "$(k -n exchange exec deploy/redis -- redis-cli ping)" = "PONG" ] || failed=1
  if [ "$LITE" = "1" ]; then
    k get crd servicemonitors.monitoring.coreos.com >/dev/null || failed=1
    [ "$failed" -eq 0 ] || die "일부 구성 요소가 준비되지 않았습니다. 위 로그를 확인하세요"
    return
  fi
  # 컨트롤러가 Ready인지 확인
  k -n monitoring wait --for=condition=Ready pod -l app.kubernetes.io/name=prometheus --timeout=120s || failed=1
  k -n chaos-mesh wait --for=condition=Ready pod -l app.kubernetes.io/component=controller-manager --timeout=120s || failed=1
  k -n chaos-mesh rollout status daemonset/chaos-daemon --timeout=120s || failed=1
  # Chaos Mesh CRD가 등록됐는지 확인(주입기가 PodChaos를 만들 수 있어야 함)
  k get crd podchaos.chaos-mesh.org networkchaos.chaos-mesh.org >/dev/null || failed=1
  # HPA용 리소스 지표 API가 응답하는지 (metrics-server 기동 후 첫 수집까지 수십 초 걸림)
  k wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout=120s >/dev/null || failed=1
  [ "$failed" -eq 0 ] || die "일부 구성 요소가 준비되지 않았습니다. 위 로그를 확인하세요"
}

# 수집기(C): 이미지 빌드 → kind 노드에 로드 → 배포 (절차는 workloads/collector/README.md와 같음)
deploy_collector() {
  log "수집기 이미지 빌드: chaos-lab-collector:dev"
  docker build -t chaos-lab-collector:dev "$ROOT/workloads/collector"
  kind load docker-image chaos-lab-collector:dev --name "$CLUSTER_NAME"
  log "수집기 배포 (namespace: exchange)"
  k apply -f "$ROOT/infra/k8s/collector/"
  # 같은 dev 태그로 다시 빌드했어도 새 이미지를 쓰도록 재시작
  k -n exchange rollout restart deployment/collector
  k -n exchange rollout status deployment/collector --timeout=180s
}

# 주문 서비스(D): D의 배포 스크립트를 그대로 사용
deploy_order() {
  log "주문 서비스 배포 (workloads/order/deploy-local.sh${ORDER_MODE:+ $ORDER_MODE})"
  local args=()
  [ "$ORDER_MODE" = "baseline" ] && args=(baseline)
  CLUSTER_NAME="$CLUSTER_NAME" bash "$ROOT/workloads/order/deploy-local.sh" "${args[@]+"${args[@]}"}"
}

# 데모 화면(E): web/이 아직 없는 브랜치에서는 건너뜀
deploy_web() {
  if [ -f "$ROOT/web/deploy-local.sh" ]; then
    log "데모 화면 배포 (web/deploy-local.sh)"
    CLUSTER_NAME="$CLUSTER_NAME" bash "$ROOT/web/deploy-local.sh"
  else
    log "web/deploy-local.sh 없음 — 데모 화면 배포 건너뜀"
  fi
}

# Redis 시세가 5초 이내로 갱신되는지 (주문 서비스 STALE_MS와 같은 기준)
price_age_ms() {
  local now_s now_us cur ts
  read -r now_s now_us <<<"$(k -n exchange exec deploy/redis -- redis-cli TIME | tr -d '\r' | tr '\n' ' ')"
  cur="$(k -n exchange exec deploy/redis -- redis-cli GET price:KRW-BTC | tr -d '\r')"
  ts="$(printf '%s' "$cur" | sed -nE 's/.*"ts": *([0-9]+).*/\1/p')"
  [ -n "${now_s:-}" ] && [ -n "${now_us:-}" ] && [ -n "$ts" ] || return 1
  echo $(( now_s * 1000 + now_us / 1000 - ts ))
}

verify_apps() {
  log "앱 확인"
  local age=""
  for _ in $(seq 1 30); do
    age="$(price_age_ms)" && [ "$age" -lt 5000 ] && break
    age=""
    sleep 2
  done
  if [ -z "$age" ]; then
    k -n exchange logs deployment/collector -c collector --tail=20 || true
    die "60초 동안 신선한 시세(5초 이내)가 없습니다. 수집기가 업비트에 접속하지 못하면 PRICE_STUB=1 $0 --with-apps 로 다시 실행하세요"
  fi
  echo "시세 신선함: ${age}ms 전에 갱신"

  # B의 판정기는 ticks로 체결가의 신선함(fresh)을 확인한다. 비어 있으면 판정 불가로 빠질 뿐이라 경고만 함
  local n
  n="$(k -n exchange exec statefulset/postgres -- psql -U exchange -d exchange -tAc \
    "SELECT count(*) FROM ticks WHERE received_at > now() - interval '30 seconds'" 2>/dev/null | tr -d '\r ')" || n=""
  if [ "${n:-0}" -gt 0 ] 2>/dev/null; then
    echo "ticks 최근 30초: ${n}건"
  else
    warn "ticks에 최근 30초 시세가 없습니다. 수집기 로그를 확인하세요 (B의 fresh 판정은 NOT_EVALUATED가 됨)"
  fi

  k -n exchange get pods -l 'app in (collector, order, web, push-server)'
}

summary() {
  if [ "$LITE" = "1" ]; then
    cat <<EOF

로컬 환경 준비 완료 — LITE 모드  (context: $CONTEXT)
  Prometheus·Grafana·Chaos Mesh 없음 → 장애 주입·지표 수집은 LITE 없이 다시 실행

  내 PC에서 열기 (각각 다른 터미널에서)
    kubectl --context $CONTEXT -n exchange port-forward svc/postgres 5432
EOF
  else
    cat <<EOF

로컬 환경 준비 완료  (context: $CONTEXT)

  클러스터 내부 주소
    PostgreSQL  postgres.exchange.svc.cluster.local:5432   (exchange / exchange, db=exchange)
    Redis       redis.exchange.svc.cluster.local:6379
    Prometheus  kps-kube-prometheus-stack-prometheus.monitoring.svc:9090
                k6 remote write: http://kps-kube-prometheus-stack-prometheus.monitoring.svc:9090/api/v1/write

  내 PC에서 열기 (각각 다른 터미널에서)
    kubectl --context $CONTEXT -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090
    kubectl --context $CONTEXT -n monitoring port-forward svc/kps-grafana 3000:80        # admin / admin
    kubectl --context $CONTEXT -n chaos-mesh port-forward svc/chaos-dashboard 2333
    kubectl --context $CONTEXT -n exchange port-forward svc/postgres 5432
EOF
  fi
  if [ "$WITH_APPS" = "1" ]; then
    cat <<EOF
    kubectl --context $CONTEXT -n exchange port-forward svc/order 8000           # 주문 API
EOF
    [ -f "$ROOT/web/deploy-local.sh" ] && cat <<EOF
    kubectl --context $CONTEXT -n exchange port-forward svc/web 8080:80          # 데모 화면 http://localhost:8080
EOF
  fi
  cat <<EOF

  시세: $([ "$PRICE_STUB" = "1" ] && echo "stub (수집기 시세가 오면 stub이 스스로 멈춤)" || echo "수집기 (stub 없음)")
  삭제: infra/local/down.sh
EOF
}

check_deps
create_cluster
add_repos
if [ "$LITE" = "1" ]; then
  install_servicemonitor_crd
else
  install_metrics_server
  install_monitoring
  install_chaos_mesh
fi
install_data
install_price_stub
if [ "$WITH_APPS" = "1" ]; then
  deploy_collector
  deploy_order
  deploy_web
fi
verify
[ "$WITH_APPS" = "1" ] && verify_apps
summary

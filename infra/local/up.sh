#!/usr/bin/env bash
# 로컬 실험 환경을 한 번에 띄움: kind + Prometheus/Grafana + Chaos Mesh + PostgreSQL + Redis
# 여러 번 실행해도 같은 결과가 되도록 작성 (이미 있는 것은 건너뛰거나 upgrade)
#
# 사용법: infra/local/up.sh
# 환경 변수: CLUSTER_NAME (기본 chaos-lab)
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# 버전 고정: 바꿀 때는 values 파일 상단 주석도 함께 수정
KPS_CHART_VERSION="92.21.1"
CHAOS_MESH_CHART_VERSION="2.8.7"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

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
  helm repo update prometheus-community chaos-mesh >/dev/null
}

install_monitoring() {
  log "kube-prometheus-stack $KPS_CHART_VERSION 설치 (namespace: monitoring)"
  h upgrade --install kps prometheus-community/kube-prometheus-stack \
    --namespace monitoring --create-namespace \
    --version "$KPS_CHART_VERSION" \
    --values "$ROOT/infra/helm/values/kube-prometheus-stack.yaml" \
    --wait --timeout 10m
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
  k apply -f "$ROOT/infra/k8s/data/"
  k -n exchange rollout status statefulset/postgres --timeout=180s
  k -n exchange rollout status deployment/redis --timeout=120s
}

verify() {
  log "설치 확인"
  local failed=0
  # 컨트롤러와 데이터 저장소가 Ready인지 확인
  k -n monitoring wait --for=condition=Ready pod -l app.kubernetes.io/name=prometheus --timeout=120s || failed=1
  k -n chaos-mesh wait --for=condition=Ready pod -l app.kubernetes.io/component=controller-manager --timeout=120s || failed=1
  k -n chaos-mesh rollout status daemonset/chaos-daemon --timeout=120s || failed=1
  k -n exchange exec statefulset/postgres -- pg_isready -U exchange -d exchange >/dev/null || failed=1
  [ "$(k -n exchange exec deploy/redis -- redis-cli ping)" = "PONG" ] || failed=1
  # Chaos Mesh CRD가 등록됐는지 (주입기가 PodChaos를 만들 수 있어야 함)
  k get crd podchaos.chaos-mesh.org networkchaos.chaos-mesh.org >/dev/null || failed=1
  [ "$failed" -eq 0 ] || die "일부 구성 요소가 준비되지 않았습니다. 위 로그를 확인하세요"
}

summary() {
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

  삭제: infra/local/down.sh
EOF
}

check_deps
create_cluster
add_repos
install_monitoring
install_chaos_mesh
install_data
verify
summary

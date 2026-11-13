#!/usr/bin/env bash
# 주문 서비스를 로컬 kind 클러스터(exchange 네임스페이스)에 배포
#   workloads/order/deploy-local.sh                 # 회복탄력성 설정 켠 상태
#   workloads/order/deploy-local.sh baseline        # 개선 전 비교용
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
DIR="$(cd "$(dirname "$0")" && pwd)"
TAG="${TAG:-dev}"

VALUES=()
if [[ "${1:-}" == "baseline" ]]; then
  VALUES=(-f "$DIR/chart/values-baseline.yaml")
  shift
fi

echo "==> 이미지 빌드: chaos-lab-order:$TAG"
docker build -t "chaos-lab-order:$TAG" "$DIR"

echo "==> kind 노드에 이미지 넣기"
kind load docker-image "chaos-lab-order:$TAG" --name "$CLUSTER_NAME"

echo "==> Helm 배포 (namespace: exchange)"
helm --kube-context "$CONTEXT" upgrade --install order "$DIR/chart" \
  -n exchange --set image.tag="$TAG" "${VALUES[@]+"${VALUES[@]}"}" "$@" --wait --timeout 180s

# 같은 태그로 다시 배포해도 새 이미지를 쓰도록 Pod 재시작
kubectl --context "$CONTEXT" -n exchange rollout restart deployment/order
kubectl --context "$CONTEXT" -n exchange rollout status deployment/order --timeout=180s

echo "==> 완료"
kubectl --context "$CONTEXT" -n exchange get pods -l app=order

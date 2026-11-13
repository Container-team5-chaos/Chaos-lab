#!/usr/bin/env bash
# 데모 화면과 임시 푸시 서버를 로컬 kind 클러스터(exchange 네임스페이스)에 배포
# 열기: kubectl --context kind-chaos-lab -n exchange port-forward svc/web 8080:80 → http://localhost:8080
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TAG="${TAG:-dev}"

k() { kubectl --context "$CONTEXT" "$@"; }

echo "==> 임시 푸시 서버 이미지 빌드: chaos-lab-dev-push:$TAG"
docker build -t "chaos-lab-dev-push:$TAG" "$DIR/dev-push"

echo "==> kind 노드에 이미지 넣기"
kind load docker-image "chaos-lab-dev-push:$TAG" --name "$CLUSTER_NAME"

echo "==> 화면·nginx 설정 ConfigMap"
k -n exchange create configmap web-html --from-file=index.html="$DIR/index.html" \
  --dry-run=client -o yaml | k apply -f -
k -n exchange create configmap web-nginx --from-file=nginx.conf="$DIR/nginx.conf" \
  --dry-run=client -o yaml | k apply -f -

echo "==> 배포 (namespace: exchange)"
k apply -f "$DIR/k8s/"
# ConfigMap·같은 태그 이미지가 바뀌어도 새로 읽도록 재시작
k -n exchange rollout restart deployment/dev-push deployment/web
k -n exchange rollout status deployment/dev-push --timeout=120s
k -n exchange rollout status deployment/web --timeout=120s

if ! k -n exchange get deployment order >/dev/null 2>&1; then
  echo "주의: 주문 서비스가 없습니다. 주문·계좌는 workloads/order/deploy-local.sh 후에 동작합니다."
fi

cat <<EOF

==> 완료. 화면 열기:
  kubectl --context $CONTEXT -n exchange port-forward svc/web 8080:80
  브라우저: http://localhost:8080
EOF

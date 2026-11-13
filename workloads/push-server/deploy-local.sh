#!/usr/bin/env bash
set -euo pipefail
CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
TAG="${TAG:-dev}"
[[ "$TAG" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || { echo "Invalid image tag" >&2; exit 1; }
k() { kubectl --context "$CONTEXT" "$@"; }

# Explicit context avoids traversing Windows/WSL virtualenv and pytest cache metadata.
tar -C "$DIR" --exclude='__pycache__' --exclude='*.pyc' \
  -cf - Dockerfile requirements.txt push_server \
  | docker build -t "chaos-lab-push-server:$TAG" -
kind load docker-image "chaos-lab-push-server:$TAG" --name "$CLUSTER_NAME"
sed "s|chaos-lab-push-server:dev|chaos-lab-push-server:$TAG|" \
  "$ROOT/infra/k8s/push-server/deployment.yaml" | k apply -f -
if k get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then
  k apply -f "$ROOT/infra/k8s/push-server/servicemonitor.yaml"
fi
k -n exchange rollout restart deployment/push-server
k -n exchange rollout status deployment/push-server --timeout=180s

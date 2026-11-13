#!/usr/bin/env bash
set -euo pipefail
CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
DIR="$(cd "$(dirname "$0")" && pwd)"
TAG="${TAG:-dev}"
# WSL /mnt/c의 Windows 생성 캐시에서 xattr 권한 오류가 나지 않도록
# 필요한 파일만 tar context로 전달한다. 가상환경/pytest 캐시는 탐색하지 않는다.
tar -C "$DIR" --exclude='__pycache__' --exclude='*.pyc' \
  -cf - Dockerfile requirements.txt adapter \
  | docker build -t "chaos-lab-adapter:$TAG" -
kind load docker-image "chaos-lab-adapter:$TAG" --name "$CLUSTER_NAME"
helm --kube-context "$CONTEXT" upgrade --install adapter "$DIR/chart" \
  --namespace exchange --set image.tag="$TAG" "$@" --wait --timeout 180s
kubectl --context "$CONTEXT" -n exchange rollout restart deployment/adapter
kubectl --context "$CONTEXT" -n exchange rollout status deployment/adapter --timeout=180s

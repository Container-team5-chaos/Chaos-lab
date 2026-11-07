#!/usr/bin/env bash
# 실험 시작 전 DB 초기화: 주문·포지션·계좌를 비우고 테스트 사용자 1,000명에게 1,000만 원씩 다시 지급
# (B의 정합성 검사: 보낸 주문 ID와 DB 대조, 잔고 불변식 → 깨끗한 시작 상태가 기준)
#
# 사용법: infra/local/reset-db.sh [-y]
#   -y  확인 질문 없이 실행
# 실험 중(Chaos 리소스나 실행 중인 k6 Job이 있음)이면 판정이 깨지므로 실행하지 않는다.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-chaos-lab}"
CONTEXT="kind-${CLUSTER_NAME}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCHEMA_SQL="$ROOT/workloads/order/db/schema.sql"
SEED_SQL="$ROOT/workloads/order/db/seed.sql"
LABEL_KEY="chaos/run-id" # orchestrator inject.LABEL_KEY

k() { kubectl --context "$CONTEXT" "$@"; }
psql_exec() { k -n exchange exec -i statefulset/postgres -- psql -v ON_ERROR_STOP=1 -U exchange -d exchange "$@"; }

YES=0
[ "${1:-}" = "-y" ] && YES=1

# 실험 진행 중이면 거부
chaos="$(k get podchaos,networkchaos -A -l "$LABEL_KEY" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
k6="$(k get jobs -A -l app.kubernetes.io/name=k6 -o jsonpath='{range .items[?(@.status.active)]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -c . || true)"
if [ "$chaos" != "0" ] || [ "$k6" != "0" ]; then
  echo "실험 진행 중으로 보여 중단합니다 (Chaos 리소스 ${chaos}개, 실행 중 k6 Job ${k6}개)" >&2
  echo "실험을 정리한 뒤 다시 실행하세요." >&2
  exit 1
fi

if [ "$YES" -ne 1 ]; then
  read -r -p "[$CONTEXT] 주문·포지션·계좌를 모두 지우고 다시 채웁니다. 계속할까요? [y/N] " answer
  [ "$answer" = "y" ] || [ "$answer" = "Y" ] || { echo "취소"; exit 0; }
fi

exists="$(psql_exec -tAc "SELECT to_regclass('public.orders') IS NOT NULL" | tr -d '\r[:space:]')"
if [ "$exists" != "t" ]; then
  echo "스키마 없음 — 생성"
  psql_exec <"$SCHEMA_SQL"
fi

psql_exec <"$SEED_SQL"
psql_exec -tAc "SELECT 'accounts=' || count(*) FROM accounts"

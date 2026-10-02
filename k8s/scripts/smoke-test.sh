#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

dashboard="$(kubectl get pod -n tpsd \
  -l app.kubernetes.io/name=dashboard \
  -o jsonpath='{.items[0].metadata.name}')"
nats_box="$(kubectl get pod -n tpsd \
  -l app.kubernetes.io/component=nats-box \
  -o jsonpath='{.items[0].metadata.name}')"

kubectl exec -n tpsd "${dashboard}" -- \
  python benchmark.py \
  --msgs "${TPSD_SMOKE_MESSAGES:-100}" \
  --timeout 120 \
  --label kind-jetstream-smoke \
  --fail-on-loss

kubectl exec -n tpsd "${dashboard}" -- python rpc_client.py
kubectl exec -n tpsd "${nats_box}" -- \
  nats --server nats://nats:4222 consumer list ORDERS

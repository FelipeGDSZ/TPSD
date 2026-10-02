#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

NATS_POD="${1:-nats-0}"
dashboard="$(kubectl get pod -n tpsd \
  -l app.kubernetes.io/name=dashboard \
  -o jsonpath='{.items[0].metadata.name}')"

kubectl delete pod -n tpsd "${NATS_POD}" --wait=false
kubectl exec -n tpsd "${dashboard}" -- \
  python benchmark.py \
  --msgs "${TPSD_FAILURE_MESSAGES:-300}" \
  --timeout 180 \
  --label "kind-falha-${NATS_POD}" \
  --fail-on-loss
kubectl wait --for=condition=Ready "pod/${NATS_POD}" -n tpsd --timeout=180s
kubectl get pods -n tpsd \
  -l app.kubernetes.io/component=nats \
  -o wide

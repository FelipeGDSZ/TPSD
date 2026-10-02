#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

kubectl delete job jetstream-setup -n tpsd --ignore-not-found
kubectl apply -f "${PROJECT_ROOT}/k8s/app/jetstream-setup-job.yaml"
kubectl wait --for=condition=complete job/jetstream-setup -n tpsd --timeout=120s
kubectl logs -n tpsd job/jetstream-setup

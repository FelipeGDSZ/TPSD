#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

if kind get clusters | grep -Fxq tpsd; then
  echo "O cluster Kind 'tpsd' ja existe."
else
  kind create cluster --config "${PROJECT_ROOT}/k8s/kind-config.yaml"
fi

kubectl cluster-info --context kind-tpsd
kubectl get nodes -o wide

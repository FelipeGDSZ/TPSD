#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

HELM_HOME="${PROJECT_ROOT}/.tools/helm"
export HELM_REPOSITORY_CONFIG="${HELM_HOME}/repositories.yaml"
export HELM_REPOSITORY_CACHE="${HELM_HOME}/cache"

mkdir -p "${HELM_HOME}"

if ! helm repo list 2>/dev/null | awk 'NR > 1 {print $1}' | grep -Fxq nats; then
  helm repo add nats https://nats-io.github.io/k8s/helm/charts/
fi

helm repo update nats
kubectl apply -f "${PROJECT_ROOT}/k8s/namespace.yaml"

helm upgrade --install nats nats/nats \
  --namespace tpsd \
  --version 2.15.0 \
  --values "${PROJECT_ROOT}/k8s/nats/values-local.yaml" \
  --wait \
  --timeout 5m

kubectl get pods,pvc,services -n tpsd -o wide

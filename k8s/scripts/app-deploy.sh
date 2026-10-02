#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

kubectl apply -f "${PROJECT_ROOT}/k8s/app/configmap.yaml"
kubectl apply -f "${PROJECT_ROOT}/k8s/app/workloads.yaml"
kubectl apply -f "${PROJECT_ROOT}/k8s/app/disruption-budgets.yaml"

# A tag local e fixa; reiniciar garante que os pods usem a imagem recarregada no Kind.
kubectl rollout restart \
  deployment/payment \
  deployment/stock \
  deployment/notification \
  deployment/rpc \
  deployment/dashboard \
  -n tpsd

for deployment in payment stock notification rpc dashboard; do
  kubectl rollout status "deployment/${deployment}" -n tpsd --timeout=180s
done

kubectl get deployments,pods,services,pdb -n tpsd -o wide

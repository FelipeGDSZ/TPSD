#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TOOLS_BIN="${PROJECT_ROOT}/.tools/bin"
DOWNLOADS="${PROJECT_ROOT}/.tools/downloads"

KUBECTL_VERSION="v1.37.1"
KIND_VERSION="v0.33.0"
HELM_VERSION="v4.3.0"

if [[ "$(uname -m)" != "x86_64" ]]; then
  echo "Este instalador local suporta apenas Linux x86_64." >&2
  exit 1
fi

mkdir -p "${TOOLS_BIN}" "${DOWNLOADS}"

curl --fail --silent --show-error --location \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  --output "${DOWNLOADS}/kubectl"
curl --fail --silent --show-error --location \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256" \
  --output "${DOWNLOADS}/kubectl.sha256"
printf '%s  %s\n' "$(tr -d '\n' < "${DOWNLOADS}/kubectl.sha256")" \
  "${DOWNLOADS}/kubectl" | sha256sum --check
install -m 0755 "${DOWNLOADS}/kubectl" "${TOOLS_BIN}/kubectl"

curl --fail --silent --show-error --location \
  "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64" \
  --output "${DOWNLOADS}/kind"
curl --fail --silent --show-error --location \
  "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64.sha256sum" \
  --output "${DOWNLOADS}/kind.sha256sum"
printf '%s  %s\n' "$(awk '{print $1}' "${DOWNLOADS}/kind.sha256sum")" \
  "${DOWNLOADS}/kind" | sha256sum --check
install -m 0755 "${DOWNLOADS}/kind" "${TOOLS_BIN}/kind"

curl --fail --silent --show-error --location \
  "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" \
  --output "${DOWNLOADS}/helm.tar.gz"
curl --fail --silent --show-error --location \
  "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz.sha256sum" \
  --output "${DOWNLOADS}/helm.sha256sum"
printf '%s  %s\n' "$(awk '{print $1}' "${DOWNLOADS}/helm.sha256sum")" \
  "${DOWNLOADS}/helm.tar.gz" | sha256sum --check
tar -xzf "${DOWNLOADS}/helm.tar.gz" -C "${DOWNLOADS}"
install -m 0755 "${DOWNLOADS}/linux-amd64/helm" "${TOOLS_BIN}/helm"

"${TOOLS_BIN}/kubectl" version --client
"${TOOLS_BIN}/kind" version
"${TOOLS_BIN}/helm" version --short

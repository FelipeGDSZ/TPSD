#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "Dashboard disponivel em http://127.0.0.1:5000"
echo "Pressione Ctrl+C para encerrar o port-forward."
kubectl port-forward -n tpsd service/dashboard 5000:5000

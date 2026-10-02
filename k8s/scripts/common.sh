#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_BIN="${PROJECT_ROOT}/.tools/bin"

export PATH="${TOOLS_BIN}:${PATH}"

for command_name in docker kubectl kind helm; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Ferramenta obrigatoria nao encontrada: ${command_name}" >&2
    exit 1
  fi
done

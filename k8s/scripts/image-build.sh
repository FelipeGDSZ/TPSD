#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

IMAGE_NAME="${TPSD_IMAGE:-tpsd-jetstream:dev-001}"

docker build --tag "${IMAGE_NAME}" "${PROJECT_ROOT}"
kind load docker-image "${IMAGE_NAME}" --name tpsd

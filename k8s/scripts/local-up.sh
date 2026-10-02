#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/cluster-up.sh"
"${SCRIPT_DIR}/nats-install.sh"
"${SCRIPT_DIR}/image-build.sh"
"${SCRIPT_DIR}/jetstream-setup.sh"
"${SCRIPT_DIR}/app-deploy.sh"

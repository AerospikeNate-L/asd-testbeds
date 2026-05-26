#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ASD_TESTBEDS="$(cd "$HARNESS_DIR/../.." && pwd)"

: "${ASD_BINARY:?Set ASD_BINARY to your built EE asd path}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-smd-sync-ako}"
# AKO: image must contain "enterprise"|"federal", and the tag must embed a version like 8.1.2.0 (regex \\d+(\\.\\d+)+); see aerospike-kubernetes-operator/api/v1 GetImageVersion.
ASD_IMAGE="${ASD_IMAGE:-smd-sync-asd-enterprise:8.1.2.0-dev}"

STAGING="$HARNESS_DIR/asd-staging"
mkdir -p "$STAGING"
cp -f "$ASD_BINARY" "$STAGING/asd"

docker build -f "$HARNESS_DIR/Dockerfile.asd-dev" -t "$ASD_IMAGE" "$ASD_TESTBEDS"

if command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER_NAME"; then
  kind load docker-image "$ASD_IMAGE" --name "$KIND_CLUSTER_NAME"
else
  echo "Note: kind cluster '$KIND_CLUSTER_NAME' not found; skipped kind load docker-image."
  echo "      Push $ASD_IMAGE to your registry or run ./scripts/setup-kind.sh first."
fi

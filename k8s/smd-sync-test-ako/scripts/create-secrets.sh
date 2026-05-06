#!/bin/bash
set -euo pipefail

NS="${NAMESPACE:-smd-sync-ako}"
: "${FEATURES_CONF:?Set FEATURES_CONF to your features.conf path}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_DIR="$SCRIPT_DIR/../manifests"

kubectl apply -f "$MANIFEST_DIR/namespace.yaml"
sed "s/__TESTBED_NAMESPACE__/${NS}/g" "$MANIFEST_DIR/workload-operator-rbac.yaml" | kubectl apply -f -

kubectl -n "$NS" create secret generic aerospike-secret \
  --from-file=features.conf="$FEATURES_CONF" \
  --dry-run=client -o yaml | kubectl apply -f -

PASS="${ADMIN_PASSWORD:-admin123}"
kubectl -n "$NS" create secret generic auth-secret \
  --from-literal=password="$PASS" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secrets applied in namespace $NS (auth-secret password default matches ADMIN_PASSWORD in test script)."

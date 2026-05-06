#!/bin/bash
set -euo pipefail

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-smd-sync-ako}"

if ! command -v kind >/dev/null 2>&1; then
  echo "kind not found. Install from https://kind.sigs.k8s.io/docs/user/quick-start/"
  exit 1
fi

if kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER_NAME"; then
  echo "kind cluster '$KIND_CLUSTER_NAME' already exists"
else
  kind create cluster --name "$KIND_CLUSTER_NAME"
fi

# Ensure kubectl uses this cluster (avoids default api-server localhost:8080 when kubeconfig has no context).
kind export kubeconfig --name "$KIND_CLUSTER_NAME"

kubectl cluster-info

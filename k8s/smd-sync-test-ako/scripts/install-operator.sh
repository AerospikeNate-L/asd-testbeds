#!/bin/bash
set -euo pipefail

: "${OPERATOR_IMG:?Set OPERATOR_IMG to the controller image (build in AKO repo: docker build -t \$OPERATOR_IMG --build-arg VERSION=dev .)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TOOLS_DIR="$HARNESS_DIR/.tools"
KUSTOMIZE_VERSION="${KUSTOMIZE_VERSION:-v5.6.0}"
# Must match config/default kustomization namespace + namePrefix on Certificate serving-cert.
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-aerospike}"
SERVING_CERT_NAME="${SERVING_CERT_NAME:-aerospike-operator-serving-cert}"
# Upstream AKO manager.yaml sets WATCH_NAMESPACE=aerospike only; AerospikeClusters in smd-sync-ako are never reconciled unless listed here (comma-separated).
OPERATOR_WATCH_NAMESPACES="${OPERATOR_WATCH_NAMESPACES:-aerospike,smd-sync-ako}"

if [[ -z "${AKO_REPO:-}" ]]; then
  for cand in \
    "$HARNESS_DIR/../../../aerospike-kubernetes-operator" \
    "$HARNESS_DIR/../../../../aerospike-kubernetes-operator"; do
    if [[ -f "$cand/Makefile" ]]; then
      AKO_REPO="$(cd "$cand" && pwd)"
      break
    fi
  done
fi

if [[ -z "${AKO_REPO:-}" ]] || [[ ! -f "${AKO_REPO}/Makefile" ]]; then
  echo "Set AKO_REPO to your aerospike-kubernetes-operator clone (contains Makefile)."
  exit 1
fi

# kind cluster nodes cannot pull images from your laptop Docker; load locally built tags first.
maybe_hint_kind_load() {
  local ctx
  ctx="$(kubectl config current-context 2>/dev/null || true)"
  if [[ "$ctx" != kind-* ]]; then
    return 0
  fi
  local cluster="${ctx#kind-}"
  echo "kind context '${ctx}': if ${OPERATOR_IMG} exists only on this host (not a registry), load it first:" >&2
  echo "  kind load docker-image ${OPERATOR_IMG} --name ${cluster}" >&2
}

if ! kubectl get namespace cert-manager >/dev/null 2>&1; then
  echo "Installing cert-manager (required by AKO webhooks)..."
  kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.yaml
  kubectl wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=180s
  kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=180s
fi

# Standalone kustomize (AKO's Makefile installs kustomize via `go`; avoid that dependency here).
ensure_kustomize() {
  local arch
  arch=$(uname -m)
  case "$arch" in
    x86_64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *)
      echo "Unsupported machine arch for kustomize download: $(uname -m)" >&2
      exit 1
      ;;
  esac

  mkdir -p "$TOOLS_DIR"
  local stamp="$TOOLS_DIR/kustomize.${KUSTOMIZE_VERSION}.${arch}.stamp"
  local tgz="$TOOLS_DIR/kustomize.${KUSTOMIZE_VERSION}.${arch}.tar.gz"
  if [[ ! -x "$TOOLS_DIR/kustomize" ]] || [[ ! -f "$stamp" ]]; then
    echo "Downloading kustomize ${KUSTOMIZE_VERSION} (${arch})..." >&2
    local url="https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2F${KUSTOMIZE_VERSION}/kustomize_${KUSTOMIZE_VERSION}_linux_${arch}.tar.gz"
    curl -fsSL "$url" -o "$tgz"
    tar -xzf "$tgz" -C "$TOOLS_DIR" kustomize
    chmod +x "$TOOLS_DIR/kustomize"
    : >"$stamp"
    rm -f "$tgz"
  fi
  echo "$TOOLS_DIR/kustomize"
}

echo "Deploying operator from $AKO_REPO with IMG=$OPERATOR_IMG"
maybe_hint_kind_load
KUZ="$(ensure_kustomize)"
echo "Note: updates ${AKO_REPO}/config/manager/kustomization.yaml (same as make deploy)."
(cd "$AKO_REPO/config/manager" && "$KUZ" edit set image "controller=${OPERATOR_IMG}")

# Client-side kubectl apply stores last-applied-configuration on each object; Aerospike CRDs are
# large enough that annotation size exceeds the apiserver limit (~256KiB). Server-side apply avoids that.
#
# Kustomize v5 no longer substitutes $(CERTIFICATE_NAMESPACE)/$(CERTIFICATE_NAME) in webhookcainjection_patch.yaml,
# so cert-manager never gets inject-ca-from → apiserver sees x509: certificate signed by unknown authority.
fix_webhook_certmanager_inject_ref() {
  local inject_ref="${OPERATOR_NAMESPACE}/${SERVING_CERT_NAME}"
  sed "s|cert-manager.io/inject-ca-from: \\\$(CERTIFICATE_NAMESPACE)/\\\$(CERTIFICATE_NAME)|cert-manager.io/inject-ca-from: ${inject_ref}|g"
}

TMP_BUILD="$(mktemp)"
TMP_FIXED="$(mktemp)"
cleanup_ako_build() {
  rm -f "$TMP_BUILD" "$TMP_FIXED"
}
trap cleanup_ako_build EXIT

echo "Applying config/default with kubectl apply --server-side (required for large CRDs)..."
"$KUZ" build "$AKO_REPO/config/default" >"$TMP_BUILD"

fix_webhook_certmanager_inject_ref <"$TMP_BUILD" >"$TMP_FIXED"

kubectl apply --server-side --force-conflicts \
  --field-manager=aerospike-smd-testbed-install -f "$TMP_FIXED"

kubectl rollout status deployment/aerospike-operator-controller-manager -n "$OPERATOR_NAMESPACE" --timeout=300s

echo "Waiting for webhook TLS Certificate to be Ready (cert-manager cainjector)..."
kubectl wait --for=condition=Ready "certificate/${SERVING_CERT_NAME}" -n "$OPERATOR_NAMESPACE" --timeout=180s

echo "Waiting for AerospikeCluster CRD to be Established..."
kubectl wait --for=condition=Established "crd/aerospikeclusters.asdb.aerospike.com" --timeout=180s

echo "Setting operator WATCH_NAMESPACE=${OPERATOR_WATCH_NAMESPACES} (see config/manager/manager.yaml in AKO; default is aerospike-only)."
kubectl set env deployment/aerospike-operator-controller-manager -n "$OPERATOR_NAMESPACE" \
  WATCH_NAMESPACE="$OPERATOR_WATCH_NAMESPACES"
kubectl rollout status deployment/aerospike-operator-controller-manager -n "$OPERATOR_NAMESPACE" --timeout=300s

echo "Operator deployment reported ready."

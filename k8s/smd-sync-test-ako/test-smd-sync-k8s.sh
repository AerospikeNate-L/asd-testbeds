#!/bin/bash
# SMD sync tests against an AerospikeCluster managed by AKO (Kubernetes).
# Pods use operator-assigned node IDs (e.g. 0a0, 0a1, 0a2), not Docker's a1/a2/a3.
#
# Usage: see bottom case statement.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_DIR="$SCRIPT_DIR/manifests"

NS="${NAMESPACE:-smd-sync-ako}"
CLUSTER="${CLUSTER_NAME:-smdsync}"
TIMEOUT="${TIMEOUT:-300}"
CLEANUP_ON_SUCCESS="${CLEANUP_ON_SUCCESS:-false}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}"

# StatefulSet name for default single rack (rack id 0).
STS_NAME="${CLUSTER}-0"
WORKDIR_VOL="${WORKDIR_VOL_NAME:-workdir}"

LABEL_SELECTOR="app=aerospike-cluster,aerospike.com/cr=${CLUSTER}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-smd-sync-ako}"

require_kubectl_cluster() {
  if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "kubectl cannot reach a Kubernetes API server (often kubeconfig points at nothing -> localhost:8080)." >&2
    echo "If using kind:  kind export kubeconfig --name ${KIND_CLUSTER_NAME}" >&2
    echo "Or run:        ./scripts/setup-kind.sh" >&2
    exit 1
  fi
}

require_aerospike_crd() {
  if kubectl get crd aerospikeclusters.asdb.aerospike.com >/dev/null 2>&1; then
    return 0
  fi
  echo "No AerospikeCluster CRD on this cluster (kind AerospikeCluster / asdb.aerospike.com/v1)." >&2
  echo "Deploy the Aerospike Kubernetes Operator CRDs and controller first:" >&2
  echo "  export OPERATOR_IMG=<your-operator-image>" >&2
  echo "  ./scripts/install-operator.sh" >&2
  echo "Confirm: kubectl get crd aerospikeclusters.asdb.aerospike.com" >&2
  exit 1
}

log() {
  echo "[$(date '+%H:%M:%S')] $*"
}

list_pods_sorted() {
  kubectl get pods -n "$NS" -l "$LABEL_SELECTOR" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | sort -u
}

# Zero-based index (0 = lowest ordinal pod).
pod_at_index() {
  local idx="$1"
  local line
  line=$(list_pods_sorted | sed -n "$((idx + 1))p")
  if [[ -z "$line" ]]; then
    log "ERROR: no pod at index $idx"
    return 1
  fi
  echo "$line"
}

pod_ready() {
  local pod=$1
  [[ "$(kubectl get pod -n "$NS" "$pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" == "True" ]]
}

wait_ready_pod_count() {
  local want=$1
  local max=$((TIMEOUT / 2))
  local n=0
  log "Waiting for $want Ready pod(s) (timeout ${TIMEOUT}s)..."
  while [[ $n -lt $max ]]; do
    mapfile -t pods < <(list_pods_sorted)
    if [[ ${#pods[@]} -eq "$want" ]]; then
      local ok=1
      for p in "${pods[@]}"; do
        pod_ready "$p" || ok=0
      done
      if [[ $ok -eq 1 ]]; then
        log "All $want pod(s) Ready"
        return 0
      fi
    fi
    sleep 2
    ((n++)) || true
  done
  log "ERROR: timed out waiting for $want ready pods"
  kubectl get pods -n "$NS" -l "$LABEL_SELECTOR" -o wide || true
  return 1
}

# Args: expected_cluster_size [pod_name_for_asinfo]
wait_for_cluster() {
  local expected=$1
  local probe_pod=${2:-}
  local max=$((TIMEOUT / 2))
  local n=0
  local size=0

  [[ -n "$probe_pod" ]] || probe_pod="$(pod_at_index 0)"

  log "Waiting for cluster_size=$expected (probe pod $probe_pod)..."
  while [[ $n -lt $max ]]; do
    if pod_ready "$probe_pod"; then
      if [[ "${USE_AUTH:-0}" == "1" ]]; then
        size=$(kubectl exec -n "$NS" "$probe_pod" -c aerospike-server -- \
          timeout 5 asinfo -Uadmin -P"$ADMIN_PASSWORD" -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo 0)
      else
        size=$(kubectl exec -n "$NS" "$probe_pod" -c aerospike-server -- \
          timeout 5 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo 0)
      fi
      if [[ "$size" == "$expected" ]]; then
        log "Cluster formed with size $size"
        return 0
      fi
    fi
    sleep 2
    ((n++)) || true
  done
  log "ERROR: cluster_size did not reach $expected (last: $size)"
  return 1
}

asinfo_exec() {
  local pod=$1
  shift
  if [[ "${USE_AUTH:-0}" == "1" ]]; then
    kubectl exec -n "$NS" "$pod" -c aerospike-server -- timeout 15 asinfo -Uadmin -P"$ADMIN_PASSWORD" "$@"
  else
    kubectl exec -n "$NS" "$pod" -c aerospike-server -- timeout 15 asinfo "$@"
  fi
}

collect_server_logs() {
  local out=""
  local p
  while IFS= read -r p; do
    [[ -z "$p" ]] && continue
    out+=$(kubectl logs -n "$NS" "$p" -c aerospike-server --tail=2000 2>/dev/null || true)
    out+=$'\n'
  done < <(list_pods_sorted)
  echo "$out"
}

apply_namespace() {
  kubectl apply -f "$MANIFEST_DIR/namespace.yaml"
  sed "s/__TESTBED_NAMESPACE__/${NS}/g" "$MANIFEST_DIR/workload-operator-rbac.yaml" | kubectl apply -f -
}

cleanup_full() {
  log "Cleaning up AerospikeCluster and PVCs..."
  kubectl delete aerospikecluster "$CLUSTER" -n "$NS" --ignore-not-found --wait=true --timeout=600s || true
  kubectl delete pvc -n "$NS" -l "$LABEL_SELECTOR" --ignore-not-found --wait=false || true
  local wait_n=0
  while kubectl get pvc -n "$NS" -l "$LABEL_SELECTOR" -o name 2>/dev/null | grep -q .; do
    sleep 2
    wait_n=$((wait_n + 1))
    if [[ $wait_n -gt 150 ]]; then
      log "WARN: PVCs still present after timeout"
      break
    fi
  done
  kubectl delete clusterrolebinding "aerospike-operator-workload-${NS}" --ignore-not-found || true
  kubectl delete clusterrolebinding "aerospike-operator-workload-nodes-${NS}" --ignore-not-found || true
  kubectl delete clusterrole "aerospike-operator-workload-nodes-${NS}" --ignore-not-found || true
  kubectl delete rolebinding aerospike-operator-leader-election-rolebinding -n "$NS" --ignore-not-found || true
  kubectl delete role aerospike-operator-leader-election-role -n "$NS" --ignore-not-found || true
}

patch_cluster_size() {
  local sz=$1
  log "Patch AerospikeCluster $CLUSTER spec.size=$sz"
  kubectl patch aerospikecluster "$CLUSTER" -n "$NS" --type=merge -p "{\"spec\":{\"size\":${sz}}}"
}

pvc_for_ordinal() {
  local ord=$1
  echo "${WORKDIR_VOL}-${STS_NAME}-${ord}"
}

ensure_three_node_cluster() {
  local cnt
  cnt=$(list_pods_sorted | grep -c . || true)
  if [[ "$cnt" != 3 ]]; then
    log "Bringing up fresh 3-node cluster..."
    cleanup_full
    USE_AUTH=0
    apply_namespace
    kubectl apply -f "$MANIFEST_DIR/aerospikecluster.yaml"
    wait_ready_pod_count 3
    wait_for_cluster 3 "$(pod_at_index 0)"
  fi
}

test_basic_sync_ordering() {
  log "=== Test 1: Basic SMD Sync Ordering (AKO) ==="
  USE_AUTH=0
  cleanup_full
  apply_namespace
  kubectl apply -f "$MANIFEST_DIR/aerospikecluster.yaml"
  wait_ready_pod_count 3
  wait_for_cluster 3 "$(pod_at_index 0)"

  log "Checking logs for SMD sync / timeouts..."
  local logs
  logs=$(collect_server_logs)
  if echo "$logs" | grep -q "sync wait start"; then
    log "PASS: Found 'sync wait start'"
  else
    log "INFO: No 'sync wait start' (normal with empty SMD)"
  fi
  if echo "$logs" | grep -q "sync wait done\|all modules settled"; then
    log "PASS: Found sync completion message"
  else
    log "INFO: No explicit sync completion in tail (may be normal)"
  fi
  if echo "$logs" | grep -q "SMD sync timed out"; then
    log "FAIL: SMD sync timed out"
    return 1
  fi
  log "PASS: No SMD sync timeout in recent logs"
  log "Test 1 complete"
}

test_security_auth() {
  log "=== Test 2: Security SMD Sync (AKO) ==="
  USE_AUTH=1
  cleanup_full
  apply_namespace
  kubectl apply -f "$MANIFEST_DIR/aerospikecluster-security.yaml"
  wait_ready_pod_count 3
  wait_for_cluster 3 "$(pod_at_index 0)"

  local p0 p1 p2
  p0=$(pod_at_index 0)
  p1=$(pod_at_index 1)
  p2=$(pod_at_index 2)

  log "Creating user testuser on $p0..."
  kubectl exec -n "$NS" "$p0" -c aerospike-server -- \
    asadm --enable -Uadmin -P"$ADMIN_PASSWORD" -e "manage acl create user testuser password testpass roles read-write" 2>&1 || true
  sleep 3

  log "Verify auth on $p1..."
  if kubectl exec -n "$NS" "$p1" -c aerospike-server -- asinfo -Utestuser -Ptestpass -v "namespaces" 2>&1 | grep -q "test"; then
    log "PASS: User visible on second pod"
  else
    log "FAIL: Auth failed on second pod"
    return 1
  fi
  log "Verify auth on $p2..."
  if kubectl exec -n "$NS" "$p2" -c aerospike-server -- asinfo -Utestuser -Ptestpass -v "namespaces" 2>&1 | grep -q "test"; then
    log "PASS: auth on third pod"
  else
    log "FAIL: auth on third pod"
    return 1
  fi

  cleanup_full
  log "Test 2 complete"
}

test_node_rejoin() {
  log "=== Test 3: Node Rejoin with Cleared SMD (AKO) ==="
  USE_AUTH=0
  ensure_three_node_cluster

  local p0 p2 pvc
  p0=$(pod_at_index 0)
  p2=$(pod_at_index 2)

  log "Creating sindex on $p0..."
  asinfo_exec "$p0" -v "sindex-create:ns=test;set=demo;indexname=rejoin_idx;bin=rejoin;type=string" 2>/dev/null || true
  sleep 3

  log "Scaling down to remove highest-ordinal pod ($(pod_at_index 2))..."
  patch_cluster_size 2
  wait_ready_pod_count 2
  wait_for_cluster 2 "$p0"

  pvc=$(pvc_for_ordinal 2)
  log "Deleting PVC $pvc (fresh workdir for ordinal 2)..."
  kubectl delete pvc -n "$NS" "$pvc" --wait=true --ignore-not-found

  log "Scaling back to 3 nodes..."
  patch_cluster_size 3
  wait_ready_pod_count 3
  p2=$(pod_at_index 2)
  wait_for_cluster 3 "$p0"

  log "Verify rejoined pod has sindex..."
  if asinfo_exec "$p2" -v "sindex" 2>&1 | grep -q "rejoin_idx"; then
    log "PASS: Rejoined pod has sindex"
  else
    log "FAIL: Rejoined pod missing sindex"
    return 1
  fi

  local logs
  logs=$(kubectl logs -n "$NS" "$p2" -c aerospike-server --tail=400 2>&1 || true)
  if echo "$logs" | grep -q "SMD sync timed out"; then
    log "FAIL: SMD sync timed out on rejoin"
    return 1
  fi
  log "Test 3 complete"
}

test_preexisting_smd() {
  log "=== Test 4: First Pod Has SMD, Others Join Empty (AKO) ==="
  USE_AUTH=0
  cleanup_full
  apply_namespace
  kubectl apply -f "$MANIFEST_DIR/aerospikecluster-size1.yaml"
  wait_ready_pod_count 1
  local p0
  p0=$(pod_at_index 0)
  wait_for_cluster 1 "$p0"

  log "Creating secondary index on single-node cluster..."
  asinfo_exec "$p0" -v "sindex-create:ns=test;set=demo;indexname=preexist_idx;bin=preexist;type=string" 2>/dev/null || true
  sleep 3

  log "Scaling to 3 nodes..."
  patch_cluster_size 3
  wait_ready_pod_count 3
  wait_for_cluster 3 "$p0"

  local all=1 i p
  for i in 0 1 2; do
    p=$(pod_at_index "$i")
    if asinfo_exec "$p" -v "sindex" 2>&1 | grep -q "preexist_idx"; then
      log "Pod $p has sindex"
    else
      log "FAIL: Pod $p missing sindex"
      all=0
    fi
  done
  [[ $all -eq 1 ]] || return 1

  local logs
  logs=$(collect_server_logs)
  if echo "$logs" | grep -q "SMD sync timed out"; then
    log "FAIL: SMD sync timed out"
    return 1
  fi
  log "Test 4 complete"
}

test_pull_join() {
  log "=== Test 5: New Pod Joins Existing Cluster with SMD (AKO) ==="
  log "NOTE: StatefulSet scale 2→3 adds the highest ordinal last (node-id e.g. 0a2)."
  log "      Docker smd-sync-test starts middle+high nodes then adds lowest — topology differs but empty member still pulls SMD."

  USE_AUTH=0
  cleanup_full
  apply_namespace
  kubectl apply -f "$MANIFEST_DIR/aerospikecluster-size2.yaml"
  wait_ready_pod_count 2
  local p0 p1 p2
  p0=$(pod_at_index 0)
  wait_for_cluster 2 "$p0"

  log "Creating secondary index..."
  asinfo_exec "$p0" -v "sindex-create:ns=test;set=demo;indexname=pull_idx;bin=pull;type=string" 2>/dev/null || true
  sleep 3

  local px
  for i in 0 1; do
    px=$(pod_at_index "$i")
    if asinfo_exec "$px" -v "sindex" 2>&1 | grep -q "pull_idx"; then
      log "Pod $px has sindex"
    else
      log "WARN: Pod $px missing sindex before scale-up"
    fi
  done

  log "Scaling to 3 (new empty pod should receive SMD)..."
  patch_cluster_size 3
  wait_ready_pod_count 3
  wait_for_cluster 3 "$p0"

  local all=1 pjoin
  for i in 0 1 2; do
    pjoin=$(pod_at_index "$i")
    if asinfo_exec "$pjoin" -v "sindex" 2>&1 | grep -q "pull_idx"; then
      log "Pod $pjoin has sindex"
    else
      log "FAIL: Pod $pjoin missing sindex"
      all=0
    fi
  done
  [[ $all -eq 1 ]] || return 1

  local logs
  logs=$(collect_server_logs)
  if echo "$logs" | grep -q "SMD sync timed out"; then
    log "FAIL: SMD sync timed out"
    return 1
  fi
  log "Test 5 complete"
}

run_all() {
  local failed=0
  test_basic_sync_ordering || failed=1
  test_node_rejoin || failed=1
  test_preexisting_smd || failed=1
  test_pull_join || failed=1

  if [[ $failed -eq 0 ]]; then
    log "=== All tests PASSED ==="
    if [[ "$CLEANUP_ON_SUCCESS" == "true" ]]; then
      cleanup_full
    else
      log "Cluster left running. Use '$0 cleanup-full' to delete CR/PVCs."
    fi
  else
    log "=== Some tests FAILED ==="
    exit 1
  fi
}

case "${1:-}" in
  basic|auth|rejoin|preexisting|pull|all|cleanup-full)
    require_kubectl_cluster
    ;;
esac

case "${1:-}" in
  basic|auth|rejoin|preexisting|pull|all)
    require_aerospike_crd
    ;;
esac

case "${1:-}" in
  basic) test_basic_sync_ordering ;;
  auth) test_security_auth ;;
  rejoin) test_node_rejoin ;;
  preexisting) test_preexisting_smd ;;
  pull) test_pull_join ;;
  all) run_all ;;
  cleanup-full) cleanup_full ;;
  *)
    echo "Usage: $0 {basic|auth|rejoin|preexisting|pull|all|cleanup-full}"
    echo ""
    echo "Requires: kubectl, Aerospike Kubernetes Operator, namespace/secrets (see README)."
    echo "Env: NAMESPACE=$NS CLUSTER_NAME=$CLUSTER TIMEOUT=$TIMEOUT ADMIN_PASSWORD=..."
    echo "     USE_AUTH is set internally; non-auth tests use open asinfo."
    exit 1
    ;;
esac

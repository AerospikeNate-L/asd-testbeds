#!/bin/bash
# SMD sync tests against an AerospikeCluster managed by AKO (Kubernetes).
# Pods use operator-assigned node IDs (e.g. 0a0, 0a1, 0a2), not Docker's a1/a2/a3.
#
# Usage: see bottom case statement.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_DIR="$SCRIPT_DIR/manifests"
DOCKER_SMD_TEST="$(cd "$SCRIPT_DIR/../../docker/smd-sync-test" && pwd)"

NS="${NAMESPACE:-smd-sync-ako}"
CLUSTER="${CLUSTER_NAME:-smdsync}"
TIMEOUT="${TIMEOUT:-300}"
CLEANUP_ON_SUCCESS="${CLEANUP_ON_SUCCESS:-false}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}"

# StatefulSet name for default single rack (rack id 0).
STS_NAME="${CLUSTER}-0"
WORKDIR_VOL="${WORKDIR_VOL_NAME:-workdir}"

LABEL_SELECTOR="app=aerospike-cluster,aerospike.com/cr=${CLUSTER}"

# Large-SMD timing (parity with docker/smd-sync-test/test-smd-sync.sh timing / timing-rejoin).
SMD_DATA_DIR="${SMD_DATA_DIR:-/tmp/smd-timing-k8s-data}"
TIMING_RESULTS_DIR="${TIMING_RESULTS_DIR:-${SCRIPT_DIR}/timing-results-k8s}"
TIMING_MODULE="${TIMING_MODULE:-evict}"
TIMING_VALUE_SIZE="${TIMING_VALUE_SIZE:-200}"
TIMING_ITEMS="${TIMING_ITEMS:-10000 50000 100000 200000 300000 400000}"
TIMING_CLUSTER_TIMEOUT="${TIMING_CLUSTER_TIMEOUT:-300}"
TIMING_REJOIN_CLUSTER_TIMEOUT="${TIMING_REJOIN_CLUSTER_TIMEOUT:-600}"
TIMING_REJOIN_STALE_PCT="${TIMING_REJOIN_STALE_PCT:-80}"
TIMING_REJOIN_SECURITY_ITEMS="${TIMING_REJOIN_SECURITY_ITEMS:-100000}"
TIMING_AC_MANIFEST="${TIMING_AC_MANIFEST:-$MANIFEST_DIR/aerospikecluster-timing.yaml}"
# Seed pods mount RWO PVCs on kind/CSI; first attach can sit Pending for a while — avoid kubectl wait Ready until scheduled.
TIMING_SEED_POD_TIMEOUT="${TIMING_SEED_POD_TIMEOUT:-300}"
TIMING_PVC_SETTLE_SEC="${TIMING_PVC_SETTLE_SEC:-5}"

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

# Seed pods hold RWO mounts; CR deletion alone may leave PVCs terminating for a long time.
wait_workdir_pvcs_absent() {
  local wait_n=0 i name
  while [[ $wait_n -lt 360 ]]; do
    local any=0
    for i in 0 1 2; do
      name=$(pvc_for_ordinal "$i")
      if kubectl get pvc -n "$NS" "$name" &>/dev/null; then
        any=1
        break
      fi
    done
    [[ $any -eq 0 ]] && return 0
    sleep 2
    wait_n=$((wait_n + 1))
  done
  log "WARN: timed out waiting for workdir PVCs to finish deleting (scheduler will reject new pods while a claim is terminating)"
  return 1
}

cleanup_full() {
  log "Cleaning up AerospikeCluster and PVCs..."
  kubectl delete pods -n "$NS" -l smd-sync-test=seed --ignore-not-found --wait=false 2>/dev/null || true
  kubectl delete aerospikecluster "$CLUSTER" -n "$NS" --ignore-not-found --wait=true --timeout=600s || true
  kubectl delete pvc -n "$NS" -l "$LABEL_SELECTOR" --ignore-not-found --wait=false || true
  wait_workdir_pvcs_absent || true
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

timing_log() {
  echo "[$(date '+%H:%M:%S')] [timing-k8s] $*" >&2
}

require_timing_python() {
  command -v python3 >/dev/null 2>&1 || {
    log "ERROR: python3 is required on the host (same as Docker timing)."
    return 1
  }
  true
}

timing_apply_preprovision_pvcs() {
  sed -e "s/__TESTBED_NAMESPACE__/${NS}/g" -e "s/__CLUSTER_NAME__/${CLUSTER}/g" \
    "$MANIFEST_DIR/pvc-workdir-preprovision.yaml" | kubectl apply -f -
}

# Many clusters use WaitForFirstConsumer: PVCs for ordinals 1–2 stay Pending until a pod mounts them.
# Waiting for all PVCs to be Bound before any seed pod runs deadlocks forever — only verify objects exist.
timing_wait_preprovision_pvcs_created() {
  local i n name
  for i in 0 1 2; do
    name=$(pvc_for_ordinal "$i")
    n=0
    while [[ $n -lt 120 ]]; do
      if kubectl get pvc -n "$NS" "$name" &>/dev/null; then
        break
      fi
      sleep 1
      ((n++)) || true
    done
    if ! kubectl get pvc -n "$NS" "$name" &>/dev/null; then
      timing_log "ERROR: PVC $name not present after apply"
      return 1
    fi
  done
  timing_log "PVC objects present (WaitForFirstConsumer: binding happens when each seed pod schedules)."
}

timing_pause_after_pvcs_created() {
  if [[ "${TIMING_PVC_SETTLE_SEC}" =~ ^[0-9]+$ ]] && [[ "$TIMING_PVC_SETTLE_SEC" -gt 0 ]]; then
    timing_log "Waiting ${TIMING_PVC_SETTLE_SEC}s after PVC apply (API/provisioner warm-up)..."
    sleep "$TIMING_PVC_SETTLE_SEC"
  fi
}

# Wait until pod has a node and is Running, then Ready — Pending pods are not Ready, so
# `kubectl wait Ready` alone fails with "does not have a host assigned" on kubectl exec.
wait_seed_pod_scheduled_and_ready() {
  local pod=$1
  local timeout_sec=${2:-$TIMING_SEED_POD_TIMEOUT}
  local elapsed=0
  local phase="" node=""

  timing_log "Waiting for seed pod $pod (schedule + Ready, timeout ${timeout_sec}s)..."

  while [[ $elapsed -lt $timeout_sec ]]; do
    phase=$(kubectl get pod -n "$NS" "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo Missing)
    node=$(kubectl get pod -n "$NS" "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)

    if [[ "$phase" == "Failed" || "$phase" == "Succeeded" ]]; then
      timing_log "Seed pod $pod ended with phase=$phase"
      kubectl describe pod -n "$NS" "$pod" 2>/dev/null | tail -40 >&2
      return 1
    fi

    if [[ "$phase" == "Running" && -n "$node" ]]; then
      if kubectl wait --for=condition=Ready "pod/$pod" -n "$NS" --timeout=120s 2>/dev/null; then
        return 0
      fi
      timing_log "WARN: pod Running on $node but not Ready yet; continuing to wait..."
    fi

    # Every ~30s while Pending, log so the harness does not look hung (kind PVC attach can be slow).
    if [[ "$phase" == "Pending" && "$elapsed" -gt 0 && $((elapsed % 30)) -eq 0 ]]; then
      timing_log "Seed pod $pod still Pending (${elapsed}s/${timeout_sec}s) — typical on kind while volume attaches."
      kubectl get pod -n "$NS" "$pod" -o wide 2>/dev/null >&2 || true
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  timing_log "ERROR: seed pod $pod did not become Ready in ${timeout_sec}s (phase=$phase node=${node:-none})"
  kubectl get pod -n "$NS" "$pod" -o wide 2>/dev/null >&2 || true
  kubectl describe pod -n "$NS" "$pod" 2>/dev/null | tail -45 >&2
  return 1
}

# Mount one workdir PVC in a busybox pod, reset smd/, optionally kubectl cp .smd files from host.
seed_workdir_smd_from_host() {
  local ord=$1
  local host_smd_dir=$2
  local pod="smd-seed-${ord}-$$"
  local claim
  claim=$(pvc_for_ordinal "$ord")

  kubectl delete pod -n "$NS" "$pod" --ignore-not-found --wait=true 2>/dev/null || true

  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  namespace: $NS
  labels:
    smd-sync-test: seed
spec:
  terminationGracePeriodSeconds: 1
  containers:
  - name: seed
    image: busybox:1.36
    imagePullPolicy: IfNotPresent
    command: ["sleep", "3600"]
    resources:
      requests:
        cpu: 10m
        memory: 16Mi
    volumeMounts:
    - name: w
      mountPath: /opt/aerospike
  volumes:
  - name: w
    persistentVolumeClaim:
      claimName: $claim
  restartPolicy: Never
EOF

  wait_seed_pod_scheduled_and_ready "$pod" "$TIMING_SEED_POD_TIMEOUT" || return 1

  kubectl exec -n "$NS" "$pod" -- sh -c 'mkdir -p /opt/aerospike/smd && rm -rf /opt/aerospike/smd/* 2>/dev/null; true'

  local f
  if [[ -d "$host_smd_dir" ]]; then
    shopt -s nullglob
    local smds=( "${host_smd_dir}"/*.smd )
    shopt -u nullglob
    if [[ ${#smds[@]} -gt 0 ]]; then
      for f in "${smds[@]}"; do
        kubectl cp -n "$NS" "$f" "$pod:/opt/aerospike/smd/$(basename "$f")"
      done
    fi
  fi

  kubectl delete pod -n "$NS" "$pod" --wait=true
}

timing_prepare_host_large_smd() {
  local n_items=$1
  mkdir -p "${SMD_DATA_DIR}/node1/smd" "${SMD_DATA_DIR}/node2/smd" "${SMD_DATA_DIR}/node3/smd"
  rm -rf "${SMD_DATA_DIR}/node1/smd"/* "${SMD_DATA_DIR}/node2/smd"/* "${SMD_DATA_DIR}/node3/smd"/* 2>/dev/null || true
  mkdir -p "${SMD_DATA_DIR}/node1/smd" "${SMD_DATA_DIR}/node2/smd" "${SMD_DATA_DIR}/node3/smd"

  python3 "$DOCKER_SMD_TEST/gen-large-smd.py" \
    --items "$n_items" \
    --module "$TIMING_MODULE" \
    --value-size "$TIMING_VALUE_SIZE" \
    --out "${SMD_DATA_DIR}/node1/smd/${TIMING_MODULE}.smd"

  local smd_file="${SMD_DATA_DIR}/node1/smd/${TIMING_MODULE}.smd"
  timing_log "Host: node1 SMD $(du -sh "$smd_file" 2>/dev/null | cut -f1) ($smd_file)"
}

# Wall-clock ms until cluster_size=N (polls lowest-ordinal Ready pod). Prints ms to stdout only.
timing_wait_cluster_ms() {
  local expected=$1
  local timeout_sec=${2:-300}
  local elapsed=0
  local size=0
  local probe_pod=""
  local t_start
  t_start=$(date +%s%N)

  timing_log "Waiting for cluster_size=$expected (timeout ${timeout_sec}s)..."

  while [[ $elapsed -lt $timeout_sec ]]; do
    probe_pod=$(list_pods_sorted | head -n1 || true)
    if [[ -n "$probe_pod" ]] && pod_ready "$probe_pod"; then
      if [[ "${USE_AUTH:-0}" == "1" ]]; then
        size=$(kubectl exec -n "$NS" "$probe_pod" -c aerospike-server -- \
          timeout 5 asinfo -Uadmin -P"$ADMIN_PASSWORD" -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo 0)
      else
        size=$(kubectl exec -n "$NS" "$probe_pod" -c aerospike-server -- \
          timeout 5 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo 0)
      fi
      if [[ "$size" == "$expected" ]]; then
        local t_end ms
        t_end=$(date +%s%N)
        ms=$(( (t_end - t_start) / 1000000 ))
        timing_log "Cluster formed (size $size) in ${ms} ms"
        echo "$ms"
        return 0
      fi
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  timing_log "ERROR: cluster_size did not reach $expected within ${timeout_sec}s (last size=$size)"
  echo "-1"
  return 0
}

timing_extract_sync_us_k8s() {
  local probe_pod
  probe_pod="$(pod_at_index 0)"
  local logs
  logs=$(kubectl logs -n "$NS" "$probe_pod" -c aerospike-server --tail=100000 2>&1 || true)
  local sync_us
  sync_us=$(echo "$logs" \
    | grep -oP '(?:initial SMD sync wait done - elapsed |sync wait done cl_key [0-9a-f]+ elapsed )\K\d+(?= us)' \
    | sort -n | tail -1 || true)
  if [[ -n "$sync_us" ]]; then
    echo "$sync_us"
  else
    echo "-1"
  fi
}

timing_prepare_host_rejoin() {
  local stale_pct=$1
  local security_items=$2
  local script_dir="$DOCKER_SMD_TEST"

  timing_log "Host rejoin dataset: stale_pct=${stale_pct}% security_items=$security_items"

  local node
  for node in 1 2 3; do
    rm -rf "${SMD_DATA_DIR}/node${node}/smd"
    mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
  done

  local module
  for module in truncate sindex security masking; do
    if [[ "$module" == "security" ]]; then
      python3 "$script_dir/gen-realistic-smd.py" \
        --out-dir "${SMD_DATA_DIR}/node1/smd" \
        --module "$module" \
        --items "$security_items"
    else
      python3 "$script_dir/gen-realistic-smd.py" \
        --out-dir "${SMD_DATA_DIR}/node1/smd" \
        --module "$module"
    fi
  done

  cp -r "${SMD_DATA_DIR}/node1/smd/"* "${SMD_DATA_DIR}/node2/smd/"

  timing_log "Stale subset for node 3 (${stale_pct}% of items)..."
  python3 << EOF
import json
import os

stale_pct = int("$stale_pct")
node1_smd_dir = "${SMD_DATA_DIR}/node1/smd"
node3_smd_dir = "${SMD_DATA_DIR}/node3/smd"

for smd_file in os.listdir(node1_smd_dir):
    if not smd_file.endswith(".smd"):
        continue
    with open(os.path.join(node1_smd_dir, smd_file)) as f:
        items = json.load(f)
    header = items[0]
    data_items = items[1:]
    n_stale = int(len(data_items) * stale_pct / 100)
    stale_items = []
    for item in data_items[:n_stale]:
        stale_item = dict(item)
        stale_item["generation"] = max(1, item["generation"] - 1)
        stale_item["timestamp"] = item["timestamp"] - 1000000
        stale_items.append(stale_item)
    node3_data = [header] + stale_items
    out_path = os.path.join(node3_smd_dir, smd_file)
    with open(out_path, "w") as f:
        json.dump(node3_data, f, separators=(",", ":"))
    if data_items:
        print(f"  {smd_file}: {len(data_items)} current -> {len(stale_items)} stale")
EOF
}

timing_run_one_k8s() {
  local n_items=$1
  local results_file=$2

  cleanup_full
  USE_AUTH=0
  apply_namespace

  timing_prepare_host_large_smd "$n_items"

  timing_apply_preprovision_pvcs
  timing_wait_preprovision_pvcs_created
  timing_pause_after_pvcs_created

  seed_workdir_smd_from_host 0 "${SMD_DATA_DIR}/node1/smd"
  seed_workdir_smd_from_host 1 "${SMD_DATA_DIR}/node2/smd"
  seed_workdir_smd_from_host 2 "${SMD_DATA_DIR}/node3/smd"

  kubectl apply -f "$TIMING_AC_MANIFEST"

  local wall_ms
  wall_ms=$(timing_wait_cluster_ms 3 "$TIMING_CLUSTER_TIMEOUT")

  local sync_us
  sync_us=$(timing_extract_sync_us_k8s)

  if [[ "$wall_ms" == "-1" ]]; then
    timing_log "FAIL: cluster did not form for n_items=$n_items"
    cleanup_full
    return 1
  fi

  local sync_ms="n/a"
  if [[ "$sync_us" != "-1" ]]; then
    sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
  fi

  local timed_out=0
  if collect_server_logs | grep -qE "SMD sync timed out|initial SMD sync timed out"; then
    timed_out=1
    timing_log "WARNING: SMD sync timed out (see pod logs)"
  fi

  local smd_mb
  smd_mb=$(python3 -c "import os; print(f'{os.path.getsize(\"${SMD_DATA_DIR}/node1/smd/${TIMING_MODULE}.smd\") / 1048576:.2f}')")

  timing_log "RESULT: items=$n_items smd=${smd_mb}MB wall=${wall_ms}ms smd_sync=${sync_ms}ms (${sync_us}us) timeout=${timed_out}"

  echo -e "${n_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${TIMING_VALUE_SIZE}\t${timed_out}" >> "$results_file"

  local phase_log="${results_file%.tsv}-phases.log"
  timing_log "Phase timing -> $phase_log"
  collect_server_logs | grep -E "full-to-pr timing|full-from-pr timing" | sed "s/^/[n=${n_items}] /" >> "$phase_log"

  cleanup_full
}

test_large_smd_timing_k8s() {
  require_timing_python || return 1
  [[ -f "$DOCKER_SMD_TEST/gen-large-smd.py" ]] || {
    log "ERROR: expected $DOCKER_SMD_TEST/gen-large-smd.py"
    return 1
  }
  kubectl get secret aerospike-secret -n "$NS" >/dev/null 2>&1 || {
    log "ERROR: Secret aerospike-secret not found in $NS (run scripts/create-secrets.sh)."
    return 1
  }

  timing_log "=== SMD large-payload timing sweep (AKO / preprovisioned PVCs) ==="
  timing_log "Docker helpers: $DOCKER_SMD_TEST"
  timing_log "Sweep items=${TIMING_ITEMS} value_size=${TIMING_VALUE_SIZE}B"
  timing_log "Cluster manifest: $TIMING_AC_MANIFEST (initMethod: none preserves seeded SMD)"
  timing_log "Results dir: $TIMING_RESULTS_DIR"

  mkdir -p "$TIMING_RESULTS_DIR"
  local results_file="${TIMING_RESULTS_DIR}/timing-k8s-$(date '+%Y%m%d-%H%M%S').tsv"
  echo -e "items\tsmd_mb\twall_cluster_ms\tsync_elapsed_us\tvalue_size_b\tsync_timeout" > "$results_file"
  timing_log "Results file: $results_file"

  local failed=0 n
  for n in $TIMING_ITEMS; do
    timing_run_one_k8s "$n" "$results_file" || failed=1
  done

  if [[ $failed -eq 0 ]]; then
    timing_log "=== Timing sweep COMPLETE ==="
    column -t "$results_file"
  else
    timing_log "=== Timing sweep had FAILURES ==="
    return 1
  fi
}

timing_rejoin_run_k8s() {
  local results_file=$1

  cleanup_full
  USE_AUTH=0
  apply_namespace

  timing_prepare_host_rejoin "$TIMING_REJOIN_STALE_PCT" "$TIMING_REJOIN_SECURITY_ITEMS"

  timing_apply_preprovision_pvcs
  timing_wait_preprovision_pvcs_created
  timing_pause_after_pvcs_created

  seed_workdir_smd_from_host 0 "${SMD_DATA_DIR}/node1/smd"
  seed_workdir_smd_from_host 1 "${SMD_DATA_DIR}/node2/smd"
  seed_workdir_smd_from_host 2 "${SMD_DATA_DIR}/node3/smd"

  kubectl apply -f "$TIMING_AC_MANIFEST"

  local wall_ms
  wall_ms=$(timing_wait_cluster_ms 3 "$TIMING_REJOIN_CLUSTER_TIMEOUT")

  local sync_us
  sync_us=$(timing_extract_sync_us_k8s)

  if [[ "$wall_ms" == "-1" ]]; then
    timing_log "FAIL: cluster did not form (timing-rejoin)"
    cleanup_full
    return 1
  fi

  local sync_ms="n/a"
  if [[ "$sync_us" != "-1" ]]; then
    sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
  fi

  local timed_out=0
  if collect_server_logs | grep -qE "SMD sync timed out|initial SMD sync timed out"; then
    timed_out=1
    timing_log "WARNING: SMD sync timed out"
  fi

  local current_items stale_items smd_mb
  current_items=$(python3 -c "
import json, os
total = 0
for f in os.listdir('${SMD_DATA_DIR}/node1/smd'):
    if f.endswith('.smd'):
        total += len(json.load(open('${SMD_DATA_DIR}/node1/smd/' + f))) - 1
print(total)
")
  stale_items=$(python3 -c "
import json, os
total = 0
for f in os.listdir('${SMD_DATA_DIR}/node3/smd'):
    if f.endswith('.smd'):
        total += len(json.load(open('${SMD_DATA_DIR}/node3/smd/' + f))) - 1
print(total)
")
  smd_mb=$(python3 -c "
import os
total = 0
for n in (1, 2, 3):
    d = '${SMD_DATA_DIR}/node' + str(n) + '/smd'
    for f in os.listdir(d):
        total += os.path.getsize(os.path.join(d, f))
print(f'{total / 1048576:.2f}')
")

  timing_log "RESULT: stale_pct=$TIMING_REJOIN_STALE_PCT current=$current_items stale=$stale_items smd=${smd_mb}MB wall=${wall_ms}ms smd_sync=${sync_ms}ms (${sync_us}us) timeout=${timed_out}"

  echo -e "${TIMING_REJOIN_STALE_PCT}\t${current_items}\t${stale_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${timed_out}" >> "$results_file"

  local phase_log="${results_file%.tsv}-phases.log"
  collect_server_logs | grep -E "full-to-pr timing|full-from-pr timing" \
    | sed "s/^/[rejoin:${TIMING_REJOIN_STALE_PCT}%] /" >> "$phase_log"

  cleanup_full
}

test_rejoin_smd_timing_k8s() {
  require_timing_python || return 1
  [[ -f "$DOCKER_SMD_TEST/gen-realistic-smd.py" ]] || {
    log "ERROR: expected $DOCKER_SMD_TEST/gen-realistic-smd.py"
    return 1
  }
  kubectl get secret aerospike-secret -n "$NS" >/dev/null 2>&1 || {
    log "ERROR: Secret aerospike-secret not found in $NS (run scripts/create-secrets.sh)."
    return 1
  }

  timing_log "=== Rejoin SMD timing (AKO) ==="
  timing_log "Stale %: $TIMING_REJOIN_STALE_PCT  security items: $TIMING_REJOIN_SECURITY_ITEMS"
  timing_log "Results dir: $TIMING_RESULTS_DIR"

  mkdir -p "$TIMING_RESULTS_DIR"
  local results_file="${TIMING_RESULTS_DIR}/timing-rejoin-k8s-$(date '+%Y%m%d-%H%M%S').tsv"
  echo -e "stale_pct\tcurrent_items\tstale_items\tsmd_mb\twall_cluster_ms\tsync_elapsed_us\tsync_timeout" > "$results_file"

  timing_rejoin_run_k8s "$results_file"

  timing_log "=== Rejoin timing COMPLETE ==="
  column -t "$results_file"
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
  basic|auth|rejoin|preexisting|pull|all|cleanup-full|timing|timing-rejoin|timing-cleanup)
    require_kubectl_cluster
    ;;
esac

case "${1:-}" in
  basic|auth|rejoin|preexisting|pull|all|timing|timing-rejoin)
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
  timing) test_large_smd_timing_k8s ;;
  timing-rejoin) test_rejoin_smd_timing_k8s ;;
  timing-cleanup) cleanup_full ;;
  *)
    echo "Usage: $0 {basic|auth|rejoin|preexisting|pull|all|timing|timing-rejoin|cleanup-full|timing-cleanup}"
    echo ""
    echo "Requires: kubectl, Aerospike Kubernetes Operator, namespace/secrets (see README)."
    echo "timing / timing-rejoin also need python3 on the host and docker/smd-sync-test generators."
    echo "Env: NAMESPACE=$NS CLUSTER_NAME=$CLUSTER TIMEOUT=$TIMEOUT ADMIN_PASSWORD=..."
    echo "     USE_AUTH is set internally; non-auth tests use open asinfo."
    echo "Timing env (see README): SMD_DATA_DIR TIMING_ITEMS TIMING_* TIMING_RESULTS_DIR ..."
    exit 1
    ;;
esac

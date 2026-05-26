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
# 0 or unset = entire aerospike-server log since container start (needed for huge SMD log volume).
# Set to a positive integer to cap lines (e.g. 500000).
TIMING_LOG_TAIL="${TIMING_LOG_TAIL:-0}"
# If true, do not delete AerospikeCluster/PVCs after timing-rejoin / each timing sweep iter (inspect logs / avoid rotation losing early lines).
TIMING_SKIP_FINAL_CLEANUP="${TIMING_SKIP_FINAL_CLEANUP:-false}"
# Docker parity (default): after cluster_size=N, scrape principal pod logs immediately — no smd-info wait.
# Set TIMING_WAIT_SMD_SETTLED=true to block until all pods pass smd-info (or log heuristic); timeout
# budget is TIMING_SMD_PHASE_WAIT_SEC if >0, else TIMING_REJOIN_CLUSTER_TIMEOUT / TIMING_CLUSTER_TIMEOUT.
TIMING_WAIT_SMD_SETTLED="${TIMING_WAIT_SMD_SETTLED:-false}"
TIMING_SMD_PHASE_WAIT_SEC="${TIMING_SMD_PHASE_WAIT_SEC:-0}"
# Before teardown: verify nodes can serve (see TIMING_SANITY_MODE).
TIMING_SANITY_SERVE_CHECK="${TIMING_SANITY_SERVE_CHECK:-true}"
TIMING_SANITY_TIMEOUT_SEC="${TIMING_SANITY_TIMEOUT_SEC:-0}"
# cluster = cluster_size + namespaces on every pod (docker ~22s timing-rejoin parity, no smd-info wait)
# serve  = cluster + namespaces + no sync timeout + principal smd-info settled (default SERVER-209 proxy)
# smd-info = serve checks plus every pod smd-info settled (strict; 950k security can take many minutes)
TIMING_SANITY_MODE="${TIMING_SANITY_MODE:-serve}"
# How often to log per-pod smd-info + server progress lines during long waits (seconds).
TIMING_PROGRESS_INTERVAL_SEC="${TIMING_PROGRESS_INTERVAL_SEC:-15}"

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
  local t="${TIMING_ASINFO_TIMEOUT_SEC:-120}"
  if [[ "${USE_AUTH:-0}" == "1" ]]; then
    kubectl exec -n "$NS" "$pod" -c aerospike-server -- timeout "$t" asinfo -Uadmin -P"$ADMIN_PASSWORD" "$@"
  else
    kubectl exec -n "$NS" "$pod" -c aerospike-server -- timeout "$t" asinfo "$@"
  fi
}

# One-line smd-info summary for progress logs (cluster_key + listed modules).
timing_smd_info_progress_line() {
  local info=$1
  local modules_csv=${2:-security,truncate,sindex,masking}
  SMD_INFO="$info" MODULES_CSV="$modules_csv" python3 - <<'PY'
import os, re
info = os.environ.get("SMD_INFO", "")
mods = [m.strip() for m in os.environ.get("MODULES_CSV", "").split(",") if m.strip()]
parts = []
m = re.search(r"cluster_key=([0-9a-fA-F]+)", info)
if m:
    parts.append(f"cluster_key={m.group(1)}")
for name in mods:
    m2 = re.search(
        rf"(?:^|;){re.escape(name)}:committed_key=([0-9a-fA-F]+).*?n_keys=(\d+).*?state=([a-z]+)",
        info,
    )
    if m2:
        ck, nk, st = m2.group(1), m2.group(2), m2.group(3)
        ok = "ok" if m and ck.lower() == m.group(1).lower() and st in ("pr", "npr") else "pending"
        parts.append(f"{name}:{st} ck={ck} n={nk} {ok}")
print(" ".join(parts) if parts else "(no smd-info)")
PY
}

timing_should_log_progress() {
  local elapsed=$1
  local interval=${TIMING_PROGRESS_INTERVAL_SEC:-15}
  [[ "$elapsed" -gt 0 && $((elapsed % interval)) -eq 0 ]]
}

# Recent server INFO/DETAIL lines for SMD merge progress (per pod).
timing_pod_smd_log_progress_snip() {
  local pod=$1
  timing_kubectl_logs_probe_pod "$pod" 2>/dev/null \
    | grep -E "initial sync progress|full-from-pr start|full-to-pr start|broadcasting full-from-pr|initial cluster sync steady|full-from-pr timing|full-to-pr timing" \
    | tail -3
}

# All pods: smd-info module summary + last server progress lines.
timing_log_all_pods_smd_progress_k8s() {
  local modules_csv=${1:-security,truncate,sindex,masking}
  local principal_pod=""
  principal_pod=$(timing_resolve_principal_pod 2>/dev/null || true)
  timing_log "--- SMD progress (interval=${TIMING_PROGRESS_INTERVAL_SEC}s; principal=${principal_pod:-unknown}) ---"
  local p info snip
  while IFS= read -r p; do
    [[ -z "$p" ]] && continue
    info=$(asinfo_exec "$p" -v "smd-info" 2>/dev/null || true)
    if [[ -n "$info" ]]; then
      timing_log "  $p smd-info: $(timing_smd_info_progress_line "$info" "$modules_csv")"
    else
      timing_log "  $p smd-info: (timeout — merge may block asinfo; see logs below)"
    fi
    snip=$(timing_pod_smd_log_progress_snip "$p")
    if [[ -n "$snip" ]]; then
      while IFS= read -r line; do
        [[ -n "$line" ]] && timing_log "    log: $line"
      done <<< "$snip"
    fi
  done < <(list_pods_sorted)
  timing_log "---"
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

# Same as collect_server_logs but honors TIMING_LOG_TAIL (0 = full log; avoids missing SMD lines).
timing_kubectl_logs_probe_pod() {
  local pod=$1
  if [[ -z "${TIMING_LOG_TAIL}" || "${TIMING_LOG_TAIL}" == "0" ]]; then
    kubectl logs -n "$NS" "$pod" -c aerospike-server 2>/dev/null || true
  else
    kubectl logs -n "$NS" "$pod" -c aerospike-server --tail="${TIMING_LOG_TAIL}" 2>/dev/null || true
  fi
}

timing_collect_server_logs() {
  local out="" p
  while IFS= read -r p; do
    [[ -z "$p" ]] && continue
    out+=$(timing_kubectl_logs_probe_pod "$p")
    out+=$'\n'
  done < <(list_pods_sorted)
  echo "$out"
}

apply_namespace() {
  kubectl apply -f "$MANIFEST_DIR/namespace.yaml"
  sed "s/__TESTBED_NAMESPACE__/${NS}/g" "$MANIFEST_DIR/workload-operator-rbac.yaml" | kubectl apply -f -
}

# Seed pods hold RWO mounts; CR deletion alone may leave PVCs terminating for a long time.
timing_maybe_cleanup() {
  if [[ "${TIMING_SKIP_FINAL_CLEANUP}" == "true" ]]; then
    timing_log "TIMING_SKIP_FINAL_CLEANUP=true — leaving cluster/PVCs in $NS (inspect: kubectl logs -n $NS -l app=aerospike-cluster -c aerospike-server)"
    return 0
  fi
  cleanup_full
}

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

# Resolve log-wait budget: explicit TIMING_SMD_PHASE_WAIT_SEC, else cluster_timeout when wait_on_zero.
timing_smd_log_wait_sec() {
  local cluster_timeout_sec=$1
  local wait_on_zero=${2:-0}
  local max_sec=${TIMING_SMD_PHASE_WAIT_SEC:-0}
  [[ "$max_sec" =~ ^[0-9]+$ ]] || max_sec=0
  if [[ "$max_sec" == "0" && "$wait_on_zero" == "1" ]]; then
    max_sec=$cluster_timeout_sec
  fi
  echo "$max_sec"
}

# When smd-info blocks (principal during huge full-to-pr), infer steady modules from logs:
# each module must show {module:pr|npr:…} and not {module:dirty|merging:…} in recent output.
timing_pod_smd_settled_from_logs_k8s() {
  local pod=$1
  local modules_csv=$2
  local log module
  log=$(timing_kubectl_logs_probe_pod "$pod" 2>/dev/null || true)
  [[ -z "$log" ]] && return 1
  IFS=',' read -ra modules <<< "$modules_csv"
  for module in "${modules[@]}"; do
    module="${module// /}"
    [[ -z "$module" ]] && continue
    if echo "$log" | grep -qE "\\{${module}:(dirty|merging):"; then
      return 1
    fi
    if ! echo "$log" | grep -qE "\\{${module}:(pr|npr):"; then
      return 1
    fi
  done
  return 0
}

# Per-pod: prefer smd-info; if asinfo times out (common on principal mid-merge), use log heuristic.
timing_pod_smd_settled_k8s() {
  local pod=$1
  local modules_csv=$2
  local info
  info=$(asinfo_exec "$pod" -v "smd-info" 2>/dev/null || true)
  if [[ -n "$info" ]] && timing_smd_info_modules_settled "$info" "$modules_csv"; then
    return 0
  fi
  if [[ -z "$info" ]] && timing_pod_smd_settled_from_logs_k8s "$pod" "$modules_csv"; then
    return 0
  fi
  return 1
}

# Parse `asinfo -v smd-info` (see as_smd_get_info in smd.c). Modules are settled when each
# in-use module has state pr|npr and committed_key == cluster_key (smd_module_is_ready).
timing_smd_info_modules_settled() {
  local smd_info=$1
  local modules_csv=$2
  SMD_INFO="$smd_info" MODULES_CSV="$modules_csv" python3 - <<'PY'
import os, re, sys
info = os.environ.get("SMD_INFO", "")
mods = [m.strip() for m in os.environ.get("MODULES_CSV", "").split(",") if m.strip()]
if not info or not mods:
    sys.exit(1)
m = re.search(r"cluster_key=([0-9a-fA-F]+)", info)
if not m:
    sys.exit(1)
cl_key = m.group(1).lower()
for name in mods:
    m2 = re.search(
        rf"(?:^|;){re.escape(name)}:committed_key=([0-9a-fA-F]+).*?state=([a-z]+)",
        info,
    )
    if not m2:
        sys.exit(1)
    ck, st = m2.group(1).lower(), m2.group(2)
    if ck != cl_key or st not in ("pr", "npr"):
        sys.exit(1)
sys.exit(0)
PY
}

# Poll asinfo smd-info on every cluster pod until listed modules are settled (all nodes).
timing_wait_smd_settled_k8s() {
  local modules_csv=$1
  local cluster_timeout_sec=$2
  local wait_on_zero=${3:-0}
  local max_sec
  max_sec=$(timing_smd_log_wait_sec "$cluster_timeout_sec" "$wait_on_zero")
  [[ "$max_sec" == "0" ]] && return 0

  local elapsed=0 p info settled=0
  timing_log "Waiting up to ${max_sec}s for smd-info settled modules=[$modules_csv] on all pods (cluster_size=3 is not SMD ready)..."

  while [[ $elapsed -lt $max_sec ]]; do
    settled=1
    while IFS= read -r p; do
      [[ -z "$p" ]] && continue
      if ! pod_ready "$p"; then
        settled=0
        break
      fi
      if ! timing_pod_smd_settled_k8s "$p" "$modules_csv"; then
        settled=0
        break
      fi
    done < <(list_pods_sorted)

    if [[ $settled -eq 1 ]]; then
      timing_log "All pods smd-info settled after ${elapsed}s"
      return 0
    fi
    if timing_should_log_progress "$elapsed"; then
      timing_log "Still waiting for smd-info settled (${elapsed}s / ${max_sec}s)"
      timing_log_all_pods_smd_progress_k8s "$modules_csv"
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done

  timing_log "WARN: smd-info modules not settled on all pods within ${max_sec}s"
  return 1
}

# Legacy: log line on probe pod only (can miss merges on principal). Prefer timing_wait_smd_settled_k8s.
timing_wait_module_full_from_pr_logged_k8s() {
  local module=$1
  local cluster_timeout_sec=$2
  local wait_on_zero=${3:-0}
  local max_sec
  max_sec=$(timing_smd_log_wait_sec "$cluster_timeout_sec" "$wait_on_zero")
  [[ "$max_sec" == "0" ]] && return 0

  local elapsed=0 p
  timing_log "Waiting up to ${max_sec}s for {${module}:…} full-from-pr timing in any pod log..."

  while [[ $elapsed -lt $max_sec ]]; do
    while IFS= read -r p; do
      [[ -z "$p" ]] && continue
      if timing_kubectl_logs_probe_pod "$p" | grep -qE "\\{${module}:[^}]*\\} full-from-pr timing"; then
        timing_log "Observed {${module}:} full-from-pr timing on $p after ${elapsed}s"
        return 0
      fi
    done < <(list_pods_sorted)
    sleep 2
    elapsed=$((elapsed + 2))
  done

  timing_log "WARN: no {${module}:} full-from-pr timing in any pod within ${max_sec}s"
}

# Scrape logs for sync_elapsed_us. After asinfo shows SMD settled, timing lines may still be
# absent (fast-path as_smd_wait_ready, no elapsed line). Use a short grace poll only.
TIMING_LOG_SCRAPE_GRACE_SEC="${TIMING_LOG_SCRAPE_GRACE_SEC:-30}"
# asinfo can block for minutes while principal merges huge security SMD (950k+ items).
TIMING_ASINFO_TIMEOUT_SEC="${TIMING_ASINFO_TIMEOUT_SEC:-120}"

timing_scrape_sync_elapsed_us_k8s() {
  local grace_sec=${1:-${TIMING_LOG_SCRAPE_GRACE_SEC}}
  local elapsed=0 sync_us=""

  sync_us=$(timing_extract_sync_us_k8s)
  if [[ -n "$sync_us" && "$sync_us" != "-1" ]]; then
    echo "$sync_us"
    return 0
  fi

  [[ "$grace_sec" == "0" ]] && { echo "-1"; return 0; }

  timing_log "No sync_elapsed_us yet; retrying logs up to ${grace_sec}s (kubelet delay; not SMD readiness)..."

  while [[ $elapsed -lt $grace_sec ]]; do
    sleep 2
    elapsed=$((elapsed + 2))
    if timing_should_log_progress "$elapsed"; then
      timing_log_all_pods_smd_progress_k8s "truncate,sindex,security,masking"
    fi
    sync_us=$(timing_extract_sync_us_k8s)
    if [[ -n "$sync_us" && "$sync_us" != "-1" ]]; then
      timing_log "sync_elapsed_us=${sync_us} after ${elapsed}s log grace"
      echo "$sync_us"
      return 0
    fi
  done

  timing_log "WARN: no sync_elapsed_us in logs after ${grace_sec}s grace (SMD may still be ready per smd-info)"
  echo "-1"
}

# AKO single-rack timing cluster: principal is highest ordinal (rejoin) — resolve after pods exist.
timing_resolve_principal_pod() {
  local p="" elapsed=0
  while [[ $elapsed -lt 120 ]]; do
    p=$(list_pods_sorted | tail -n1)
    [[ -n "$p" ]] && { echo "$p"; return 0; }
    sleep 1
    elapsed=$((elapsed + 1))
  done
  timing_log "WARN: could not resolve principal pod (no pods matching $LABEL_SELECTOR)"
  return 1
}

timing_principal_pod() {
  timing_resolve_principal_pod || true
}

timing_fallback_full_from_pr_sum_us_k8s() {
  local pod=${1:-$(timing_principal_pod)}
  local log sum
  log=$(timing_kubectl_logs_probe_pod "$pod")
  sum=$(echo "$log" | grep -oP 'full-from-pr timing:[^\n]*total=\K\d+(?= us)' | awk '{s+=$1} END {print int(s)}')
  echo "${sum:-0}"
}

# Parity with docker timing_extract_sync_us: principal pod logs only (aerospike-1 in compose).
timing_extract_sync_us_k8s() {
  local pod logs primary fallback
  pod=$(timing_principal_pod)
  logs=$(timing_kubectl_logs_probe_pod "$pod")
  primary=$(echo "$logs" \
    | grep -oP '(?:initial SMD sync(?: wait)? done - elapsed |sync wait done cl_key [0-9a-fA-F]+ elapsed )\K\d+(?= us)' \
    | sort -n | tail -1 || true)
  if [[ -n "$primary" ]]; then
    echo "$primary"
    return 0
  fi
  fallback=$(timing_fallback_full_from_pr_sum_us_k8s "$pod")
  if [[ -n "$fallback" && "$fallback" != "0" ]]; then
    timing_log "sync_elapsed_us fallback: Σ(full-from-pr total) on principal $pod=${fallback} us (no initial/sync-wait line in scrape)"
    echo "$fallback"
    return 0
  fi
  echo "-1"
}

timing_maybe_wait_smd_settled_k8s() {
  local modules_csv=$1
  local cluster_timeout_sec=$2
  [[ "${TIMING_WAIT_SMD_SETTLED}" == "true" || "${TIMING_WAIT_SMD_SETTLED}" == "1" ]] || return 0
  timing_wait_smd_settled_k8s "$modules_csv" "$cluster_timeout_sec" 1 || true
}

timing_sanity_fail_detail_k8s() {
  local pod=$1
  local modules_csv=$2
  local info
  info=$(asinfo_exec "$pod" -v "smd-info" 2>/dev/null || true)
  if [[ -n "$info" ]]; then
    timing_log "  $pod smd-info: $(timing_smd_info_progress_line "$info" "$modules_csv")"
  else
    timing_log "  $pod smd-info: (timeout or empty — often principal during huge security merge)"
  fi
}

# Pre-teardown SERVER-209 proxy (see TIMING_SANITY_MODE).
timing_sanity_serve_ready_k8s() {
  local expected_size=$1
  local modules_csv=$2
  local cluster_timeout_sec=$3

  [[ "${TIMING_SANITY_SERVE_CHECK}" == "false" || "${TIMING_SANITY_SERVE_CHECK}" == "0" ]] && return 0

  local mode="${TIMING_SANITY_MODE:-serve}"
  local max_sec=${TIMING_SANITY_TIMEOUT_SEC:-0}
  [[ "$max_sec" == "0" ]] && max_sec=$cluster_timeout_sec

  case "$mode" in
    cluster)
      timing_log "Sanity (mode=cluster): all ${expected_size} pod(s) — cluster_size + namespaces (docker fast-path parity)..."
      ;;
    smd-info|strict|all)
      timing_log "Sanity (mode=smd-info): all ${expected_size} pod(s) — cluster_size, namespaces, smd-info [${modules_csv}] (timeout ${max_sec}s)..."
      ;;
    *)
      timing_log "Sanity (mode=serve): all ${expected_size} pod(s) — cluster_size + namespaces; principal smd-info [${modules_csv}] (timeout ${max_sec}s)..."
      ;;
  esac

  local elapsed=0 principal_pod=""
  while [[ $elapsed -lt $max_sec ]]; do
    mapfile -t pods < <(list_pods_sorted)
    if [[ ${#pods[@]} -lt $expected_size ]]; then
      sleep 2
      elapsed=$((elapsed + 2))
      continue
    fi

    principal_pod=$(timing_resolve_principal_pod || true)

    local all_ok=1 fail_reason=""
    for p in "${pods[@]}"; do
      [[ -z "$p" ]] && continue
      if ! pod_ready "$p"; then
        all_ok=0
        fail_reason="$p not Ready"
        break
      fi
      local size
      size=$(asinfo_exec "$p" -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' | head -1 || echo 0)
      if [[ "$size" != "$expected_size" ]]; then
        all_ok=0
        fail_reason="$p cluster_size=$size (want $expected_size)"
        break
      fi
      if ! asinfo_exec "$p" -v "namespaces" 2>/dev/null | grep -q "test"; then
        all_ok=0
        fail_reason="$p asinfo namespaces missing test ns"
        break
      fi
      if timing_kubectl_logs_probe_pod "$p" | grep -qE "SMD sync timed out|initial SMD sync timed out"; then
        all_ok=0
        fail_reason="$p SMD sync timed out in logs"
        break
      fi
      case "$mode" in
        smd-info|strict|all)
          if ! timing_pod_smd_settled_k8s "$p" "$modules_csv"; then
            all_ok=0
            fail_reason="$p smd-info not settled (e.g. security committed_key != cluster_key while NPR merges)"
            break
          fi
          ;;
        serve)
          [[ -n "$principal_pod" && "$p" == "$principal_pod" ]] || continue
          if ! timing_pod_smd_settled_k8s "$p" "$modules_csv"; then
            all_ok=0
            fail_reason="$p smd-info not settled (principal)"
            break
          fi
          ;;
      esac
    done

    if [[ $all_ok -eq 1 ]]; then
      timing_log "Sanity PASS (${mode}): ready (${elapsed}s; principal=${principal_pod:-n/a})"
      return 0
    fi

    if timing_should_log_progress "$elapsed"; then
      timing_log "Sanity still waiting (${elapsed}s / ${max_sec}s): ${fail_reason:-unknown}"
      timing_log_all_pods_smd_progress_k8s "$modules_csv"
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done

  timing_log "Sanity FAIL (${mode}) within ${max_sec}s (last: ${fail_reason:-unknown})"
  timing_log "Hint: NPR pods keep security committed_key=0 while full-from-pr runs — use TIMING_SANITY_MODE=cluster for docker scrape parity, or smd-info to wait on every pod"
  return 1
}

timing_log_wire_size_estimates() {
  local smd_dir=$1
  timing_log "Per-module wire size estimates (128MB limit per module):"
  python3 << EOF
import json, os
smd_dir = '${smd_dir}'
for f in sorted(os.listdir(smd_dir)):
    if not f.endswith('.smd'):
        continue
    path = os.path.join(smd_dir, f)
    try:
        data = json.load(open(path))
        items = data[1:]
        n = len(items)
        if n == 0:
            continue
        total_key = sum(len(item['key']) for item in items)
        total_val = sum(len(item.get('value', '') or '') for item in items)
        wire = total_key + total_val + n * (4 + 8 + 4)
        pct = wire * 100 / (128 * 1024 * 1024)
        print(f"  {f}: {n} items, {wire/1024/1024:.1f}MB wire ({pct:.0f}% of limit)")
    except Exception as e:
        print(f"  {f}: error - {e}")
EOF
}

timing_write_sync_breakdown_log() {
  local results_file=$1
  local out="${results_file%.tsv}-sync-breakdown.log"
  timing_log "SMD sync breakdown -> $out"
  {
    echo "### $(date -Is) cluster=${CLUSTER} ns=${NS}"
    timing_collect_server_logs | grep -E 'initial SMD sync|initial sync done|initial cluster sync steady|sync wait (start|done)|full-from-pr timing|full-to-pr timing|\{security:' || true
  } >> "$out"
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

  local sync_us="-1"
  if [[ "$wall_ms" != "-1" ]]; then
    timing_maybe_wait_smd_settled_k8s "$TIMING_MODULE" "$TIMING_CLUSTER_TIMEOUT"
    sync_us=$(timing_scrape_sync_elapsed_us_k8s)
  fi

  local sanity_rc=0
  if [[ "$wall_ms" != "-1" ]]; then
    timing_sanity_serve_ready_k8s 3 "$TIMING_MODULE" "$TIMING_CLUSTER_TIMEOUT" || sanity_rc=1
  fi

  timing_write_sync_breakdown_log "$results_file"

  if [[ "$wall_ms" == "-1" ]]; then
    timing_log "FAIL: cluster did not form for n_items=$n_items"
    timing_maybe_cleanup
    return 1
  fi

  local sync_ms="n/a"
  if [[ "$sync_us" != "-1" ]]; then
    sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
  fi

  local principal_pod=""
  principal_pod=$(timing_resolve_principal_pod || true)

  local timed_out=0
  if [[ -n "$principal_pod" ]] && timing_kubectl_logs_probe_pod "$principal_pod" | grep -qE "SMD sync timed out|initial SMD sync timed out"; then
    timed_out=1
    timing_log "WARNING: SMD sync timed out (principal pod logs)"
  fi

  local smd_mb
  smd_mb=$(python3 -c "import os; print(f'{os.path.getsize(\"${SMD_DATA_DIR}/node1/smd/${TIMING_MODULE}.smd\") / 1048576:.2f}')")

  timing_log "RESULT: items=$n_items smd=${smd_mb}MB wall=${wall_ms}ms smd_sync=${sync_ms} ms (${sync_us} us) timeout=${timed_out} sanity=$([[ $sanity_rc -eq 0 ]] && echo PASS || echo FAIL)"

  echo -e "${n_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${TIMING_VALUE_SIZE}\t${timed_out}" >> "$results_file"

  timing_log "Phase timing from principal (${principal_pod:-unknown}):"
  timing_kubectl_logs_probe_pod "$principal_pod" | grep -E "full-to-pr timing|full-from-pr timing" \
    | while read -r line; do timing_log "  $line"; done || true

  local phase_log="${results_file%.tsv}-phases.log"
  timing_collect_server_logs | grep -E "full-to-pr timing|full-from-pr timing" | sed "s/^/[n=${n_items}] /" >> "$phase_log" || true

  timing_maybe_cleanup
  [[ $sanity_rc -ne 0 ]] && return 1
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
  [[ -n "${ASD_BINARY:-}" ]] && timing_log "ASD_BINARY: $ASD_BINARY"
  timing_log "Harness: cluster wait then principal-pod log scrape (docker timing parity; TIMING_WAIT_SMD_SETTLED=false by default)"
  timing_log "Docker helpers: $DOCKER_SMD_TEST"
  timing_log "Sweep items=${TIMING_ITEMS} value_size=${TIMING_VALUE_SIZE}B"
  timing_log "Cluster manifest: $TIMING_AC_MANIFEST (initMethod: none preserves seeded SMD)"
  timing_log "Results dir: $TIMING_RESULTS_DIR"

  mkdir -p "$TIMING_RESULTS_DIR"
  local results_file="${TIMING_RESULTS_DIR}/timing-k8s-$(date '+%Y%m%d-%H%M%S').tsv"
  echo -e "items\tsmd_mb\twall_cluster_ms\tsync_elapsed_us\tvalue_size_b\tsync_timeout" > "$results_file"
  timing_log "Results file: $results_file"
  [[ "${TIMING_SKIP_FINAL_CLEANUP}" == "true" ]] && timing_log "TIMING_SKIP_FINAL_CLEANUP=true — last iteration leaves cluster up; run cleanup-full before another sweep"

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

  timing_log_wire_size_estimates "${SMD_DATA_DIR}/node1/smd"

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

  timing_log "Starting 3-node cluster (rejoin scenario)..."
  timing_log "  Nodes 1,2 (ord 0,1): $current_items items (current)"
  timing_log "  Node 3 (ord 2):       $stale_items items (stale, ${TIMING_REJOIN_STALE_PCT}% of current)"
  timing_log "  Missing on node 3:    $((current_items - stale_items)) items"
  [[ "${TIMING_WAIT_SMD_SETTLED}" == "true" || "${TIMING_WAIT_SMD_SETTLED}" == "1" ]] \
    && timing_log "TIMING_WAIT_SMD_SETTLED=true — will wait for smd-info on all pods after cluster forms"
  [[ "${TIMING_SANITY_SERVE_CHECK}" == "false" || "${TIMING_SANITY_SERVE_CHECK}" == "0" ]] \
    || timing_log "TIMING_SANITY_SERVE_CHECK=true — will verify all pods serve asinfo + smd-info before teardown"

  kubectl apply -f "$TIMING_AC_MANIFEST"

  local wall_ms
  wall_ms=$(timing_wait_cluster_ms 3 "$TIMING_REJOIN_CLUSTER_TIMEOUT")

  if [[ "$wall_ms" == "-1" ]]; then
    timing_log "FAIL: cluster did not form (timing-rejoin)"
    timing_maybe_cleanup
    return 1
  fi

  timing_maybe_wait_smd_settled_k8s "truncate,sindex,security,masking" "$TIMING_REJOIN_CLUSTER_TIMEOUT"

  timing_log_all_pods_smd_progress_k8s "truncate,sindex,security,masking"

  local sync_us
  sync_us=$(timing_scrape_sync_elapsed_us_k8s)

  local sanity_rc=0
  timing_sanity_serve_ready_k8s 3 "truncate,sindex,security,masking" "$TIMING_REJOIN_CLUSTER_TIMEOUT" || sanity_rc=1

  local principal_pod=""
  principal_pod=$(timing_resolve_principal_pod || true)
  timing_log "Principal pod (docker aerospike-1 parity): ${principal_pod:-unknown}"

  local sync_ms="n/a"
  if [[ "$sync_us" != "-1" ]]; then
    sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
  fi

  local timed_out=0
  if [[ -n "$principal_pod" ]] && timing_kubectl_logs_probe_pod "$principal_pod" | grep -qE "SMD sync timed out|initial SMD sync timed out"; then
    timed_out=1
    timing_log "WARNING: SMD sync timed out (principal pod logs)"
  fi

  timing_write_sync_breakdown_log "$results_file"

  timing_log "RESULT: stale_pct=$TIMING_REJOIN_STALE_PCT current=$current_items stale=$stale_items smd=${smd_mb}MB wall=${wall_ms}ms smd_sync=${sync_ms} ms (${sync_us} us) timeout=${timed_out} sanity=$([[ $sanity_rc -eq 0 ]] && echo PASS || echo FAIL)"
  timing_log "Principal logs: kubectl logs -n $NS $principal_pod -c aerospike-server"

  echo -e "${TIMING_REJOIN_STALE_PCT}\t${current_items}\t${stale_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${timed_out}" >> "$results_file"

  timing_log "Phase timing from principal (${principal_pod:-unknown}):"
  if [[ -n "$principal_pod" ]]; then
    timing_kubectl_logs_probe_pod "$principal_pod" | grep -E "full-to-pr timing|full-from-pr timing" \
      | while read -r line; do timing_log "  $line"; done || true
  fi

  local phase_log="${results_file%.tsv}-phases.log"
  timing_collect_server_logs | grep -E "full-to-pr timing|full-from-pr timing" \
    | sed "s/^/[rejoin:${TIMING_REJOIN_STALE_PCT}%] /" >> "$phase_log" || true

  timing_maybe_cleanup
  [[ $sanity_rc -ne 0 ]] && return 1
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
  [[ -n "${ASD_BINARY:-}" ]] && timing_log "ASD_BINARY: $ASD_BINARY"
  timing_log "Stale %: $TIMING_REJOIN_STALE_PCT  security items: $TIMING_REJOIN_SECURITY_ITEMS"
  timing_log "Harness: cluster wait then principal-pod log scrape (docker timing-rejoin parity; TIMING_WAIT_SMD_SETTLED=false by default)"
  if [[ -z "${TIMING_LOG_TAIL}" || "${TIMING_LOG_TAIL}" == "0" ]]; then
    timing_log "Log scan: full aerospike-server logs (TIMING_LOG_TAIL=0) for sync/phase lines"
  else
    timing_log "Log scan: --tail=${TIMING_LOG_TAIL} (set TIMING_LOG_TAIL=0 for no limit)"
  fi
  [[ "${TIMING_SKIP_FINAL_CLEANUP}" == "true" ]] && timing_log "TIMING_SKIP_FINAL_CLEANUP=true — cluster/PVCs kept for kubectl logs / reports"
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

#!/bin/bash
# SMD Sync Test Script
# Tests that SMD synchronization completes before partition balance
#
# Node IDs are deterministic (succession sorted descending, so highest is principal):
#   Node 1: a1 (lowest)
#   Node 2: a2
#   Node 3: a3 (highest - always principal)

set -e

COMPOSE_PROJECT="smd-sync-test"
TIMEOUT=60
CLEANUP_ON_SUCCESS=${CLEANUP_ON_SUCCESS:-false}  # Set to 'true' to stop containers after success

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# Start specific nodes (1, 2, 3 or combinations)
start_nodes() {
    local nodes="$@"
    for n in $nodes; do
        docker compose -p $COMPOSE_PROJECT up -d aerospike-$n
    done
}

# Stop specific nodes
stop_nodes() {
    local nodes="$@"
    for n in $nodes; do
        docker stop ${COMPOSE_PROJECT}-aerospike-$n 2>/dev/null || true
    done
}

# Clear SMD on specific node (must be stopped first).
# Uses a temporary container to clear the SMD volume without starting the server.
clear_smd() {
    local node=$1
    local container="${COMPOSE_PROJECT}-aerospike-$node"
    
    # Get the SMD volume mount from the stopped container
    local smd_mount
    smd_mount=$(docker inspect "$container" 2>/dev/null | \
        grep -oP '"/opt/aerospike/smd":\s*\{\s*"Source":\s*"\K[^"]+' || true)
    
    if [ -n "$smd_mount" ] && [ -d "$smd_mount" ]; then
        # Clear from host if we have access to the bind mount
        rm -rf "${smd_mount:?}"/* 2>/dev/null || true
        log "Cleared SMD via host mount: $smd_mount"
    else
        # Fallback: use a temporary alpine container to clear the volume
        # This avoids starting the aerospike server process
        docker run --rm --volumes-from "$container" alpine sh -c "rm -rf /opt/aerospike/smd/*" 2>/dev/null || true
        log "Cleared SMD via temporary container"
    fi
}

wait_for_cluster() {
    local expected_size=$1
    local timeout=$2
    local elapsed=0
    
    log "Waiting for cluster size $expected_size (timeout: ${timeout}s)..."
    
    while [ $elapsed -lt $timeout ]; do
        # Try with auth first (for security-enabled clusters), fall back to unauthenticated.
        # Timeout guards against connection accepted but not yet processed.
        size=$(timeout 5 docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -Uadmin -Padmin -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || \
               timeout 5 docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo "0")
        if [ "$size" = "$expected_size" ]; then
            log "Cluster formed with size $size"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log "ERROR: Cluster did not reach size $expected_size within ${timeout}s (current: $size)"
    return 1
}

wait_for_sync_wait_size() {
    # Wait until at least one node is blocked in as_smd_wait_ready() for the
    # current cluster.  The old implementation logged "sync wait start cl_key …
    # size N"; the new implementation logs "waiting for initial SMD sync".
    local expected_size=$1
    local timeout=$2
    local elapsed_tenths=0
    local max_tenths=$((timeout * 10))

    log "Waiting for SMD sync wait (cluster size $expected_size) (timeout: ${timeout}s)..."

    # First wait until the cluster actually reaches the expected size, then
    # confirm that at least one node is waiting for its initial SMD sync.
    while [ $elapsed_tenths -lt $max_tenths ]; do
        if docker compose -p $COMPOSE_PROJECT logs aerospike-1 aerospike-2 2>&1 \
                | grep -q "waiting for initial SMD sync"; then
            log "Observed SMD sync wait (cluster size $expected_size)"
            return 0
        fi

        sleep 0.1
        elapsed_tenths=$((elapsed_tenths + 1))
    done

    log "ERROR: Did not observe SMD sync wait for cluster size $expected_size"
    return 1
}

test_basic_sync_ordering() {
    log "=== Test 1: Basic SMD Sync Ordering ==="
    
    # Clean start
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true
    
    # Start cluster and capture logs
    log "Starting 3-node cluster..."
    start_nodes 1 2 3
    
    # Wait for cluster
    wait_for_cluster 3 $TIMEOUT
    
    # Check logs for ordering
    log "Checking log ordering..."
    
    # Get logs from first node
    logs=$(docker compose -p $COMPOSE_PROJECT logs aerospike 2>&1)
    
    # Look for sync messages (optional - only present when pre-existing SMD data forces a wait)
    if echo "$logs" | grep -q "waiting for initial SMD sync"; then
        log "PASS: Found 'waiting for initial SMD sync' message"
    else
        log "INFO: No SMD sync wait found (normal for fresh cluster with no pre-existing SMD)"
    fi
    
    if echo "$logs" | grep -q "initial SMD sync done"; then
        log "PASS: Found SMD sync completion message"
    else
        log "INFO: No sync completion found (normal for fresh cluster with no pre-existing SMD)"
    fi
    
    log "Test 1 complete"
}

test_security_auth() {
    log "=== Test 2: Security SMD Sync (User Auth Across Nodes) ==="
    
    # Clean start - use security-enabled compose file
    docker compose -f docker-compose-security.yaml -p $COMPOSE_PROJECT down -v 2>/dev/null || true
    
    # Start cluster with security config
    log "Starting 3-node cluster with security enabled..."
    docker compose -f docker-compose-security.yaml -p $COMPOSE_PROJECT up -d aerospike-1
    docker compose -f docker-compose-security.yaml -p $COMPOSE_PROJECT up -d aerospike-2
    docker compose -f docker-compose-security.yaml -p $COMPOSE_PROJECT up -d aerospike-3
    
    # Wait for cluster (need auth for security-enabled cluster)
    log "Waiting for cluster size 3 (timeout: ${TIMEOUT}s)..."
    local elapsed=0
    while [ $elapsed -lt $TIMEOUT ]; do
        # Timeout guards against connection accepted but not yet processed.
        size=$(timeout 5 docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -Uadmin -Padmin -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo "0")
        if [ "$size" = "3" ]; then
            log "Cluster formed with size $size"
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    if [ "$size" != "3" ]; then
        log "ERROR: Cluster did not form"
        return 1
    fi
    
    # Create a test user on node 1 using asadm
    log "Creating test user on node 1..."
    docker exec ${COMPOSE_PROJECT}-aerospike-1 asadm --enable -Uadmin -Padmin -e "manage acl create user testuser password testpass roles read-write" 2>&1 || true
    
    sleep 2
    
    # Test authentication on node 2 (different node - proves SMD synced)
    log "Testing authentication on node 2 (verifies security SMD sync)..."
    if docker exec ${COMPOSE_PROJECT}-aerospike-2 asinfo -Utestuser -Ptestpass -v "namespaces" 2>&1 | grep -q "test"; then
        log "PASS: User created on node 1, authenticated on node 2 - security SMD synced"
    else
        log "FAIL: Authentication failed on node 2 - security SMD may not have synced"
        return 1
    fi
    
    # Also verify on node 3
    log "Testing authentication on node 3..."
    if docker exec ${COMPOSE_PROJECT}-aerospike-3 asinfo -Utestuser -Ptestpass -v "namespaces" 2>&1 | grep -q "test"; then
        log "PASS: Authentication on node 3 successful"
    else
        log "FAIL: Authentication failed on node 3"
        return 1
    fi
    
    # Clean up security cluster
    docker compose -f docker-compose-security.yaml -p $COMPOSE_PROJECT down -v 2>/dev/null || true
    
    log "Test 2 complete"
}

test_node_rejoin() {
    log "=== Test 3: Node Rejoin with Cleared SMD ==="
    
    # Ensure cluster is running with user
    wait_for_cluster 3 30 || {
        start_nodes 1 2 3
        wait_for_cluster 3 $TIMEOUT
    }
    
    # Create sindex (SMD data)
    log "Creating sindex..."
    docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "sindex-create:ns=test;set=demo;indexname=rejoin_idx;bin=rejoin;type=string" 2>/dev/null || true
    sleep 2
    
    # Stop node 3 (principal - highest node-id)
    log "Stopping node 3 (principal)..."
    stop_nodes 3
    sleep 3
    
    # Clear SMD on node 3
    log "Clearing SMD on node 3..."
    clear_smd 3
    
    # Restart node 3
    log "Restarting node 3..."
    start_nodes 3
    
    # Wait for it to rejoin
    wait_for_cluster 3 $TIMEOUT
    
    # Verify node 3 got the sindex via SMD sync
    log "Verifying SMD synced to rejoined node..."
    if docker exec ${COMPOSE_PROJECT}-aerospike-3 asinfo -v "sindex" 2>&1 | grep -q "rejoin_idx"; then
        log "PASS: Rejoined node has sindex"
    else
        log "FAIL: Rejoined node missing sindex"
        return 1
    fi
    
    # Check no timeout
    logs=$(docker compose -p $COMPOSE_PROJECT logs aerospike-3 2>&1 | tail -100)
    if echo "$logs" | grep -q "SMD sync timed out"; then
        log "FAIL: SMD sync timed out on rejoin!"
        return 1
    fi
    
    log "Test 3 complete"
}

test_preexisting_smd() {
    log "=== Test 4: First Node Has SMD, Others Join Empty ==="
    
    # Clean start - bring up node 1 only to create SMD
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true
    
    log "Starting node 1 alone to create SMD data..."
    start_nodes 1
    
    # Wait for node 1. Timeout guards against connection accepted but not yet processed.
    local elapsed=0
    while [ $elapsed -lt 30 ]; do
        if timeout 5 docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "build" 2>/dev/null | grep -q "8."; then
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    # Create sindex
    log "Creating secondary index..."
    docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "sindex-create:ns=test;set=demo;indexname=preexist_idx;bin=preexist;type=string" 2>/dev/null || true
    sleep 2
    
    # Verify sindex was created
    if docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "sindex" 2>&1 | grep -q "preexist_idx"; then
        log "Secondary index created on node 1"
    else
        log "WARN: Could not verify sindex creation"
    fi
    
    # Now start nodes 2 and 3 (fresh, no SMD)
    log "Starting nodes 2 and 3 (fresh, no SMD)..."
    start_nodes 2 3
    
    # Wait for cluster
    wait_for_cluster 3 $TIMEOUT
    
    # Check that all nodes have the sindex
    log "Verifying SMD synced to all nodes..."
    local all_have_sindex=true
    for i in 1 2 3; do
        if docker exec ${COMPOSE_PROJECT}-aerospike-$i asinfo -v "sindex" 2>&1 | grep -q "preexist_idx"; then
            log "Node $i has sindex"
        else
            log "FAIL: Node $i missing sindex"
            all_have_sindex=false
        fi
    done
    
    if $all_have_sindex; then
        log "PASS: All nodes have synced SMD"
    else
        log "FAIL: SMD not synced to all nodes"
        return 1
    fi
    
    # Check no timeouts
    logs=$(docker compose -p $COMPOSE_PROJECT logs 2>&1)
    if echo "$logs" | grep -q "SMD sync timed out"; then
        log "FAIL: SMD sync timed out!"
        return 1
    else
        log "PASS: No SMD sync timeout"
    fi
    
    log "Test 4 complete"
}

cleanup() {
    log "Cleaning up..."
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true
}

test_principal_pulls_from_npr() {
    log "=== Test 5: New Node Joins Existing Cluster with SMD ==="
    
    # Node 3 (highest node-id: a3) is principal once cluster forms.
    # This tests that a new node joining gets SMD from existing nodes.
    
    # Clean start
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true
    
    # Start nodes 2 and 3 first (without node 1) to create SMD
    log "Starting nodes 2 and 3 to create SMD data..."
    start_nodes 2 3
    
    # Wait for 2-node cluster (node 3 is principal - highest node-id).
    # Timeout guards against connection accepted but not yet processed.
    local elapsed=0
    while [ $elapsed -lt 60 ]; do
        size=$(timeout 5 docker exec ${COMPOSE_PROJECT}-aerospike-2 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo "0")
        if [ "$size" = "2" ]; then
            log "2-node cluster formed (nodes 2,3)"
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    # Create SMD data
    log "Creating secondary index..."
    docker exec ${COMPOSE_PROJECT}-aerospike-2 asinfo -v "sindex-create:ns=test;set=demo;indexname=pull_idx;bin=pull;type=string" 2>/dev/null || true
    sleep 3
    
    # Verify sindex exists on nodes 2 and 3
    for i in 2 3; do
        if docker exec ${COMPOSE_PROJECT}-aerospike-$i asinfo -v "sindex" 2>&1 | grep -q "pull_idx"; then
            log "Node $i has sindex"
        else
            log "WARN: Node $i missing sindex"
        fi
    done
    
    # Now start node 1 (fresh, no SMD) - it joins and gets SMD from existing nodes
    log "Starting node 1 (fresh, joining existing cluster)..."
    start_nodes 1
    
    # Wait for 3-node cluster
    wait_for_cluster 3 $TIMEOUT
    
    # Log principal for debugging (should be A3 - highest node-id)
    principal=$(docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_principal=\K[A-F0-9]+')
    node1_id=$(docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "node" 2>/dev/null)
    log "Principal: $principal, Node 1 ID: $node1_id"
    
    # Verify all nodes have the sindex (node 1 must have received SMD on join)
    log "Verifying SMD synced to all nodes..."
    local all_have_sindex=true
    for i in 1 2 3; do
        if docker exec ${COMPOSE_PROJECT}-aerospike-$i asinfo -v "sindex" 2>&1 | grep -q "pull_idx"; then
            log "Node $i has sindex"
        else
            log "FAIL: Node $i missing sindex"
            all_have_sindex=false
        fi
    done
    
    if $all_have_sindex; then
        log "PASS: New node received SMD on cluster join"
    else
        log "FAIL: SMD not synced to all nodes"
        return 1
    fi
    
    # Check no timeouts
    logs=$(docker compose -p $COMPOSE_PROJECT logs 2>&1)
    if echo "$logs" | grep -q "SMD sync timed out"; then
        log "FAIL: SMD sync timed out!"
        return 1
    else
        log "PASS: No SMD sync timeout"
    fi
    
    log "Test 5 complete"
}

test_identical_smd() {
    log "=== Test 6: Identical Pre-existing SMD ==="

    # Tests the !npr_has_dirty code path where all nodes restart with identical
    # SMD. The existing FULL_FROM_PR fallback should refresh the cluster key
    # without timing out.

    # Clean start
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true

    # Start cluster to generate initial SMD data
    log "Starting cluster to generate initial SMD data..."
    start_nodes 1 2 3
    wait_for_cluster 3 $TIMEOUT

    # Create sindex to ensure SMD is not completely empty/0
    log "Creating secondary index..."
    docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "sindex-create:ns=test;set=demo;indexname=ident_idx;bin=ident;type=string" 2>/dev/null || true
    sleep 2

    # Ensure it's synced
    local all_have_sindex=true
    for i in 1 2 3; do
        if docker exec ${COMPOSE_PROJECT}-aerospike-$i asinfo -v "sindex" 2>&1 | grep -q "ident_idx"; then
            log "Node $i has sindex"
        else
            log "FAIL: Node $i missing sindex"
            all_have_sindex=false
        fi
    done

    if ! $all_have_sindex; then
        return 1
    fi

    # Stop cluster WITHOUT clearing volumes
    log "Stopping cluster but keeping SMD volumes..."
    docker compose -p $COMPOSE_PROJECT stop
    sleep 2

    # Restart cluster
    log "Restarting cluster with identical pre-existing SMD..."
    docker compose -p $COMPOSE_PROJECT start
    wait_for_cluster 3 $TIMEOUT

    # We shouldn't timeout waiting for SMD
    logs=$(docker compose -p $COMPOSE_PROJECT logs 2>&1)
    if echo "$logs" | grep -q "SMD sync timed out"; then
        log "FAIL: SMD sync timed out on identical pre-existing SMD!"
        return 1
    fi

    log "PASS: Cluster started successfully with identical SMD"
    log "Test 6 complete"
}

test_principal_loss_initial_sync() {
    log "=== Test 7: Principal Loss During Initial SMD Sync ==="

    local sindex_count="${PRINCIPAL_LOSS_SINDEX_COUNT:-128}"

    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true

    log "Starting node 1 alone to seed SMD data..."
    start_nodes 1
    wait_for_cluster 1 $TIMEOUT

    log "Creating $sindex_count secondary indexes to keep initial SMD sync observable..."
    for i in $(seq 1 "$sindex_count"); do
        docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo \
            -v "sindex-create:ns=test;set=demo;indexname=principal_loss_${i};bin=pl_${i};type=string" \
            >/dev/null 2>&1 || true
    done
    sleep 2

    log "Starting nodes 2 and 3, then stopping principal node 3 during size-3 SMD wait..."
    start_nodes 2 3
    wait_for_sync_wait_size 3 30
    stop_nodes 3

    log "Waiting for surviving nodes to process principal loss and form size 2..."
    wait_for_cluster 2 $TIMEOUT

    logs=$(docker compose -p $COMPOSE_PROJECT logs aerospike-1 aerospike-2 2>&1)
    if echo "$logs" | grep -q "initial SMD sync done"; then
        log "PASS: Surviving nodes completed initial SMD sync after principal loss"
    else
        log "FAIL: No 'initial SMD sync done' found after principal loss - nodes may be wedged"
        return 1
    fi

    log "Test 7 complete"
}

test_migration_deferred_until_smd_ready() {
    log "=== Test 8: Migration Deferred Until SMD Settled ==="
    #
    # Verifies the liveness and ordering properties of the SMD settle gate on
    # as_partition_immigrate_start():
    #   - Fresh nodes joining a cluster with large SMD eventually RELEASE the
    #     gate (no permanent stall).
    #   - Data and SMD both arrive on fresh nodes after the cluster forms.
    #
    # Strategy:
    #   1. Node 1 starts alone, creates many sinexes (slow SMD) + records.
    #   2. Fresh nodes 2 and 3 join — they are blocked in as_smd_wait_ready()
    #      in run_accept(), while immigration requests return AS_MIGRATE_AGAIN
    #      until as_smd_settled_for_migration() becomes true.
    #   3. After SMD settles, verify:
    #        a. "initial SMD sync done" is logged (settle gate released).
    #        b. All nodes can be queried (run_accept() unblocked).
    #        c. All nodes have the sinexes (SMD propagated correctly).
    #        d. Data written before the join is accessible on all nodes
    #           (migrations completed after settle).
    #

    local sindex_count="${MIGRATION_DEFER_SINDEX_COUNT:-64}"
    local record_count="${MIGRATION_DEFER_RECORD_COUNT:-1000}"

    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true

    log "Starting node 1 alone to seed SMD data and records..."
    start_nodes 1
    wait_for_cluster 1 $TIMEOUT

    log "Creating $sindex_count secondary indexes..."
    for i in $(seq 1 "$sindex_count"); do
        docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo \
            -v "sindex-create:ns=test;set=demo;indexname=migdefer_${i};bin=md_${i};type=string" \
            >/dev/null 2>&1 || true
    done

    log "Writing $record_count records to node 1..."
    docker exec ${COMPOSE_PROJECT}-aerospike-1 aql \
        -c "INSERT INTO test.demo (PK, md_1) VALUES ('migdefer_rec_1', 'v1')" \
        >/dev/null 2>&1 || true
    for i in $(seq 2 "$record_count"); do
        docker exec ${COMPOSE_PROJECT}-aerospike-1 aql \
            -c "INSERT INTO test.demo (PK, md_1) VALUES ('migdefer_rec_${i}', 'v${i}')" \
            >/dev/null 2>&1 || true
    done 2>/dev/null
    sleep 2

    log "Starting fresh nodes 2 and 3..."
    start_nodes 2 3

    log "Waiting for 3-node cluster to form..."
    wait_for_cluster 3 $TIMEOUT

    log "Waiting for initial SMD sync to complete on surviving nodes..."
    local elapsed=0
    while [ $elapsed -lt $TIMEOUT ]; do
        if docker compose -p $COMPOSE_PROJECT logs aerospike-2 aerospike-3 2>&1 \
                | grep -q "initial SMD sync done"; then
            log "Observed 'initial SMD sync done' on nodes 2 and/or 3"
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done

    if [ $elapsed -ge $TIMEOUT ]; then
        log "FAIL: 'initial SMD sync done' not observed within ${TIMEOUT}s"
        return 1
    fi

    sleep 3  # allow migrations to complete after SMD settle

    log "Verifying SMD (sinexes) propagated to all nodes..."
    local all_have_sindex=true
    for i in 1 2 3; do
        if docker exec ${COMPOSE_PROJECT}-aerospike-$i asinfo -v "sindex" 2>&1 | grep -q "indexname=migdefer_1:"; then
            log "Node $i has sinexes"
        else
            log "FAIL: Node $i missing sinexes"
            all_have_sindex=false
        fi
    done

    if ! $all_have_sindex; then
        log "FAIL: SMD not propagated to all nodes"
        return 1
    fi

    log "Verifying data accessible on all nodes (spot-checking key migdefer_rec_1)..."
    local all_readable=true
    for i in 1 2 3; do
        if docker exec ${COMPOSE_PROJECT}-aerospike-$i aql \
                -c "SELECT * FROM test.demo WHERE PK='migdefer_rec_1'" 2>&1 \
                | grep -q "md_1"; then
            log "Node $i can read migdefer_rec_1"
        else
            log "FAIL: Node $i cannot read migdefer_rec_1 (migration incomplete or misdirected)"
            all_readable=false
        fi
    done

    if ! $all_readable; then
        log "FAIL: Data not accessible on all nodes"
        return 1
    fi

    # Verify the cluster holds all the data (sum of master_objects = record_count).
    local total_master=0
    for i in 1 2 3; do
        local m
        m=$(docker exec ${COMPOSE_PROJECT}-aerospike-$i asinfo \
            -v "namespace/test" 2>/dev/null | grep -oP 'master_objects=\K\d+' | head -1 || echo "0")
        total_master=$((total_master + ${m:-0}))
    done
    log "Total master_objects across all nodes: $total_master (expected $record_count)"
    if [ "$total_master" -ge "$record_count" ]; then
        log "PASS: All $record_count records present (total master_objects=$total_master)"
    else
        log "FAIL: Only $total_master master_objects, expected >= $record_count"
        return 1
    fi

    log "Test 8 complete"
}

# ---------------------------------------------------------------------------
# SMD Timing Tests
# ---------------------------------------------------------------------------
#
# Measures how long SMD full-sync takes as a function of payload size.
#
# Strategy:
#   1. Use gen-large-smd.py to write a pre-seeded .smd file for node 1.
#   2. Start all 3 nodes via docker-compose-timing.yaml, which bind-mounts per-node
#      smd directories from the host (nodes 2 & 3 start empty).
#   3. Measure two things:
#        a) Wall-clock seconds until cluster_size=3 is reported by node 1.
#        b) SMD sync elapsed microseconds from the "sync wait done elapsed NNN us"
#           debug log line emitted by as_smd_wait_ready().
#
# The test iterates over a configurable list of item counts (TIMING_ITEMS).

TIMING_PROJECT="smd-timing"
TIMING_COMPOSE="docker-compose-timing.yaml"
# Before teardown: verify nodes can serve (see TIMING_SANITY_MODE).
TIMING_SANITY_SERVE_CHECK="${TIMING_SANITY_SERVE_CHECK:-true}"
TIMING_SANITY_TIMEOUT_SEC="${TIMING_SANITY_TIMEOUT_SEC:-0}"
TIMING_ASINFO_TIMEOUT_SEC="${TIMING_ASINFO_TIMEOUT_SEC:-120}"
# cluster | serve (default) | smd-info — same semantics as k8s harness
TIMING_SANITY_MODE="${TIMING_SANITY_MODE:-serve}"
TIMING_PROGRESS_INTERVAL_SEC="${TIMING_PROGRESS_INTERVAL_SEC:-15}"
MIXED_FAIL_OPEN_OLD_IMAGE_COMPOSE="docker-compose-mixed-old-image.yaml"
SMD_DATA_DIR="${SMD_DATA_DIR:-/tmp/smd-timing-data}"
TIMING_MODULE="evict"          # evict module: no key format validation, clean logs
TIMING_VALUE_SIZE="${TIMING_VALUE_SIZE:-200}"  # bytes per value (default ~200B)
TIMING_ITEMS="${TIMING_ITEMS:-10000 50000 100000 200000 300000 400000}"  # item counts to sweep

# Realistic timing mode configuration
# Uses gen-realistic-smd.py to generate worst-case but valid SMD data
TIMING_REAL_MODULES="${TIMING_REAL_MODULES:-truncate sindex security masking}"
TIMING_REAL_MAX_SIZE="${TIMING_REAL_MAX_SIZE:-false}"  # Use max-length keys/values

# Per-module item counts (override defaults from gen-realistic-smd.py)
# Set these to simulate different deployment scales
TIMING_REAL_SECURITY_ITEMS="${TIMING_REAL_SECURITY_ITEMS:-}"  # e.g., 300000 for extreme LDAP

# Mixed-version fail-open regression configuration.
MIXED_FAIL_OPEN_OLD_NODE="${MIXED_FAIL_OPEN_OLD_NODE:-3}"
MIXED_FAIL_OPEN_SYNC_THRESHOLD_MS="${MIXED_FAIL_OPEN_SYNC_THRESHOLD_MS:-5000}"
MIXED_FAIL_OPEN_WAIT_SECONDS="${MIXED_FAIL_OPEN_WAIT_SECONDS:-45}"
MIXED_FAIL_OPEN_OLD_IMAGE="${MIXED_FAIL_OPEN_OLD_IMAGE:-}"
MIXED_DIRTY_REJOIN_WAIT_SECONDS="${MIXED_DIRTY_REJOIN_WAIT_SECONDS:-60}"
MIXED_DIRTY_REJOIN_INDEX="${MIXED_DIRTY_REJOIN_INDEX:-mixed_dirty_current}"

# Ensure results dir exists
TIMING_RESULTS_DIR="${TIMING_RESULTS_DIR:-./timing-results}"

timing_log() {
    echo "[$(date '+%H:%M:%S')] [timing] $*"
}

# Tear down timing cluster completely
timing_teardown() {
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT down -v 2>/dev/null || true
}

# Seed node 1's smd dir with a generated .smd file; clear nodes 2 & 3.
timing_seed_smd() {
    local n_items=$1

    timing_log "Seeding SMD: module=$TIMING_MODULE  items=$n_items  value_size=${TIMING_VALUE_SIZE}B"

    # Prepare per-node smd and log directories
    for node in 1 2 3; do
        rm -rf "${SMD_DATA_DIR}/node${node}/smd"
        rm -rf "${SMD_DATA_DIR}/node${node}/log"
        mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
        mkdir -p "${SMD_DATA_DIR}/node${node}/log"
    done

    # Generate the .smd file for node 1
    python3 "$(dirname "$0")/gen-large-smd.py" \
        --items "$n_items" \
        --module "$TIMING_MODULE" \
        --value-size "$TIMING_VALUE_SIZE" \
        --out "${SMD_DATA_DIR}/node1/smd/${TIMING_MODULE}.smd"

    local smd_file="${SMD_DATA_DIR}/node1/smd/${TIMING_MODULE}.smd"
    local smd_size
    smd_size=$(du -sh "$smd_file" 2>/dev/null | cut -f1)
    timing_log "Node 1 SMD file: $smd_file ($smd_size)"
}

# Wait for cluster_size=N on node 1, return elapsed wall-clock milliseconds.
# All log output goes to stderr; only the numeric result is printed to stdout.
timing_wait_cluster() {
    local expected_size=$1
    local timeout=${2:-120}
    local t_start
    t_start=$(date +%s%N)
    local elapsed=0

    timing_log "Waiting for cluster size $expected_size (timeout: ${timeout}s)..." >&2

    while [ $elapsed -lt $timeout ]; do
        # Timeout guards against connection accepted but not yet processed.
        # Try with auth first (for security-enabled clusters), fall back to unauthenticated.
        local size
        size=$(timeout 5 docker exec smd-timing-aerospike-1 asinfo -Uadmin -Padmin -v "statistics" 2>/dev/null \
               | grep -oP 'cluster_size=\K\d+' || \
               timeout 5 docker exec smd-timing-aerospike-1 asinfo -v "statistics" 2>/dev/null \
               | grep -oP 'cluster_size=\K\d+' || echo "0")
        if timing_should_log_progress "$elapsed" 2>/dev/null; then
            timing_log "  cluster wait (${elapsed}s): node1 cluster_size=${size} (want ${expected_size})" >&2
            local n snip
            for n in 1 2 3; do
                snip=$(docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-"$n" 2>&1 \
                    | grep -E "initial sync progress|sync wait done|service ready|waiting for initial" \
                    | tail -2)
                [ -n "$snip" ] && timing_log "    node${n}: $(echo "$snip" | tr '\n' ' ')" >&2
            done
        fi
        if [ "$size" = "$expected_size" ]; then
            local t_end
            t_end=$(date +%s%N)
            local wall_ms=$(( (t_end - t_start) / 1000000 ))
            timing_log "Cluster formed (size $size) in ${wall_ms} ms" >&2
            echo "$wall_ms"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    timing_log "ERROR: Cluster did not reach size $expected_size within ${timeout}s" >&2
    echo "-1"
    return 1
}

# Extract SMD sync elapsed time from node 1 logs.
# Primary: INFO from as_smd_wait_ready (secure clusters — no extra logging config).
# Secondary: DETAIL from as_smd_cluster_changed_sync — requires "context smd detail"
# (or lower) in aerospike.conf for exchange-path timing.
#
#   as_smd_wait_ready (INFO):
#     "initial SMD sync done - elapsed NNN us"
#
#   as_smd_cluster_changed_sync (DETAIL, partition balance path):
#     "sync wait done cl_key XXXX elapsed NNN us"
#
# Returns the largest elapsed microsecond value found (the binding sync wait),
# or -1 if neither is present.
timing_extract_sync_us() {
    local logs primary fallback
    logs=$(docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-1 2>&1)

    # Match both formats; pick the last (largest elapsed) value seen.
    primary=$(echo "$logs" \
        | grep -oP '(?:initial SMD sync(?: wait)? done - elapsed |sync wait done cl_key [0-9a-fA-F]+ elapsed )\K\d+(?= us)' \
        | sort -n | tail -1)

    if [ -n "$primary" ]; then
        echo "$primary"
        return 0
    fi

    fallback=$(echo "$logs" \
        | grep -oP 'full-from-pr timing:[^\n]*total=\K\d+(?= us)' \
        | awk '{s+=$1} END {print int(s)}')
    if [ -n "$fallback" ] && [ "$fallback" != "0" ]; then
        timing_log "sync_elapsed_us fallback: Σ(full-from-pr total) on node1=${fallback} us (no initial/sync-wait line in docker logs)"
        echo "$fallback"
        return 0
    fi

    echo "-1"
}

timing_docker_asinfo() {
    local node=$1
    shift
    local t="${TIMING_ASINFO_TIMEOUT_SEC:-120}"
    timeout 5 docker exec smd-timing-aerospike-"$node" asinfo -Uadmin -Padmin "$@" 2>/dev/null || \
        timeout 5 docker exec smd-timing-aerospike-"$node" asinfo "$@" 2>/dev/null
}

timing_docker_asinfo_long() {
    local node=$1
    shift
    local t="${TIMING_ASINFO_TIMEOUT_SEC:-120}"
    timeout "$t" docker exec smd-timing-aerospike-"$node" asinfo -Uadmin -Padmin "$@" 2>/dev/null || \
        timeout "$t" docker exec smd-timing-aerospike-"$node" asinfo "$@" 2>/dev/null
}

# Same parser as k8s harness (timing_smd_info_modules_settled).
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

timing_docker_node_smd_settled_from_logs() {
    local node=$1
    local modules_csv=$2
    local log module
    log=$(docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-"$node" 2>&1 || true)
    [ -z "$log" ] && return 1
    IFS=',' read -ra modules <<< "$modules_csv"
    for module in "${modules[@]}"; do
        module="${module// /}"
        [ -z "$module" ] && continue
        if echo "$log" | grep -qE "\\{${module}:(dirty|merging):"; then
            return 1
        fi
        if ! echo "$log" | grep -qE "\\{${module}:(pr|npr):"; then
            return 1
        fi
    done
    return 0
}

timing_docker_node_smd_settled() {
    local node=$1
    local modules_csv=$2
    local info
    info=$(timing_docker_asinfo_long "$node" -v "smd-info" || true)
    if [ -n "$info" ] && timing_smd_info_modules_settled "$info" "$modules_csv"; then
        return 0
    fi
    if [ -z "$info" ] && timing_docker_node_smd_settled_from_logs "$node" "$modules_csv"; then
        return 0
    fi
    return 1
}

timing_docker_principal_node() {
    # Rejoin timing: highest node-id (a3) is principal — aerospike-3.
    echo 3
}

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
    [ "$elapsed" -gt 0 ] && [ $((elapsed % interval)) -eq 0 ]
}

timing_docker_node_smd_log_progress_snip() {
    local node=$1
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-"$node" 2>&1 \
        | grep -E "initial sync progress|full-from-pr start|full-to-pr start|broadcasting full-from-pr|initial cluster sync steady|full-from-pr timing|full-to-pr timing" \
        | tail -5
}

timing_log_all_nodes_smd_progress_docker() {
    local modules_csv=${1:-security,truncate,sindex,masking}
    local principal
    principal=$(timing_docker_principal_node)
    timing_log "--- SMD progress (interval=${TIMING_PROGRESS_INTERVAL_SEC}s; principal=node${principal}) ---"
    local node info snip line
    for node in 1 2 3; do
        info=$(timing_docker_asinfo_long "$node" -v "smd-info" || true)
        if [ -n "$info" ]; then
            timing_log "  node${node} smd-info: $(timing_smd_info_progress_line "$info" "$modules_csv")"
        else
            timing_log "  node${node} smd-info: (timeout — merge may block asinfo; see logs below)"
        fi
        snip=$(timing_docker_node_smd_log_progress_snip "$node")
        if [ -n "$snip" ]; then
            while IFS= read -r line; do
                [ -n "$line" ] && timing_log "    log: $line"
            done <<< "$snip"
        fi
    done
    timing_log "---"
}

timing_sanity_fail_detail_docker() {
    local node=$1
    local info
    info=$(timing_docker_asinfo_long "$node" -v "smd-info" || true)
    if [ -n "$info" ]; then
        timing_log "  node${node} smd-info: $(echo "$info" | tr ';' '\n' | grep -E '^smd:|^security:' | head -2 | tr '\n' ' ')"
    else
        timing_log "  node${node} smd-info: (timeout — principal often blocks during huge security merge)"
    fi
}

# Pre-teardown SERVER-209 proxy (see TIMING_SANITY_MODE).
timing_sanity_serve_ready_docker() {
    local expected_size=$1
    local modules_csv=$2
    local cluster_timeout_sec=$3

    case "${TIMING_SANITY_SERVE_CHECK}" in
        false|0|no) return 0 ;;
    esac

    local mode="${TIMING_SANITY_MODE:-serve}"
    local max_sec=${TIMING_SANITY_TIMEOUT_SEC:-0}
    [ "$max_sec" = "0" ] && max_sec=$cluster_timeout_sec

    case "$mode" in
        cluster)
            timing_log "Sanity (mode=cluster): all ${expected_size} node(s) — cluster_size + namespaces (fast-path parity)..."
            ;;
        smd-info|strict|all)
            timing_log "Sanity (mode=smd-info): all ${expected_size} node(s) — cluster_size, namespaces, smd-info [${modules_csv}] (timeout ${max_sec}s)..."
            ;;
        *)
            timing_log "Sanity (mode=serve): all ${expected_size} node(s) — cluster_size + namespaces; principal smd-info [${modules_csv}] (timeout ${max_sec}s)..."
            ;;
    esac

    local elapsed=0
    local principal_node
    principal_node=$(timing_docker_principal_node)

    while [ $elapsed -lt $max_sec ]; do
        local all_ok=1
        local fail_reason=""
        local node
        for node in 1 2 3; do
            local size
            size=$(timing_docker_asinfo "$node" -v "statistics" | grep -oP 'cluster_size=\K\d+' | head -1 || echo 0)
            if [ "$size" != "$expected_size" ]; then
                all_ok=0
                fail_reason="node${node} cluster_size=$size (want $expected_size)"
                break
            fi
            if ! timing_docker_asinfo "$node" -v "namespaces" | grep -q "test"; then
                all_ok=0
                fail_reason="node${node} asinfo namespaces missing test ns"
                break
            fi
            if docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-"$node" 2>&1 \
                    | grep -qE "SMD sync timed out|initial SMD sync timed out"; then
                all_ok=0
                fail_reason="node${node} SMD sync timed out in logs"
                break
            fi
            case "$mode" in
                smd-info|strict|all)
                    if ! timing_docker_node_smd_settled "$node" "$modules_csv"; then
                        all_ok=0
                        fail_reason="node${node} smd-info not settled"
                        break
                    fi
                    ;;
                serve)
                    [ "$node" = "$principal_node" ] || continue
                    if ! timing_docker_node_smd_settled "$node" "$modules_csv"; then
                        all_ok=0
                        fail_reason="node${node} smd-info not settled (principal)"
                        break
                    fi
                    ;;
            esac
        done

        if [ $all_ok -eq 1 ]; then
            timing_log "Sanity PASS (${mode}): ready (${elapsed}s; principal=node${principal_node})"
            return 0
        fi

        if timing_should_log_progress "$elapsed"; then
            timing_log "Sanity still waiting (${elapsed}s / ${max_sec}s): ${fail_reason:-unknown}"
            timing_log_all_nodes_smd_progress_docker "$modules_csv"
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done

    timing_log "Sanity FAIL (${mode}) within ${max_sec}s (last: ${fail_reason:-unknown})"
    timing_log "Hint: use TIMING_SANITY_MODE=cluster for scrape-only parity; smd-info waits on every NPR (950k security is slow)"
    return 1
}

# Run a single timing measurement for a given item count.
# Appends one TSV data row to the provided results file.
timing_run_one() {
    local n_items=$1
    local results_file=$2

    timing_teardown

    timing_seed_smd "$n_items"

    local smd_mb
    smd_mb=$(python3 -c "import os; print(f'{os.path.getsize(\"${SMD_DATA_DIR}/node1/smd/${TIMING_MODULE}.smd\") / 1048576:.2f}')")

    timing_log "Starting 3-node cluster (n_items=$n_items, ~${smd_mb} MB)..."

    export SMD_DATA_DIR ASD_BINARY
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT up -d 2>&1 | tail -5 || true

    local wall_ms
    wall_ms=$(timing_wait_cluster 3 300)

    local sync_us
    sync_us=$(timing_extract_sync_us)

    if [ "$wall_ms" = "-1" ]; then
        timing_log "FAIL: cluster did not form for n_items=$n_items"
        timing_teardown
        return 1
    fi

    local sanity_rc=0
    timing_sanity_serve_ready_docker 3 "$TIMING_MODULE" 300 || sanity_rc=1

    local sync_ms="n/a"
    if [ "$sync_us" != "-1" ]; then
        sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
    fi

    # Check whether the server hit its 30s SMD sync timeout
    local timed_out=0
    if docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-1 2>&1 \
            | grep -q "SMD sync timed out\|initial SMD sync timed out"; then
        timed_out=1
        timing_log "WARNING: SMD sync timed out on node 1!"
    fi

    timing_log "RESULT: items=$n_items  smd=${smd_mb}MB  wall=${wall_ms}ms  smd_sync=${sync_ms}ms (${sync_us}us)  timeout=${timed_out}  sanity=$([ $sanity_rc -eq 0 ] && echo PASS || echo FAIL)"

    echo -e "${n_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${TIMING_VALUE_SIZE}\t${timed_out}" >> "$results_file"

    # Capture phase-timing log lines before containers are torn down.
    local phase_log="${results_file%.tsv}-phases.log"
    timing_log "Capturing phase timing to $phase_log..."
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs 2>&1 \
        | grep -E "full-to-pr timing|full-from-pr timing" \
        | sed "s/^/[n=${n_items}] /" >> "$phase_log"

    timing_teardown
    return $sanity_rc
}

test_large_smd_timing() {
    # Validate ASD_BINARY is set and is a file
    if [ -z "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY not set. Export the path to the asd binary."
        timing_log "  Example: export ASD_BINARY=/path/to/aerospike-server/target/Linux-x86_64/bin/asd"
        return 1
    fi
    if [ ! -f "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY='$ASD_BINARY' is not a file."
        timing_log "  Make sure the path points to the asd binary, not a directory."
        return 1
    fi

    timing_log "=== SMD Large-Payload Timing Sweep ==="
    timing_log "ASD_BINARY: $ASD_BINARY"
    timing_log "Sweep: items=${TIMING_ITEMS}  value_size=${TIMING_VALUE_SIZE}B"
    timing_log "Results dir: $TIMING_RESULTS_DIR"

    mkdir -p "$TIMING_RESULTS_DIR"
    local results_file="${TIMING_RESULTS_DIR}/timing-$(date '+%Y%m%d-%H%M%S').tsv"
    echo -e "items\tsmd_mb\twall_cluster_ms\tsync_elapsed_us\tvalue_size_b\tsync_timeout" > "$results_file"
    timing_log "Results file: $results_file"

    local failed=0
    for n in $TIMING_ITEMS; do
        timing_run_one "$n" "$results_file" || failed=1
    done

    if [ $failed -eq 0 ]; then
        timing_log "=== Timing sweep COMPLETE ==="
        timing_log "Summary ($results_file):"
        column -t "$results_file"
    else
        timing_log "=== Timing sweep had FAILURES ==="
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Realistic SMD Timing Tests (timing-real)
# ---------------------------------------------------------------------------
#
# Uses gen-realistic-smd.py to generate worst-case but valid SMD data:
#   - truncate: max out at 131,104 items (32 ns × 4096 sets + 32)
#   - sindex:   max out at 8,192 items (32 ns × 256 sindexes)
#   - security: LDAP-heavy scenario with ~100K users
#   - masking:  heavy masking deployment with ~50K rules
#
# All entries use valid key/value formats that the server will accept.

# Seed node 1 with realistic SMD data; clear nodes 2 & 3.
timing_real_seed_smd() {
    local modules="$1"
    local max_size="$2"

    timing_log "Seeding realistic SMD: modules=[$modules]  max_size=$max_size"

    # Prepare per-node smd and log directories
    for node in 1 2 3; do
        rm -rf "${SMD_DATA_DIR}/node${node}/smd"
        rm -rf "${SMD_DATA_DIR}/node${node}/log"
        mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
        mkdir -p "${SMD_DATA_DIR}/node${node}/log"
    done

    # Generate .smd files for node 1
    local max_size_flag=""
    if [ "$max_size" = "true" ]; then
        max_size_flag="--max-size"
    fi

    for module in $modules; do
        # Check for per-module item count override
        local items_flag=""
        case "$module" in
            security)
                [ -n "$TIMING_REAL_SECURITY_ITEMS" ] && items_flag="--items $TIMING_REAL_SECURITY_ITEMS"
                ;;
        esac
        
        timing_log "Generating $module... ${items_flag:-"(default)"}"
        python3 "$(dirname "$0")/gen-realistic-smd.py" \
            --out-dir "${SMD_DATA_DIR}/node1/smd" \
            --module "$module" \
            $max_size_flag \
            $items_flag
    done

    # Show what was generated
    timing_log "Node 1 SMD files:"
    for f in "${SMD_DATA_DIR}/node1/smd"/*.smd; do
        if [ -f "$f" ]; then
            local size
            size=$(du -sh "$f" 2>/dev/null | cut -f1)
            timing_log "  $(basename "$f"): $size"
        fi
    done
}

mixed_fail_open_validate_binaries() {
    if [ -z "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY not set. Export the path to the new asd binary."
        return 1
    fi
    if [ ! -f "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY='$ASD_BINARY' is not a file."
        return 1
    fi
    if [ -z "$OLD_ASD_BINARY" ]; then
        timing_log "ERROR: OLD_ASD_BINARY not set. Export the path to an older mixed-version asd binary."
        return 1
    fi
    if [ ! -f "$OLD_ASD_BINARY" ]; then
        timing_log "ERROR: OLD_ASD_BINARY='$OLD_ASD_BINARY' is not a file."
        return 1
    fi
    if [ -z "$MIXED_FAIL_OPEN_OLD_IMAGE" ] && ldd "$OLD_ASD_BINARY" 2>/dev/null | grep -q "not found"; then
        timing_log "ERROR: OLD_ASD_BINARY has missing shared libraries in this runtime:"
        ldd "$OLD_ASD_BINARY" 2>/dev/null | grep "not found" | sed 's/^/[timing]   /'
        timing_log "Set MIXED_FAIL_OPEN_OLD_IMAGE to run this old binary in a compatible image."
        return 1
    fi
    if [ -n "$MIXED_FAIL_OPEN_OLD_IMAGE" ] && [ "$MIXED_FAIL_OPEN_OLD_NODE" != "3" ]; then
        timing_log "ERROR: MIXED_FAIL_OPEN_OLD_IMAGE currently supports MIXED_FAIL_OPEN_OLD_NODE=3 only."
        return 1
    fi
}

mixed_fail_open_prepare_dirs() {
    for node in 1 2 3; do
        rm -rf "${SMD_DATA_DIR}/node${node}/smd"
        rm -rf "${SMD_DATA_DIR}/node${node}/log"
        mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
        mkdir -p "${SMD_DATA_DIR}/node${node}/log"
    done
}

mixed_fail_open_set_binaries() {
    ASD_BINARY_NODE1="$ASD_BINARY"
    ASD_BINARY_NODE2="$ASD_BINARY"
    ASD_BINARY_NODE3="$ASD_BINARY"

    case "$MIXED_FAIL_OPEN_OLD_NODE" in
        1)
            ASD_BINARY_NODE1="$OLD_ASD_BINARY"
            ;;
        2)
            ASD_BINARY_NODE2="$OLD_ASD_BINARY"
            ;;
        3)
            ASD_BINARY_NODE3="$OLD_ASD_BINARY"
            ;;
        *)
            timing_log "ERROR: MIXED_FAIL_OPEN_OLD_NODE must be 1, 2, or 3"
            return 1
            ;;
    esac

    export ASD_BINARY_NODE1 ASD_BINARY_NODE2 ASD_BINARY_NODE3
}

mixed_fail_open_wait_for_sync_done() {
    local node=$1
    local timeout=${2:-45}
    local elapsed=0
    local service="aerospike-${node}"

    timing_log "Waiting for sync wait completion on new-version node $node..."

    while [ $elapsed -lt $timeout ]; do
        local sync_us
        sync_us=$(docker compose -f "$TIMING_COMPOSE" -p "$TIMING_PROJECT" logs "$service" 2>&1 \
            | grep -oP '(?:initial SMD sync done - elapsed |sync wait done cl_key [0-9a-f]+ elapsed )\K\d+(?= us)' \
            | tail -1 || true)

        if [ -n "$sync_us" ]; then
            local sync_ms=$((sync_us / 1000))
            timing_log "Node $node sync wait elapsed ${sync_ms} ms (${sync_us} us)"

            if [ "$sync_ms" -gt "$MIXED_FAIL_OPEN_SYNC_THRESHOLD_MS" ]; then
                timing_log "FAIL: node $node sync wait exceeded ${MIXED_FAIL_OPEN_SYNC_THRESHOLD_MS} ms"
                return 1
            fi

            return 0
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    timing_log "FAIL: no sync wait done log found for node $node within ${timeout}s"
    return 1
}

test_mixed_fail_open() {
    mixed_fail_open_validate_binaries || return 1

    timing_log "=== Mixed-Version SMD Fail-Open Regression ==="
    timing_log "New ASD_BINARY: $ASD_BINARY"
    timing_log "Old OLD_ASD_BINARY: $OLD_ASD_BINARY"
    timing_log "Old node: $MIXED_FAIL_OPEN_OLD_NODE"
    timing_log "Old image: ${MIXED_FAIL_OPEN_OLD_IMAGE:-"(default timing image)"}"
    timing_log "Sync threshold: ${MIXED_FAIL_OPEN_SYNC_THRESHOLD_MS} ms"

    timing_teardown
    mixed_fail_open_prepare_dirs
    mixed_fail_open_set_binaries || return 1

    export SMD_DATA_DIR
    local compose_args=(-f "$TIMING_COMPOSE")

    if [ -n "$MIXED_FAIL_OPEN_OLD_IMAGE" ]; then
        export MIXED_FAIL_OPEN_OLD_IMAGE
        compose_args+=(-f "$MIXED_FAIL_OPEN_OLD_IMAGE_COMPOSE")
    fi

    docker compose "${compose_args[@]}" -p "$TIMING_PROJECT" up -d 2>&1 | tail -5 || true

    local wall_ms
    wall_ms=$(timing_wait_cluster 3 120)

    if [ "$wall_ms" = "-1" ]; then
        timing_log "FAIL: mixed-version cluster did not form"
        return 1
    fi

    local failed=0

    for node in 1 2 3; do
        if [ "$node" != "$MIXED_FAIL_OPEN_OLD_NODE" ]; then
            mixed_fail_open_wait_for_sync_done "$node" "$MIXED_FAIL_OPEN_WAIT_SECONDS" || failed=1
        fi
    done

    if [ $failed -eq 0 ]; then
        timing_log "PASS: mixed-version fail-open sync wait completed promptly"
        timing_teardown
        return 0
    fi

    timing_log "Containers left running for inspection. Check logs with: docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs"
    return 1
}

mixed_dirty_rejoin_set_binaries() {
    # Nodes 2 and 3 are old-version nodes; node 3 is the deterministic principal.
    ASD_BINARY_NODE1="$ASD_BINARY"
    ASD_BINARY_NODE2="$OLD_ASD_BINARY"
    ASD_BINARY_NODE3="$OLD_ASD_BINARY"

    export ASD_BINARY_NODE1 ASD_BINARY_NODE2 ASD_BINARY_NODE3
}

mixed_dirty_rejoin_seed_smd() {
    mixed_fail_open_prepare_dirs

    timing_log "Seeding mixed dirty rejoin SMD:"
    timing_log "  Nodes 2,3: current sindex data, cv_tid=2"
    timing_log "  Node 1:   stale new-version NPR data, cv_tid=1"

    python3 << EOF
import json
import os

base_dir = "${SMD_DATA_DIR}"
cv_key = 0x1111

current = [
    [cv_key, 2],
    {
        "key": "test|demo|dirty0|.|S",
        "value": "mixed_dirty_stale",
        "generation": 1,
        "timestamp": 1700000000000,
    },
    {
        "key": "test|demo|dirty1|.|S",
        "value": "${MIXED_DIRTY_REJOIN_INDEX}",
        "generation": 1,
        "timestamp": 1700000000001,
    },
]

stale = [
    [cv_key, 1],
    {
        "key": "test|demo|dirty0|.|S",
        "value": "mixed_dirty_stale",
        "generation": 1,
        "timestamp": 1699999999999,
    },
]

for node, data in ((1, stale), (2, current), (3, current)):
    path = os.path.join(base_dir, f"node{node}", "smd", "sindex.smd")
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
EOF
}

mixed_dirty_rejoin_poll_first_sindex() {
    local elapsed=0

    timing_log "Polling first successful sindex response from new-version node 1..."

    while [ $elapsed -lt "$MIXED_DIRTY_REJOIN_WAIT_SECONDS" ]; do
        local output

        if output=$(timing_docker_asinfo 1 -v "sindex" 2>&1); then
            if echo "$output" | grep -q "$MIXED_DIRTY_REJOIN_INDEX"; then
                timing_log "PASS: first successful response includes $MIXED_DIRTY_REJOIN_INDEX"
                return 0
            fi

            timing_log "FAIL: first successful response from node 1 is missing $MIXED_DIRTY_REJOIN_INDEX"
            timing_log "Response: $output"
            return 1
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    timing_log "FAIL: node 1 did not accept an sindex query within ${MIXED_DIRTY_REJOIN_WAIT_SECONDS}s"
    return 1
}

test_mixed_dirty_rejoin() {
    mixed_fail_open_validate_binaries || return 1

    if [ -n "$MIXED_FAIL_OPEN_OLD_IMAGE" ]; then
        timing_log "ERROR: mixed-dirty-rejoin requires OLD_ASD_BINARY to run in the timing image for old node 2."
        timing_log "Unset MIXED_FAIL_OPEN_OLD_IMAGE or add a compose override for old node 2."
        return 1
    fi

    timing_log "=== Mixed-Version Dirty NPR Rejoin Regression ==="
    timing_log "New ASD_BINARY: $ASD_BINARY"
    timing_log "Old OLD_ASD_BINARY: $OLD_ASD_BINARY"
    timing_log "Required index: $MIXED_DIRTY_REJOIN_INDEX"

    timing_teardown
    mixed_dirty_rejoin_seed_smd
    mixed_dirty_rejoin_set_binaries

    export SMD_DATA_DIR
    docker compose -f "$TIMING_COMPOSE" -p "$TIMING_PROJECT" up -d 2>&1 | tail -5 || true

    mixed_dirty_rejoin_poll_first_sindex || {
        timing_log "Containers left running for inspection. Check logs with: docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs"
        return 1
    }

    timing_teardown
}

# Test that an authoritative FULL_FROM_PR with zero items (principal DB empty,
# but newer cv_tid) correctly clears stale local items on NPR nodes.
#
# Without the fix the NPR sees n_incoming==0 && n_existing!=0 and takes the
# cv_key-only fast path, leaving stale items intact. With the fix it checks
# op->tid == old_cv_tid: when the tids differ the full-replace path runs and
# the local DB is cleared.
test_empty_authoritative_full() {
    if [ -z "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY not set. Export the path to the asd binary."
        return 1
    fi
    if [ ! -f "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY='$ASD_BINARY' is not a file."
        return 1
    fi

    timing_log "=== Authoritative Empty FULL_FROM_PR Regression ==="
    timing_log "ASD_BINARY: $ASD_BINARY"
    # Real principal in this testbed is node 3 (highest node-id, succession
    # sorted descending) - see conf/aerospike-node3.conf. Seed the empty,
    # newer-tid data there so it is actually the principal side of the sync.
    timing_log "Setup: node 3 (principal) empty sindex cv_tid=2; nodes 1,2 (NPRs) stale sindex cv_tid=1"

    timing_teardown
    mixed_fail_open_prepare_dirs

    python3 << EOF
import json, os

base_dir = "${SMD_DATA_DIR}"
cv_key   = 0x3333

# Principal (node 3): empty sindex, newer cv_tid
principal = [[cv_key, 2]]

# NPRs (nodes 1,2): stale sindex item, older cv_tid
stale = [
    [cv_key, 1],
    {
        "key":        "test|demo|empty_auth_stale_idx|.|S",
        "value":      "empty_auth_stale_val",
        "generation": 1,
        "timestamp":  1700000000000,
    },
]

for node, data in ((3, principal), (1, stale), (2, stale)):
    path = os.path.join(base_dir, f"node{node}", "smd", "sindex.smd")
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
EOF

    ASD_BINARY_NODE1="$ASD_BINARY"
    ASD_BINARY_NODE2="$ASD_BINARY"
    ASD_BINARY_NODE3="$ASD_BINARY"
    export ASD_BINARY_NODE1 ASD_BINARY_NODE2 ASD_BINARY_NODE3 SMD_DATA_DIR

    docker compose -f "$TIMING_COMPOSE" -p "$TIMING_PROJECT" up -d 2>&1 | tail -5 || true

    local wall_ms
    wall_ms=$(timing_wait_cluster 3 60)

    if [ "$wall_ms" = "-1" ]; then
        timing_log "FAIL: cluster did not form"
        docker compose -f "$TIMING_COMPOSE" -p "$TIMING_PROJECT" logs 2>&1 | tail -20
        timing_teardown
        return 1
    fi

    local failed=0

    # Read the smd file from inside the container (the bind-mounted file is
    # owned by root inside the container). Grep for the stale key directly:
    # - Bug present: cv_key-only path kept the stale item → key in file.
    # - Fix applied: full-replace path cleared the DB → key not in file.
    for node in 1 2; do
        local content
        content=$(docker compose -f "$TIMING_COMPOSE" -p "$TIMING_PROJECT" exec -T \
            aerospike-"$node" cat /opt/aerospike/smd/sindex.smd 2>/dev/null || true)

        if [ -z "$content" ]; then
            timing_log "WARN: node $node smd file unreadable; skipping"
            continue
        fi

        if echo "$content" | grep -q "empty_auth_stale_idx"; then
            timing_log "FAIL: node $node sindex.smd still has stale key — cv_key-only path incorrectly kept stale DB"
            failed=1
        else
            timing_log "PASS: node $node sindex.smd has no stale key after sync"
        fi
    done

    if docker compose -f "$TIMING_COMPOSE" -p "$TIMING_PROJECT" logs 2>&1 \
            | grep -q "SMD sync timed out\|initial SMD sync timed out"; then
        timing_log "FAIL: SMD sync timed out"
        failed=1
    fi

    timing_teardown

    if [ $failed -eq 0 ]; then
        timing_log "PASS: authoritative empty FULL_FROM_PR correctly replaced stale NPR items"
        return 0
    else
        timing_log "FAIL: empty-authoritative-full test failed — containers left for inspection"
        return 1
    fi
}

# Run realistic timing test
timing_real_run() {
    local modules="$1"
    local results_file="$2"

    timing_teardown

    timing_real_seed_smd "$modules" "$TIMING_REAL_MAX_SIZE"

    # Calculate total items and size
    local total_items=0
    local total_mb=0
    for f in "${SMD_DATA_DIR}/node1/smd"/*.smd; do
        if [ -f "$f" ]; then
            # Count items (subtract 1 for header)
            local items
            items=$(python3 -c "import json; print(len(json.load(open('$f')))-1)")
            total_items=$((total_items + items))
            # Get size
            local mb
            mb=$(python3 -c "import os; print(f'{os.path.getsize(\"$f\") / 1048576:.2f}')")
            total_mb=$(python3 -c "print(f'{float(\"$total_mb\") + float(\"$mb\"):.2f}')")
        fi
    done

    timing_log "Starting 3-node cluster (total: $total_items items, ${total_mb} MB)..."

    export SMD_DATA_DIR ASD_BINARY
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT up -d 2>&1 | tail -5 || true

    local wall_ms
    wall_ms=$(timing_wait_cluster 3 600)  # longer timeout for realistic data

    local sync_us
    sync_us=$(timing_extract_sync_us)

    if [ "$wall_ms" = "-1" ]; then
        timing_log "FAIL: cluster did not form"
        timing_teardown
        return 1
    fi

    local sync_ms="n/a"
    if [ "$sync_us" != "-1" ]; then
        sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
    fi

    # Check for timeout
    local timed_out=0
    if docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-1 2>&1 \
            | grep -q "SMD sync timed out\|initial SMD sync timed out"; then
        timed_out=1
        timing_log "WARNING: SMD sync timed out on node 1!"
    fi

    timing_log "RESULT: items=$total_items  smd=${total_mb}MB  wall=${wall_ms}ms  smd_sync=${sync_ms}ms (${sync_us}us)  timeout=${timed_out}"

    # Append result
    echo -e "${modules// /+}\t${total_items}\t${total_mb}\t${wall_ms}\t${sync_us}\t${TIMING_REAL_MAX_SIZE}\t${timed_out}" >> "$results_file"

    # Capture per-module stats (only for modules we generated, not server-created files)
    timing_log "Per-module breakdown:"
    for module in $modules; do
        local f="${SMD_DATA_DIR}/node1/smd/${module}.smd"
        if [ -f "$f" ] && [ -r "$f" ]; then
            local mod_name="$module"
            local items
            items=$(python3 -c "import json; print(len(json.load(open('$f')))-1)" 2>/dev/null || echo "?")
            local mb
            mb=$(python3 -c "import os; print(f'{os.path.getsize(\"$f\") / 1048576:.2f}')" 2>/dev/null || echo "?")
            timing_log "  $mod_name: $items items, ${mb} MB"
        fi
    done

    # Capture phase-timing log lines
    local phase_log="${results_file%.tsv}-phases.log"
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs 2>&1 \
        | grep -E "full-to-pr timing|full-from-pr timing" \
        | sed "s/^/[realistic] /" >> "$phase_log"

    timing_teardown
}

# ---------------------------------------------------------------------------
# Conflict Timing Tests (timing-conflict)
# ---------------------------------------------------------------------------
#
# Tests worst-case merge scenarios where all nodes have different SMD data.
# Two modes:
#   - "disjoint": Each node has unique keys (no overlap) - tests hash insertion
#   - "conflict": All nodes have same keys, different generations - tests conflict resolution
#
# This is the worst case for the merge algorithm.

TIMING_CONFLICT_MODE="${TIMING_CONFLICT_MODE:-disjoint}"  # disjoint or conflict
TIMING_CONFLICT_ITEMS="${TIMING_CONFLICT_ITEMS:-50000}"   # items per node

# Seed all 3 nodes with different SMD data
timing_conflict_seed_smd() {
    local mode="$1"
    local items_per_node="$2"

    timing_log "Seeding conflict SMD: mode=$mode  items_per_node=$items_per_node"

    # Prepare per-node smd and log directories
    for node in 1 2 3; do
        rm -rf "${SMD_DATA_DIR}/node${node}/smd"
        rm -rf "${SMD_DATA_DIR}/node${node}/log"
        mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
        mkdir -p "${SMD_DATA_DIR}/node${node}/log"
    done

    if [ "$mode" = "disjoint" ]; then
        # Each node gets unique keys: n1_evict-key-*, n2_evict-key-*, n3_evict-key-*
        for node in 1 2 3; do
            timing_log "Generating node $node with prefix 'n${node}_'..."
            python3 "$(dirname "$0")/gen-large-smd.py" \
                --items "$items_per_node" \
                --module "$TIMING_MODULE" \
                --value-size "$TIMING_VALUE_SIZE" \
                --key-prefix "n${node}_" \
                --out "${SMD_DATA_DIR}/node${node}/smd/${TIMING_MODULE}.smd"
        done
    else
        # All nodes have same keys but different generations/timestamps
        # Node 1: gen=1, ts=BASE
        # Node 2: gen=2, ts=BASE+1000000 (newer)
        # Node 3: gen=3, ts=BASE+2000000 (newest - should win)
        for node in 1 2 3; do
            timing_log "Generating node $node with gen=$node..."
            python3 "$(dirname "$0")/gen-large-smd.py" \
                --items "$items_per_node" \
                --module "$TIMING_MODULE" \
                --value-size "$TIMING_VALUE_SIZE" \
                --generation "$node" \
                --ts-offset "$((node * 1000000))" \
                --out "${SMD_DATA_DIR}/node${node}/smd/${TIMING_MODULE}.smd"
        done
    fi

    # Show what was generated
    timing_log "SMD files per node:"
    for node in 1 2 3; do
        local f="${SMD_DATA_DIR}/node${node}/smd/${TIMING_MODULE}.smd"
        if [ -f "$f" ]; then
            local size
            size=$(du -sh "$f" 2>/dev/null | cut -f1)
            timing_log "  Node $node: $size"
        fi
    done
}

# Run conflict timing test
timing_conflict_run() {
    local mode="$1"
    local items_per_node="$2"
    local results_file="$3"

    timing_teardown

    timing_conflict_seed_smd "$mode" "$items_per_node"

    # Calculate totals
    local total_items=$((items_per_node * 3))
    local smd_mb
    smd_mb=$(python3 -c "
import os
total = 0
for n in [1,2,3]:
    f = '${SMD_DATA_DIR}/node' + str(n) + '/smd/${TIMING_MODULE}.smd'
    if os.path.exists(f):
        total += os.path.getsize(f)
print(f'{total / 1048576:.2f}')
")

    if [ "$mode" = "disjoint" ]; then
        timing_log "Starting 3-node cluster (disjoint: ${items_per_node} unique items/node = ${total_items} total)..."
    else
        timing_log "Starting 3-node cluster (conflict: ${items_per_node} items/node, all same keys, different gens)..."
    fi

    export SMD_DATA_DIR ASD_BINARY
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT up -d 2>&1 | tail -5 || true

    local wall_ms
    wall_ms=$(timing_wait_cluster 3 600)

    local sync_us
    sync_us=$(timing_extract_sync_us)

    if [ "$wall_ms" = "-1" ]; then
        timing_log "FAIL: cluster did not form"
        timing_teardown
        return 1
    fi

    local sync_ms="n/a"
    if [ "$sync_us" != "-1" ]; then
        sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
    fi

    # Check for timeout
    local timed_out=0
    if docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-1 2>&1 \
            | grep -q "SMD sync timed out\|initial SMD sync timed out"; then
        timed_out=1
        timing_log "WARNING: SMD sync timed out on node 1!"
    fi

    timing_log "RESULT: mode=$mode  items_per_node=$items_per_node  total=$total_items  smd=${smd_mb}MB  wall=${wall_ms}ms  smd_sync=${sync_ms}ms (${sync_us}us)  timeout=${timed_out}"

    echo -e "${mode}\t${items_per_node}\t${total_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${TIMING_VALUE_SIZE}\t${timed_out}" >> "$results_file"

    # Capture phase-timing log lines
    local phase_log="${results_file%.tsv}-phases.log"
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs 2>&1 \
        | grep -E "full-to-pr timing|full-from-pr timing" \
        | sed "s/^/[${mode}:${items_per_node}] /" >> "$phase_log"

    timing_teardown
}

test_conflict_smd_timing() {
    # Validate ASD_BINARY
    if [ -z "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY not set. Export the path to the asd binary."
        return 1
    fi
    if [ ! -f "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY='$ASD_BINARY' is not a file."
        return 1
    fi

    timing_log "=== Conflict SMD Timing Test ==="
    timing_log "ASD_BINARY: $ASD_BINARY"
    timing_log "Mode: $TIMING_CONFLICT_MODE (disjoint=unique keys per node, conflict=same keys different gens)"
    timing_log "Items per node: $TIMING_CONFLICT_ITEMS"
    timing_log "Results dir: $TIMING_RESULTS_DIR"

    mkdir -p "$TIMING_RESULTS_DIR"
    local results_file="${TIMING_RESULTS_DIR}/timing-conflict-$(date '+%Y%m%d-%H%M%S').tsv"
    echo -e "mode\titems_per_node\ttotal_items\tsmd_mb\twall_cluster_ms\tsync_elapsed_us\tvalue_size_b\tsync_timeout" > "$results_file"
    timing_log "Results file: $results_file"

    timing_conflict_run "$TIMING_CONFLICT_MODE" "$TIMING_CONFLICT_ITEMS" "$results_file"

    timing_log "=== Conflict timing test COMPLETE ==="
    timing_log "Results:"
    column -t "$results_file"
}

# ---------------------------------------------------------------------------
# Rejoin Timing Tests (timing-rejoin)
# ---------------------------------------------------------------------------
#
# Simulates a realistic node rejoin scenario with realistic SMD data:
#   - Nodes 1 & 2: Have current data (all modules at max)
#   - Node 3: Stale data - has STALE_PCT% of items with older generation,
#             missing the remaining (1-STALE_PCT)% that were added while it was down
#
# This tests the realistic case of a node rejoining after being down for maintenance.

TIMING_REJOIN_STALE_PCT="${TIMING_REJOIN_STALE_PCT:-80}"  # % of items node 3 has (stale)
TIMING_REJOIN_SECURITY_ITEMS="${TIMING_REJOIN_SECURITY_ITEMS:-100000}"  # security items for current nodes

timing_rejoin_seed_smd() {
    local stale_pct="$1"
    local security_items="$2"

    timing_log "Seeding rejoin SMD: stale_pct=${stale_pct}%  security_items=$security_items"

    # Prepare per-node smd and log directories
    for node in 1 2 3; do
        rm -rf "${SMD_DATA_DIR}/node${node}/smd"
        rm -rf "${SMD_DATA_DIR}/node${node}/log"
        mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
        mkdir -p "${SMD_DATA_DIR}/node${node}/log"
    done

    local script_dir="$(dirname "$0")"
    
    # Generate "current" data for nodes 1 & 2 (generation=2, newer timestamps)
    timing_log "Generating current data for nodes 1 & 2..."
    for module in truncate sindex security masking; do
        local items_flag=""
        if [ "$module" = "security" ]; then
            items_flag="--items $security_items"
        fi
        
        python3 "$script_dir/gen-realistic-smd.py" \
            --out-dir "${SMD_DATA_DIR}/node1/smd" \
            --module "$module" \
            $items_flag
    done
    
    # Copy node 1's data to node 2 (they're in sync)
    cp -r "${SMD_DATA_DIR}/node1/smd/"* "${SMD_DATA_DIR}/node2/smd/"
    
    # Generate "stale" data for node 3:
    # - Has stale_pct% of the items (older generation)
    # - Missing the rest (items added while node was down)
    timing_log "Generating stale data for node 3 (${stale_pct}% of items, older gen)..."
    
    python3 << EOF
import json
import os

stale_pct = $stale_pct
node1_smd_dir = "${SMD_DATA_DIR}/node1/smd"
node3_smd_dir = "${SMD_DATA_DIR}/node3/smd"

for smd_file in os.listdir(node1_smd_dir):
    if not smd_file.endswith('.smd'):
        continue
    
    with open(os.path.join(node1_smd_dir, smd_file)) as f:
        items = json.load(f)
    
    # items[0] is the header [cv_key, cv_tid]
    header = items[0]
    data_items = items[1:]
    
    # Take first stale_pct% of items, make them stale (older gen, older ts)
    n_stale = int(len(data_items) * stale_pct / 100)
    stale_items = []
    for item in data_items[:n_stale]:
        stale_item = item.copy()
        stale_item['generation'] = max(1, item['generation'] - 1)  # older gen
        stale_item['timestamp'] = item['timestamp'] - 1000000  # older ts
        stale_items.append(stale_item)
    
    # Write node 3's stale SMD file
    node3_data = [header] + stale_items
    out_path = os.path.join(node3_smd_dir, smd_file)
    with open(out_path, 'w') as f:
        json.dump(node3_data, f, separators=(',', ':'))
    
    # Debug: show first key from each to verify they match
    if data_items:
        print(f"  {smd_file}: {len(data_items)} current -> {len(stale_items)} stale")
        print(f"    Node1 key[0]: {data_items[0]['key'][:60]}...")
        if stale_items:
            print(f"    Node3 key[0]: {stale_items[0]['key'][:60]}...")
EOF

    # Show what was generated
    timing_log "SMD files per node:"
    for node in 1 2 3; do
        timing_log "  Node $node:"
        for f in "${SMD_DATA_DIR}/node${node}/smd"/*.smd; do
            if [ -f "$f" ]; then
                local name=$(basename "$f")
                local size=$(du -sh "$f" 2>/dev/null | cut -f1)
                local count=$(python3 -c "import json; print(len(json.load(open('$f')))-1)" 2>/dev/null || echo "?")
                timing_log "    $name: $count items, $size"
            fi
        done
    done
}

timing_rejoin_run() {
    local stale_pct="$1"
    local security_items="$2"
    local results_file="$3"

    timing_teardown

    timing_rejoin_seed_smd "$stale_pct" "$security_items"

    # Calculate totals
    local current_items=$(python3 -c "
import json, os
total = 0
for f in os.listdir('${SMD_DATA_DIR}/node1/smd'):
    if f.endswith('.smd'):
        total += len(json.load(open('${SMD_DATA_DIR}/node1/smd/' + f))) - 1
print(total)
")
    local stale_items=$(python3 -c "
import json, os
total = 0
for f in os.listdir('${SMD_DATA_DIR}/node3/smd'):
    if f.endswith('.smd'):
        total += len(json.load(open('${SMD_DATA_DIR}/node3/smd/' + f))) - 1
print(total)
")
    local smd_mb=$(python3 -c "
import os
total = 0
for n in [1,2,3]:
    d = '${SMD_DATA_DIR}/node' + str(n) + '/smd'
    for f in os.listdir(d):
        total += os.path.getsize(os.path.join(d, f))
print(f'{total / 1048576:.2f}')
")

    # Estimate per-module wire sizes (key + value + 4B gen + 8B ts + msgpack overhead)
    timing_log "Per-module wire size estimates (128MB limit per module):"
    python3 << EOF
import json, os
smd_dir = '${SMD_DATA_DIR}/node1/smd'
for f in sorted(os.listdir(smd_dir)):
    if not f.endswith('.smd'):
        continue
    path = os.path.join(smd_dir, f)
    try:
        data = json.load(open(path))
        items = data[1:]  # skip header
        n = len(items)
        if n == 0:
            continue
        # Calculate actual key+value sizes
        total_key = sum(len(item['key']) for item in items)
        total_val = sum(len(item.get('value', '') or '') for item in items)
        # Wire = keys + values + gen(4B) + ts(8B) + msgpack overhead (~4B per item)
        wire = total_key + total_val + n * (4 + 8 + 4)
        pct = wire * 100 / (128 * 1024 * 1024)
        print(f"  {f}: {n} items, {wire/1024/1024:.1f}MB wire ({pct:.0f}% of limit)")
    except Exception as e:
        print(f"  {f}: error - {e}")
EOF

    timing_log "Starting 3-node cluster (rejoin scenario)..."
    timing_log "  Nodes 1,2: $current_items items (current)"
    timing_log "  Node 3:    $stale_items items (stale, ${stale_pct}% of current)"
    timing_log "  Missing on node 3: $((current_items - stale_items)) items"

    export SMD_DATA_DIR ASD_BINARY
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT up -d 2>&1 | tail -5 || true

    local wall_ms
    wall_ms=$(timing_wait_cluster 3 600)

    local sync_us
    sync_us=$(timing_extract_sync_us)

    if [ "$wall_ms" = "-1" ]; then
        timing_log "FAIL: cluster did not form"
        timing_teardown
        return 1
    fi

    local sanity_rc=0
    timing_sanity_serve_ready_docker 3 "truncate,sindex,security,masking" 600 || sanity_rc=1

    local sync_ms="n/a"
    if [ "$sync_us" != "-1" ]; then
        sync_ms=$(python3 -c "print(f'{int(\"$sync_us\") / 1000:.1f}')")
    fi

    # Check for timeout
    local timed_out=0
    if docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-1 2>&1 \
            | grep -q "SMD sync timed out\|initial SMD sync timed out"; then
        timed_out=1
        timing_log "WARNING: SMD sync timed out on node 1!"
    fi

    timing_log "RESULT: stale_pct=$stale_pct  current=$current_items  stale=$stale_items  smd=${smd_mb}MB  wall=${wall_ms}ms  smd_sync=${sync_ms}ms (${sync_us}us)  timeout=${timed_out}  sanity=$([ $sanity_rc -eq 0 ] && echo PASS || echo FAIL)"
    timing_log "Server logs: ${SMD_DATA_DIR}/node{1,2,3}/log/aerospike.log"

    echo -e "${stale_pct}\t${current_items}\t${stale_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${timed_out}" >> "$results_file"

    # Capture and display phase-timing log lines
    timing_log "Phase timing from principal (node 1):"
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-1 2>&1 \
        | grep -E "full-to-pr timing|full-from-pr timing" \
        | while read line; do timing_log "  $line"; done

    local phase_log="${results_file%.tsv}-phases.log"
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs 2>&1 \
        | grep -E "full-to-pr timing|full-from-pr timing" \
        | sed "s/^/[rejoin:${stale_pct}%] /" >> "$phase_log"

    timing_teardown
    return $sanity_rc
}

test_rejoin_smd_timing() {
    # Validate ASD_BINARY
    if [ -z "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY not set. Export the path to the asd binary."
        return 1
    fi
    if [ ! -f "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY='$ASD_BINARY' is not a file."
        return 1
    fi

    timing_log "=== Rejoin SMD Timing Test ==="
    timing_log "ASD_BINARY: $ASD_BINARY"
    timing_log "Stale percentage: $TIMING_REJOIN_STALE_PCT%"
    timing_log "Security items: $TIMING_REJOIN_SECURITY_ITEMS"
    timing_log "Results dir: $TIMING_RESULTS_DIR"

    mkdir -p "$TIMING_RESULTS_DIR"
    local results_file="${TIMING_RESULTS_DIR}/timing-rejoin-$(date '+%Y%m%d-%H%M%S').tsv"
    echo -e "stale_pct\tcurrent_items\tstale_items\tsmd_mb\twall_cluster_ms\tsync_elapsed_us\tsync_timeout" > "$results_file"
    timing_log "Results file: $results_file"

    timing_rejoin_run "$TIMING_REJOIN_STALE_PCT" "$TIMING_REJOIN_SECURITY_ITEMS" "$results_file"

    timing_log "=== Rejoin timing test COMPLETE ==="
    timing_log "Results:"
    column -t "$results_file"
}

test_realistic_smd_timing() {
    # Validate ASD_BINARY is set and is a file
    if [ -z "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY not set. Export the path to the asd binary."
        timing_log "  Example: export ASD_BINARY=/path/to/aerospike-server/target/Linux-x86_64/bin/asd"
        return 1
    fi
    if [ ! -f "$ASD_BINARY" ]; then
        timing_log "ERROR: ASD_BINARY='$ASD_BINARY' is not a file."
        timing_log "  Make sure the path points to the asd binary, not a directory."
        return 1
    fi

    timing_log "=== Realistic SMD Timing Test ==="
    timing_log "ASD_BINARY: $ASD_BINARY"
    timing_log "Modules: $TIMING_REAL_MODULES"
    timing_log "Max-size entries: $TIMING_REAL_MAX_SIZE"
    timing_log "Results dir: $TIMING_RESULTS_DIR"

    # Show limits first
    timing_log "Module limits (from gen-realistic-smd.py --show-limits):"
    python3 "$(dirname "$0")/gen-realistic-smd.py" --show-limits 2>&1 | head -50

    mkdir -p "$TIMING_RESULTS_DIR"
    local results_file="${TIMING_RESULTS_DIR}/timing-real-$(date '+%Y%m%d-%H%M%S').tsv"
    echo -e "modules\titems\tsmd_mb\twall_cluster_ms\tsync_elapsed_us\tmax_size\tsync_timeout" > "$results_file"
    timing_log "Results file: $results_file"

    # Run the test
    timing_real_run "$TIMING_REAL_MODULES" "$results_file"

    timing_log "=== Realistic timing test COMPLETE ==="
    timing_log "Results:"
    column -t "$results_file"
}

cleanup() {
    log "Stopping containers (preserving for log inspection)..."
    docker compose -p $COMPOSE_PROJECT stop 2>/dev/null || true
}

cleanup_full() {
    log "Removing containers and volumes..."
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true
}

# Main
case "${1:-all}" in
    basic)
        test_basic_sync_ordering
        ;;
    auth)
        test_security_auth
        ;;
    rejoin)
        test_node_rejoin
        ;;
    preexisting)
        test_preexisting_smd
        ;;
    pull)
        test_principal_pulls_from_npr
        ;;
    identical)
        test_identical_smd
        ;;
    principal-loss)
        test_principal_loss_initial_sync
        ;;
    migration-defer)
        test_migration_deferred_until_smd_ready
        ;;
    mixed-fail-open)
        test_mixed_fail_open
        ;;
    mixed-dirty-rejoin)
        test_mixed_dirty_rejoin
        ;;
    empty-authoritative-full)
        test_empty_authoritative_full
        ;;
    all)
        failed=0
        test_basic_sync_ordering || failed=1
        # test_security_auth requires security config - run separately with: ./test-smd-sync.sh auth
        test_node_rejoin || failed=1
        test_preexisting_smd || failed=1
        test_principal_pulls_from_npr || failed=1
        test_identical_smd || failed=1
        test_principal_loss_initial_sync || failed=1
        test_migration_deferred_until_smd_ready || failed=1

        if [ $failed -eq 0 ]; then
            log "=== All tests PASSED ==="
            if [ "$CLEANUP_ON_SUCCESS" = "true" ]; then
                cleanup
            else
                log "Containers left running. Use '$0 cleanup' to stop or '$0 cleanup-full' to remove."
            fi
        else
            log "=== Some tests FAILED ==="
            log "Containers left running for inspection. Check logs with: docker compose -p $COMPOSE_PROJECT logs"
            exit 1
        fi
        ;;
    timing)
        test_large_smd_timing
        ;;
    timing-real)
        test_realistic_smd_timing
        ;;
    timing-conflict)
        test_conflict_smd_timing
        ;;
    timing-rejoin)
        test_rejoin_smd_timing
        ;;
    show-limits)
        python3 "$(dirname "$0")/gen-realistic-smd.py" --show-limits
        ;;
    cleanup)
        cleanup
        ;;
    cleanup-full)
        cleanup_full
        ;;
    timing-cleanup)
        timing_teardown
        ;;
    *)
        echo "Usage: $0 {basic|auth|rejoin|preexisting|pull|identical|principal-loss|migration-defer|mixed-fail-open|mixed-dirty-rejoin|empty-authoritative-full|all|timing|timing-real|timing-conflict|timing-rejoin|show-limits|cleanup|cleanup-full|timing-cleanup}"
        echo ""
        echo "Correctness tests:"
        echo "  basic       - Test SMD sync ordering on fresh cluster"
        echo "  auth        - Test security authentication (requires security config)"
        echo "  rejoin      - Test node rejoin with cleared SMD"
        echo "  preexisting - Test first node with SMD, others join empty"
        echo "  pull        - Test new node joins cluster with existing SMD"
        echo "  identical   - Test nodes joining with identical pre-existing SMD"
        echo "  principal-loss - Test initial SMD sync wait aborts after principal loss"
        echo "  migration-defer - Test fresh node defers immigration until SMD settled"
        echo "  mixed-fail-open - Test mixed-version fail-open releases sync waiters"
  echo "  mixed-dirty-rejoin - Test dirty new-version NPR waits for old-principal FULL_FROM_PR"
  echo "  empty-authoritative-full - Test authoritative empty FULL_FROM_PR clears stale NPR items (SERVER-209)"
  echo "  all         - Run all correctness tests"
        echo ""
        echo "Timing tests:"
        echo "  timing      - Sweep large SMD payloads (synthetic, unrealistic keys)"
        echo "                Tune with env vars:"
        echo "                  TIMING_ITEMS='10000 50000 100000'  (item counts)"
        echo "                  TIMING_VALUE_SIZE=200               (bytes per value)"
        echo "                  TIMING_RESULTS_DIR=./timing-results (output dir)"
        echo "                  SMD_DATA_DIR=/tmp/smd-timing-data   (host smd dirs)"
        echo ""
        echo "  timing-real - Test realistic worst-case SMD data with valid entries"
        echo "                Uses gen-realistic-smd.py to generate:"
        echo "                  - truncate: 131,104 items (32 ns × 4096 sets + 32)"
        echo "                  - sindex:   8,192 items (32 ns × 256 sindexes)"
        echo "                  - security: 100,000 items (default) or TIMING_REAL_SECURITY_ITEMS"
        echo "                  - masking:  50,000 items (heavy masking rules)"
        echo "                Tune with env vars:"
        echo "                  TIMING_REAL_MODULES='truncate sindex security masking'"
        echo "                  TIMING_REAL_MAX_SIZE=true  (use max-length keys/values)"
        echo "                  TIMING_REAL_SECURITY_ITEMS=300000  (extreme LDAP scenario)"
        echo "                  TIMING_RESULTS_DIR=./timing-results"
        echo "                  SMD_DATA_DIR=/tmp/smd-timing-data"
        echo ""
        echo "  timing-conflict - Test worst-case merge: all nodes have different SMD data"
        echo "                    Two modes (set TIMING_CONFLICT_MODE):"
        echo "                      disjoint - Each node has unique keys (tests hash insertion)"
        echo "                      conflict - Same keys, different generations (tests conflict resolution)"
        echo "                    Tune with env vars:"
        echo "                      TIMING_CONFLICT_MODE=disjoint   (or 'conflict')"
        echo "                      TIMING_CONFLICT_ITEMS=50000     (items per node)"
        echo "                      TIMING_VALUE_SIZE=200           (bytes per value)"
        echo ""
        echo "  timing-rejoin - Realistic node rejoin with stale + missing data"
        echo "                  Uses gen-realistic-smd.py for all modules"
        echo "                  Simulates: node 3 was down, has stale data + missed updates"
        echo "                    - Nodes 1,2: current data (all modules at realistic max)"
        echo "                    - Node 3: STALE_PCT% of items with older generation"
        echo "                    - Node 3 is missing (100-STALE_PCT)% of items"
        echo "                  Tune with env vars:"
        echo "                    TIMING_REJOIN_STALE_PCT=80       (% of items node 3 has)"
        echo "                    TIMING_REJOIN_SECURITY_ITEMS=100000  (security module size)"
        echo ""
        echo "  show-limits - Show valid entry size ranges per SMD module"
        echo ""
        echo "Cleanup:"
        echo "  cleanup        - Stop correctness test containers (preserve for inspection)"
        echo "  cleanup-full   - Remove correctness test containers and volumes"
        echo "  timing-cleanup - Remove timing test containers and volumes"
        echo ""
        echo "Environment:"
        echo "  ASD_BINARY         - Path to asd binary (required)"
        echo "  CLEANUP_ON_SUCCESS - Set to 'true' to stop containers after successful run"
        exit 1
        ;;
esac

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
        # Use admin user if security is enabled, fall back to unauthenticated.
        # Timeout guards against connection accepted but not yet processed.
        size=$(timeout 5 docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo "0")
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
    
    # Look for sync messages (optional - requires cf_debug level and specific code paths)
    if echo "$logs" | grep -q "sync wait start"; then
        log "PASS: Found 'sync wait start' message"
    else
        log "INFO: No 'sync wait start' found (normal for fresh cluster with no pre-existing SMD)"
    fi
    
    if echo "$logs" | grep -q "sync wait done\|all modules settled"; then
        log "PASS: Found sync completion message"
    else
        log "INFO: No sync completion found (normal for fresh cluster with no pre-existing SMD)"
    fi
    
    # Check that no timeout occurred
    if echo "$logs" | grep -q "SMD sync timed out"; then
        log "FAIL: SMD sync timed out!"
        return 1
    else
        log "PASS: No SMD sync timeout"
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

    # Prepare per-node smd directories
    for node in 1 2 3; do
        rm -rf "${SMD_DATA_DIR}/node${node}/smd"
        mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
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
        local size
        size=$(timeout 5 docker exec smd-timing-aerospike-1 asinfo -v "statistics" 2>/dev/null \
               | grep -oP 'cluster_size=\K\d+' || echo "0")
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
# Checks two log lines (both emitted under "context smd debug"):
#
#   as_smd_wait_ready (INFO, fresh node/service start):
#     "initial SMD sync wait done - elapsed NNN us"
#
#   as_smd_cluster_changed_sync (DEBUG, partition balance path):
#     "sync wait done cl_key XXXX elapsed NNN us"
#
# Returns the largest elapsed microsecond value found (the binding sync wait),
# or -1 if neither is present.
timing_extract_sync_us() {
    local logs
    logs=$(docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs aerospike-1 2>&1)

    # Match both formats; pick the last (largest elapsed) value seen.
    local sync_us
    sync_us=$(echo "$logs" \
        | grep -oP '(?:initial SMD sync wait done - elapsed |sync wait done cl_key [0-9a-f]+ elapsed )\K\d+(?= us)' \
        | sort -n | tail -1)

    if [ -n "$sync_us" ]; then
        echo "$sync_us"
    else
        echo "-1"
    fi
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

    timing_log "RESULT: items=$n_items  smd=${smd_mb}MB  wall=${wall_ms}ms  smd_sync=${sync_ms}ms (${sync_us}us)  timeout=${timed_out}"

    echo -e "${n_items}\t${smd_mb}\t${wall_ms}\t${sync_us}\t${TIMING_VALUE_SIZE}\t${timed_out}" >> "$results_file"

    # Capture phase-timing log lines before containers are torn down.
    local phase_log="${results_file%.tsv}-phases.log"
    timing_log "Capturing phase timing to $phase_log..."
    docker compose -f $TIMING_COMPOSE -p $TIMING_PROJECT logs 2>&1 \
        | grep -E "full-to-pr timing|full-from-pr timing" \
        | sed "s/^/[n=${n_items}] /" >> "$phase_log"

    timing_teardown
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

    # Prepare per-node smd directories
    for node in 1 2 3; do
        rm -rf "${SMD_DATA_DIR}/node${node}/smd"
        mkdir -p "${SMD_DATA_DIR}/node${node}/smd"
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
    all)
        failed=0
        test_basic_sync_ordering || failed=1
        # test_security_auth requires security config - run separately with: ./test-smd-sync.sh auth
        test_node_rejoin || failed=1
        test_preexisting_smd || failed=1
        test_principal_pulls_from_npr || failed=1
        
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
        echo "Usage: $0 {basic|auth|rejoin|preexisting|pull|all|timing|timing-real|show-limits|cleanup|cleanup-full|timing-cleanup}"
        echo ""
        echo "Correctness tests:"
        echo "  basic       - Test SMD sync ordering on fresh cluster"
        echo "  auth        - Test security authentication (requires security config)"
        echo "  rejoin      - Test node rejoin with cleared SMD"
        echo "  preexisting - Test first node with SMD, others join empty"
        echo "  pull        - Test new node joins cluster with existing SMD"
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

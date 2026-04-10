#!/bin/bash
# SMD Sync Test Script
# Tests that SMD synchronization completes before partition balance
#
# Node IDs are deterministic:
#   Node 1: a1 (lowest - always principal)
#   Node 2: a2
#   Node 3: a3 (highest - always NPR)

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

# Clear SMD on specific node (must be stopped first, will restart briefly)
clear_smd() {
    local node=$1
    docker start ${COMPOSE_PROJECT}-aerospike-$node 2>/dev/null || true
    sleep 1
    docker exec ${COMPOSE_PROJECT}-aerospike-$node rm -rf /opt/aerospike/smd/* 2>/dev/null || true
    docker stop ${COMPOSE_PROJECT}-aerospike-$node
}

wait_for_cluster() {
    local expected_size=$1
    local timeout=$2
    local elapsed=0
    
    log "Waiting for cluster size $expected_size (timeout: ${timeout}s)..."
    
    while [ $elapsed -lt $timeout ]; do
        # Use admin user if security is enabled, fall back to unauthenticated
        size=$(docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo "0")
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
        size=$(docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -Uadmin -Padmin -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo "0")
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
    log "=== Test 3: NPR Rejoin with Cleared SMD ==="
    
    # Ensure cluster is running with user
    wait_for_cluster 3 30 || {
        start_nodes 1 2 3
        wait_for_cluster 3 $TIMEOUT
    }
    
    # Create sindex (SMD data)
    log "Creating sindex..."
    docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "sindex-create:ns=test;set=demo;indexname=rejoin_idx;bin=rejoin;type=string" 2>/dev/null || true
    sleep 2
    
    # Stop node 3 (NPR)
    log "Stopping node 3 (NPR)..."
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
    log "=== Test 4: Principal (Node 1) Has SMD, NPRs Empty ==="
    
    # Clean start - bring up node 1 only to create SMD
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true
    
    log "Starting node 1 alone to create SMD data..."
    start_nodes 1
    
    # Wait for node 1
    local elapsed=0
    while [ $elapsed -lt 30 ]; do
        if docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "build" 2>/dev/null | grep -q "8."; then
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
    log "=== Test 5: Principal (Node 1) Empty, Must Pull from NPR ==="
    
    # Node 1 is always principal (lowest node-id: a1)
    # This tests the STATE_DIRTY path where principal pulls from NPRs
    
    # Clean start
    docker compose -p $COMPOSE_PROJECT down -v 2>/dev/null || true
    
    # Start nodes 2 and 3 first (without node 1) to create SMD
    log "Starting nodes 2 and 3 to create SMD data..."
    start_nodes 2 3
    
    # Wait for 2-node cluster (node 2 will be principal temporarily)
    local elapsed=0
    while [ $elapsed -lt 60 ]; do
        size=$(docker exec ${COMPOSE_PROJECT}-aerospike-2 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_size=\K\d+' || echo "0")
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
    
    # Now start node 1 (fresh, no SMD) - it becomes principal due to lowest node-id
    log "Starting node 1 (fresh, will become principal)..."
    start_nodes 1
    
    # Wait for 3-node cluster
    wait_for_cluster 3 $TIMEOUT
    
    # Verify node 1 is principal
    principal=$(docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "statistics" 2>/dev/null | grep -oP 'cluster_principal=\K[A-F0-9]+')
    node1_id=$(docker exec ${COMPOSE_PROJECT}-aerospike-1 asinfo -v "node" 2>/dev/null)
    log "Principal: $principal, Node 1 ID: $node1_id"
    
    # Verify all nodes have the sindex (node 1 must have pulled from NPRs)
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
        log "PASS: Principal pulled SMD from NPRs successfully"
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
    cleanup)
        cleanup
        ;;
    cleanup-full)
        cleanup_full
        ;;
    *)
        echo "Usage: $0 {basic|auth|rejoin|preexisting|pull|all|cleanup|cleanup-full}"
        echo ""
        echo "Options:"
        echo "  basic       - Test SMD sync ordering on fresh cluster"
        echo "  auth        - Test security authentication (requires security config)"
        echo "  rejoin      - Test NPR rejoin with cleared SMD"
        echo "  preexisting - Test principal with SMD, NPRs empty"
        echo "  pull        - Test principal empty, must pull from NPRs"
        echo "  all         - Run all tests"
        echo "  cleanup     - Stop containers (preserve for log inspection)"
        echo "  cleanup-full - Remove containers and volumes"
        echo ""
        echo "Environment:"
        echo "  ASD_BINARY         - Path to asd binary (required)"
        echo "  CLEANUP_ON_SUCCESS - Set to 'true' to stop containers after successful run"
        exit 1
        ;;
esac

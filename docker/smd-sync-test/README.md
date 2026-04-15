# SMD Sync Test Environment

Test environment for validating SMD synchronization behavior during cluster formation,
particularly with heterogeneous SMD state (e.g., nodes with different security/RBAC data).

## Deterministic Principal Selection

Each node has a fixed `node-id` in its config:
- **Node 1**: `a1` (lowest - always principal)
- **Node 2**: `a2`
- **Node 3**: `a3` (highest - always NPR)

This ensures tests are deterministic and repeatable.

## Test Scenarios

| Test | Description |
|------|-------------|
| `basic` | Fresh 3-node cluster, verify SMD sync completes before partition balance |
| `auth` | Security authentication works immediately after cluster start (requires security config) |
| `rejoin` | NPR (node 3) rejoins with cleared SMD, syncs from principal |
| `preexisting` | Principal (node 1) has SMD data, NPRs start empty |
| `pull` | Principal (node 1) starts empty, must pull SMD from NPRs |

## Prerequisites

- Built `asd` binary with SMD sync changes
- Valid `features.conf` license file
- Docker with Compose v2

## Quick Start

```bash
cd local/docker/smd-sync-test

# Set binary path
export ASD_BINARY=/path/to/aerospike-server/target/Linux-x86_64/bin/asd

# Run sindex SMD sync tests (tests 1, 3, 4, 5)
./test-smd-sync.sh all

# Run security SMD sync test separately (test 2 - uses security-enabled config)
./test-smd-sync.sh auth

# Run individual tests
./test-smd-sync.sh basic
./test-smd-sync.sh pull

# View logs after test
docker compose -p smd-sync-test logs

# Cleanup
./test-smd-sync.sh cleanup       # Stop containers (keep for inspection)
./test-smd-sync.sh cleanup-full  # Remove containers and volumes
```

**Note:** The `auth` test uses a separate docker-compose file with security-enabled configs
and is not included in `all` to avoid config conflicts.

## Test Details

### Test 1: Basic SMD Sync Ordering (`basic`)

Verifies SMD sync completes before partition balance on fresh cluster formation.

**What it checks:**
- "sync wait start" appears in logs
- "sync wait done" or "all modules settled" appears
- No "SMD sync timed out" warning

### Test 2: Security SMD Sync (`auth`)

Verifies security SMD (users/roles) syncs across nodes. Creates a user on node 1
and verifies authentication works on nodes 2 and 3.

**Note:** Uses separate `docker-compose-security.yaml` with security-enabled configs.
Run separately with `./test-smd-sync.sh auth`.

### Test 3: NPR Rejoin (`rejoin`)

Tests that an NPR with cleared SMD syncs correctly when rejoining.

**Scenario:**
1. 3-node cluster running with sindex
2. Stop node 3 (NPR), clear its SMD
3. Restart node 3
4. Verify node 3 gets the sindex via SMD sync

### Test 4: Principal Has SMD (`preexisting`)

Tests the common case where principal has data and NPRs are empty.

**Scenario:**
1. Start node 1 alone, create sindex
2. Start nodes 2 and 3 (fresh, no SMD)
3. Verify all nodes get the sindex

### Test 5: Principal Pulls from NPR (`pull`)

Tests the STATE_DIRTY path where principal must pull data from NPRs.

**Scenario:**
1. Start nodes 2 and 3, create sindex
2. Start node 1 (fresh, becomes principal due to lowest node-id)
3. Verify node 1 pulls sindex from NPRs
4. Verify all nodes have the sindex

## Configuration Files

| File | Description |
|------|-------------|
| `conf/aerospike-node[1-3].conf` | Node configs without security (for sindex tests) |
| `conf-security/aerospike-node[1-3].conf` | Node configs with security enabled (for auth test) |
| `conf/features.conf` | Enterprise license file |
| `docker-compose.yaml` | Default compose (no security) |
| `docker-compose-security.yaml` | Security-enabled compose (for auth test) |

## Debug Logging

The node configs have debug logging enabled for `smd` and `exchange` contexts.
Look for these log messages:

```
DEBUG (smd): sync wait start cl_key XXX size N
DEBUG (smd): all modules settled - signaling sync complete
DEBUG (smd): sync wait done cl_key XXX elapsed NNN us
```

If you see this warning, there's a problem:
```
WARNING (smd): SMD sync timed out after 30 seconds - proceeding anyway
```

## Timing Tests

Measures SMD full-sync time as a function of payload size. Uses pre-seeded `.smd`
JSON files injected directly into the principal's work directory before node start,
bypassing `asinfo` entirely to reach MB–100 MB scale quickly.

### Quick Start

```bash
# Default sweep: 10K, 50K, 100K items at 200B/value (~3–38 MB)
export ASD_BINARY=/path/to/asd
./test-smd-sync.sh timing

# Custom sweep: larger items
TIMING_ITEMS="10000 50000 100000" TIMING_VALUE_SIZE=1024 ./test-smd-sync.sh timing

# Results are written to ./timing-results/timing-YYYYMMDD-HHMMSS.tsv
```

### What Gets Measured

| Metric | Source |
|--------|--------|
| `wall_cluster_ms` | Wall-clock ms from container start until `cluster_size=3` |
| `sync_elapsed_us` | Microseconds from SMD sync log: `initial SMD sync wait done - elapsed NNN us` (service-start path) or `sync wait done cl_key … elapsed NNN us` (partition-balance path) |

### Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `TIMING_ITEMS` | `"10000 50000 100000"` | Space-separated item counts to sweep |
| `TIMING_VALUE_SIZE` | `200` | Bytes per SMD value string |
| `SMD_DATA_DIR` | `/tmp/smd-timing-data` | Host directory for per-node smd bind mounts |
| `TIMING_RESULTS_DIR` | `./timing-results` | Where TSV results are written |

### File Layout

```
gen-large-smd.py          -- generates .smd JSON for any item count
docker-compose-timing.yaml -- 3-node compose with per-node smd bind mounts
timing-results/           -- TSV output from timing runs
```

### How It Works

1. `gen-large-smd.py` writes `${SMD_DATA_DIR}/node1/smd/sindex.smd` with N items.
2. Nodes 2 & 3 get empty smd dirs — they start with no local SMD.
3. When the cluster forms, node 1 (principal, lowest node-id `a1`) must
   full-sync its entire DB to nodes 2 and 3 via `module_fill_msg` → fabric msgpack.
4. The test captures the elapsed time from the server's own sync-completion log.

## Limitations

SMD sync completing does **not** guarantee all subsystems are fully ready:

| Module | What SMD Sync Guarantees | What It Does NOT Guarantee |
|--------|--------------------------|----------------------------|
| **Sindex** | Index definition exists on all nodes | Index is populated and queryable |
| **UDF** | UDF files written to disk | Lua modules compiled |
| **Security** | Users/roles replicated | N/A - immediately usable |
| **Roster** | Roster config replicated | N/A - immediately usable |
| **XDR** | DC configs replicated | Connections established |

### Secondary Index Caveat

This is the most significant limitation. After SMD sync completes:
1. The sindex **definition** exists on the node
2. The sindex **data structure** is created
3. But `si->readable = false` until population completes

Population scans all records in the namespace/set, which can take significant time
for large datasets. Until population completes, queries against the sindex will
return incomplete results.

**To check sindex readiness:**
```bash
asinfo -v 'sindex-list:ns=test'
# Look for sync_state=synced (definition synced) vs actual population status
```

The sync only ensures metadata is consistent across nodes before partition
balance runs - it does not wait for background initialization tasks.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ASD_BINARY` | Path to asd binary (required) |
| `CLEANUP_ON_SUCCESS` | Set to `true` to stop containers after successful run |

## Troubleshooting

### Containers exit immediately
Check the binary path:
```bash
ls -la $ASD_BINARY
```

### Cluster not forming
Check mesh seed addresses resolve:
```bash
docker exec smd-sync-test-aerospike-1 getent hosts smd-sync-test-aerospike-2
```

### SMD sync timing out
Check debug logs for which module is not settling:
```bash
docker compose -p smd-sync-test logs 2>&1 | grep -E "smd|settled|timeout"
```

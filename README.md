# asd-testbeds

Local development and testing environments for Aerospike Server.

## Testbeds

| Directory | Description |
|-----------|-------------|
| [`docker/`](docker/) | Docker Compose cluster (1-N nodes) |
| [`docker/smd-sync-test/`](docker/smd-sync-test/) | SMD synchronization test harness |
| [`native/`](native/) | Run asd directly on host (for IDE debugging) |
| [`workloads/python/`](workloads/python/) | Python scripts to exercise server code paths |

## Quick Start

### Docker (recommended for cluster testing)

```bash
cd docker
cp env.example .env
# Edit .env to set ASD_BINARY path
cp /path/to/features.conf conf/
docker compose up -d
```

### Native (recommended for debugging)

```bash
cd native
sudo ./setup.sh
sudo cp /path/to/features.conf /etc/aerospike/
./run.sh
```

See individual README files in each directory for details.

## Requirements

- Locally-built `asd` binary (Enterprise Edition)
- Valid `features.conf` license file
- Docker with Compose v2 (for docker testbeds)

## License Keys

Feature keys are gitignored. Copy your `features.conf` to the appropriate `conf/` directory:

```bash
# For docker testbed
cp /path/to/features.conf docker/conf/

# For smd-sync-test
cp /path/to/features.conf docker/smd-sync-test/conf/
cp /path/to/features.conf docker/smd-sync-test/conf-security/
```

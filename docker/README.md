# Aerospike Server Local Development Environment

Docker Compose setup for running a local Aerospike cluster with a locally-built `asd` binary.

## Prerequisites

- Docker with Compose v2
- A locally-built `asd` binary (Enterprise Edition)
- Valid `features.conf` license file

## Quick Start

1. **Configure your environment**

   Copy the example env file and edit it:
   ```bash
   cp env.example .env
   ```

   Set `ASD_BINARY` to point to your built binary:
   ```bash
   # .env
   ASD_BINARY=/path/to/aerospike-server/target/Linux-x86_64/bin/asd
   ```

2. **Add your license file**

   Copy your `features.conf` to `conf/`:
   ```bash
   cp /path/to/features.conf conf/
   ```

3. **Build and start the cluster**
   ```bash
   docker compose build
   docker compose up -d
   ```

4. **Verify the cluster**
   ```bash
   docker compose ps
   docker exec docker-aerospike-1 asinfo -v "statistics" | grep cluster_size
   ```

## Usage

### Start/Stop
```bash
docker compose up -d      # Start cluster
docker compose down       # Stop cluster
docker compose restart    # Restart after rebuilding asd
```

### View Logs
```bash
docker compose logs -f aerospike           # All nodes
docker compose logs -f docker-aerospike-1     # Single node
```

### Access Tools
```bash
docker exec docker-aerospike-1 asinfo -v build
docker exec docker-aerospike-1 asadm
docker exec -it docker-aerospike-1 aql
```

### Get a Shell
```bash
docker exec -it docker-aerospike-1 bash
```

### Change Replica Count
```bash
REPLICAS=1 docker compose up -d   # Single node
REPLICAS=5 docker compose up -d   # 5-node cluster
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASD_BINARY` | `./bin/asd` | Path to locally-built asd binary |
| `REPLICAS` | `3` | Number of cluster nodes |

### Files

| File | Description |
|------|-------------|
| `conf/aerospike.conf` | Server configuration |
| `conf/features.conf` | Enterprise license file |
| `.env` | Environment variables (create from `.env.example`) |

## Development Workflow

1. Make changes to aerospike-server source
2. Build: `make -j$(nproc) EEREPO=/path/to/aerospike-server-enterprise USE_EE=1`
3. Restart containers: `docker compose restart`

The mounted binary is picked up immediately on container restart.

## Cluster Configuration

The default `conf/aerospike.conf` configures:
- 3-node mesh cluster (nodes: `docker-aerospike-1`, `docker-aerospike-2`, `docker-aerospike-3`)
- Strong consistency enabled
- In-memory storage (1GB)
- Namespace: `test`

Edit `conf/aerospike.conf` to customize. Changes require container restart.

## Troubleshooting

### Container exits immediately
Check logs: `docker compose logs aerospike`

Common issues:
- Missing/invalid `features.conf`
- Invalid config syntax
- Missing directories (rebuild image: `docker compose build --no-cache`)

### Cluster not forming
Verify all nodes can resolve each other:
```bash
docker exec docker-aerospike-1 getent hosts docker-aerospike-2
```

### "not on roster" warnings
Normal for strong-consistency namespace without roster set. Set roster via asadm if needed.

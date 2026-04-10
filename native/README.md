# Native Local Development

Run asd directly on your host machine for debugging.

## One-Time Setup

```bash
# Create directories (requires sudo)
sudo ./setup.sh

# Copy your license
sudo cp /path/to/features.conf /etc/aerospike/
```

## Usage

### Run from terminal
```bash
./run.sh
./run.sh --cold-start
```

### Debug with IDE
Use the VS Code/Cursor debug configurations in `.vscode/launch.json`:
- **Debug asd (native)** - Start under debugger
- **Debug asd (cold-start)** - Start with cold-start flag
- **Attach to running asd** - Attach to existing process

Press F5 or use Run & Debug panel (Ctrl+Shift+D).

## Files

| File | Description |
|------|-------------|
| `aerospike.conf` | Single-node config |
| `run.sh` | Run asd in foreground |
| `setup.sh` | One-time directory setup (run with sudo) |

## vs Docker Setup

| | Native | Docker (`../docker/`) |
|---|--------|---------------------|
| IDE debugging | ✅ Direct | Remote only |
| Multi-node | Manual | ✅ Easy |
| Clean environment | No | ✅ Yes |

Use **native** for debugging, **Docker** for cluster testing.

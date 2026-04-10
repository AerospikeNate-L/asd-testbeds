#!/bin/bash
# Run asd locally for development/debugging

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ASD="$REPO_ROOT/aerospike-server/target/Linux-x86_64/bin/asd"
CONFIG="$SCRIPT_DIR/aerospike.conf"

# proto-fd-max in aerospike.conf; integrated terminals often inherit a high nofile
# limit from the IDE, while a new login/tty shell commonly defaults to 1024.
REQUIRED_NOFILE=15000
current_nofile=$(ulimit -n)
if [[ "$current_nofile" != "unlimited" && "$current_nofile" -lt "$REQUIRED_NOFILE" ]]; then
	if ! ulimit -n "$REQUIRED_NOFILE"; then
		echo "Error: cannot raise open file limit to $REQUIRED_NOFILE (matches proto-fd-max in $CONFIG)."
		echo "Current ulimit -n: $current_nofile. Raise the hard nofile limit (e.g. /etc/security/limits.conf) or run: ulimit -n $REQUIRED_NOFILE"
		exit 1
	fi
fi

if [[ ! -x "$ASD" ]]; then
    echo "Error: asd binary not found at $ASD"
    echo "Build with: cd $REPO_ROOT/aerospike-server && make -j\$(nproc) EEREPO=$REPO_ROOT/aerospike-server-enterprise USE_EE=1"
    exit 1
fi

if [[ ! -f /etc/aerospike/features.conf ]]; then
    echo "Error: /etc/aerospike/features.conf not found"
    echo "Copy your license file: sudo cp /path/to/features.conf /etc/aerospike/"
    exit 1
fi

echo "Starting: $ASD"
echo "Config: $CONFIG"
echo "Logs: /var/log/aerospike/aerospike.log"
echo "---"
exec "$ASD" --foreground --config-file "$CONFIG" "$@"

#!/bin/bash
set -e

# Wait for network
sleep 0.5

# Create directories if needed
mkdir -p /var/lib/aerospike /var/log/aerospike /var/run/aerospike

case "$1" in
    "asd")
        shift
        if [ ! -x /usr/bin/asd ]; then
            echo "Error: No asd binary found at /usr/bin/asd"
            exit 1
        fi
        # Add --foreground if not present
        if [[ ! " $* " =~ " --foreground " ]] && [[ ! " $* " =~ " -f " ]]; then
            set -- --foreground "$@"
        fi
        echo "Starting: /usr/bin/asd $@"
        exec /usr/bin/asd "$@"
        ;;
    "shell" | "bash")
        exec /bin/bash
        ;;
    *)
        exec "$@"
        ;;
esac

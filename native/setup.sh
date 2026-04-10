#!/bin/bash
# One-time setup for running asd natively (run with sudo)

set -e

USER_ID=${SUDO_USER:-$USER}

# Create directories
mkdir -p /opt/aerospike/{smd,usr/udf/lua,sys/udf/lua,data,xdr}
mkdir -p /var/{lib,log,run}/aerospike
mkdir -p /etc/aerospike

# Set ownership
chown -R "$USER_ID:$USER_ID" /opt/aerospike /var/{lib,log,run}/aerospike /etc/aerospike

echo "Directories created and owned by $USER_ID"
echo ""
echo "Next steps:"
echo "  1. Copy features.conf to /etc/aerospike/"
echo "  2. Run: ./run.sh"

#!/usr/bin/env bash
# Extract SMD sync elapsed microseconds from Aerospike logs (stdin).
# Matches Docker smd-sync-test test-smd-sync.sh timing_extract_sync_us():
#   - INFO:  initial SMD sync done - elapsed NNN us
#   - DETAIL: sync wait done cl_key XXXX elapsed NNN us  (requires context smd detail+)
set -euo pipefail
sync_us=$(grep -oP '(?:initial SMD sync done - elapsed |sync wait done cl_key [0-9a-f]+ elapsed )\K\d+(?= us)' | sort -n | tail -1 || true)
if [[ -n "${sync_us:-}" ]]; then
	echo "$sync_us"
else
	echo "-1"
fi

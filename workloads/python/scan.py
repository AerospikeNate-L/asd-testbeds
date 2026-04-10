#!/usr/bin/env python3
"""Scan operations - full namespace/set scan"""

import aerospike
from config import HOSTS, NAMESPACE, SET

client = aerospike.client({"hosts": HOSTS}).connect()

# Populate some data
for i in range(20):
    key = (NAMESPACE, SET, f"scan_{i}")
    client.put(key, {"id": i, "category": "A" if i % 2 == 0 else "B"})
print("Wrote 20 records")

# Scan all records in set
print(f"\nScanning {NAMESPACE}.{SET}:")
scan = client.scan(NAMESPACE, SET)
count = 0
for record in scan.results():
    key, meta, bins = record
    count += 1
    if count <= 5:
        print(f"  {key[2]}: {bins}")
if count > 5:
    print(f"  ... and {count - 5} more")

# Cleanup
for i in range(20):
    client.remove((NAMESPACE, SET, f"scan_{i}"))
print(f"\nCleaned up {count} records")

client.close()

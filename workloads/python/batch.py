#!/usr/bin/env python3
"""Batch operations - batch read, batch write"""

import aerospike
from config import HOSTS, NAMESPACE, SET

client = aerospike.client({"hosts": HOSTS}).connect()

# Write several records
keys = [(NAMESPACE, SET, f"batch_{i}") for i in range(10)]
for i, key in enumerate(keys):
    client.put(key, {"id": i, "value": i * 100})
print(f"Wrote {len(keys)} records")

# Batch read
records = client.get_many(keys)
print(f"Batch read {len(records)} records:")
for key, meta, bins in records:
    print(f"  {key[2]}: {bins}")

# Batch delete
for key in keys:
    client.remove(key)
print(f"Deleted {len(keys)} records")

client.close()

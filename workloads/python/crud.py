#!/usr/bin/env python3
"""Basic CRUD operations - put, get, delete"""

import aerospike
from config import HOSTS, NAMESPACE, SET

client = aerospike.client({"hosts": HOSTS}).connect()

# Write
key = (NAMESPACE, SET, "key1")
client.put(key, {"name": "alice", "age": 30, "score": 95.5})
print(f"PUT {key}")

# Read
_, _, bins = client.get(key)
print(f"GET {key} -> {bins}")

# Update (merge)
client.put(key, {"age": 31, "city": "NYC"})
_, _, bins = client.get(key)
print(f"UPDATE {key} -> {bins}")

# Delete
client.remove(key)
print(f"DELETE {key}")

# Verify deleted
try:
    client.get(key)
except aerospike.exception.RecordNotFound:
    print("Record confirmed deleted")

client.close()

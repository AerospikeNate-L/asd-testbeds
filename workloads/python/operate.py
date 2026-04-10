#!/usr/bin/env python3
"""Operate - multi-op transactions on single record"""

import aerospike
from aerospike_helpers.operations import operations as op
from config import HOSTS, NAMESPACE, SET

client = aerospike.client({"hosts": HOSTS}).connect()

key = (NAMESPACE, SET, "counter")

# Initialize
client.put(key, {"count": 0, "name": "counter1"})
print("Created counter record")

# Atomic increment + read in single operation
ops = [
    op.increment("count", 1),
    op.read("count"),
]
_, _, result = client.operate(key, ops)
print(f"Increment -> count={result['count']}")

# Multiple increments
for _ in range(5):
    _, _, result = client.operate(key, [op.increment("count", 1), op.read("count")])
print(f"After 5 more increments -> count={result['count']}")

# Append to string + increment in one op
client.put(key, {"count": 0, "log": "start"})
ops = [
    op.increment("count", 1),
    op.append("log", "|event1"),
    op.read("count"),
    op.read("log"),
]
_, _, result = client.operate(key, ops)
print(f"Multi-op result: count={result['count']}, log={result['log']}")

client.remove(key)
print("Cleaned up")

client.close()

#!/usr/bin/env python3
"""Bitwise operations - bit-level manipulation on blob data"""

import aerospike
from aerospike_helpers.operations import bitwise_operations as bw
from aerospike_helpers.operations import operations as op
from config import HOSTS, NAMESPACE, SET

client = aerospike.client({"hosts": HOSTS}).connect()

# Long timeout for debugging - client won't timeout while server paused on breakpoint
DEBUG_POLICY = {"total_timeout": 0, "socket_timeout": 0}

# Set to True to pause before each operation (for stepping through server code)
STEP_MODE = True

def step(msg):
    if STEP_MODE:
        input(f"[STEP] {msg} -- Press Enter to execute...")

key = (NAMESPACE, SET, "bits")

# Initialize a bytes bin
initial_bytes = bytearray(8)  # 64 bits of zeros
step("put: initialize 8 zero bytes")
client.put(key, {"flags": initial_bytes}, policy=DEBUG_POLICY)
print(f"Created record with 8 zero bytes\n")

# Set bits: set bit offset 0 (MSB of first byte)
ops = [
    bw.bit_set("flags", 0, 8, 1, bytearray([0xFF])),
    op.read("flags"),
]
step("bit_set: set first byte to 0xFF")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_set first byte to 0xFF: {result['flags'].hex()}\n")

# OR operation: OR second byte with 0xF0
ops = [
    bw.bit_or("flags", 8, 8, 1, bytearray([0xF0])),
    op.read("flags"),
]
step("bit_or: OR second byte with 0xF0")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_or second byte with 0xF0: {result['flags'].hex()}\n")

# AND operation: AND first byte with 0x0F
ops = [
    bw.bit_and("flags", 0, 8, 1, bytearray([0x0F])),
    op.read("flags"),
]
step("bit_and: AND first byte with 0x0F")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_and first byte with 0x0F: {result['flags'].hex()}\n")

# XOR operation: XOR second byte with 0xFF (invert it)
ops = [
    bw.bit_xor("flags", 8, 8, 1, bytearray([0xFF])),
    op.read("flags"),
]
step("bit_xor: XOR second byte with 0xFF")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_xor second byte with 0xFF: {result['flags'].hex()}\n")

# NOT operation: invert third byte
ops = [
    bw.bit_not("flags", 16, 8),
    op.read("flags"),
]
step("bit_not: invert third byte")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_not on third byte: {result['flags'].hex()}\n")

# Left shift: shift 4th byte left by 2 bits
step("put: reset flags for shift tests")
client.put(key, {"flags": bytearray([0x00, 0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x00])}, policy=DEBUG_POLICY)
ops = [
    bw.bit_lshift("flags", 24, 8, 2),
    op.read("flags"),
]
step("bit_lshift: shift 4th byte left by 2")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_lshift 4th byte (0x0F << 2): {result['flags'].hex()}\n")

# Right shift: shift same byte right
ops = [
    bw.bit_rshift("flags", 24, 8, 4),
    op.read("flags"),
]
step("bit_rshift: shift 4th byte right by 4")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_rshift 4th byte by 4: {result['flags'].hex()}\n")

# Bit add: treat bits as unsigned integer and add
step("put: initialize counter=5")
client.put(key, {"counter": bytearray([0x00, 0x00, 0x00, 0x05])}, policy=DEBUG_POLICY)
ops = [
    bw.bit_add("counter", 0, 32, 10, False, aerospike.BIT_OVERFLOW_FAIL),
    op.read("counter"),
]
step("bit_add: add 10 to counter")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
val = int.from_bytes(result['counter'], 'big')
print(f"After bit_add 10 to 5: {result['counter'].hex()} (decimal: {val})\n")

# Bit subtract
ops = [
    bw.bit_subtract("counter", 0, 32, 3, False, aerospike.BIT_OVERFLOW_FAIL),
    op.read("counter"),
]
step("bit_subtract: subtract 3 from counter")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
val = int.from_bytes(result['counter'], 'big')
print(f"After bit_subtract 3: {result['counter'].hex()} (decimal: {val})\n")

# Bit get: read specific bits
step("put: data=0xABCDEF12")
client.put(key, {"data": bytearray([0xAB, 0xCD, 0xEF, 0x12])}, policy=DEBUG_POLICY)
ops = [
    bw.bit_get("data", 8, 16),
]
step("bit_get: read 16 bits at offset 8")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"bit_get 16 bits at offset 8 from 0xABCDEF12: {result['data'].hex()}\n")

# Bit count: count number of set bits
ops = [
    bw.bit_count("data", 0, 32),
]
step("bit_count: count set bits in 32 bits")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"bit_count in 0xABCDEF12: {result['data']} bits set\n")

# Bit lscan: find first bit set to specified value (scan from left)
step("put: scan=0x00080000")
client.put(key, {"scan": bytearray([0x00, 0x08, 0x00, 0x00])}, policy=DEBUG_POLICY)
ops = [
    bw.bit_lscan("scan", 0, 32, True),
]
step("bit_lscan: find first 1-bit from left")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"bit_lscan for first 1-bit in 0x00080000: position {result['scan']}\n")

# Bit rscan: find first bit from right
ops = [
    bw.bit_rscan("scan", 0, 32, True),
]
step("bit_rscan: find first 1-bit from right")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"bit_rscan for first 1-bit from right: position {result['scan']}\n")

# Bit get_int: read bits as integer
step("put: num=0x00000100 (256)")
client.put(key, {"num": bytearray([0x00, 0x00, 0x01, 0x00])}, policy=DEBUG_POLICY)
ops = [
    bw.bit_get_int("num", 0, 32, False),
]
step("bit_get_int: read 32 bits as unsigned int")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"bit_get_int from 0x00000100: {result['num']}\n")

# Bit resize: resize the blob
step("put: grow=0xFFFF")
client.put(key, {"grow": bytearray([0xFF, 0xFF])}, policy=DEBUG_POLICY)
ops = [
    bw.bit_resize("grow", 4, {"bit_resize_flags": aerospike.BIT_RESIZE_FROM_FRONT}),
    op.read("grow"),
]
step("bit_resize: grow to 4 bytes from front")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_resize to 4 bytes (from front): {result['grow'].hex()}\n")

# Insert bits
step("put: insert=0xAABB")
client.put(key, {"insert": bytearray([0xAA, 0xBB])}, policy=DEBUG_POLICY)
ops = [
    bw.bit_insert("insert", 1, 1, bytearray([0xCC])),
    op.read("insert"),
]
step("bit_insert: insert 0xCC at byte offset 1")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_insert 0xCC at byte 1: {result['insert'].hex()}\n")

# Remove bits
ops = [
    bw.bit_remove("insert", 1, 1),
    op.read("insert"),
]
step("bit_remove: remove 1 byte at offset 1")
_, _, result = client.operate(key, ops, policy=DEBUG_POLICY)
print(f"After bit_remove byte at offset 1: {result['insert'].hex()}\n")

client.remove(key, policy=DEBUG_POLICY)
print("\nCleaned up")

client.close()

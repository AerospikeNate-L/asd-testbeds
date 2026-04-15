#!/usr/bin/env python3
"""
gen-large-smd.py  --  generate a large .smd JSON file for timing tests

Usage:
    python3 gen-large-smd.py --items 50000 --module sindex --out /tmp/smd-data/sindex.smd
    python3 gen-large-smd.py --items 10000 --value-size 1024 --module sindex --out /tmp/smd-data/sindex.smd

Each item:
    key   : "<module>-key-<N>"  (e.g. "sindex-key-00042")
    value : a string of --value-size bytes (default 200 bytes)
    generation: 1
    timestamp : fixed epoch ms (1700000000000)

The output array starts with the cv_key/cv_tid header as required by smd.c:
    [ [0, 1], {item0}, {item1}, ... ]

Approximate file size:
    200-byte value => ~380 bytes/item JSON => 10K items ~= 3.8 MB, 100K items ~= 38 MB
    1024-byte value => ~1200 bytes/item JSON => 10K items ~= 12 MB, 100K items ~= 120 MB
"""

import argparse
import json
import os
import sys
import time

BASE_TS = 1700000000000  # fixed ms timestamp so items look "real"


def build_smd(module: str, n_items: int, value_size: int) -> list:
    items = [[0, 1]]  # cv_key=0, cv_tid=1
    value_template = ("x" * value_size)
    for i in range(n_items):
        items.append({
            "key": f"{module}-key-{i:08d}",
            "value": value_template,
            "generation": 1,
            "timestamp": BASE_TS + i,
        })
    return items


def main():
    parser = argparse.ArgumentParser(description="Generate large SMD JSON file")
    parser.add_argument("--items", type=int, default=10000, help="Number of SMD items")
    parser.add_argument("--module", default="sindex", help="Module name (used as key prefix)")
    parser.add_argument("--value-size", type=int, default=200, help="Bytes per value string")
    parser.add_argument("--out", required=True, help="Output .smd file path")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    t0 = time.monotonic()
    data = build_smd(args.module, args.items, args.value_size)
    build_ms = (time.monotonic() - t0) * 1000

    save_path = args.out + ".save"
    t0 = time.monotonic()
    with open(save_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    write_ms = (time.monotonic() - t0) * 1000
    os.rename(save_path, args.out)

    size_mb = os.path.getsize(args.out) / (1024 * 1024)
    print(f"Generated {args.items} items  module={args.module}  value_size={args.value_size}B")
    print(f"  File: {args.out}  ({size_mb:.1f} MB)")
    print(f"  Build: {build_ms:.0f} ms  Write: {write_ms:.0f} ms")


if __name__ == "__main__":
    main()

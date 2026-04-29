#!/usr/bin/env python3
"""
gen-realistic-smd.py  --  generate realistic worst-case .smd files for timing tests

Generates SMD data that matches real server constraints and formats, pushing
each module to its maximum cardinality with valid entries.

Limits from codebase:
  - AS_ID_NAMESPACE_SZ = 32 (max ns name length)
  - AS_SET_NAME_MAX_SIZE = 64 (includes null terminator, so 63 chars)
  - AS_SET_MAX_COUNT = 4095 per namespace
  - AS_BIN_NAME_MAX_SZ = 16
  - MAX_N_SINDEXES = 256 per namespace
  - MAX_USER_SIZE = 64
  - MAX_ROLE_NAME_SIZE = 64

Module cardinality ceilings:
  - truncate: 32 namespaces × 4096 sets + 32 ns-only = 131,104 max
  - sindex:   32 namespaces × 256 sindexes = 8,192 max
  - security: U + Σr_i + Σ(v_j + w_j + q_j) — unbounded by users (LDAP)
  - masking:  32 namespaces × 4096 sets × bins (unbounded, but realistic ~1000 bins)
  - evict/roster/udf/xdr: ≤32 each (negligible)

Usage:
    # Generate all modules at realistic max
    python3 gen-realistic-smd.py --out-dir /tmp/smd-data --mode realistic-max

    # Generate specific module
    python3 gen-realistic-smd.py --out-dir /tmp/smd-data --module truncate

    # Show valid entry sizes per module
    python3 gen-realistic-smd.py --show-limits
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Optional

# Server constants from codebase
AS_ID_NAMESPACE_SZ = 32        # max namespace name length
AS_SET_NAME_MAX_SIZE = 64      # includes null terminator (63 chars usable)
AS_SET_MAX_COUNT = 4095        # per namespace ((1 << 12) - 1, ID 0 = no set)
AS_BIN_NAME_MAX_SZ = 16        # max bin name length
MAX_N_SINDEXES = 256           # per namespace
MAX_USER_SIZE = 64             # max username length
MAX_ROLE_NAME_SIZE = 64        # max role name length
CTX_B64_MAX_SZ = 2048          # max CDT context base64
EXP_B64_MAX_SZ = 16 * 1024     # max expression base64

# Realistic deployment limits (what we'll actually generate)
MAX_NAMESPACES = 32
MAX_SETS_PER_NS = 4095
MAX_BINS_PER_SET = 100         # realistic bin count for masking

BASE_TS = 1700000000000  # fixed ms timestamp


@dataclass
class ModuleLimits:
    """Describes limits and size ranges for an SMD module."""
    name: str
    max_items: int
    min_key_size: int
    max_key_size: int
    min_value_size: int
    max_value_size: int
    key_format: str
    value_format: str
    notes: str


# Pre-computed limits for each module
MODULE_LIMITS = {
    "truncate": ModuleLimits(
        name="truncate",
        max_items=MAX_NAMESPACES * (MAX_SETS_PER_NS + 1) + MAX_NAMESPACES,  # 32*4096+32=131,104
        min_key_size=1,                                     # "x" (1 char ns)
        max_key_size=AS_ID_NAMESPACE_SZ - 1 + 1 + AS_SET_NAME_MAX_SIZE - 1,  # 31 + | + 63 = 95
        min_value_size=1,                                   # "0"
        max_value_size=13,                                  # max 40-bit timestamp
        key_format="ns-name|set-name or ns-name",
        value_format="LUT as decimal string (40-bit clepoch ms)",
        notes="Key is unique per (ns, set) pair. Tombstones persist. Max: 32 ns × 4096 sets + 32 ns-only."
    ),
    "sindex": ModuleLimits(
        name="sindex",
        max_items=MAX_NAMESPACES * MAX_N_SINDEXES,  # 8,192
        min_key_size=5,                              # "x||b|.|S"
        max_key_size=AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE + AS_BIN_NAME_MAX_SZ + 4 + CTX_B64_MAX_SZ + EXP_B64_MAX_SZ,  # ~18KB
        min_value_size=1,                            # "x" (1 char index name)
        max_value_size=256,                          # INAME_MAX_SZ
        key_format="ns|set|bin|itype|ktype or ns|set|bin|c<ctx>|itype|ktype or ns|set||e<exp>|itype|ktype",
        value_format="index name",
        notes="Per-namespace limit of 256 sindexes. CDT context and expressions are base64."
    ),
    "security": ModuleLimits(
        name="security",
        max_items=1_000_000,  # unbounded, but realistic LDAP-heavy is ~100K-300K
        min_key_size=3,       # "x|P"
        max_key_size=MAX_USER_SIZE + 1 + 1 + 1 + MAX_ROLE_NAME_SIZE + 1 + 1 + 1 + 3 + 1 + AS_ID_NAMESPACE_SZ + 1 + AS_SET_NAME_MAX_SIZE,
        min_value_size=0,     # empty for role bindings
        max_value_size=64,    # bcrypt hash (60 chars)
        key_format="user|P (password), user|R|role (binding), |R|role|V|perm|ns|set (priv), |R|role|W (whitelist), |R|role|Q|r/w (quota)",
        value_format="bcrypt hash for password, empty for others",
        notes="N = U + Σr_i + Σ(v_j + w_j + q_j). LDAP users persist indefinitely. Wire cap: 256U + 258R."
    ),
    "masking": ModuleLimits(
        name="masking",
        max_items=MAX_NAMESPACES * MAX_SETS_PER_NS * MAX_BINS_PER_SET,  # 13M theoretical, ~100K realistic
        min_key_size=5,       # "x|y|z|"
        max_key_size=AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE + AS_BIN_NAME_MAX_SZ + 3,  # 31+63+15+3 = 112
        min_value_size=1,     # minimal masking spec
        max_value_size=256,   # masking function + args
        key_format="ns|set|bin|",
        value_format="masking function specification",
        notes="One entry per (ns, set, bin) triple with masking policy."
    ),
    "evict": ModuleLimits(
        name="evict",
        max_items=MAX_NAMESPACES,  # 32
        min_key_size=1,
        max_key_size=AS_ID_NAMESPACE_SZ - 1,
        min_value_size=1,
        max_value_size=20,  # evict percentage
        key_format="ns-name",
        value_format="evict threshold",
        notes="One entry per namespace. Never exceeds 32 items."
    ),
}


def generate_ns_name(idx: int, max_len: bool = False) -> str:
    """Generate a namespace name. If max_len, use full 31 chars."""
    if max_len:
        return f"ns{idx:06d}".ljust(31, 'x')[:31]
    return f"ns{idx:06d}"


def generate_set_name(idx: int, max_len: bool = False) -> str:
    """Generate a set name. If max_len, use full 63 chars."""
    if max_len:
        return f"set{idx:08d}".ljust(63, 's')[:63]
    return f"set{idx:08d}"


def generate_bin_name(idx: int, max_len: bool = False) -> str:
    """Generate a bin name. If max_len, use full 15 chars."""
    if max_len:
        return f"b{idx:06d}".ljust(15, 'b')[:15]
    return f"b{idx:06d}"


def generate_user_name(idx: int, max_len: bool = False) -> str:
    """Generate a username. If max_len, use full 63 chars."""
    if max_len:
        return f"user{idx:08d}".ljust(63, 'u')[:63]
    return f"user{idx:08d}"


def generate_role_name(idx: int, max_len: bool = False) -> str:
    """Generate a role name. If max_len, use full 63 chars."""
    if max_len:
        return f"role{idx:06d}".ljust(63, 'r')[:63]
    return f"role{idx:06d}"


def build_truncate_smd(n_items: int, max_size: bool = False) -> list:
    """
    Build truncate SMD entries.
    Key: "ns-name|set-name" or "ns-name"
    Value: LUT as decimal string (max 13 digits)
    """
    items = [[0, 1]]  # cv_key, cv_tid header
    
    # Generate namespace-level truncates first
    ns_count = min(MAX_NAMESPACES, n_items)
    for ns_idx in range(ns_count):
        ns_name = generate_ns_name(ns_idx, max_size)
        # Max value: 13-digit decimal
        value = str(BASE_TS + ns_idx) if not max_size else "9999999999999"
        items.append({
            "key": ns_name,
            "value": value,
            "generation": 1,
            "timestamp": BASE_TS + ns_idx,
        })
        if len(items) - 1 >= n_items:
            break
    
    # Generate set-level truncates (up to 4096 sets per ns: IDs 1-4095 + set ID 0 edge case)
    remaining = n_items - (len(items) - 1)
    if remaining > 0:
        sets_per_ns = min(MAX_SETS_PER_NS + 1, (remaining + MAX_NAMESPACES - 1) // MAX_NAMESPACES)
        for ns_idx in range(MAX_NAMESPACES):
            ns_name = generate_ns_name(ns_idx, max_size)
            for set_idx in range(sets_per_ns):
                set_name = generate_set_name(set_idx, max_size)
                key = f"{ns_name}|{set_name}"
                value = str(BASE_TS + ns_idx * MAX_SETS_PER_NS + set_idx)
                if max_size:
                    value = "9999999999999"
                items.append({
                    "key": key,
                    "value": value,
                    "generation": 1,
                    "timestamp": BASE_TS + len(items),
                })
                if len(items) - 1 >= n_items:
                    break
            if len(items) - 1 >= n_items:
                break
    
    return items


def build_sindex_smd(n_items: int, max_size: bool = False) -> list:
    """
    Build sindex SMD entries.
    Key: ns|set|bin|itype|ktype or ns|set|bin|c<ctx>|itype|ktype
    Value: index name
    """
    items = [[0, 1]]
    
    itypes = ['.', 'L', 'K', 'V']  # DEFAULT, LIST, MAPKEYS, MAPVALUES
    ktypes = ['S', 'N', 'G', 'B']  # STRING, NUMERIC, GEOJSON, BLOB
    
    idx = 0
    for ns_idx in range(MAX_NAMESPACES):
        ns_name = generate_ns_name(ns_idx, max_size)
        sindexes_in_ns = 0
        
        for set_idx in range(min(32, MAX_SETS_PER_NS)):  # limit sets for sindex
            set_name = generate_set_name(set_idx, max_size) if set_idx > 0 else ""
            
            for bin_idx in range(8):  # 8 bins per set
                bin_name = generate_bin_name(bin_idx, max_size)
                itype = itypes[bin_idx % 4]
                ktype = ktypes[bin_idx % 4]
                
                # Basic key format: ns|set|bin|itype|ktype
                key = f"{ns_name}|{set_name}|{bin_name}|{itype}|{ktype}"
                
                # Add CDT context for some entries if max_size
                if max_size and bin_idx % 3 == 0:
                    # Simulate a long base64 CDT context
                    ctx_b64 = "c" + "A" * min(200, CTX_B64_MAX_SZ - 1)
                    key = f"{ns_name}|{set_name}|{bin_name}|{ctx_b64}|{itype}|{ktype}"
                
                # Index name (value)
                value = f"idx_{ns_idx}_{set_idx}_{bin_idx}"
                if max_size:
                    value = value.ljust(128, 'i')[:128]
                
                items.append({
                    "key": key,
                    "value": value,
                    "generation": 1,
                    "timestamp": BASE_TS + idx,
                })
                idx += 1
                sindexes_in_ns += 1
                
                if sindexes_in_ns >= MAX_N_SINDEXES or len(items) - 1 >= n_items:
                    break
            if sindexes_in_ns >= MAX_N_SINDEXES or len(items) - 1 >= n_items:
                break
        if len(items) - 1 >= n_items:
            break
    
    return items


def build_security_smd(n_items: int, max_size: bool = False) -> list:
    """
    Build security SMD entries.
    Key formats:
      - user|P (password)
      - user|R|role (role binding)
      - |R|role|V|perm|ns|set (privilege)
      - |R|role|W (whitelist)
      - |R|role|Q|r or |R|role|Q|w (quota)
    Value: bcrypt hash for password, empty for others
    
    Uses max-length names and bcrypt hashes by default for realistic stress testing.
    """
    items = [[0, 1]]
    
    # Fake bcrypt hash (60 chars) - always use for stress testing
    bcrypt_hash = "$2b$10$" + "x" * 53
    
    # Strategy: Many users with few roles each (LDAP-like)
    # Each user: 1 password + 3 role bindings = 4 entries
    # Plus some role definitions with privs
    
    roles_to_define = min(50, n_items // 10)  # Define some custom roles
    users_to_create = (n_items - roles_to_define * 6) // 4  # 4 entries per user, 6 per role
    
    idx = 0
    
    # Create custom role definitions first (always use max-length names for stress)
    role_names = []
    for role_idx in range(roles_to_define):
        role_name = generate_role_name(role_idx, max_len=True)  # always max length
        role_names.append(role_name)
        
        # Role privilege entries: |R|role|V|perm|ns|set
        # Use "test" namespace to match test config (server validates ns exists)
        for priv_idx in range(3):  # 3 privs per role
            ns_name = "test"
            set_name = generate_set_name(priv_idx, max_len=True) if priv_idx > 0 else ""
            perm_code = 10 + priv_idx  # read=10, write=11, etc.
            key = f"|R|{role_name}|V|{perm_code}|{ns_name}|{set_name}"
            items.append({
                "key": key,
                "value": "",
                "generation": 1,
                "timestamp": BASE_TS + idx,
            })
            idx += 1
            if len(items) - 1 >= n_items:
                break
        
        # Role whitelist: |R|role|W
        key = f"|R|{role_name}|W"
        items.append({
            "key": key,
            "value": "10.0.0.0/8,192.168.0.0/16,172.16.0.0/12",  # realistic whitelist
            "generation": 1,
            "timestamp": BASE_TS + idx,
        })
        idx += 1
        
        # Role quotas: |R|role|E (read) and |R|role|I (write)
        for quota_tok in ['E', 'I']:
            key = f"|R|{role_name}|{quota_tok}"
            items.append({
                "key": key,
                "value": "10000",  # TPS quota
                "generation": 1,
                "timestamp": BASE_TS + idx,
            })
            idx += 1
        
        if len(items) - 1 >= n_items:
            break
    
    # Create users (always use max-length names for stress)
    for user_idx in range(users_to_create):
        if len(items) - 1 >= n_items:
            break
            
        user_name = generate_user_name(user_idx, max_len=True)  # always max length
        
        # Password entry: user|P
        # All users get bcrypt hash for stress testing (even simulated LDAP)
        key = f"{user_name}|P"
        items.append({
            "key": key,
            "value": bcrypt_hash,
            "generation": 1,
            "timestamp": BASE_TS + idx,
        })
        idx += 1
        
        # Role bindings: user|R|role (typically 2-5 roles per user)
        n_roles = min(3, len(role_names))
        for role_bind_idx in range(n_roles):
            if len(items) - 1 >= n_items:
                break
            role_name = role_names[role_bind_idx % len(role_names)] if role_names else "read-write"
            key = f"{user_name}|R|{role_name}"
            items.append({
                "key": key,
                "value": "",
                "generation": 1,
                "timestamp": BASE_TS + idx,
            })
            idx += 1
    
    return items


def build_masking_smd(n_items: int, max_size: bool = False) -> list:
    """
    Build masking SMD entries.
    Key: ns|set|bin|
    Value: func:type:params (colon-delimited, parsed by masking_ee.c:smd_accept_cb)
    
    Valid functions (from MASKING_FN_BUILTINS):
      - constant (string, integer, float, bool)
      - redact (string only)
    """
    items = [[0, 1]]
    
    # Format: func:type:params
    # func = masking function name (must be in MASKING_FN_BUILTINS)
    # type = particle type
    # params = function-specific parameters
    masking_funcs = [
        "redact:string:",                           # redact string bins
        "constant:string:value=MASKED",             # constant string replacement
        "constant:integer:value=0",                 # constant integer replacement
        "constant:float:value=0.0",                 # constant float replacement
        "constant:bool:value=false",                # constant bool replacement
    ]
    
    idx = 0
    for ns_idx in range(MAX_NAMESPACES):
        ns_name = generate_ns_name(ns_idx, max_size)
        
        sets_needed = min(MAX_SETS_PER_NS, (n_items + MAX_NAMESPACES - 1) // MAX_NAMESPACES // MAX_BINS_PER_SET + 1)
        for set_idx in range(sets_needed):
            set_name = generate_set_name(set_idx, max_size)
            
            bins_needed = min(MAX_BINS_PER_SET, n_items - (len(items) - 1))
            for bin_idx in range(bins_needed):
                bin_name = generate_bin_name(bin_idx, max_size)
                
                key = f"{ns_name}|{set_name}|{bin_name}|"
                value = masking_funcs[bin_idx % len(masking_funcs)]
                if max_size:
                    # Pad masking spec with extra params
                    value = value + ":param=" + "x" * 100
                
                items.append({
                    "key": key,
                    "value": value,
                    "generation": 1,
                    "timestamp": BASE_TS + idx,
                })
                idx += 1
                
                if len(items) - 1 >= n_items:
                    break
            if len(items) - 1 >= n_items:
                break
        if len(items) - 1 >= n_items:
            break
    
    return items


def build_evict_smd(n_items: int, max_size: bool = False) -> list:
    """
    Build evict SMD entries (one per namespace, max 32).
    Key: ns-name
    Value: evict threshold percentage
    """
    items = [[0, 1]]
    
    for ns_idx in range(min(n_items, MAX_NAMESPACES)):
        ns_name = generate_ns_name(ns_idx, max_size)
        items.append({
            "key": ns_name,
            "value": str(50 + ns_idx % 50),  # 50-99% threshold
            "generation": 1,
            "timestamp": BASE_TS + ns_idx,
        })
    
    return items


MODULE_BUILDERS = {
    "truncate": build_truncate_smd,
    "sindex": build_sindex_smd,
    "security": build_security_smd,
    "masking": build_masking_smd,
    "evict": build_evict_smd,
}


def write_smd_file(items: list, path: str) -> tuple[float, int]:
    """Write SMD items to file. Returns (size_mb, item_count)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    
    save_path = path + ".save"
    with open(save_path, "w") as f:
        json.dump(items, f, separators=(",", ":"))
    os.rename(save_path, path)
    
    size_mb = os.path.getsize(path) / (1024 * 1024)
    return size_mb, len(items) - 1  # subtract header


def show_limits():
    """Print valid entry size ranges for each module."""
    print("=" * 80)
    print("SMD Module Entry Size Ranges (from codebase)")
    print("=" * 80)
    print()
    
    for name, limits in MODULE_LIMITS.items():
        print(f"Module: {name}")
        print(f"  Max items:     {limits.max_items:,}")
        print(f"  Key size:      {limits.min_key_size} - {limits.max_key_size} bytes")
        print(f"  Value size:    {limits.min_value_size} - {limits.max_value_size} bytes")
        print(f"  Key format:    {limits.key_format}")
        print(f"  Value format:  {limits.value_format}")
        print(f"  Notes:         {limits.notes}")
        print()
    
    print("=" * 80)
    print("Constants from codebase:")
    print("=" * 80)
    print(f"  AS_ID_NAMESPACE_SZ     = {AS_ID_NAMESPACE_SZ}")
    print(f"  AS_SET_NAME_MAX_SIZE   = {AS_SET_NAME_MAX_SIZE} (63 usable)")
    print(f"  AS_SET_MAX_COUNT       = {AS_SET_MAX_COUNT}")
    print(f"  AS_BIN_NAME_MAX_SZ     = {AS_BIN_NAME_MAX_SZ}")
    print(f"  MAX_N_SINDEXES         = {MAX_N_SINDEXES}")
    print(f"  MAX_USER_SIZE          = {MAX_USER_SIZE}")
    print(f"  MAX_ROLE_NAME_SIZE     = {MAX_ROLE_NAME_SIZE}")
    print(f"  CTX_B64_MAX_SZ         = {CTX_B64_MAX_SZ}")
    print(f"  EXP_B64_MAX_SZ         = {EXP_B64_MAX_SZ}")


def generate_realistic_max(out_dir: str, max_size: bool = False):
    """Generate all modules at their realistic maximum cardinality."""
    print(f"Generating realistic worst-case SMD data in {out_dir}")
    print(f"Max-size entries: {max_size}")
    print()
    
    # Realistic worst-case counts
    module_counts = {
        "truncate": 131104,   # 32 ns × 4096 sets + 32 = full max
        "sindex": 8192,       # 32 ns × 256 sindexes = full max
        "security": 100000,   # LDAP-heavy deployment
        "masking": 50000,     # heavy masking deployment
    }
    
    total_items = 0
    total_mb = 0.0
    
    for module, count in module_counts.items():
        print(f"Generating {module} ({count:,} items)...")
        t0 = time.monotonic()
        
        builder = MODULE_BUILDERS[module]
        items = builder(count, max_size)
        
        path = os.path.join(out_dir, f"{module}.smd")
        size_mb, actual_count = write_smd_file(items, path)
        
        elapsed_ms = (time.monotonic() - t0) * 1000
        print(f"  -> {path} ({actual_count:,} items, {size_mb:.1f} MB, {elapsed_ms:.0f} ms)")
        
        total_items += actual_count
        total_mb += size_mb
    
    print()
    print(f"Total: {total_items:,} items, {total_mb:.1f} MB")


def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic worst-case SMD files for timing tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show entry size limits for all modules
  %(prog)s --show-limits

  # Generate all modules at realistic max (normal size entries)
  %(prog)s --out-dir /tmp/smd-data --mode realistic-max

  # Generate all modules at realistic max with max-size entries
  %(prog)s --out-dir /tmp/smd-data --mode realistic-max --max-size

  # Generate specific module with custom count
  %(prog)s --out-dir /tmp/smd-data --module truncate --items 50000

  # Generate security module simulating LDAP-heavy deployment
  %(prog)s --out-dir /tmp/smd-data --module security --items 300000
"""
    )
    
    parser.add_argument("--show-limits", action="store_true",
                        help="Show valid entry size ranges for each module")
    parser.add_argument("--out-dir", help="Output directory for .smd files")
    parser.add_argument("--mode", choices=["realistic-max"],
                        help="Generation mode")
    parser.add_argument("--module", choices=list(MODULE_BUILDERS.keys()),
                        help="Generate specific module only")
    parser.add_argument("--items", type=int,
                        help="Number of items (overrides mode defaults)")
    parser.add_argument("--max-size", action="store_true",
                        help="Generate max-size keys and values where possible")
    
    args = parser.parse_args()
    
    if args.show_limits:
        show_limits()
        return
    
    if not args.out_dir:
        parser.error("--out-dir required unless --show-limits")
    
    # Realistic defaults for single-module generation (use these unless --items is specified)
    REALISTIC_DEFAULTS = {
        "truncate": 131104,   # full max: 32 ns × 4096 sets + 32
        "sindex": 8192,       # full max: 32 ns × 256 sindexes
        "security": 100000,   # LDAP-heavy deployment
        "masking": 50000,     # heavy masking deployment
        "evict": 32,          # full max: one per namespace
    }
    
    if args.mode == "realistic-max":
        generate_realistic_max(args.out_dir, args.max_size)
    elif args.module:
        # Use realistic default, not theoretical max, unless explicitly specified
        count = args.items if args.items is not None else REALISTIC_DEFAULTS.get(args.module, 1000)
        print(f"Generating {args.module} ({count:,} items)...")
        t0 = time.monotonic()
        
        builder = MODULE_BUILDERS[args.module]
        items = builder(count, args.max_size)
        
        path = os.path.join(args.out_dir, f"{args.module}.smd")
        size_mb, actual_count = write_smd_file(items, path)
        
        elapsed_ms = (time.monotonic() - t0) * 1000
        print(f"Generated {actual_count:,} items to {path}")
        print(f"  Size: {size_mb:.1f} MB  Time: {elapsed_ms:.0f} ms")
    else:
        parser.error("Specify --mode or --module")


if __name__ == "__main__":
    main()

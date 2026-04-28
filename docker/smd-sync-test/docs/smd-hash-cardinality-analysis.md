# SMD Hash — Cardinality & Performance Analysis

*Supporting detail for [smd-hash-performance-summary.md](smd-hash-performance-summary.md)*

---

## Timing data

3-node cluster, single module seeded via `gen-large-smd.py`, `timing-20260415-150036.tsv`:


| N (items) | sync_ms | per_item_µs | slope vs prior    |
| --------- | ------- | ----------- | ----------------- |
| 10K       | 1,633   | 163         | —                 |
| 50K       | 1,828   | 37          | 0.07 (flat)       |
| 100K      | 2,713   | 27          | 0.57 (sub-linear) |
| 200K      | 7,089   | 35          | 1.39              |
| 300K      | 23,876  | 80          | 2.99              |
| 400K      | timeout | —           | —                 |


Sub-linear below 131K because fixed startup/fabric/disk overhead dominates; merge cost is a small slice of total sync time. The slope flips super-linear crossing the 131K boundary (N²/256 term overtakes the constant floor).

---

## Merge cost model

Each `smd_hash_get` call: `strlen(key)` + `cf_wyhash32` over those bytes + `strcmp` chain-walk. For N items in a 256-row table:

- Average chain length: N / 256
- Per-lookup cost: `strlen + hash + (N/256) × strcmp`
- Total merge cost for one module: **O(N² / 256)**

At N = 300K: (300K)² / 256 ≈ 352 billion op-units → measured ~24 s.  
At N = 100K: (100K)² / 256 ≈ 39 billion → measured ~200–300 ms (small fraction of 2.7 s total).

### All-modules-at-max worst case

If every module simultaneously sits at its realistic ceiling:


| Module                  | Realistic max N | Avg chain @ 256 rows | Est. merge time |
| ----------------------- | --------------- | -------------------- | --------------- |
| truncate                | 131,072         | 512                  | ~660 ms         |
| sindex                  | 8,192           | 32                   | ~2 ms           |
| security                | 100,000         | 391                  | ~400 ms         |
| masking                 | ~100,000        | 391                  | ~400 ms         |
| evict, roster, udf, xdr | ~32 each        | ~0                   | negligible      |
| **Total**               |                 |                      | **~1.5 s**      |


Plus commit-to-disk (linear in total N across all modules): ~~340K items × ~500K items/s ≈ 700 ms.~~  
~~Grand total: ~2–3 s merge + commit, on top of the fixed cluster-formation floor (~~3 s).  
`O(N × NUM_MODULES) = O(N)` — the per-module ceilings are constants, so total merge cost is a constant.

---

## Per-module cardinality ceilings


| Module           | Key structure               | Theoretical max N               | Practical max N          | Reaches 2^17?            |
| ---------------- | --------------------------- | ------------------------------- | ------------------------ | ------------------------ |
| truncate         | `ns|set` or `ns`            | 32 × 4096 + 32 = **131,104**    | much lower (typical ops) | at absolute maximum only |
| sindex           | one per sindex definition   | 32 × 256 = **8,192**            | same                     | never                    |
| security         | see formula below           | `256U + 258R` (unbounded in U)  | LDAP-heavy deployments   | yes (LDAP-at-scale)      |
| masking          | `ns|set|bin` triple         | 32 × 4096 × `bin_count` (large) | PM: unlikely > 131K      | probably not             |
| evict            | one per namespace           | ≤ 32                            | same                     | never                    |
| roster, udf, xdr | bounded by ns / files / DCs | ≤ 32                            | same                     | never                    |


**Note on evict in the test harness**: the 300K figure is synthetic — `gen-large-smd.py` writes fake namespace keys (`ns_000001`, …) directly to the `.smd` file. `nsup_smd_accept_cb` silently discards unknown namespaces, but the SMD layer stores all items regardless. Production evict never exceeds 32 items.

**Note on tombstones**: deleted items stay in SMD with `value == NULL`; they inflate the effective N. The merge code has a dedicated branch for them (`has_tombstone` check), and they survive conflict resolution to ensure deletions propagate. `truncate-undo` leaves tombstones; operators may not realize the count grows with deletions as well as insertions. There is no garbage-collection path for tombstones in the SMD file.

**Note on truncate successive ops**: successive `truncate up to Y` calls on the same `(ns, set)` *replace* the prior item — key is `ns|set`, value is the LUT. N is therefore bounded by `(distinct namespaces) + (distinct sets that have ever been truncated, including tombstoned-undone ones)`, not by the number of truncate operations.

---

## Security module key formula

### Five key families

```
<user>|P                            — 1 row per user (password / empty marker)
<user>|R|<role>                     — 1 row per (user, role) binding
|R|<role>|V|<perm>|<ns>|<set>       — 1 row per privilege on a user-defined role
|R|<role>|W                         — 1 row per role with a whitelist (0 or 1)
|R|<role>|Q|<r|w>                   — up to 2 rows per role (read/write quotas)
```

### Variables

- `U` = persisted users (one password row each)
- `R` = **user-defined roles only**. The 13 predefined roles (`user-admin`, `sys-admin`, `data-admin`, `udf-admin`, `sindex-admin`, `read`, `read-write`, `read-write-udf`, `write`, `truncate`, `masking-admin`, `read-masked`, `write-masked`) are **never written to SMD**. `role_cache_init` inserts them directly into the in-memory `g_roles` hash at startup; every mutating path (`create_role`, `add_privs`, `set_whitelist`, `set_quotas`) calls `is_predefined_role()` and bails if true.
- `r_i` = roles bound to user i, `0 ≤ r_i ≤ 255` (uint8 wire cap)
- `v_j` = privs on role j, `0 ≤ v_j ≤ 255` (uint8 wire cap)
- `w_j ∈ {0,1}` = whitelist present on role j
- `q_j ∈ {0,1,2}` = quota keys on role j

### Exact formula

$$N_{\text{sec}} = U + \sum_{i} r_i + \sum_{j}(v_j + w_j + q_j)$$

Using averages (`r̄`, `v̄`, `w̄`, `q̄`):

$$N_{\text{sec}} \approx U(1+\bar r) + R(\bar v + \bar w + \bar q)$$

### Wire-saturated ceiling (all caps maxed)

$$N_{\text{sec}}^{\max} = 256U + 258R$$

i.e. each user contributes up to 256 rows (1 password + 255 role bindings), each user-defined role up to 258 rows (255 privs + 1 whitelist + 2 quotas).

### Realistic deployment bracket


| Scenario                     | U      | R   | r̄  | v̄  | N_sec         |
| ---------------------------- | ------ | --- | --- | --- | ------------- |
| Minimum functional           | 1      | 0   | 0   | —   | 1             |
| Small shop                   | 10     | 5   | 2   | 10  | ~83           |
| Typical enterprise           | 100    | 20  | 3   | 15  | ~720          |
| Medium deployment            | 1,000  | 50  | 5   | 20  | ~7,100        |
| Large RBAC (local users)     | 10,000 | 200 | 10  | 40  | ~118,000      |
| LDAP-heavy org               | 50,000 | 100 | 5   | 30  | ~303,000      |
| Wire-saturated (theoretical) | U      | R   | 255 | 255 | `256U + 258R` |


In practice, R is small (tens to low hundreds) and `r̄ ∈ [2, 5]`, so **the U term dominates by ~50–100× once U ≳ 1K**.

---

## LDAP user persistence mechanics

### Why external users are persisted to SMD

On first login, `as_security_new_session` writes an empty-password record + role bindings to SMD:

```c
// Creates new user.
smd_add_password(p_user, user_len, NULL, 0);
adjust_roles_in_smd(NULL, p_user, user_len, roles, num_roles);
```

This serves two purposes:

1. **Iterable target list for the polling thread.** `run_polling` in `ldap_ee.c` iterates every persisted external user and re-queries LDAP for existence + current roles. Without persistence, role/existence changes wouldn't propagate until each user re-authenticated.
2. **Cluster-wide consistent identity set.** All nodes share the same user view via SMD sync; auth/permission checks can execute on any node regardless of where the user connected.

### Why stale users accumulate

There is no time-based eviction. The only removal paths are:

- LDAP poll returns `AS_SEC_ERR_USER` (user no longer exists in directory) → `as_security_drop_external_user`
- Operator manually runs `drop-user`

A user who authenticated once and never returned stays persisted and polled forever. In large or long-lived LDAP deployments (employee turnover, decommissioned service accounts, dev/test users) this causes unbounded growth.

### Secondary effect: LDAP polling DDoS

The polling thread iterates the full persisted-user set every `ldap_polling_period` seconds. A 100K-user SMD set means 100K LDAP queries per cycle on the principal node. This is independent of the merge performance problem and is a motivation for Track A (TTL/LRU eviction) even in clusters where merge time is acceptable.

### Session token independence

Aerospike session tokens are signed and validate cryptographically. An already-issued, unexpired token remains valid even if the user's SMD record is evicted. Re-login after TTL expiry does a fresh LDAP query and produces the correct current role set — no staleness window introduced by eviction.

---

## Failover analysis: LDAP user count vs. merge cost

Merge cost scales as `(N/131K)²` past the knee. Anchored to ~3 s at N ≈ 131K:


| Zone                     | N_sec       | Approx merge cost |
| ------------------------ | ----------- | ----------------- |
| Green (flat/sub-linear)  | < 131K      | < 1 s             |
| Yellow (knee)            | 131K – 262K | ~3–12 s           |
| Orange (clear quadratic) | 262K – 524K | ~12–50 s          |
| Red (painful)            | 524K – 1M   | ~50 s – 3 min     |
| Black (pathological)     | > 1M        | multi-minute+     |


### Max U before crossing each zone boundary (solving `U·(1+r̄) = N_threshold`)


| r̄    | Green→Yellow (N=131K) | Yellow→Orange (N=262K) | Orange→Red (N=524K) |
| ----- | --------------------- | ---------------------- | ------------------- |
| 1     | 65,536                | 131,072                | 262,144             |
| 2     | 43,691                | 87,381                 | 174,763             |
| **3** | **32,768**            | **65,536**             | **131,072**         |
| 5     | 21,845                | 43,691                 | 87,381              |
| 10    | 11,915                | 23,831                 | 47,661              |
| 32    | 3,972                 | 7,944                  | 15,888              |


At the most common LDAP deployment shape (`r̄ ≈ 3`), the knee is around **~33K persisted users**.

### 2^17 threshold solved for extreme cases (R-term only, U=0)

`R > 131,072 / 258 ≈ 508` user-defined roles, each fully saturated with 255 privs. This is far outside any realistic deployment.

---

## Sort-merge as an alternative to hash-based merge

An alternative to resizing `smd_hash` is replacing the hash-based merge with a sort-merge over the `cf_vector`. Evaluated and set aside in favor of Track A; recorded here for completeness.

### Operation count at N=300K


| Approach                    | strcmps            | hash computes | strlen scans | Notes                                                            |
| --------------------------- | ------------------ | ------------- | ------------ | ---------------------------------------------------------------- |
| Hash, chain=5 (right-sized) | ~5N = 1.5M         | N = 300K      | N = 300K     | `smd_hash_get_row_i` does `strlen` then `cf_wyhash32` per lookup |
| Hash, chain=1 (perfect)     | ~N = 300K          | N = 300K      | N = 300K     | —                                                                |
| Sort-merge                  | ~2N log N = ~10.8M | 0             | 0            | log₂(300K) ≈ 18                                                  |


On strcmp count alone, a right-sized hash wins (~3×). `N log N` does not beat `N` when the constant factor is ~5.

### Where sort-merge recovers ground

1. **Per-lookup overhead.** Every `smd_hash_get` does `strlen(key)` + `cf_wyhash32` (two passes over the key bytes) before touching any chain node. For 50–100 byte keys that's 100–200 byte-reads per lookup that sort-merge avoids entirely — it just compares pointers it already holds.
2. **Cache behavior.** Chained hash = pointer-chasing through scattered 24-byte `smd_hash_ele` nodes (potential cache miss per node). Sort-merge = sequential walk through `cf_vector` after an in-place sort. At N=300K the sort-merge working set fits in L2; the hash chain walks don't.
3. **Code footprint.** Sort-merge would let you delete ~80 lines of custom hash (`smd_hash_init`, `_clear`, `_put`, `_get`, `_get_row_i`), both `smd_hash` fields in `smd_module`, and the entire `module_regen_key2index` rebuild path. Resizing *adds* code.

### Net assessment

Once a resize is in place (N stays O(1) per lookup), sort-merge drops from "fixes the real problem" to "is a nice cleanup" — ~1.5–3× wall-time win at N=300K from cache behavior, but a larger change with more risk. The main complication is `op_full_to_pr`: merge accumulates across multiple per-NPR replies into `merge_h`, so a pure sort-merge would need to either batch the replies or keep a small per-reply hash and only replace the `db_h` lookup with a sorted binary search. The minimal variant (sort-merge only for `op_full_from_pr`, leave `merge_h` alone) is a single-function change and removes `db_h` from the NPR receive path — but becomes optional once resize is in place.

---

## Hash-resize option (Track B — deferred)

Documented here for reference; not being implemented until a high-cardinality module exists.

### Design space considered

Three options were evaluated:

**Option 1 — Heap-allocated, sized at known-N rebuild points only.**  
`smd_hash.table` becomes a heap pointer; sized at `module_restore_from_disk` (boot) and `module_regen_key2index` (post-merge) — the two places where N is known exactly and the hash is bulk-rebuilt from scratch. Formula: `n_rows = clamp(next_pow2(N × 2), MIN, MAX)`. Shrink happens automatically on the next rebuild. No incremental resize logic; no trigger on individual `smd_hash_put` calls.

**Option 2 — Rebuild points + overload trigger on `smd_hash_put`.**  
Same as Option 1, but also rehashes if `N / n_rows` exceeds a load threshold on any put. Protects against incremental growth between syncs (e.g. operator mass-sets items via info commands). Assessed as not a real scenario; adds code for no practical benefit.

**Option 3 — Two-tier: inline 256 + optional heap overflow (chosen design if implemented).**  
Keep the inline `table[256]` always present (zero heap for small modules, identical to today). Add an optional `big_table` heap pointer that is `NULL` by default. When a module crosses the upgrade threshold, allocate `big_table` and route all ops through it. `smd_hash_rows(h)` picks the active table. The 6 KB inline becomes dead weight post-upgrade; accepted trade-off (~48 KB total across all potentially-upgraded modules) vs. the code complexity of a full all-heap approach.

Option 1 and Option 3 have nearly identical memory profiles in practice — the difference is 48 KB `.bss` vs 48 KB heap for small modules (both floored at 256 rows). Option 3 was preferred to avoid any boot-time heap allocation for modules that never upgrade.

### Sizing formula examples (load factor = 2, `n_rows = next_pow2(N × 2)`)


| N                     | n_rows  | Memory per hash |
| --------------------- | ------- | --------------- |
| 32 (roster)           | 64      | 1.5 KB          |
| 1K (sindex medium)    | 2,048   | 48 KB           |
| 10K (sindex heavy)    | 16,384  | 384 KB          |
| 300K (truncate heavy) | 524,288 | 12 MB           |


Two hashes per module × 4 potentially-growing modules worst-case = **96 MB upper bound** at N=300K each.

### Limits: compile-time vs. config

Decided: **compile-time constants only.** SMD hash sizing is not an operator concern; operators who are in pathological cardinality territory need the sizing policy re-examined, not tuned around. Config exposure would add docs, support burden, and foot-gun risk for something that should be invisible.

### Shrink policy

Automatic: rebuild points re-size from scratch, so a module that shrinks between reformations gets a smaller table on the next `module_regen_key2index` or reboot. No explicit shrink-in-place logic needed.

### Module-level gate

Not needed as an explicit annotation. `clamp(next_pow2(N × 2), INLINE_ROWS, BIG_MAX_ROWS)` floors at 256 for any module with N ≤ 128. `evict`, `roster`, `udf`, `xdr` never exceed that floor and never allocate `big_table`.

### Two-tier struct

```c
typedef struct smd_hash_s {
    smd_hash_ele inline_table[SMD_HASH_INLINE_ROWS];  // always present, zero heap
    smd_hash_ele* big_table;                           // NULL unless upgraded
    uint32_t n_rows;                                   // active row count
} smd_hash;

static inline smd_hash_ele*
smd_hash_rows(const smd_hash* h) {
    return h->big_table != NULL ? h->big_table : (smd_hash_ele*)h->inline_table;
}
```

**Constants:**

- `SMD_HASH_INLINE_ROWS = 256` (current behavior; small modules never pay heap)
- `SMD_HASH_UPGRADE_COUNT = 131072` (2^17 — cliff trigger)
- `SMD_HASH_BIG_MIN_ROWS = 262144` (2^18 — minimum big table size)
- `SMD_HASH_BIG_MAX_ROWS = 1048576` (2^20 — ~24 MB per upgraded hash)
- Size formula: `clamp(next_pow2(N × 2), BIG_MIN_ROWS, BIG_MAX_ROWS)`

**Resize triggers:** `module_restore_from_disk` (boot) and `module_regen_key2index` (post-merge) only — both are known-N bulk-rebuild points. No incremental resize logic needed.

**Memory at ceiling (N = 300K, 4 modules upgraded × 2 hashes each):**

- `next_pow2(300K × 2) = 524,288 rows × 24 B = 12 MB per hash`
- 8 upgraded hashes × 12 MB = **96 MB** worst case (all 4 potentially-large modules each at 300K)
- Small modules stay on inline 6 KB each; dead weight post-upgrade is 48 KB total (8 upgraded × 6 KB) — accepted trade-off vs. code complexity of two-tier branching

**Row lookup improvement:** `hash & (n_rows - 1)` instead of current `% N_HASH_ROWS` (power-of-two enforcement makes this a no-op bitmask).
# SMD Hash Performance — Investigation Summary

_Derived from [SMD hash versus cf_shash](cursor_smd_hash_performance)_

---

## Why SMD uses its own hash instead of `cf_shash`

`cf_shash` requires fixed-size keys and copies them into its own buckets. SMD's per-module index is keyed by variable-length C strings that are already owned by the backing `cf_vector`. The custom `smd_hash` avoids copying keys, needs no locking (SMD operates under a single `g_smd.lock`), and is embedded directly in `smd_module` (no allocation at create/destroy time). `cf_shash` *is* used in SMD where keys are fixed-size scalars (the `uint32_t` transaction-id set).

---

## Hash structure

```c
typedef struct smd_hash_ele_s {
    struct smd_hash_ele_s* next;
    const char* key;   // points into cf_vector db — not owned
    uint32_t value;    // index into the vector
} smd_hash_ele;

#define N_HASH_ROWS 256

typedef struct smd_hash_s {
    smd_hash_ele table[N_HASH_ROWS];
} smd_hash;
```

Per node: 8 modules × 2 hashes (`db_h` + `merge_h`) = **16 `smd_hash` instances**, statically allocated in `g_module_table`.

---

## Performance profile

The merge paths (`op_full_from_pr`, `op_full_to_pr`) are O(N²/256): each incoming item calls `smd_hash_get` (which does `strlen` + `cf_wyhash32` + strcmp chain-walk) against a 256-row table. The super-linear knee is at ~131K items (= 2^17). Below that, fixed startup/fabric/disk overhead dominates and merge is invisible. At 300K items the test harness measured ~24 s; at 400K it timed out.

See [smd-hash-cardinality-analysis.md](smd-hash-cardinality-analysis.md) for the full timing table, per-module cost breakdown, and all-modules-at-max worst-case calculation.

---

## Module cardinality analysis

| Module | Theoretical max N | Reaches 2^17? |
|---|---:|---|
| truncate | 32 × 4096 + 32 = **131,104** | at absolute maximum only |
| sindex | 32 × 256 = **8,192** | never |
| security (RBAC) | `U·(1+r̄)` — unbounded in U | yes, if LDAP user count is large |
| masking | 32 × 4096 × `bin_count` | probably not (PM: unlikely > 131K) |
| evict, roster, udf, xdr | ≤ 32 each | never |

The only realistic production path to crossing 2^17 is the **security module via LDAP user persistence**: `as_security_new_session` persists every first-time LDAP login to SMD with no time-based eviction. At `r̄ = 3` (avg roles/user), the knee is around **~33K persisted LDAP users**.

For the full key formula, predefined-role clarification, realistic deployment bracket, and per-`r̄` failover table, see [smd-hash-cardinality-analysis.md](smd-hash-cardinality-analysis.md).

---

## Root cause of cardinality growth: LDAP user persistence

On first LDAP login, `as_security_new_session` calls `smd_add_password(..., NULL, 0)` + `adjust_roles_in_smd(...)` — an empty-password record plus role bindings persisted to SMD. A background thread then polls LDAP for every persisted external user every `ldap_polling_period` seconds. There is **no time-based eviction**; users are removed only when LDAP says they no longer exist or an operator runs `drop-user`.

Side effect: a stale user set becomes an LDAP polling DDoS over time.

---

## Decision

### Track A — External-user TTL + LRU eviction (primary, pursue now)

Directly attacks cardinality at its source.

Minimal design:
1. Track `last_seen_ms` per external user in-memory on the principal (adjacent to `conn_tracker`).
2. Update on login / data op.
3. In `run_polling`: if `now − last_seen_ms > external_user_ttl_sec` → `as_security_drop_external_user(...)`. Also apply LRU cap if the live set exceeds a configured ceiling.
4. New config knob `security.external-user-ttl` (default e.g. 30–90 days, `0` = current never-evict behavior for back-compat).

Estimated scope: ~60 LOC in `security.c` + `ldap_ee.c`. No SMD format change, no protocol change.

### Track B — `smd_hash` two-tier resize (deferred)

Would make merge O(N) even for pathological N by replacing the static 256-row inline table with an inline-256 + optional heap `big_table` (upgrade at 2^17 items, sized to `next_pow2(N×2)`, max 2^20 rows). No production module realistically sustains N near this threshold today; the fix is documented-but-dormant until a future high-cardinality SMD use case arrives.

Key parameters and struct design in [smd-hash-cardinality-analysis.md](smd-hash-cardinality-analysis.md#hash-resize-option-track-b--deferred).

---

## References

- Phase timing data: `local/docker/smd-sync-test/timing-results/`
- Test harness: `local/docker/smd-sync-test/test-smd-sync.sh`
- Detailed analysis: [smd-hash-cardinality-analysis.md](smd-hash-cardinality-analysis.md)
- Prior chat: [SMD changes timing measurements](8911c825-3fb0-4e27-8e85-ca3f14a58697)

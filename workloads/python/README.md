# Python Test Workloads

Simple Python scripts to exercise Aerospike server code paths.

## Setup

```bash
cd local/workloads/python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Scripts

| Script | Code Paths Exercised |
|--------|---------------------|
| `crud.py` | Basic read/write/delete (`as_rw_*`) |
| `batch.py` | Batch operations (`batch_*`) |
| `scan.py` | Scan/query infrastructure (`as_scan_*`) |
| `operate.py` | Multi-op transactions (`as_operate_*`) |
| `stringops.py` | String binops — read (strlen, substr, char_at, find, contains, starts_with, ends_with, to_integer, to_double, byte_length, is_numeric, is_upper, is_lower, to_blob, split, b64_decode, regex_compare) and modify (insert, overwrite, concat, snip, replace, replace_all, upper, lower, case_fold, normalize, trim_start, trim_end, trim, pad_start, pad_end, repeat, regex_replace) via `OP_STRING_READ`/`OP_STRING_MODIFY` |
| `stringexprs.py` | Same string ops as `stringops.py` but via the expression evaluation path (`CALL_STRING`). Also tests `TO_STRING` (`CALL_REPR`) for int, float, bool, and string bins |

## Usage

**Prerequisite:** A running Aerospike server at `127.0.0.1:3000` (edit `config.py` to change).

Then run scripts:

```bash
# Single script
python crud.py

# All scripts
for f in crud.py batch.py scan.py operate.py stringops.py stringexprs.py; do
    echo "=== $f ==="
    python $f --no-step
    echo
done
```

### String workload modes

```bash
python stringops.py                        # interactive step mode (default)
python stringops.py --no-step              # run all tests without pausing
python stringops.py --bench                # benchmark mode (1000 iters)
python stringops.py --bench 5000           # benchmark mode (5000 iters)
python stringops.py --bench --threads 8    # 8 concurrent clients

# Same flags for stringexprs.py
python stringexprs.py --no-step
python stringexprs.py --bench --threads 4
```

## Configuration

Edit `config.py` to change host/port or namespace:

```python
HOSTS = [("127.0.0.1", 3000)]
NAMESPACE = "test"
SET = "demo"
```

## Debugging Tips

Set breakpoints in server code, then run a script to hit that code path:

- **crud.py** → `as_rw_start()`, `write_master()`, `read_local()`
- **batch.py** → `batch_read_start()`, `batch_write_start()`
- **scan.py** → `as_scan_start()`, `scan_job_reduce_cb()`
- **operate.py** → `as_operate_start()`, single-record transaction flow
- **stringops.py** → `particle_string.c` string op dispatch, `particle_blob.c` b64 ops
- **stringexprs.py** → `exp.c` expression evaluation → string ops, `CALL_REPR` to_string

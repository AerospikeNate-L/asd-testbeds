#!/usr/bin/env python3
"""List join operations - CDT list join (op 28) on list bins

Uses asclient (raw wire protocol) to exercise AS_CDT_OP_LIST_JOIN through
the CDT read path. Also tests join via expressions (EXP_SYS_CALL_CDT) and
split-then-join roundtrips combining string split with list join.

Usage:
    python listjoin.py                    # interactive step mode (default)
    python listjoin.py --no-step          # run all tests without pausing
    python listjoin.py --bench            # benchmark mode (1000 iters, 1 thread)
    python listjoin.py --bench 5000       # benchmark mode (5000 iters)
    python listjoin.py --bench --threads 8
"""

import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
    "../../../../aerospike-tests-python/lib"))
os.environ.setdefault("OPENSSL_CONF",
    os.path.join(os.path.dirname(__file__), "openssl-legacy.cnf"))

from asclient.client import Client
from asclient.listops import ListOperations
from asclient.stringops import StringOperations
from asclient.expressions import (
    Expressions, build_code,
    get_call_expression, get_function_expression, get_bin_expression,
)
from asclient.const import (
    EXP_TYPE_STR, EXP_TYPE_LIST, EXP_SYS_CALL_CDT, EXP_SYS_CALL_STRING,
    STRING_OP_SPLIT, OP_LIST_JOIN,
)
from config import HOSTS, NAMESPACE, SET

# --- Mode selection ---
BENCH_MODE = False
BENCH_N = 1000
BENCH_THREADS = 1
STEP_MODE = True

args = sys.argv[1:]
if "--bench" in args:
    BENCH_MODE = True
    STEP_MODE = False
    idx = args.index("--bench")
    if idx + 1 < len(args) and args[idx + 1].isdigit():
        BENCH_N = int(args[idx + 1])
    if "--threads" in args:
        tidx = args.index("--threads")
        if tidx + 1 < len(args) and args[tidx + 1].isdigit():
            BENCH_THREADS = int(args[tidx + 1])
elif "--no-step" in args:
    STEP_MODE = False

client = Client(HOSTS[0][0], HOSTS[0][1])
client.connect()
list_ops = ListOperations(client)
string_ops = StringOperations(client)
exp = Expressions(client)

pass_count = 0
fail_count = 0
bench_results = []

def step(msg):
    if STEP_MODE:
        input(f"\n[STEP] {msg} -- Press Enter to execute...")

def reset_list(values):
    client.put(key, [("mylist", values)])

def reset_str(value):
    client.put(key, [("text", value)])

def show(label, rc, bins):
    if not BENCH_MODE:
        print(f"  {label} -> rc={rc}, bins={bins}")

def get():
    rc, meta, bins = client.get(key)
    if not BENCH_MODE:
        print(f"  get -> rc={rc}, bins={bins}")
    return bins

def binval(bins, name):
    name_b = name.encode() if isinstance(name, str) else name
    for k, v in bins:
        if k == name_b:
            return v.decode() if isinstance(v, bytes) else v
    return None

def listval(bins, name):
    name_b = name.encode() if isinstance(name, str) else name
    for k, v in bins:
        if k == name_b:
            if isinstance(v, list):
                return [e.decode() if isinstance(e, bytes) else e for e in v]
            return v
    return None

def check(label, actual, expected):
    global pass_count, fail_count
    if actual == expected:
        pass_count += 1
        if not BENCH_MODE:
            print(f"  PASS: {label}")
    else:
        fail_count += 1
        print(f"  FAIL: {label}")
        print(f"    expected: {expected!r}")
        print(f"    actual:   {actual!r}")
        if STEP_MODE:
            try:
                input("  Press Enter to continue (Ctrl-D to quit)...")
            except EOFError:
                print(f"\n\nStopped early. passed={pass_count} failed={fail_count}")
                client.delete(key)
                client.close()
                sys.exit(0)

_ops_done = 0
_ops_lock = threading.Lock()

def _progress_printer(label, total_ops, stop_event):
    t0 = time.perf_counter()
    while not stop_event.wait(2.0):
        with _ops_lock:
            done = _ops_done
        elapsed = time.perf_counter() - t0
        pct = done * 100 / total_ops if total_ops else 0
        rate = done / elapsed if elapsed > 0 else 0
        print(f"  {label}: {done}/{total_ops} ({pct:.0f}%) {rate:.0f} ops/s",
              flush=True)

def _thread_worker(thread_id, make_setup, make_op, n, results, barrier):
    global _ops_done
    c = Client(HOSTS[0][0], HOSTS[0][1])
    c.connect()
    lops = ListOperations(c)
    tkey = (NAMESPACE, SET, f"listjoin_t{thread_id}")
    c.put(tkey, [("mylist", ["a", "b", "c"])])

    setup_fn = make_setup(c, lops, tkey)
    op_fn = make_op(c, lops, tkey)

    setup_fn()
    op_fn()
    setup_fn()

    barrier.wait()
    t0 = time.perf_counter()
    for i in range(n):
        op_fn()
        if (i & 0xff) == 0:
            with _ops_lock:
                _ops_done += 0x100 if i > 0 else 1
    with _ops_lock:
        _ops_done += n - ((n - 1) & ~0xff) - 1 if n > 0 else 0
    elapsed = time.perf_counter() - t0

    results[thread_id] = elapsed
    c.delete(tkey)
    c.close()

def bench(label, setup_fn, op_fn, make_setup=None, make_op=None):
    global _ops_done
    if BENCH_THREADS > 1 and make_setup and make_op:
        per_thread = BENCH_N // BENCH_THREADS
        total_ops = per_thread * BENCH_THREADS
        _ops_done = 0
        stop_event = threading.Event()
        progress = threading.Thread(target=_progress_printer,
                                    args=(label, total_ops, stop_event),
                                    daemon=True)
        progress.start()

        results = [0.0] * BENCH_THREADS
        barrier = threading.Barrier(BENCH_THREADS)
        threads = []
        for tid in range(BENCH_THREADS):
            t = threading.Thread(target=_thread_worker,
                                 args=(tid, make_setup, make_op, per_thread,
                                       results, barrier))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        stop_event.set()
        progress.join()

        wall_time = max(results)
        agg_ops_sec = total_ops / wall_time if wall_time > 0 else float('inf')
        avg_ms = (wall_time / per_thread) * 1000
        bench_results.append((label, total_ops, wall_time, agg_ops_sec, avg_ms))
    else:
        setup_fn()
        op_fn()
        setup_fn()
        total = BENCH_N
        _ops_done = 0
        stop_event = threading.Event()
        progress = threading.Thread(target=_progress_printer,
                                    args=(label, total, stop_event),
                                    daemon=True)
        progress.start()
        t0 = time.perf_counter()
        for i in range(BENCH_N):
            op_fn()
            if (i & 0xff) == 0:
                with _ops_lock:
                    _ops_done = i + 1
        elapsed = time.perf_counter() - t0
        stop_event.set()
        progress.join()
        ops_sec = BENCH_N / elapsed if elapsed > 0 else float('inf')
        avg_ms = (elapsed / BENCH_N) * 1000
        bench_results.append((label, BENCH_N, elapsed, ops_sec, avg_ms))

key = (NAMESPACE, SET, "listjoin")

# ===================================================================
# Initialize
# ===================================================================
step("put: create record with mylist=['hello', 'world']")
rc, meta, bins = client.put(key, [("mylist", ["hello", "world"])])
if not BENCH_MODE:
    print(f"  put -> rc={rc}")
if rc != 0:
    print(f"  meta={meta}, bins={bins}")
    sys.exit(1)

# ===================================================================
# Section 1: Binop join tests
# ===================================================================

if BENCH_MODE:
    bench("join(',')",
          lambda: reset_list(["a", "b", "c"]),
          lambda: list_ops.join(key, "mylist", ","),
          make_setup=lambda c, l, k: lambda: c.put(k, [("mylist", ["a", "b", "c"])]),
          make_op=lambda c, l, k: lambda: l.join(k, "mylist", ","))
    bench("join('')",
          lambda: reset_list(["a", "b", "c"]),
          lambda: list_ops.join(key, "mylist", ""),
          make_setup=lambda c, l, k: lambda: c.put(k, [("mylist", ["a", "b", "c"])]),
          make_op=lambda c, l, k: lambda: l.join(k, "mylist", ""))
    bench("join(' - ') multi-char",
          lambda: reset_list(["x", "y", "z"]),
          lambda: list_ops.join(key, "mylist", " - "),
          make_setup=lambda c, l, k: lambda: c.put(k, [("mylist", ["x", "y", "z"])]),
          make_op=lambda c, l, k: lambda: l.join(k, "mylist", " - "))
else:
    # --- basic comma join ---
    reset_list(["hello", "world"])
    step("join(','): basic comma join")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',')", rc, bins)
    check("join(['hello','world'], ',')", binval(bins, "mylist"), "hello,world")

    # --- empty separator ---
    reset_list(["a", "b", "c"])
    step("join(''): empty separator -> direct concatenation")
    rc, meta, bins = list_ops.join(key, "mylist", "")
    show("join('')", rc, bins)
    check("join(['a','b','c'], '')", binval(bins, "mylist"), "abc")

    # --- single element ---
    reset_list(["only"])
    step("join(','): single element list")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',') single", rc, bins)
    check("join(['only'], ',')", binval(bins, "mylist"), "only")

    # --- empty list ---
    reset_list([])
    step("join(','): empty list -> empty string")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',') empty", rc, bins)
    check("join([], ',')", binval(bins, "mylist"), "")

    # --- multi-char separator ---
    reset_list(["x", "y"])
    step("join(' - '): multi-char separator")
    rc, meta, bins = list_ops.join(key, "mylist", " - ")
    show("join(' - ')", rc, bins)
    check("join(['x','y'], ' - ')", binval(bins, "mylist"), "x - y")

    # --- empty string elements ---
    reset_list(["", "a", ""])
    step("join(','): list with empty string elements")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',') empties", rc, bins)
    check("join(['','a',''], ',')", binval(bins, "mylist"), ",a,")

    # --- all empty strings ---
    reset_list(["", "", ""])
    step("join(','): all empty strings")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',') all empty", rc, bins)
    check("join(['','',''], ',')", binval(bins, "mylist"), ",,")

    # --- error: non-string element ---
    reset_list([1, "b"])
    step("join(','): non-string element -> ERR_PARAMETER")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',') non-string", rc, bins)
    check("join([1,'b'], ',') error rc=4", rc, 4)

    # --- error: mixed types ---
    reset_list(["a", 42, "c"])
    step("join(','): mixed types -> ERR_PARAMETER")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',') mixed", rc, bins)
    check("join(['a',42,'c'], ',') error rc=4", rc, 4)

    # --- unicode: CJK content ---
    reset_list(["\u65e5", "\u672c", "\u8a9e"])
    step("join(','): CJK strings")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',') CJK", rc, bins)
    check("join CJK", binval(bins, "mylist"), "\u65e5,\u672c,\u8a9e")

    # --- unicode separator ---
    reset_list(["a", "b", "c"])
    step("join('\u2192'): Unicode separator")
    rc, meta, bins = list_ops.join(key, "mylist", "\u2192")
    show("join('\u2192')", rc, bins)
    check("join(['a','b','c'], '\u2192')", binval(bins, "mylist"), "a\u2192b\u2192c")

# ===================================================================
# Section 2: Expression join tests
# ===================================================================

def list_join_exp(separator, bin_name="mylist"):
    """Build a CDT join expression reading from a list bin."""
    params = [separator] if separator != "" else []
    func = get_function_expression(OP_LIST_JOIN, params)
    bin_exp = get_bin_expression(EXP_TYPE_LIST, bin_name)
    call = get_call_expression(EXP_TYPE_STR, EXP_SYS_CALL_CDT, func, bin_exp)
    return build_code(call)

def split_then_join_exp(split_sep, join_sep, bin_name="text"):
    """Build a chained expression: split string bin -> join resulting list."""
    split_params = [split_sep] if split_sep != "" else []
    split_func = get_function_expression(STRING_OP_SPLIT, split_params)
    split_input = get_bin_expression(EXP_TYPE_STR, bin_name)
    split_result = get_call_expression(EXP_TYPE_LIST, EXP_SYS_CALL_STRING,
                                       split_func, split_input)

    join_params = [join_sep] if join_sep != "" else []
    join_func = get_function_expression(OP_LIST_JOIN, join_params)
    join_result = get_call_expression(EXP_TYPE_STR, EXP_SYS_CALL_CDT,
                                      join_func, split_result)
    return build_code(join_result)

code_join_comma = list_join_exp(",")
code_join_empty = list_join_exp("")
code_join_dash = list_join_exp("-")

if BENCH_MODE:
    bench("exp join(',')",
          lambda: reset_list(["a", "b", "c"]),
          lambda: exp.read(key, "result", code_join_comma),
          make_setup=lambda c, l, k: lambda: c.put(k, [("mylist", ["a", "b", "c"])]),
          make_op=lambda c, l, k: lambda: Expressions(c).read(k, "result", code_join_comma))
else:
    # --- expression: basic comma join ---
    reset_list(["hello", "world"])
    step("exp join(','): expression-based join")
    rc, meta, bins = exp.read(key, "result", code_join_comma)
    show("exp join(',')", rc, bins)
    check("exp join(['hello','world'], ',')", binval(bins, "result"), "hello,world")

    # --- expression: empty separator ---
    reset_list(["a", "b", "c"])
    step("exp join(''): expression empty separator")
    rc, meta, bins = exp.read(key, "result", code_join_empty)
    show("exp join('')", rc, bins)
    check("exp join(['a','b','c'], '')", binval(bins, "result"), "abc")

    # --- expression: empty list ---
    reset_list([])
    step("exp join(','): expression empty list")
    rc, meta, bins = exp.read(key, "result", code_join_comma)
    show("exp join(',') empty", rc, bins)
    check("exp join([], ',')", binval(bins, "result"), "")

    # --- expression: single element ---
    reset_list(["only"])
    step("exp join('-'): expression single element")
    rc, meta, bins = exp.read(key, "result", code_join_dash)
    show("exp join('-') single", rc, bins)
    check("exp join(['only'], '-')", binval(bins, "result"), "only")

    # --- chained: split then join (identity roundtrip) ---
    code_roundtrip = split_then_join_exp(",", ",")
    reset_str("a,b,c")
    step("exp split(',')+join(','): chained roundtrip")
    rc, meta, bins = exp.read(key, "result", code_roundtrip)
    show("exp split+join roundtrip", rc, bins)
    check("exp split+join 'a,b,c' -> 'a,b,c'", binval(bins, "result"), "a,b,c")

    # --- chained: split by X, join by Y ---
    code_transform = split_then_join_exp(" ", "-")
    reset_str("hello world")
    step("exp split(' ')+join('-'): change separator")
    rc, meta, bins = exp.read(key, "result", code_transform)
    show("exp split+join transform", rc, bins)
    check("exp 'hello world' split(' ') join('-') -> 'hello-world'",
          binval(bins, "result"), "hello-world")

    # --- chained: split empty sep, join with comma ---
    code_explode_rejoin = split_then_join_exp("", ",")
    reset_str("abc")
    step("exp split('')+join(','): explode then rejoin")
    rc, meta, bins = exp.read(key, "result", code_explode_rejoin)
    show("exp explode+rejoin", rc, bins)
    check("exp 'abc' split('') join(',') -> 'a,b,c'",
          binval(bins, "result"), "a,b,c")

    # --- chained: CJK content (split then join via expression) ---
    code_cjk_roundtrip = split_then_join_exp(",", ",", bin_name="text")
    reset_str("\u65e5,\u672c,\u8a9e")
    step("exp split(',')+join(','): CJK roundtrip")
    rc, meta, bins = exp.read(key, "result", code_cjk_roundtrip)
    show("exp CJK roundtrip", rc, bins)
    check("exp CJK split+join '\u65e5,\u672c,\u8a9e'",
          binval(bins, "result"), "\u65e5,\u672c,\u8a9e")

    # --- chained: CJK with separator change ---
    code_cjk_transform = split_then_join_exp(",", "-", bin_name="text")
    reset_str("\u65e5,\u672c,\u8a9e")
    step("exp split(',')+join('-'): CJK separator change")
    rc, meta, bins = exp.read(key, "result", code_cjk_transform)
    show("exp CJK transform", rc, bins)
    check("exp CJK split(',') join('-') -> '\u65e5-\u672c-\u8a9e'",
          binval(bins, "result"), "\u65e5-\u672c-\u8a9e")

# ===================================================================
# Section 3: Split-join roundtrip (binop path)
# ===================================================================

if not BENCH_MODE:
    # --- split a string, then join the result list ---
    reset_str("a,b,c")
    step("binop roundtrip: split 'a,b,c' by ','")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',')", rc, bins)
    split_result = listval(bins, "text")
    check("split('a,b,c', ',')", split_result, ["a", "b", "c"])

    # Write the split result as a list bin, then join it
    client.put(key, [("mylist", split_result)])
    step("binop roundtrip: join split result by ','")
    rc, meta, bins = list_ops.join(key, "mylist", ",")
    show("join(',')", rc, bins)
    check("roundtrip split+join 'a,b,c'", binval(bins, "mylist"), "a,b,c")

    # --- split by space, join by dash ---
    reset_str("hello world")
    step("binop roundtrip: split 'hello world' by ' '")
    rc, meta, bins = string_ops.split(key, "text", " ")
    show("split(' ')", rc, bins)
    split_result = listval(bins, "text")
    check("split('hello world', ' ')", split_result, ["hello", "world"])

    client.put(key, [("mylist", split_result)])
    step("binop roundtrip: join by '-'")
    rc, meta, bins = list_ops.join(key, "mylist", "-")
    show("join('-')", rc, bins)
    check("'hello world' split(' ') join('-') -> 'hello-world'",
          binval(bins, "mylist"), "hello-world")

# ===================================================================
# Cleanup & Summary
# ===================================================================
client.delete(key)
client.close()

if BENCH_MODE:
    thread_info = f", {BENCH_THREADS} threads" if BENCH_THREADS > 1 else ""
    print(f"\n--- Bench Mode ({BENCH_N} iters/thread{thread_info}) ---")
    print(f"{'op':<28} {'total_ops':>10} {'wall':>9} {'ops/s':>10} {'avg/op':>10}")
    print("-" * 70)
    for label, n, elapsed, ops_sec, avg_ms in bench_results:
        print(f"{label:<28} {n:>10} {elapsed:>8.3f}s {ops_sec:>9.0f} {avg_ms:>8.3f}ms")
else:
    print(f"\nDone. passed={pass_count} failed={fail_count}")

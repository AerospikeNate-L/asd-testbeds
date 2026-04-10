#!/usr/bin/env python3
"""String operations - string-level manipulation on string bins

Uses asclient (raw wire protocol) to exercise the new STRING_MODIFY and
STRING_READ op paths through write.c and rw_utils.c.

Read ops:  STRLEN(0), SUBSTR(1), CHAR_AT(2), FIND(3), CONTAINS(4),
           STARTS_WITH(5), ENDS_WITH(6), TO_INTEGER(7), TO_DOUBLE(8),
           BYTE_LENGTH(9), IS_NUMERIC(10), IS_UPPER(11), IS_LOWER(12),
           TO_BLOB(13), SPLIT(14), B64_DECODE(15), REGEX_COMPARE(16)
Modify ops: INSERT(50), OVERWRITE(51), CONCAT(52), SNIP(53),
            REPLACE(54), REPLACE_ALL(55), UPPER(56), LOWER(57),
            CASE_FOLD(58), NORMALIZE_NFC(59),
            TRIM_START(60), TRIM_END(61), TRIM(62),
            PAD_START(63), PAD_END(64), REPEAT(65), REGEX_REPLACE(66)

Usage:
    python stringops.py                    # interactive step mode (default)
    python stringops.py --no-step          # run all tests without pausing
    python stringops.py --bench            # benchmark mode (1000 iters, 1 thread)
    python stringops.py --bench 5000       # benchmark mode (5000 iters)
    python stringops.py --bench --threads 8          # 8 concurrent clients
    python stringops.py --bench 5000 --threads 8     # 5000 iters x 8 threads
"""

import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
    "../../../../aerospike-tests-python/lib"))
os.environ.setdefault("OPENSSL_CONF",
    os.path.join(os.path.dirname(__file__), "openssl-legacy.cnf"))

from asclient.client import Client
from asclient.stringops import StringOperations
from asclient.expressions import (
    Expressions, build_code,
    get_call_expression, get_function_expression, get_bin_expression,
)
from asclient.const import EXP_TYPE_BLOB, EXP_TYPE_STR, EXP_SYS_CALL_REPR
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
string_ops = StringOperations(client)
exp = Expressions(client)

pass_count = 0
fail_count = 0
bench_results = []

def step(msg):
    if STEP_MODE:
        input(f"\n[STEP] {msg} -- Press Enter to execute...")

def reset(value="hello world"):
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
    """Extract a bin value by name from the bins list, decoding bytes."""
    name_b = name.encode() if isinstance(name, str) else name
    for k, v in bins:
        if k == name_b:
            return v.decode() if isinstance(v, bytes) else v
    return None

def listval(bins, name):
    """Extract a list bin value, decoding bytes elements to strings."""
    name_b = name.encode() if isinstance(name, str) else name
    for k, v in bins:
        if k == name_b:
            if isinstance(v, list):
                return [e.decode() if isinstance(e, bytes) else e for e in v]
            return v
    return None

def rawval(bins, name):
    """Extract a bin value by name as raw bytes (no decoding)."""
    name_b = name.encode() if isinstance(name, str) else name
    for k, v in bins:
        if k == name_b:
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
    """Background thread that prints progress every 2 seconds."""
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
    """Worker for concurrent bench. Each thread gets its own client."""
    global _ops_done
    c = Client(HOSTS[0][0], HOSTS[0][1])
    c.connect()
    sops = StringOperations(c)
    tkey = (NAMESPACE, SET, f"strops_t{thread_id}")
    c.put(tkey, [("text", "hello world")])

    setup_fn = make_setup(c, sops, tkey)
    op_fn = make_op(c, sops, tkey)

    setup_fn()
    op_fn()  # warm up
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
    """Run op_fn N times. With --threads > 1, spawns concurrent clients."""
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
        op_fn()  # warm up
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

key = (NAMESPACE, SET, "strops")

# ===================================================================
# Initialize
# ===================================================================
step("put: create record with text='hello world'")
rc, meta, bins = client.put(key, [("text", "hello world")])
if not BENCH_MODE:
    print(f"  put -> rc={rc}")
if rc != 0:
    print(f"  meta={meta}, bins={bins}")
    sys.exit(1)
bins = get()
check("put text='hello world'", binval(bins, "text"), "hello world")

# ===================================================================
# STRING_READ ops
# ===================================================================

if BENCH_MODE:
    bench("strlen",
          lambda: reset("hello world"),
          lambda: string_ops.strlen(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.strlen(k, "text"))
    bench("substr(0,5)",
          lambda: reset("hello world"),
          lambda: string_ops.substr(key, "text", 0, 5),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.substr(k, "text", 0, 5))
    bench("substr(6)",
          lambda: reset("hello world"),
          lambda: string_ops.substr(key, "text", 6),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.substr(k, "text", 6))
    bench("char_at(4)",
          lambda: reset("hello world"),
          lambda: string_ops.char_at(key, "text", 4),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.char_at(k, "text", 4))
    bench("find('world')",
          lambda: reset("hello world"),
          lambda: string_ops.find(key, "text", "world"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.find(k, "text", "world"))
    bench("find('o',2)",
          lambda: reset("hello world"),
          lambda: string_ops.find(key, "text", "o", 2),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.find(k, "text", "o", 2))
    bench("contains('world')",
          lambda: reset("hello world"),
          lambda: string_ops.contains(key, "text", "world"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.contains(k, "text", "world"))
    bench("contains('xyz')",
          lambda: reset("hello world"),
          lambda: string_ops.contains(key, "text", "xyz"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.contains(k, "text", "xyz"))
    bench("split(',')",
          lambda: reset("a,b,c"),
          lambda: string_ops.split(key, "text", ","),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "a,b,c")]),
          make_op=lambda c, s, k: lambda: s.split(k, "text", ","))
    bench("split('') codepoint",
          lambda: reset("hello"),
          lambda: string_ops.split(key, "text", ""),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello")]),
          make_op=lambda c, s, k: lambda: s.split(k, "text", ""))
else:
    step("strlen: get string length")
    rc, meta, bins = string_ops.strlen(key, "text")
    show("strlen", rc, bins)
    check("strlen('hello world')", binval(bins, "text"), 11)

    step("substr(0, 5): get first 5 chars")
    rc, meta, bins = string_ops.substr(key, "text", 0, 5)
    show("substr(0, 5)", rc, bins)
    check("substr(0, 5)", binval(bins, "text"), "hello")

    step("substr(6): from offset 6 to end")
    rc, meta, bins = string_ops.substr(key, "text", 6)
    show("substr(6)", rc, bins)
    check("substr(6)", binval(bins, "text"), "world")

    step("char_at(4): get character at index 4")
    rc, meta, bins = string_ops.char_at(key, "text", 4)
    show("char_at(4)", rc, bins)
    check("char_at(4)", binval(bins, "text"), "o")

    step("find('world'): search for substring")
    rc, meta, bins = string_ops.find(key, "text", "world")
    show("find('world')", rc, bins)
    check("find('world')", binval(bins, "text"), 6)

    step("find('o', 2): find 2nd occurrence of 'o'")
    rc, meta, bins = string_ops.find(key, "text", "o", 2)
    show("find('o', 2)", rc, bins)
    check("find('o', 2)", binval(bins, "text"), 7)

    step("contains('world'): check substring exists")
    rc, meta, bins = string_ops.contains(key, "text", "world")
    show("contains('world')", rc, bins)
    check("contains('world')", binval(bins, "text"), 1)

    step("contains('xyz'): check substring absent")
    rc, meta, bins = string_ops.contains(key, "text", "xyz")
    show("contains('xyz')", rc, bins)
    check("contains('xyz')", binval(bins, "text"), 0)

    # --- split ---
    reset("a,b,c")
    step("split(','): basic comma split")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',')", rc, bins)
    check("split('a,b,c', ',')", listval(bins, "text"), ["a", "b", "c"])

    reset("abc")
    step("split(''): empty separator splits by code point")
    rc, meta, bins = string_ops.split(key, "text", "")
    show("split('')", rc, bins)
    check("split('abc', '')", listval(bins, "text"), ["a", "b", "c"])

    reset("a-=-b-=-c")
    step("split('-=-'): multi-char separator")
    rc, meta, bins = string_ops.split(key, "text", "-=-")
    show("split('-=-')", rc, bins)
    check("split('a-=-b-=-c', '-=-')", listval(bins, "text"), ["a", "b", "c"])

    reset("hello world")
    step("split(','): no match returns whole string")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',') no match", rc, bins)
    check("split('hello world', ',')", listval(bins, "text"), ["hello world"])

    reset("")
    step("split(','): empty string")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',') empty", rc, bins)
    check("split('', ',')", listval(bins, "text"), [""])

    reset(",a,b")
    step("split(','): leading delimiter")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',') leading", rc, bins)
    check("split(',a,b', ',')", listval(bins, "text"), ["", "a", "b"])

    reset("a,b,")
    step("split(','): trailing delimiter")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',') trailing", rc, bins)
    check("split('a,b,', ',')", listval(bins, "text"), ["a", "b", ""])

    reset("a,,b")
    step("split(','): consecutive delimiters")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',') consecutive", rc, bins)
    check("split('a,,b', ',')", listval(bins, "text"), ["a", "", "b"])

    reset(",,,")
    step("split(','): all delimiters")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',') all delims", rc, bins)
    check("split(',,,', ',')", listval(bins, "text"), ["", "", "", ""])

    # --- split: unicode ---
    reset("caf\u00e9")
    step("split(''): empty sep, 2-byte UTF-8")
    rc, meta, bins = string_ops.split(key, "text", "")
    show("split('') café", rc, bins)
    check("split('café', '')", listval(bins, "text"), ["c", "a", "f", "\u00e9"])

    reset("\u65e5\u672c\u8a9e")
    step("split(''): empty sep, 3-byte UTF-8 (CJK)")
    rc, meta, bins = string_ops.split(key, "text", "")
    show("split('') CJK", rc, bins)
    check("split('日本語', '')", listval(bins, "text"), ["\u65e5", "\u672c", "\u8a9e"])

    reset("\u65e5,\u672c,\u8a9e")
    step("split(','): ASCII sep with Unicode content")
    rc, meta, bins = string_ops.split(key, "text", ",")
    show("split(',') unicode", rc, bins)
    check("split('日,本,語', ',')", listval(bins, "text"), ["\u65e5", "\u672c", "\u8a9e"])

    # asclient pack_string_value can't encode non-Latin-1 chars in op params,
    # so pass Unicode separators as pre-encoded UTF-8 bytes.
    reset("a\u2192b\u2192c")
    step("split('→'): Unicode separator")
    rc, meta, bins = string_ops.split(key, "text", "\u2192".encode("utf-8"))
    show("split('→')", rc, bins)
    check("split('a→b→c', '→')", listval(bins, "text"), ["a", "b", "c"])

    # --- starts_with ---
    reset("hello world")
    step("starts_with('hello'): match at start")
    rc, meta, bins = string_ops.starts_with(key, "text", "hello")
    show("starts_with('hello')", rc, bins)
    check("starts_with('hello')", binval(bins, "text"), 1)

    reset("hello world")
    step("starts_with('world'): no match at start")
    rc, meta, bins = string_ops.starts_with(key, "text", "world")
    show("starts_with('world')", rc, bins)
    check("starts_with('world') false", binval(bins, "text"), 0)

    reset("hello world")
    step("starts_with(''): empty needle always matches")
    rc, meta, bins = string_ops.starts_with(key, "text", "")
    show("starts_with('')", rc, bins)
    check("starts_with('')", binval(bins, "text"), 1)

    reset("hello world")
    step("starts_with('hello world!'): needle longer than haystack")
    rc, meta, bins = string_ops.starts_with(key, "text", "hello world!")
    show("starts_with longer", rc, bins)
    check("starts_with longer", binval(bins, "text"), 0)

    # --- ends_with ---
    reset("hello world")
    step("ends_with('world'): match at end")
    rc, meta, bins = string_ops.ends_with(key, "text", "world")
    show("ends_with('world')", rc, bins)
    check("ends_with('world')", binval(bins, "text"), 1)

    reset("hello world")
    step("ends_with('hello'): no match at end")
    rc, meta, bins = string_ops.ends_with(key, "text", "hello")
    show("ends_with('hello')", rc, bins)
    check("ends_with('hello') false", binval(bins, "text"), 0)

    reset("hello world")
    step("ends_with(''): empty needle always matches")
    rc, meta, bins = string_ops.ends_with(key, "text", "")
    show("ends_with('')", rc, bins)
    check("ends_with('')", binval(bins, "text"), 1)

    # --- to_integer ---
    reset("42")
    step("to_integer('42'): parse positive integer")
    rc, meta, bins = string_ops.to_integer(key, "text")
    show("to_integer('42')", rc, bins)
    check("to_integer('42')", binval(bins, "text"), 42)

    reset("-7")
    step("to_integer('-7'): parse negative integer")
    rc, meta, bins = string_ops.to_integer(key, "text")
    show("to_integer('-7')", rc, bins)
    check("to_integer('-7')", binval(bins, "text"), -7)

    reset("0")
    step("to_integer('0'): parse zero")
    rc, meta, bins = string_ops.to_integer(key, "text")
    show("to_integer('0')", rc, bins)
    check("to_integer('0')", binval(bins, "text"), 0)

    reset("abc")
    step("to_integer('abc'): non-numeric -> error")
    rc, meta, bins = string_ops.to_integer(key, "text")
    show("to_integer('abc')", rc, bins)
    check("to_integer('abc') error rc!=0", rc != 0, True)

    # --- to_double ---
    reset("3.14")
    step("to_double('3.14'): parse float")
    rc, meta, bins = string_ops.to_double(key, "text")
    show("to_double('3.14')", rc, bins)
    check("to_double('3.14')", binval(bins, "text"), 3.14)

    reset("-1.5")
    step("to_double('-1.5'): parse negative float")
    rc, meta, bins = string_ops.to_double(key, "text")
    show("to_double('-1.5')", rc, bins)
    check("to_double('-1.5')", binval(bins, "text"), -1.5)

    reset("abc")
    step("to_double('abc'): non-numeric -> error")
    rc, meta, bins = string_ops.to_double(key, "text")
    show("to_double('abc')", rc, bins)
    check("to_double('abc') error rc!=0", rc != 0, True)

    # --- byte_length ---
    reset("hello")
    step("byte_length('hello'): ASCII 5 bytes")
    rc, meta, bins = string_ops.byte_length(key, "text")
    show("byte_length('hello')", rc, bins)
    check("byte_length('hello')", binval(bins, "text"), 5)

    reset("caf\u00e9")
    step("byte_length('café'): 2-byte UTF-8 char")
    rc, meta, bins = string_ops.byte_length(key, "text")
    show("byte_length('café')", rc, bins)
    check("byte_length('café')", binval(bins, "text"), 5)

    reset("\u65e5\u672c\u8a9e")
    step("byte_length('日本語'): 3-byte UTF-8 chars")
    rc, meta, bins = string_ops.byte_length(key, "text")
    show("byte_length('日本語')", rc, bins)
    check("byte_length('日本語')", binval(bins, "text"), 9)

    reset("")
    step("byte_length(''): empty string")
    rc, meta, bins = string_ops.byte_length(key, "text")
    show("byte_length('')", rc, bins)
    check("byte_length('')", binval(bins, "text"), 0)

    # --- is_numeric ---
    reset("12345")
    step("is_numeric('12345'): integer string")
    rc, meta, bins = string_ops.is_numeric(key, "text")
    show("is_numeric('12345')", rc, bins)
    check("is_numeric('12345')", binval(bins, "text"), 1)

    reset("3.14")
    step("is_numeric('3.14'): float string")
    rc, meta, bins = string_ops.is_numeric(key, "text")
    show("is_numeric('3.14')", rc, bins)
    check("is_numeric('3.14')", binval(bins, "text"), 1)

    reset("abc")
    step("is_numeric('abc'): not numeric")
    rc, meta, bins = string_ops.is_numeric(key, "text")
    show("is_numeric('abc')", rc, bins)
    check("is_numeric('abc')", binval(bins, "text"), 0)

    reset("12345")
    step("is_numeric('12345', INT): integer sub-flag")
    rc, meta, bins = string_ops.is_numeric(key, "text", numeric_type=1)
    show("is_numeric INT", rc, bins)
    check("is_numeric('12345', INT)", binval(bins, "text"), 1)

    reset("3.14")
    step("is_numeric('3.14', INT): float fails int check")
    rc, meta, bins = string_ops.is_numeric(key, "text", numeric_type=1)
    show("is_numeric('3.14', INT)", rc, bins)
    check("is_numeric('3.14', INT) false", binval(bins, "text"), False)

    reset("3.14")
    step("is_numeric('3.14', FLOAT): float sub-flag")
    rc, meta, bins = string_ops.is_numeric(key, "text", numeric_type=2)
    show("is_numeric FLOAT", rc, bins)
    check("is_numeric('3.14', FLOAT)", binval(bins, "text"), True)

    # --- is_upper ---
    reset("HELLO")
    step("is_upper('HELLO'): all uppercase")
    rc, meta, bins = string_ops.is_upper(key, "text")
    show("is_upper('HELLO')", rc, bins)
    check("is_upper('HELLO')", binval(bins, "text"), True)

    reset("Hello")
    step("is_upper('Hello'): mixed case")
    rc, meta, bins = string_ops.is_upper(key, "text")
    show("is_upper('Hello')", rc, bins)
    check("is_upper('Hello') false", binval(bins, "text"), False)

    reset("123")
    step("is_upper('123'): digits only")
    rc, meta, bins = string_ops.is_upper(key, "text")
    show("is_upper('123')", rc, bins)
    check("is_upper('123')", binval(bins, "text"), False)

    # --- is_lower ---
    reset("hello")
    step("is_lower('hello'): all lowercase")
    rc, meta, bins = string_ops.is_lower(key, "text")
    show("is_lower('hello')", rc, bins)
    check("is_lower('hello')", binval(bins, "text"), True)

    reset("Hello")
    step("is_lower('Hello'): mixed case")
    rc, meta, bins = string_ops.is_lower(key, "text")
    show("is_lower('Hello')", rc, bins)
    check("is_lower('Hello') false", binval(bins, "text"), False)

    # --- to_blob ---
    reset("hello")
    step("to_blob('hello'): convert string to blob")
    rc, meta, bins = string_ops.to_blob(key, "text")
    show("to_blob('hello')", rc, bins)
    raw = rawval(bins, "text")
    check("to_blob('hello')", raw, b"hello")

    reset("")
    step("to_blob(''): empty string to blob")
    rc, meta, bins = string_ops.to_blob(key, "text")
    show("to_blob('')", rc, bins)
    raw = rawval(bins, "text")
    check("to_blob('')", raw, b"")

    # --- b64_decode ---
    reset("aGVsbG8=")
    step("b64_decode('aGVsbG8='): decode base64 -> 'hello'")
    rc, meta, bins = string_ops.b64_decode(key, "text")
    show("b64_decode", rc, bins)
    raw = rawval(bins, "text")
    check("b64_decode('aGVsbG8=')", raw, b"hello")

    reset("YWJj")
    step("b64_decode('YWJj'): no padding -> 'abc'")
    rc, meta, bins = string_ops.b64_decode(key, "text")
    show("b64_decode no pad", rc, bins)
    raw = rawval(bins, "text")
    check("b64_decode('YWJj')", raw, b"abc")

    reset("SGVsbG8gV29ybGQh")
    step("b64_decode: longer string -> 'Hello World!'")
    rc, meta, bins = string_ops.b64_decode(key, "text")
    show("b64_decode longer", rc, bins)
    raw = rawval(bins, "text")
    check("b64_decode('SGVsbG8gV29ybGQh')", raw, b"Hello World!")

    reset("!!invalid!!")
    step("b64_decode: invalid base64 -> error")
    rc, meta, bins = string_ops.b64_decode(key, "text")
    show("b64_decode invalid", rc, bins)
    check("b64_decode invalid error rc!=0", rc != 0, True)

    # --- regex_compare ---
    reset("hello world")
    step("regex_compare('hel+o'): basic match")
    rc, meta, bins = string_ops.regex_compare(key, "text", "hel+o")
    show("regex_compare match", rc, bins)
    check("regex_compare('hel+o') match", binval(bins, "text"), True)

    reset("hello world")
    step("regex_compare('^world'): no match")
    rc, meta, bins = string_ops.regex_compare(key, "text", "^world")
    show("regex_compare no match", rc, bins)
    check("regex_compare('^world') no match", binval(bins, "text"), False)

    reset("HELLO world")
    step("regex_compare('hello', CASE_INSENSITIVE): flag test")
    rc, meta, bins = string_ops.regex_compare(key, "text", "hello", regex_flags=1)
    show("regex_compare case-i", rc, bins)
    check("regex_compare case-insensitive", binval(bins, "text"), True)

    reset("abc 123 def")
    step("regex_compare('\\d+'): digit pattern")
    rc, meta, bins = string_ops.regex_compare(key, "text", "\\d+")
    show("regex_compare digits", rc, bins)
    check("regex_compare('\\d+')", binval(bins, "text"), True)

# ===================================================================
# STRING_MODIFY ops
# ===================================================================

if BENCH_MODE:
    bench("insert",
          lambda: reset("hello world"),
          lambda: string_ops.insert(key, "text", 5, " beautiful"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.insert(k, "text", 5, " beautiful"))
    bench("overwrite",
          lambda: reset("hello world"),
          lambda: string_ops.overwrite(key, "text", 6, "EARTH"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.overwrite(k, "text", 6, "EARTH"))
    bench("concat",
          lambda: reset("hello"),
          lambda: string_ops.concat(key, "text", " ", "world", "!"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello")]),
          make_op=lambda c, s, k: lambda: s.concat(k, "text", " ", "world", "!"))
    bench("snip",
          lambda: reset("hello beautiful world"),
          lambda: string_ops.snip(key, "text", 5, 10),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello beautiful world")]),
          make_op=lambda c, s, k: lambda: s.snip(k, "text", 5, 10))
    bench("replace",
          lambda: reset("hello world"),
          lambda: string_ops.replace(key, "text", "world", "there"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.replace(k, "text", "world", "there"))
    bench("replace_all",
          lambda: reset("abcabcabc"),
          lambda: string_ops.replace_all(key, "text", "abc", "XY"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "abcabcabc")]),
          make_op=lambda c, s, k: lambda: s.replace_all(k, "text", "abc", "XY"))
    bench("upper",
          lambda: reset("hello world"),
          lambda: string_ops.upper(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, s, k: lambda: s.upper(k, "text"))
    bench("lower",
          lambda: reset("HELLO WORLD"),
          lambda: string_ops.lower(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "HELLO WORLD")]),
          make_op=lambda c, s, k: lambda: s.lower(k, "text"))
    bench("case_fold",
          lambda: reset("Straße"),
          lambda: string_ops.case_fold(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "Straße")]),
          make_op=lambda c, s, k: lambda: s.case_fold(k, "text"))
    bench("normalize",
          lambda: reset("café"),
          lambda: string_ops.normalize(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "café")]),
          make_op=lambda c, s, k: lambda: s.normalize(k, "text"))
    bench("trim_start",
          lambda: reset("   hello   "),
          lambda: string_ops.trim_start(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "   hello   ")]),
          make_op=lambda c, s, k: lambda: s.trim_start(k, "text"))
    bench("trim_end",
          lambda: reset("   hello   "),
          lambda: string_ops.trim_end(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "   hello   ")]),
          make_op=lambda c, s, k: lambda: s.trim_end(k, "text"))
    bench("trim",
          lambda: reset("   hello   "),
          lambda: string_ops.trim(key, "text"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "   hello   ")]),
          make_op=lambda c, s, k: lambda: s.trim(k, "text"))
    bench("pad_start",
          lambda: reset("42"),
          lambda: string_ops.pad_start(key, "text", 6, "0"),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "42")]),
          make_op=lambda c, s, k: lambda: s.pad_start(k, "text", 6, "0"))
    bench("pad_end",
          lambda: reset("hi"),
          lambda: string_ops.pad_end(key, "text", 6, "."),
          make_setup=lambda c, s, k: lambda: c.put(k, [("text", "hi")]),
          make_op=lambda c, s, k: lambda: s.pad_end(k, "text", 6, "."))
else:
    # --- insert ---
    reset("hello world")
    step("insert(5, ' beautiful'): insert at offset")
    rc, meta, bins = string_ops.insert(key, "text", 5, " beautiful")
    show("insert", rc, bins)
    bins = get()
    check("insert(5, ' beautiful')", binval(bins, "text"), "hello beautiful world")

    # --- overwrite ---
    reset("hello world")
    step("overwrite(6, 'EARTH'): overwrite starting at offset 6")
    rc, meta, bins = string_ops.overwrite(key, "text", 6, "EARTH")
    show("overwrite", rc, bins)
    bins = get()
    check("overwrite(6, 'EARTH')", binval(bins, "text"), "hello EARTH")

    # --- concat ---
    reset("hello")
    step("concat(' ', 'world', '!'): append multiple strings")
    rc, meta, bins = string_ops.concat(key, "text", " ", "world", "!")
    show("concat", rc, bins)
    bins = get()
    check("concat(' ', 'world', '!')", binval(bins, "text"), "hello world!")

    # --- snip ---
    reset("hello beautiful world")
    step("snip(5, 10): remove 10 chars starting at offset 5")
    rc, meta, bins = string_ops.snip(key, "text", 5, 10)
    show("snip", rc, bins)
    bins = get()
    check("snip(5, 10)", binval(bins, "text"), "hellotiful world")

    # --- replace ---
    reset("hello world")
    step("replace('world', 'there'): replace first occurrence")
    rc, meta, bins = string_ops.replace(key, "text", "world", "there")
    show("replace", rc, bins)
    bins = get()
    check("replace('world', 'there')", binval(bins, "text"), "hello there")

    # --- replace_all ---
    reset("abcabcabc")
    step("replace_all('abc', 'XY'): replace all occurrences")
    rc, meta, bins = string_ops.replace_all(key, "text", "abc", "XY")
    show("replace_all", rc, bins)
    bins = get()
    check("replace_all('abc', 'XY')", binval(bins, "text"), "XYXYXY")

    # --- upper ---
    reset("hello world")
    step("upper: convert to uppercase")
    rc, meta, bins = string_ops.upper(key, "text")
    show("upper", rc, bins)
    bins = get()
    check("upper", binval(bins, "text"), "HELLO WORLD")

    # --- lower ---
    reset("HELLO WORLD")
    step("lower: convert to lowercase")
    rc, meta, bins = string_ops.lower(key, "text")
    show("lower", rc, bins)
    bins = get()
    check("lower", binval(bins, "text"), "hello world")

    # --- case_fold ---
    reset("Straße")
    step("case_fold: unicode case fold")
    rc, meta, bins = string_ops.case_fold(key, "text")
    show("case_fold", rc, bins)
    bins = get()
    check("case_fold('Straße')", binval(bins, "text"), "strasse")

    # --- normalize ---
    reset("café")
    step("normalize: NFC normalization")
    rc, meta, bins = string_ops.normalize(key, "text")
    show("normalize", rc, bins)
    bins = get()
    check("normalize('café')", binval(bins, "text"), "caf\u00e9")

    # --- trim_start ---
    reset("   hello   ")
    step("trim_start: remove leading whitespace")
    rc, meta, bins = string_ops.trim_start(key, "text")
    show("trim_start", rc, bins)
    bins = get()
    check("trim_start", binval(bins, "text"), "hello   ")

    # --- trim_end ---
    reset("   hello   ")
    step("trim_end: remove trailing whitespace")
    rc, meta, bins = string_ops.trim_end(key, "text")
    show("trim_end", rc, bins)
    bins = get()
    check("trim_end", binval(bins, "text"), "   hello")

    # --- trim ---
    reset("   hello   ")
    step("trim: remove leading+trailing whitespace")
    rc, meta, bins = string_ops.trim(key, "text")
    show("trim", rc, bins)
    bins = get()
    check("trim", binval(bins, "text"), "hello")

    # --- pad_start ---
    reset("42")
    step("pad_start(6, '0'): left-pad to length 6 with '0'")
    rc, meta, bins = string_ops.pad_start(key, "text", 6, "0")
    show("pad_start", rc, bins)
    bins = get()
    check("pad_start(6, '0')", binval(bins, "text"), "000042")

    # --- pad_end ---
    reset("hi")
    step("pad_end(6, '.'): right-pad to length 6 with '.'")
    rc, meta, bins = string_ops.pad_end(key, "text", 6, ".")
    show("pad_end", rc, bins)
    bins = get()
    check("pad_end(6, '.')", binval(bins, "text"), "hi....")

    # --- repeat ---
    reset("abc")
    step("repeat(3): repeat string 3 times")
    rc, meta, bins = string_ops.repeat(key, "text", 3)
    show("repeat(3)", rc, bins)
    bins = get()
    check("repeat('abc', 3)", binval(bins, "text"), "abcabcabc")

    reset("abc")
    step("repeat(1): repeat once (identity)")
    rc, meta, bins = string_ops.repeat(key, "text", 1)
    show("repeat(1)", rc, bins)
    bins = get()
    check("repeat('abc', 1)", binval(bins, "text"), "abc")

    reset("abc")
    step("repeat(0): repeat zero times (empty)")
    rc, meta, bins = string_ops.repeat(key, "text", 0)
    show("repeat(0)", rc, bins)
    bins = get()
    check("repeat('abc', 0)", binval(bins, "text"), "")

    reset("x")
    step("repeat(5): single char repeated")
    rc, meta, bins = string_ops.repeat(key, "text", 5)
    show("repeat(5)", rc, bins)
    bins = get()
    check("repeat('x', 5)", binval(bins, "text"), "xxxxx")

    # --- regex_replace ---
    reset("hello world")
    step("regex_replace('world', 'there', GLOBAL): basic replace")
    rc, meta, bins = string_ops.regex_replace(key, "text", "world", "there",
                                              regex_flags=16)
    show("regex_replace", rc, bins)
    bins = get()
    check("regex_replace basic", binval(bins, "text"), "hello there")

    reset("aaa")
    step("regex_replace('a', 'bb', GLOBAL): multiple matches")
    rc, meta, bins = string_ops.regex_replace(key, "text", "a", "bb",
                                              regex_flags=16)
    show("regex_replace multi", rc, bins)
    bins = get()
    check("regex_replace multiple", binval(bins, "text"), "bbbbbb")

    reset("hello world")
    step("regex_replace('xyz', 'ABC', GLOBAL): no match")
    rc, meta, bins = string_ops.regex_replace(key, "text", "xyz", "ABC",
                                              regex_flags=16)
    show("regex_replace no match", rc, bins)
    bins = get()
    check("regex_replace no match", binval(bins, "text"), "hello world")

    reset("hello")
    step("regex_replace('l', '', GLOBAL): deletion")
    rc, meta, bins = string_ops.regex_replace(key, "text", "l", "",
                                              regex_flags=16)
    show("regex_replace delete", rc, bins)
    bins = get()
    check("regex_replace deletion", binval(bins, "text"), "heo")

    reset("HELLO world")
    step("regex_replace('hello', 'bye', CASE_INSENSITIVE|GLOBAL)")
    rc, meta, bins = string_ops.regex_replace(key, "text", "hello", "bye",
                                              regex_flags=1|16)
    show("regex_replace case-i", rc, bins)
    bins = get()
    check("regex_replace case-insensitive", binval(bins, "text"), "bye world")

    reset("abc 123 def 456")
    step("regex_replace('\\d+', '#', GLOBAL): pattern with quantifier")
    rc, meta, bins = string_ops.regex_replace(key, "text", "\\d+", "#",
                                              regex_flags=16)
    show("regex_replace quantifier", rc, bins)
    bins = get()
    check("regex_replace quantifier", binval(bins, "text"), "abc # def #")

    reset("hello world")
    step("regex_replace('o', 'O', first only): without GLOBAL flag")
    rc, meta, bins = string_ops.regex_replace(key, "text", "o", "O")
    show("regex_replace first", rc, bins)
    bins = get()
    check("regex_replace first only", binval(bins, "text"), "hellO world")

# ===================================================================
# BLOB_TO_STRING (CALL_REPR on blob bins, counterpart to to_blob above)
# ===================================================================

def blob_to_string_exp(bin_name):
    func = get_function_expression(0, [])
    bin_exp = get_bin_expression(EXP_TYPE_BLOB, bin_name)
    call = get_call_expression(EXP_TYPE_STR, EXP_SYS_CALL_REPR, func, bin_exp)
    return build_code(call)

code_blob_to_string = blob_to_string_exp("bdata")

if not BENCH_MODE:
    # --- blob_to_string: valid ASCII ---
    client.put(key, [("bdata", bytearray(b"hello"))])
    step("blob_to_string(b'hello'): convert blob to string")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("blob_to_string(b'hello')", rc, bins)
    check("blob_to_string(b'hello')", binval(bins, "result"), "hello")

    # --- blob_to_string: valid UTF-8 ---
    client.put(key, [("bdata", bytearray(b"caf\xc3\xa9"))])
    step("blob_to_string(blob café): UTF-8 blob to string")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("blob_to_string(café)", rc, bins)
    check("blob_to_string(café)", binval(bins, "result"), "café")

    # --- blob_to_string: empty ---
    client.put(key, [("bdata", bytearray(b""))])
    step("blob_to_string(empty): empty blob to empty string")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("blob_to_string(empty)", rc, bins)
    check("blob_to_string(empty)", binval(bins, "result"), "")

    # --- blob_to_string: invalid UTF-8 should fail ---
    client.put(key, [("bdata", bytearray(b"\xff\xfe"))])
    step("blob_to_string(invalid utf8): expect error")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("blob_to_string(invalid)", rc, bins)
    check("blob_to_string(invalid) error", rc, 26)  # AS_ERR_OP_NOT_APPLICABLE

# ===================================================================
# Cleanup & Summary
# ===================================================================
client.delete(key)
client.close()

if BENCH_MODE:
    thread_info = f", {BENCH_THREADS} threads" if BENCH_THREADS > 1 else ""
    print(f"\n--- Bench Mode ({BENCH_N} iters/thread{thread_info}) ---")
    print(f"{'op':<20} {'total_ops':>10} {'wall':>9} {'ops/s':>10} {'avg/op':>10}")
    print("-" * 62)
    for label, n, elapsed, ops_sec, avg_ms in bench_results:
        print(f"{label:<20} {n:>10} {elapsed:>8.3f}s {ops_sec:>9.0f} {avg_ms:>8.3f}ms")
else:
    print(f"\nDone. passed={pass_count} failed={fail_count}")

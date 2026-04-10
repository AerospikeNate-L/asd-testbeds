#!/usr/bin/env python3
"""String operations via expressions - mirrors stringops.py coverage using
the EXP_CALL expression path (CALL_STRING = 3) instead of direct binops.

Uses asclient (raw wire protocol) expression builder to exercise string
ops through the expression evaluation path in exp.c / particle_string.c.

Read ops:  STRLEN(0), SUBSTR(1), CHAR_AT(2), FIND(3), CONTAINS(4),
           STARTS_WITH(5), ENDS_WITH(6), TO_INTEGER(7), TO_DOUBLE(8),
           BYTE_LENGTH(9), IS_NUMERIC(10), IS_UPPER(11), IS_LOWER(12),
           TO_BLOB(13), SPLIT(14), B64_DECODE(15), REGEX_COMPARE(16)
Modify ops: INSERT(50), OVERWRITE(51), CONCAT(52), SNIP(53),
            REPLACE(54), REPLACE_ALL(55), UPPER(56), LOWER(57),
            CASE_FOLD(58), NORMALIZE_NFC(59),
            TRIM_START(60), TRIM_END(61), TRIM(62),
            PAD_START(63), PAD_END(64), REPEAT(65), REGEX_REPLACE(66)
Also: TO_STRING (CALL_REPR) for int, float, bool, string bins.

Usage:
    python stringexprs.py                    # interactive step mode (default)
    python stringexprs.py --no-step          # run all tests without pausing
    python stringexprs.py --bench            # benchmark mode (1000 iters, 1 thread)
    python stringexprs.py --bench 5000       # benchmark mode (5000 iters)
    python stringexprs.py --bench --threads 8          # 8 concurrent clients
    python stringexprs.py --bench 5000 --threads 8     # 5000 iters x 8 threads
"""

import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
    "../../../../aerospike-tests-python/lib"))
os.environ.setdefault("OPENSSL_CONF",
    os.path.join(os.path.dirname(__file__), "openssl-legacy.cnf"))

from asclient.client import Client
from asclient.expressions import (
    Expressions, build_code,
    get_call_expression, get_function_expression, get_bin_expression,
)
from asclient.const import *
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
    e = Expressions(c)
    tkey = (NAMESPACE, SET, f"strexprs_t{thread_id}")
    c.put(tkey, [("text", "hello world")])

    setup_fn = make_setup(c, e, tkey)
    op_fn = make_op(c, e, tkey)

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


# Helpers to build string expression calls

def string_read_exp(opcode, params=None, return_type=EXP_TYPE_STR):
    """Build a read-only string expression (CALL_STRING)."""
    func = get_function_expression(opcode, params)
    bin_exp = get_bin_expression(EXP_TYPE_STR, "text")
    call = get_call_expression(return_type, EXP_SYS_CALL_STRING, func, bin_exp)
    return build_code(call)

def string_modify_exp(opcode, params=None):
    """Build a modify string expression (CALL_STRING | MODIFY_LOCAL)."""
    func = get_function_expression(opcode, params)
    bin_exp = get_bin_expression(EXP_TYPE_STR, "text")
    call = get_call_expression(
        EXP_TYPE_STR,
        EXP_SYS_CALL_STRING | EXP_SYS_FLAG_MODIFY_LOCAL,
        func, bin_exp)
    return build_code(call)


key = (NAMESPACE, SET, "strexprs")

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
# STRING_READ ops via expressions
# ===================================================================

# Pre-build expression codes (these are pure Python, no server round-trip)
code_strlen = string_read_exp(STRING_OP_STRLEN, [], return_type=EXP_TYPE_INT)
code_substr_0_5 = string_read_exp(STRING_OP_SUBSTR, [0, 5])
code_substr_6 = string_read_exp(STRING_OP_SUBSTR, [6])
code_char_at_4 = string_read_exp(STRING_OP_CHAR_AT, [4])
code_find_world = string_read_exp(STRING_OP_FIND, ["world"], return_type=EXP_TYPE_INT)
code_find_o_2 = string_read_exp(STRING_OP_FIND, ["o", 2], return_type=EXP_TYPE_INT)
# Server stores CONTAINS as bool; INT return type yields ERR_OP_NOT_APPLICABLE (26).
code_contains_world = string_read_exp(STRING_OP_CONTAINS, ["world"], return_type=EXP_TYPE_TRILEAN)
code_contains_xyz = string_read_exp(STRING_OP_CONTAINS, ["xyz"], return_type=EXP_TYPE_TRILEAN)

if BENCH_MODE:
    bench("exp strlen",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_strlen),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_strlen))
    bench("exp substr(0,5)",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_substr_0_5),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_substr_0_5))
    bench("exp substr(6)",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_substr_6),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_substr_6))
    bench("exp char_at(4)",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_char_at_4),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_char_at_4))
    bench("exp find('world')",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_find_world),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_find_world))
    bench("exp find('o',2)",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_find_o_2),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_find_o_2))
    bench("exp contains('world')",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_contains_world),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_contains_world))
    bench("exp contains('xyz')",
          lambda: reset("hello world"),
          lambda: exp.read(key, "result", code_contains_xyz),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_contains_xyz))
else:
    step("exp strlen: get string length")
    rc, meta, bins = exp.read(key, "result", code_strlen)
    show("exp strlen", rc, bins)
    check("exp strlen('hello world')", binval(bins, "result"), 11)

    step("exp substr(0, 5): get first 5 chars")
    rc, meta, bins = exp.read(key, "result", code_substr_0_5)
    show("exp substr(0, 5)", rc, bins)
    check("exp substr(0, 5)", binval(bins, "result"), "hello")

    step("exp substr(6): from offset 6 to end")
    rc, meta, bins = exp.read(key, "result", code_substr_6)
    show("exp substr(6)", rc, bins)
    check("exp substr(6)", binval(bins, "result"), "world")

    step("exp char_at(4): get character at index 4")
    rc, meta, bins = exp.read(key, "result", code_char_at_4)
    show("exp char_at(4)", rc, bins)
    check("exp char_at(4)", binval(bins, "result"), "o")

    step("exp find('world'): search for substring")
    rc, meta, bins = exp.read(key, "result", code_find_world)
    show("exp find('world')", rc, bins)
    check("exp find('world')", binval(bins, "result"), 6)

    step("exp find('o', 2): find 2nd occurrence of 'o'")
    rc, meta, bins = exp.read(key, "result", code_find_o_2)
    show("exp find('o', 2)", rc, bins)
    check("exp find('o', 2)", binval(bins, "result"), 7)

    step("exp contains('world'): check substring exists")
    rc, meta, bins = exp.read(key, "result", code_contains_world)
    show("exp contains('world')", rc, bins)
    check("exp contains('world')", binval(bins, "result"), True)

    step("exp contains('xyz'): check substring absent")
    rc, meta, bins = exp.read(key, "result", code_contains_xyz)
    show("exp contains('xyz')", rc, bins)
    check("exp contains('xyz')", binval(bins, "result"), False)

# ===================================================================
# STRING_READ split ops via expressions
# ===================================================================

code_split_comma = string_read_exp(STRING_OP_SPLIT, [","], return_type=EXP_TYPE_LIST)
code_split_empty = string_read_exp(STRING_OP_SPLIT, [], return_type=EXP_TYPE_LIST)
code_split_multisep = string_read_exp(STRING_OP_SPLIT, ["-=-"], return_type=EXP_TYPE_LIST)
# asclient pack_string_value can't encode non-Latin-1 chars in op params,
# so pass Unicode separators as pre-encoded UTF-8 bytes.
code_split_arrow = string_read_exp(STRING_OP_SPLIT, ["\u2192".encode("utf-8")], return_type=EXP_TYPE_LIST)

if BENCH_MODE:
    bench("exp split(',')",
          lambda: reset("a,b,c"),
          lambda: exp.read(key, "result", code_split_comma),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "a,b,c")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_split_comma))
    bench("exp split('') codepoint",
          lambda: reset("hello"),
          lambda: exp.read(key, "result", code_split_empty),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_split_empty))
else:
    # --- split: basic comma ---
    reset("a,b,c")
    step("exp split(','): basic comma split")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',')", rc, bins)
    check("exp split('a,b,c', ',')", listval(bins, "result"), ["a", "b", "c"])

    # --- split: empty separator (code points) ---
    reset("abc")
    step("exp split(''): empty separator splits by code point")
    rc, meta, bins = exp.read(key, "result", code_split_empty)
    show("exp split('')", rc, bins)
    check("exp split('abc', '')", listval(bins, "result"), ["a", "b", "c"])

    # --- split: multi-char separator ---
    reset("a-=-b-=-c")
    step("exp split('-=-'): multi-char separator")
    rc, meta, bins = exp.read(key, "result", code_split_multisep)
    show("exp split('-=-')", rc, bins)
    check("exp split('a-=-b-=-c', '-=-')", listval(bins, "result"), ["a", "b", "c"])

    # --- split: no match ---
    reset("hello world")
    step("exp split(','): no match returns whole string")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',') no match", rc, bins)
    check("exp split('hello world', ',')", listval(bins, "result"), ["hello world"])

    # --- split: empty string ---
    reset("")
    step("exp split(','): empty string")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',') empty", rc, bins)
    check("exp split('', ',')", listval(bins, "result"), [""])

    # --- split: leading delimiter ---
    reset(",a,b")
    step("exp split(','): leading delimiter")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',') leading", rc, bins)
    check("exp split(',a,b', ',')", listval(bins, "result"), ["", "a", "b"])

    # --- split: trailing delimiter ---
    reset("a,b,")
    step("exp split(','): trailing delimiter")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',') trailing", rc, bins)
    check("exp split('a,b,', ',')", listval(bins, "result"), ["a", "b", ""])

    # --- split: consecutive delimiters ---
    reset("a,,b")
    step("exp split(','): consecutive delimiters")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',') consecutive", rc, bins)
    check("exp split('a,,b', ',')", listval(bins, "result"), ["a", "", "b"])

    # --- split: all delimiters ---
    reset(",,,")
    step("exp split(','): all delimiters")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',') all delims", rc, bins)
    check("exp split(',,,', ',')", listval(bins, "result"), ["", "", "", ""])

    # --- split: unicode, empty sep, 2-byte ---
    reset("caf\u00e9")
    step("exp split(''): 2-byte UTF-8")
    rc, meta, bins = exp.read(key, "result", code_split_empty)
    show("exp split('') café", rc, bins)
    check("exp split('café', '')", listval(bins, "result"), ["c", "a", "f", "\u00e9"])

    # --- split: unicode, empty sep, 3-byte CJK ---
    reset("\u65e5\u672c\u8a9e")
    step("exp split(''): 3-byte UTF-8 (CJK)")
    rc, meta, bins = exp.read(key, "result", code_split_empty)
    show("exp split('') CJK", rc, bins)
    check("exp split('日本語', '')", listval(bins, "result"), ["\u65e5", "\u672c", "\u8a9e"])

    # --- split: ASCII sep with Unicode content ---
    reset("\u65e5,\u672c,\u8a9e")
    step("exp split(','): Unicode content")
    rc, meta, bins = exp.read(key, "result", code_split_comma)
    show("exp split(',') unicode", rc, bins)
    check("exp split('日,本,語', ',')", listval(bins, "result"), ["\u65e5", "\u672c", "\u8a9e"])

    # --- split: Unicode separator ---
    reset("a\u2192b\u2192c")
    step("exp split('→'): Unicode separator")
    rc, meta, bins = exp.read(key, "result", code_split_arrow)
    show("exp split('→')", rc, bins)
    check("exp split('a→b→c', '→')", listval(bins, "result"), ["a", "b", "c"])

# ===================================================================
# Additional STRING_READ ops via expressions
# ===================================================================

code_starts_with_hello = string_read_exp(STRING_OP_STARTS_WITH, ["hello"], return_type=EXP_TYPE_TRILEAN)
code_starts_with_world = string_read_exp(STRING_OP_STARTS_WITH, ["world"], return_type=EXP_TYPE_TRILEAN)
code_ends_with_world = string_read_exp(STRING_OP_ENDS_WITH, ["world"], return_type=EXP_TYPE_TRILEAN)
code_ends_with_hello = string_read_exp(STRING_OP_ENDS_WITH, ["hello"], return_type=EXP_TYPE_TRILEAN)
code_starts_with_empty = string_read_exp(STRING_OP_STARTS_WITH, [""], return_type=EXP_TYPE_TRILEAN)
code_starts_with_long = string_read_exp(STRING_OP_STARTS_WITH, ["hello world and more"], return_type=EXP_TYPE_TRILEAN)
code_ends_with_empty = string_read_exp(STRING_OP_ENDS_WITH, [""], return_type=EXP_TYPE_TRILEAN)
code_to_integer = string_read_exp(STRING_OP_TO_INTEGER, [], return_type=EXP_TYPE_INT)
code_to_double = string_read_exp(STRING_OP_TO_DOUBLE, [], return_type=EXP_TYPE_FLOAT)
code_byte_length = string_read_exp(STRING_OP_BYTE_LENGTH, [], return_type=EXP_TYPE_INT)
code_is_numeric = string_read_exp(STRING_OP_IS_NUMERIC, [], return_type=EXP_TYPE_TRILEAN)
code_is_numeric_int = string_read_exp(STRING_OP_IS_NUMERIC, [STRING_NUMERIC_INT], return_type=EXP_TYPE_TRILEAN)
code_is_numeric_float = string_read_exp(STRING_OP_IS_NUMERIC, [STRING_NUMERIC_FLOAT], return_type=EXP_TYPE_TRILEAN)
code_is_upper = string_read_exp(STRING_OP_IS_UPPER, [], return_type=EXP_TYPE_TRILEAN)
code_is_lower = string_read_exp(STRING_OP_IS_LOWER, [], return_type=EXP_TYPE_TRILEAN)
code_to_blob = string_read_exp(STRING_OP_TO_BLOB, [], return_type=EXP_TYPE_BLOB)
code_b64_decode = string_read_exp(STRING_OP_B64_DECODE, [], return_type=EXP_TYPE_BLOB)
code_regex_match = string_read_exp(STRING_OP_REGEX_COMPARE, ["hel+o"], return_type=EXP_TYPE_TRILEAN)
code_regex_nomatch = string_read_exp(STRING_OP_REGEX_COMPARE, ["^world"], return_type=EXP_TYPE_TRILEAN)
code_regex_casei = string_read_exp(STRING_OP_REGEX_COMPARE, ["hello", STRING_REGEX_CASE_INSENSITIVE], return_type=EXP_TYPE_TRILEAN)
code_regex_digits = string_read_exp(STRING_OP_REGEX_COMPARE, ["\\d+"], return_type=EXP_TYPE_TRILEAN)

if not BENCH_MODE:
    # --- starts_with ---
    reset("hello world")
    step("exp starts_with('hello'): match at start")
    rc, meta, bins = exp.read(key, "result", code_starts_with_hello)
    show("exp starts_with('hello')", rc, bins)
    check("exp starts_with('hello')", binval(bins, "result"), True)

    reset("hello world")
    step("exp starts_with('world'): no match at start")
    rc, meta, bins = exp.read(key, "result", code_starts_with_world)
    show("exp starts_with('world')", rc, bins)
    check("exp starts_with('world') false", binval(bins, "result"), False)

    reset("hello world")
    step("exp starts_with(''): empty needle always matches")
    rc, meta, bins = exp.read(key, "result", code_starts_with_empty)
    show("exp starts_with('')", rc, bins)
    check("exp starts_with('')", binval(bins, "result"), True)

    reset("hi")
    step("exp starts_with longer: needle longer than string")
    rc, meta, bins = exp.read(key, "result", code_starts_with_long)
    show("exp starts_with longer", rc, bins)
    check("exp starts_with longer", binval(bins, "result"), False)

    # --- ends_with ---
    reset("hello world")
    step("exp ends_with('world'): match at end")
    rc, meta, bins = exp.read(key, "result", code_ends_with_world)
    show("exp ends_with('world')", rc, bins)
    check("exp ends_with('world')", binval(bins, "result"), True)

    reset("hello world")
    step("exp ends_with('hello'): no match at end")
    rc, meta, bins = exp.read(key, "result", code_ends_with_hello)
    show("exp ends_with('hello')", rc, bins)
    check("exp ends_with('hello') false", binval(bins, "result"), False)

    reset("hello world")
    step("exp ends_with(''): empty needle always matches")
    rc, meta, bins = exp.read(key, "result", code_ends_with_empty)
    show("exp ends_with('')", rc, bins)
    check("exp ends_with('')", binval(bins, "result"), True)

    # --- to_integer ---
    reset("42")
    step("exp to_integer('42'): parse positive integer")
    rc, meta, bins = exp.read(key, "result", code_to_integer)
    show("exp to_integer('42')", rc, bins)
    check("exp to_integer('42')", binval(bins, "result"), 42)

    reset("-7")
    step("exp to_integer('-7'): parse negative integer")
    rc, meta, bins = exp.read(key, "result", code_to_integer)
    show("exp to_integer('-7')", rc, bins)
    check("exp to_integer('-7')", binval(bins, "result"), -7)

    reset("0")
    step("exp to_integer('0'): parse zero")
    rc, meta, bins = exp.read(key, "result", code_to_integer)
    show("exp to_integer('0')", rc, bins)
    check("exp to_integer('0')", binval(bins, "result"), 0)

    reset("abc")
    step("exp to_integer('abc'): non-numeric should error")
    rc, meta, bins = exp.read(key, "result", code_to_integer)
    show("exp to_integer('abc')", rc, bins)
    check("exp to_integer('abc') error rc!=0", rc != 0, True)

    # --- to_double ---
    reset("3.14")
    step("exp to_double('3.14'): parse float")
    rc, meta, bins = exp.read(key, "result", code_to_double)
    show("exp to_double('3.14')", rc, bins)
    check("exp to_double('3.14')", binval(bins, "result"), 3.14)

    reset("-1.5")
    step("exp to_double('-1.5'): parse negative float")
    rc, meta, bins = exp.read(key, "result", code_to_double)
    show("exp to_double('-1.5')", rc, bins)
    check("exp to_double('-1.5')", binval(bins, "result"), -1.5)

    reset("abc")
    step("exp to_double('abc'): non-numeric should error")
    rc, meta, bins = exp.read(key, "result", code_to_double)
    show("exp to_double('abc')", rc, bins)
    check("exp to_double('abc') error rc!=0", rc != 0, True)

    # --- byte_length ---
    reset("hello")
    step("exp byte_length('hello'): ASCII 5 bytes")
    rc, meta, bins = exp.read(key, "result", code_byte_length)
    show("exp byte_length('hello')", rc, bins)
    check("exp byte_length('hello')", binval(bins, "result"), 5)

    reset("caf\u00e9")
    step("exp byte_length('café'): 2-byte UTF-8 char")
    rc, meta, bins = exp.read(key, "result", code_byte_length)
    show("exp byte_length('café')", rc, bins)
    check("exp byte_length('café')", binval(bins, "result"), 5)

    reset("\u65e5\u672c\u8a9e")
    step("exp byte_length('日本語'): 3-byte UTF-8 chars")
    rc, meta, bins = exp.read(key, "result", code_byte_length)
    show("exp byte_length('日本語')", rc, bins)
    check("exp byte_length('日本語')", binval(bins, "result"), 9)

    reset("")
    step("exp byte_length(''): empty string")
    rc, meta, bins = exp.read(key, "result", code_byte_length)
    show("exp byte_length('')", rc, bins)
    check("exp byte_length('')", binval(bins, "result"), 0)

    # --- is_numeric ---
    reset("12345")
    step("exp is_numeric('12345'): integer string")
    rc, meta, bins = exp.read(key, "result", code_is_numeric)
    show("exp is_numeric('12345')", rc, bins)
    check("exp is_numeric('12345')", binval(bins, "result"), True)

    reset("3.14")
    step("exp is_numeric('3.14'): float is numeric (ANY)")
    rc, meta, bins = exp.read(key, "result", code_is_numeric)
    show("exp is_numeric('3.14')", rc, bins)
    check("exp is_numeric('3.14')", binval(bins, "result"), True)

    reset("abc")
    step("exp is_numeric('abc'): not numeric")
    rc, meta, bins = exp.read(key, "result", code_is_numeric)
    show("exp is_numeric('abc')", rc, bins)
    check("exp is_numeric('abc')", binval(bins, "result"), False)

    reset("12345")
    step("exp is_numeric('12345', INT): integer sub-flag")
    rc, meta, bins = exp.read(key, "result", code_is_numeric_int)
    show("exp is_numeric INT", rc, bins)
    check("exp is_numeric('12345', INT)", binval(bins, "result"), True)

    reset("3.14")
    step("exp is_numeric('3.14', INT): float fails int check")
    rc, meta, bins = exp.read(key, "result", code_is_numeric_int)
    show("exp is_numeric('3.14', INT)", rc, bins)
    check("exp is_numeric('3.14', INT) false", binval(bins, "result"), False)

    reset("3.14")
    step("exp is_numeric('3.14', FLOAT): float sub-flag")
    rc, meta, bins = exp.read(key, "result", code_is_numeric_float)
    show("exp is_numeric FLOAT", rc, bins)
    check("exp is_numeric('3.14', FLOAT)", binval(bins, "result"), True)

    # --- is_upper ---
    reset("HELLO")
    step("exp is_upper('HELLO'): all uppercase")
    rc, meta, bins = exp.read(key, "result", code_is_upper)
    show("exp is_upper('HELLO')", rc, bins)
    check("exp is_upper('HELLO')", binval(bins, "result"), True)

    reset("Hello")
    step("exp is_upper('Hello'): mixed case")
    rc, meta, bins = exp.read(key, "result", code_is_upper)
    show("exp is_upper('Hello')", rc, bins)
    check("exp is_upper('Hello') false", binval(bins, "result"), False)

    # --- is_lower ---
    reset("hello")
    step("exp is_lower('hello'): all lowercase")
    rc, meta, bins = exp.read(key, "result", code_is_lower)
    show("exp is_lower('hello')", rc, bins)
    check("exp is_lower('hello')", binval(bins, "result"), True)

    reset("Hello")
    step("exp is_lower('Hello'): mixed case")
    rc, meta, bins = exp.read(key, "result", code_is_lower)
    show("exp is_lower('Hello')", rc, bins)
    check("exp is_lower('Hello') false", binval(bins, "result"), False)

    # --- to_blob ---
    reset("hello")
    step("exp to_blob('hello'): convert string to blob")
    rc, meta, bins = exp.read(key, "result", code_to_blob)
    show("exp to_blob('hello')", rc, bins)
    check("exp to_blob('hello')", rawval(bins, "result"), b"hello")

    # --- b64_decode ---
    reset("aGVsbG8=")
    step("exp b64_decode('aGVsbG8='): decode -> 'hello'")
    rc, meta, bins = exp.read(key, "result", code_b64_decode)
    show("exp b64_decode", rc, bins)
    check("exp b64_decode('aGVsbG8=')", rawval(bins, "result"), b"hello")

    reset("SGVsbG8gV29ybGQh")
    step("exp b64_decode: longer -> 'Hello World!'")
    rc, meta, bins = exp.read(key, "result", code_b64_decode)
    show("exp b64_decode longer", rc, bins)
    check("exp b64_decode('SGVsbG8gV29ybGQh')", rawval(bins, "result"), b"Hello World!")

    # --- regex_compare ---
    reset("hello world")
    step("exp regex_compare('hel+o'): basic match")
    rc, meta, bins = exp.read(key, "result", code_regex_match)
    show("exp regex_compare match", rc, bins)
    check("exp regex_compare('hel+o') match", binval(bins, "result"), True)

    reset("hello world")
    step("exp regex_compare('^world'): no match")
    rc, meta, bins = exp.read(key, "result", code_regex_nomatch)
    show("exp regex_compare no match", rc, bins)
    check("exp regex_compare('^world') no match", binval(bins, "result"), False)

    reset("HELLO world")
    step("exp regex_compare('hello', CASE_INSENSITIVE)")
    rc, meta, bins = exp.read(key, "result", code_regex_casei)
    show("exp regex_compare case-i", rc, bins)
    check("exp regex_compare case-insensitive", binval(bins, "result"), True)

    reset("abc 123 def")
    step("exp regex_compare('\\d+'): digit pattern")
    rc, meta, bins = exp.read(key, "result", code_regex_digits)
    show("exp regex_compare digits", rc, bins)
    check("exp regex_compare('\\d+')", binval(bins, "result"), True)

# ===================================================================
# STRING_MODIFY ops via expressions
# ===================================================================

# Pre-build expression codes
code_insert = string_modify_exp(STRING_OP_INSERT, [5, " beautiful"])
code_overwrite = string_modify_exp(STRING_OP_OVERWRITE, [6, "EARTH"])
code_concat = string_modify_exp(STRING_OP_CONCAT, [[EXP_QUOTE, [" ", "world", "!"]]])
code_snip = string_modify_exp(STRING_OP_SNIP, [5, 10])
code_replace = string_modify_exp(STRING_OP_REPLACE, [[EXP_QUOTE, ["world", "there"]]])
code_replace_all = string_modify_exp(STRING_OP_REPLACE_ALL, [[EXP_QUOTE, ["abc", "XY"]]])
code_upper = string_modify_exp(STRING_OP_UPPER)
code_lower = string_modify_exp(STRING_OP_LOWER)
code_case_fold = string_modify_exp(STRING_OP_CASE_FOLD)
code_normalize = string_modify_exp(STRING_OP_NORMALIZE)
code_trim_start = string_modify_exp(STRING_OP_TRIM_START)
code_trim_end = string_modify_exp(STRING_OP_TRIM_END)
code_trim = string_modify_exp(STRING_OP_TRIM)
code_pad_start = string_modify_exp(STRING_OP_PAD_START, [6, "0"])
code_pad_end = string_modify_exp(STRING_OP_PAD_END, [6, "."])
code_repeat_3 = string_modify_exp(STRING_OP_REPEAT, [3])
code_repeat_0 = string_modify_exp(STRING_OP_REPEAT, [0])
code_regex_replace_global = string_modify_exp(STRING_OP_REGEX_REPLACE,
    [[EXP_QUOTE, ["world", "there"]], STRING_REGEX_GLOBAL])
code_regex_replace_multi = string_modify_exp(STRING_OP_REGEX_REPLACE,
    [[EXP_QUOTE, ["a", "bb"]], STRING_REGEX_GLOBAL])
code_regex_replace_delete = string_modify_exp(STRING_OP_REGEX_REPLACE,
    [[EXP_QUOTE, ["l", ""]], STRING_REGEX_GLOBAL])
code_regex_replace_first = string_modify_exp(STRING_OP_REGEX_REPLACE,
    [[EXP_QUOTE, ["o", "O"]]])

if BENCH_MODE:
    bench("exp insert",
          lambda: reset("hello world"),
          lambda: exp.write(key, "text", code_insert),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_insert))
    bench("exp overwrite",
          lambda: reset("hello world"),
          lambda: exp.write(key, "text", code_overwrite),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_overwrite))
    bench("exp concat",
          lambda: reset("hello"),
          lambda: exp.write(key, "text", code_concat),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_concat))
    bench("exp snip",
          lambda: reset("hello beautiful world"),
          lambda: exp.write(key, "text", code_snip),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello beautiful world")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_snip))
    bench("exp replace",
          lambda: reset("hello world"),
          lambda: exp.write(key, "text", code_replace),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_replace))
    bench("exp replace_all",
          lambda: reset("abcabcabc"),
          lambda: exp.write(key, "text", code_replace_all),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "abcabcabc")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_replace_all))
    bench("exp upper",
          lambda: reset("hello world"),
          lambda: exp.write(key, "text", code_upper),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hello world")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_upper))
    bench("exp lower",
          lambda: reset("HELLO WORLD"),
          lambda: exp.write(key, "text", code_lower),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "HELLO WORLD")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_lower))
    bench("exp case_fold",
          lambda: reset("Straße"),
          lambda: exp.write(key, "text", code_case_fold),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "Straße")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_case_fold))
    bench("exp normalize",
          lambda: reset("café"),
          lambda: exp.write(key, "text", code_normalize),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "café")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_normalize))
    bench("exp trim_start",
          lambda: reset("   hello   "),
          lambda: exp.write(key, "text", code_trim_start),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "   hello   ")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_trim_start))
    bench("exp trim_end",
          lambda: reset("   hello   "),
          lambda: exp.write(key, "text", code_trim_end),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "   hello   ")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_trim_end))
    bench("exp trim",
          lambda: reset("   hello   "),
          lambda: exp.write(key, "text", code_trim),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "   hello   ")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_trim))
    bench("exp pad_start",
          lambda: reset("42"),
          lambda: exp.write(key, "text", code_pad_start),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "42")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_pad_start))
    bench("exp pad_end",
          lambda: reset("hi"),
          lambda: exp.write(key, "text", code_pad_end),
          make_setup=lambda c, e, k: lambda: c.put(k, [("text", "hi")]),
          make_op=lambda c, e, k: lambda: e.write(k, "text", code_pad_end))
else:
    # --- insert ---
    reset("hello world")
    step("exp insert(5, ' beautiful'): insert at offset")
    rc, meta, bins = exp.write(key, "text", code_insert)
    show("exp insert", rc, bins)
    bins = get()
    check("exp insert(5, ' beautiful')", binval(bins, "text"), "hello beautiful world")

    # --- overwrite ---
    reset("hello world")
    step("exp overwrite(6, 'EARTH'): overwrite starting at offset 6")
    rc, meta, bins = exp.write(key, "text", code_overwrite)
    show("exp overwrite", rc, bins)
    bins = get()
    check("exp overwrite(6, 'EARTH')", binval(bins, "text"), "hello EARTH")

    # --- concat ---
    reset("hello")
    step("exp concat([' ', 'world', '!']): append multiple strings")
    rc, meta, bins = exp.write(key, "text", code_concat)
    show("exp concat", rc, bins)
    bins = get()
    check("exp concat(' ', 'world', '!')", binval(bins, "text"), "hello world!")

    # --- snip ---
    reset("hello beautiful world")
    step("exp snip(5, 10): remove 10 chars starting at offset 5")
    rc, meta, bins = exp.write(key, "text", code_snip)
    show("exp snip", rc, bins)
    bins = get()
    check("exp snip(5, 10)", binval(bins, "text"), "hellotiful world")

    # --- replace ---
    reset("hello world")
    step("exp replace('world', 'there'): replace first occurrence")
    rc, meta, bins = exp.write(key, "text", code_replace)
    show("exp replace", rc, bins)
    bins = get()
    check("exp replace('world', 'there')", binval(bins, "text"), "hello there")

    # --- replace_all ---
    reset("abcabcabc")
    step("exp replace_all('abc', 'XY'): replace all occurrences")
    rc, meta, bins = exp.write(key, "text", code_replace_all)
    show("exp replace_all", rc, bins)
    bins = get()
    check("exp replace_all('abc', 'XY')", binval(bins, "text"), "XYXYXY")

    # --- upper ---
    reset("hello world")
    step("exp upper: convert to uppercase")
    rc, meta, bins = exp.write(key, "text", code_upper)
    show("exp upper", rc, bins)
    bins = get()
    check("exp upper", binval(bins, "text"), "HELLO WORLD")

    # --- lower ---
    reset("HELLO WORLD")
    step("exp lower: convert to lowercase")
    rc, meta, bins = exp.write(key, "text", code_lower)
    show("exp lower", rc, bins)
    bins = get()
    check("exp lower", binval(bins, "text"), "hello world")

    # --- case_fold ---
    reset("Straße")
    step("exp case_fold: unicode case fold")
    rc, meta, bins = exp.write(key, "text", code_case_fold)
    show("exp case_fold", rc, bins)
    bins = get()
    check("exp case_fold('Straße')", binval(bins, "text"), "strasse")

    # --- normalize ---
    reset("café")
    step("exp normalize: NFC normalization")
    rc, meta, bins = exp.write(key, "text", code_normalize)
    show("exp normalize", rc, bins)
    bins = get()
    check("exp normalize('café')", binval(bins, "text"), "caf\u00e9")

    # --- trim_start ---
    reset("   hello   ")
    step("exp trim_start: remove leading whitespace")
    rc, meta, bins = exp.write(key, "text", code_trim_start)
    show("exp trim_start", rc, bins)
    bins = get()
    check("exp trim_start", binval(bins, "text"), "hello   ")

    # --- trim_end ---
    reset("   hello   ")
    step("exp trim_end: remove trailing whitespace")
    rc, meta, bins = exp.write(key, "text", code_trim_end)
    show("exp trim_end", rc, bins)
    bins = get()
    check("exp trim_end", binval(bins, "text"), "   hello")

    # --- trim ---
    reset("   hello   ")
    step("exp trim: remove leading+trailing whitespace")
    rc, meta, bins = exp.write(key, "text", code_trim)
    show("exp trim", rc, bins)
    bins = get()
    check("exp trim", binval(bins, "text"), "hello")

    # --- pad_start ---
    reset("42")
    step("exp pad_start(6, '0'): left-pad to length 6 with '0'")
    rc, meta, bins = exp.write(key, "text", code_pad_start)
    show("exp pad_start", rc, bins)
    bins = get()
    check("exp pad_start(6, '0')", binval(bins, "text"), "000042")

    # --- pad_end ---
    reset("hi")
    step("exp pad_end(6, '.'): right-pad to length 6 with '.'")
    rc, meta, bins = exp.write(key, "text", code_pad_end)
    show("exp pad_end", rc, bins)
    bins = get()
    check("exp pad_end(6, '.')", binval(bins, "text"), "hi....")

    # --- repeat ---
    reset("abc")
    step("exp repeat(3): repeat string 3 times")
    rc, meta, bins = exp.write(key, "text", code_repeat_3)
    show("exp repeat(3)", rc, bins)
    bins = get()
    check("exp repeat('abc', 3)", binval(bins, "text"), "abcabcabc")

    reset("abc")
    step("exp repeat(0): repeat zero times (empty)")
    rc, meta, bins = exp.write(key, "text", code_repeat_0)
    show("exp repeat(0)", rc, bins)
    bins = get()
    check("exp repeat('abc', 0)", binval(bins, "text"), "")

    # --- regex_replace ---
    reset("hello world")
    step("exp regex_replace('world', 'there', GLOBAL)")
    rc, meta, bins = exp.write(key, "text", code_regex_replace_global)
    show("exp regex_replace", rc, bins)
    bins = get()
    check("exp regex_replace basic", binval(bins, "text"), "hello there")

    reset("aaa")
    step("exp regex_replace('a', 'bb', GLOBAL): multiple matches")
    rc, meta, bins = exp.write(key, "text", code_regex_replace_multi)
    show("exp regex_replace multi", rc, bins)
    bins = get()
    check("exp regex_replace multiple", binval(bins, "text"), "bbbbbb")

    reset("hello")
    step("exp regex_replace('l', '', GLOBAL): deletion")
    rc, meta, bins = exp.write(key, "text", code_regex_replace_delete)
    show("exp regex_replace delete", rc, bins)
    bins = get()
    check("exp regex_replace deletion", binval(bins, "text"), "heo")

    reset("hello world")
    step("exp regex_replace('o', 'O'): first only (no GLOBAL)")
    rc, meta, bins = exp.write(key, "text", code_regex_replace_first)
    show("exp regex_replace first", rc, bins)
    bins = get()
    check("exp regex_replace first only", binval(bins, "text"), "hellO world")

# ===================================================================
# TO_STRING (CALL_REPR) via expressions
# ===================================================================

def to_string_exp(bin_name, bin_type):
    """Build a to_string expression (CALL_REPR) for a bin of any type."""
    func = get_function_expression(0, [])
    bin_exp = get_bin_expression(bin_type, bin_name)
    call = get_call_expression(EXP_TYPE_STR, EXP_SYS_CALL_REPR, func, bin_exp)
    return build_code(call)

code_int_to_string = to_string_exp("ival", EXP_TYPE_INT)
code_float_to_string = to_string_exp("fval", EXP_TYPE_FLOAT)
code_bool_to_string = to_string_exp("bval", EXP_TYPE_TRILEAN)
code_str_to_string = to_string_exp("sval", EXP_TYPE_STR)
code_blob_to_string = to_string_exp("blobval", EXP_TYPE_BLOB)

code_neg_int_to_string = to_string_exp("neg", EXP_TYPE_INT)
code_zero_to_string = to_string_exp("zero", EXP_TYPE_INT)
code_float_pi_to_string = to_string_exp("pi", EXP_TYPE_FLOAT)

if BENCH_MODE:
    bench("exp to_string(int)",
          lambda: client.put(key, [("ival", 42)]),
          lambda: exp.read(key, "result", code_int_to_string),
          make_setup=lambda c, e, k: lambda: c.put(k, [("ival", 42)]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_int_to_string))
    bench("exp to_string(float)",
          lambda: client.put(key, [("fval", 3.14)]),
          lambda: exp.read(key, "result", code_float_to_string),
          make_setup=lambda c, e, k: lambda: c.put(k, [("fval", 3.14)]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_float_to_string))
    bench("exp to_string(bool)",
          lambda: client.put(key, [("bval", True)]),
          lambda: exp.read(key, "result", code_bool_to_string),
          make_setup=lambda c, e, k: lambda: c.put(k, [("bval", True)]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_bool_to_string))
    bench("exp to_string(str)",
          lambda: client.put(key, [("sval", "hello")]),
          lambda: exp.read(key, "result", code_str_to_string),
          make_setup=lambda c, e, k: lambda: c.put(k, [("sval", "hello")]),
          make_op=lambda c, e, k: lambda: e.read(k, "result", code_str_to_string))
else:
    # --- integer to_string ---
    client.put(key, [("ival", 42)])
    step("exp to_string(int 42)")
    rc, meta, bins = exp.read(key, "result", code_int_to_string)
    show("to_string(42)", rc, bins)
    check("to_string(42)", binval(bins, "result"), "42")

    # --- negative integer ---
    client.put(key, [("neg", -100)])
    step("exp to_string(int -100)")
    rc, meta, bins = exp.read(key, "result", code_neg_int_to_string)
    show("to_string(-100)", rc, bins)
    check("to_string(-100)", binval(bins, "result"), "-100")

    # --- zero ---
    client.put(key, [("zero", 0)])
    step("exp to_string(int 0)")
    rc, meta, bins = exp.read(key, "result", code_zero_to_string)
    show("to_string(0)", rc, bins)
    check("to_string(0)", binval(bins, "result"), "0")

    # --- float to_string ---
    client.put(key, [("fval", 3.14)])
    step("exp to_string(float 3.14)")
    rc, meta, bins = exp.read(key, "result", code_float_to_string)
    show("to_string(3.14)", rc, bins)
    check("to_string(3.14)", binval(bins, "result"), "3.14")

    # --- float pi ---
    client.put(key, [("pi", 3.14159265358979)])
    step("exp to_string(float pi)")
    rc, meta, bins = exp.read(key, "result", code_float_pi_to_string)
    show("to_string(pi)", rc, bins)
    check("to_string(pi)", binval(bins, "result"), "3.14159")

    # --- bool true ---
    client.put(key, [("bval", True)])
    step("exp to_string(bool true)")
    rc, meta, bins = exp.read(key, "result", code_bool_to_string)
    show("to_string(true)", rc, bins)
    check("to_string(true)", binval(bins, "result"), "true")

    # --- bool false ---
    client.put(key, [("bval", False)])
    step("exp to_string(bool false)")
    rc, meta, bins = exp.read(key, "result", code_bool_to_string)
    show("to_string(false)", rc, bins)
    check("to_string(false)", binval(bins, "result"), "false")

    # --- string identity ---
    client.put(key, [("sval", "hello world")])
    step("exp to_string(string 'hello world')")
    rc, meta, bins = exp.read(key, "result", code_str_to_string)
    show("to_string('hello world')", rc, bins)
    check("to_string('hello world')", binval(bins, "result"), "hello world")

    # --- empty string ---
    client.put(key, [("sval", "")])
    step("exp to_string(string '')")
    rc, meta, bins = exp.read(key, "result", code_str_to_string)
    show("to_string('')", rc, bins)
    check("to_string('')", binval(bins, "result"), "")

    # --- blob to_string: valid ASCII ---
    client.put(key, [("blobval", bytearray(b"hello"))])
    step("exp to_string(blob b'hello')")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("to_string(blob b'hello')", rc, bins)
    check("to_string(blob b'hello')", binval(bins, "result"), "hello")

    # --- blob to_string: valid UTF-8 ---
    client.put(key, [("blobval", bytearray(b"caf\xc3\xa9"))])
    step("exp to_string(blob 'café')")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("to_string(blob café)", rc, bins)
    check("to_string(blob café)", binval(bins, "result"), "café")

    # --- blob to_string: empty ---
    client.put(key, [("blobval", bytearray(b""))])
    step("exp to_string(blob empty)")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("to_string(blob empty)", rc, bins)
    check("to_string(blob empty)", binval(bins, "result"), "")

    # --- blob to_string: invalid UTF-8 should fail ---
    client.put(key, [("blobval", bytearray(b"\xff\xfe"))])
    step("exp to_string(blob invalid utf8): expect error")
    rc, meta, bins = exp.read(key, "result", code_blob_to_string)
    show("to_string(blob invalid)", rc, bins)
    check("to_string(blob invalid) error", rc, 26)  # AS_ERR_OP_NOT_APPLICABLE

# ===================================================================
# Cleanup & Summary
# ===================================================================
client.delete(key)
client.close()

if BENCH_MODE:
    thread_info = f", {BENCH_THREADS} threads" if BENCH_THREADS > 1 else ""
    print(f"\n--- Bench Mode ({BENCH_N} iters/thread{thread_info}) ---")
    print(f"{'op':<24} {'total_ops':>10} {'wall':>9} {'ops/s':>10} {'avg/op':>10}")
    print("-" * 66)
    for label, n, elapsed, ops_sec, avg_ms in bench_results:
        print(f"{label:<24} {n:>10} {elapsed:>8.3f}s {ops_sec:>9.0f} {avg_ms:>8.3f}ms")
else:
    print(f"\nDone. passed={pass_count} failed={fail_count}")

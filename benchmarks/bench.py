#!/usr/bin/env python3
"""
Booklet performance benchmarks.

Run with:  uv run python benchmarks/bench.py
"""
import os
import sys
import time
import random
import string
import tempfile
import statistics
import json

# Ensure booklet is importable from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import booklet


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_key(length=12):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def random_value(length=200):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_dataset(n, key_len=12, val_len=200):
    """Pre-generate n unique key/value pairs (as strings)."""
    keys = set()
    while len(keys) < n:
        keys.add(random_key(key_len))
    keys = list(keys)
    values = [random_value(val_len) for _ in range(n)]
    return keys, values


class Timer:
    """Context manager that records elapsed wall-clock time."""
    def __init__(self):
        self.elapsed = 0.0
    def __enter__(self):
        self._start = time.perf_counter()
        return self
    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self._start


def ops_per_sec(n, elapsed):
    if elapsed == 0:
        return float('inf')
    return n / elapsed


def fmt(ops):
    if ops >= 1_000_000:
        return f"{ops/1_000_000:.2f}M"
    elif ops >= 1_000:
        return f"{ops/1_000:.1f}K"
    else:
        return f"{ops:.0f}"


# ---------------------------------------------------------------------------
# Benchmark functions
# ---------------------------------------------------------------------------

def bench_sequential_writes(n, key_serializer='str', value_serializer='str',
                            timestamps=False, buffer_size=2**22):
    """Write n key/value pairs sequentially."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        with Timer() as t:
            db = booklet.open(path, 'n', key_serializer=key_serializer,
                              value_serializer=value_serializer,
                              init_timestamps=timestamps,
                              buffer_size=buffer_size)
            for k, v in zip(keys, values):
                db[k] = v
            db.sync()
            db.close()

        file_size = os.path.getsize(path)
        return {
            'op': 'seq_write',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
            'file_mb': file_size / (1024 * 1024),
        }
    finally:
        os.unlink(path)


def bench_bulk_update(n, key_serializer='str', value_serializer='str',
                      timestamps=False):
    """Write n key/value pairs via update()."""
    keys, values = generate_dataset(n)
    data = dict(zip(keys, values))

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        with Timer() as t:
            db = booklet.open(path, 'n', key_serializer=key_serializer,
                              value_serializer=value_serializer,
                              init_timestamps=timestamps)
            db.update(data)
            db.sync()
            db.close()

        return {
            'op': 'bulk_update',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_random_reads(n, key_serializer='str', value_serializer='str',
                       timestamps=False):
    """Write n pairs, then read all n in random order."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()
        db.close()

        # Shuffle keys for random access
        shuffled = list(keys)
        random.shuffle(shuffled)

        db = booklet.open(path, 'r', key_serializer=key_serializer,
                          value_serializer=value_serializer)

        with Timer() as t:
            for k in shuffled:
                _ = db[k]

        db.close()

        return {
            'op': 'random_read',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_random_reads_miss(n, key_serializer='str', value_serializer='str',
                            timestamps=False):
    """Write n pairs, then try reading n keys that do NOT exist."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()
        db.close()

        # Generate keys guaranteed to miss (prefix with '!')
        miss_keys = ['!' + k for k in keys]
        random.shuffle(miss_keys)

        db = booklet.open(path, 'r', key_serializer=key_serializer,
                          value_serializer=value_serializer)

        with Timer() as t:
            for k in miss_keys:
                _ = db.get(k)

        db.close()

        return {
            'op': 'random_read_miss',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_contains(n, key_serializer='str', value_serializer='str',
                   timestamps=False):
    """Write n pairs, then check containment for all n (hit) + n (miss)."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()
        db.close()

        miss_keys = ['!' + k for k in keys]
        all_keys = keys + miss_keys
        random.shuffle(all_keys)

        db = booklet.open(path, 'r', key_serializer=key_serializer,
                          value_serializer=value_serializer)

        total = len(all_keys)
        with Timer() as t:
            for k in all_keys:
                _ = k in db

        db.close()

        return {
            'op': 'contains',
            'n': total,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(total, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_iter_keys(n, key_serializer='str', value_serializer='str',
                    timestamps=False):
    """Write n pairs, then iterate all keys."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()
        db.close()

        db = booklet.open(path, 'r', key_serializer=key_serializer,
                          value_serializer=value_serializer)

        with Timer() as t:
            for _ in db.keys():
                pass

        db.close()

        return {
            'op': 'iter_keys',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_iter_values(n, key_serializer='str', value_serializer='str',
                      timestamps=False):
    """Write n pairs, then iterate all values."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()
        db.close()

        db = booklet.open(path, 'r', key_serializer=key_serializer,
                          value_serializer=value_serializer)

        with Timer() as t:
            for _ in db.values():
                pass

        db.close()

        return {
            'op': 'iter_values',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_iter_items(n, key_serializer='str', value_serializer='str',
                     timestamps=False):
    """Write n pairs, then iterate all items."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()
        db.close()

        db = booklet.open(path, 'r', key_serializer=key_serializer,
                          value_serializer=value_serializer)

        with Timer() as t:
            for _ in db.items():
                pass

        db.close()

        return {
            'op': 'iter_items',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_overwrite(n, key_serializer='str', value_serializer='str',
                    timestamps=False):
    """Write n pairs, then overwrite all n with new values."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()

        new_values = [random_value(200) for _ in range(n)]

        with Timer() as t:
            for k, v in zip(keys, new_values):
                db[k] = v
            db.sync()

        db.close()

        return {
            'op': 'overwrite',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_delete(n, key_serializer='str', value_serializer='str',
                 timestamps=False):
    """Write n pairs, then delete all n."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()

        shuffled = list(keys)
        random.shuffle(shuffled)

        with Timer() as t:
            for k in shuffled:
                del db[k]

        db.close()

        return {
            'op': 'delete',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_mixed_read_write(n, read_ratio=0.8, key_serializer='str',
                           value_serializer='str', timestamps=False):
    """
    Pre-populate n/2 pairs, then do n ops: read_ratio reads + (1-read_ratio) writes.
    """
    pre_n = n // 2
    keys, values = generate_dataset(pre_n)
    extra_keys, extra_values = generate_dataset(n - pre_n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()

        # Build ops list
        ops = []
        write_idx = 0
        for _ in range(n):
            if random.random() < read_ratio:
                ops.append(('r', random.choice(keys)))
            else:
                if write_idx < len(extra_keys):
                    ops.append(('w', extra_keys[write_idx], extra_values[write_idx]))
                    write_idx += 1
                else:
                    ops.append(('r', random.choice(keys)))

        with Timer() as t:
            for op in ops:
                if op[0] == 'r':
                    _ = db.get(op[1])
                else:
                    db[op[1]] = op[2]
            db.sync()

        db.close()

        return {
            'op': f'mixed_rw_{int(read_ratio*100)}r',
            'n': n,
            'elapsed': t.elapsed,
            'ops_sec': ops_per_sec(n, t.elapsed),
        }
    finally:
        os.unlink(path)


def bench_prune(n, key_serializer='str', value_serializer='str',
                timestamps=False):
    """Write n pairs, overwrite half, delete a quarter, then prune."""
    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        db = booklet.open(path, 'n', key_serializer=key_serializer,
                          value_serializer=value_serializer,
                          init_timestamps=timestamps)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()

        # Overwrite first half
        for k in keys[:n // 2]:
            db[k] = random_value(200)
        db.sync()

        # Delete a quarter
        for k in keys[:n // 4]:
            del db[k]

        size_before = os.path.getsize(path)

        with Timer() as t:
            removed = db.prune()

        size_after = os.path.getsize(path)
        db.close()

        return {
            'op': 'prune',
            'n': n,
            'elapsed': t.elapsed,
            'removed': removed,
            'size_before_mb': size_before / (1024 * 1024),
            'size_after_mb': size_after / (1024 * 1024),
        }
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_suite(sizes=None, repeats=3):
    if sizes is None:
        sizes = [1_000, 10_000, 50_000]

    all_results = []

    benchmarks = [
        ('seq_write',       bench_sequential_writes),
        ('bulk_update',     bench_bulk_update),
        ('random_read',     bench_random_reads),
        ('random_read_miss', bench_random_reads_miss),
        ('contains',        bench_contains),
        ('iter_keys',       bench_iter_keys),
        ('iter_values',     bench_iter_values),
        ('iter_items',      bench_iter_items),
        ('overwrite',       bench_overwrite),
        ('delete',          bench_delete),
        ('mixed_rw_80r',    lambda n: bench_mixed_read_write(n, read_ratio=0.8)),
        ('prune',           bench_prune),
    ]

    for size in sizes:
        print(f"\n{'='*60}")
        print(f"  Dataset size: {size:,}")
        print(f"{'='*60}")
        print(f"  {'Operation':<22} {'ops/sec':>12}  {'time (s)':>10}  {'detail'}")
        print(f"  {'-'*22} {'-'*12}  {'-'*10}  {'-'*20}")

        for name, func in benchmarks:
            times = []
            last_result = None
            for _ in range(repeats):
                result = func(size)
                times.append(result['elapsed'])
                last_result = result

            # Use the median run
            median_time = statistics.median(times)
            median_ops = ops_per_sec(last_result['n'], median_time)

            detail = ''
            if 'file_mb' in last_result:
                detail = f"file={last_result['file_mb']:.1f}MB"
            elif 'removed' in last_result:
                detail = (f"removed={last_result['removed']}, "
                          f"{last_result['size_before_mb']:.1f}->"
                          f"{last_result['size_after_mb']:.1f}MB")

            print(f"  {name:<22} {fmt(median_ops):>12}  {median_time:>10.4f}  {detail}")

            all_results.append({
                'size': size,
                'op': name,
                'median_time': median_time,
                'median_ops_sec': median_ops,
                'times': times,
                **{k: v for k, v in last_result.items()
                   if k not in ('op', 'n', 'elapsed', 'ops_sec')},
            })

    return all_results


def save_results(results, path='benchmarks/results.json'):
    """Save results to JSON for later comparison."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {path}")


def compare_results(baseline_path, current_results):
    """Compare current results against a saved baseline."""
    if not os.path.exists(baseline_path):
        print(f"\nNo baseline found at {baseline_path} — skipping comparison.")
        return

    with open(baseline_path) as f:
        baseline = json.load(f)

    # Index baseline by (size, op)
    base_lookup = {}
    for r in baseline:
        base_lookup[(r['size'], r['op'])] = r

    print(f"\n{'='*60}")
    print("  Comparison vs baseline")
    print(f"{'='*60}")
    print(f"  {'Operation':<22} {'Size':>8} {'Base ops/s':>12} {'Now ops/s':>12} {'Change':>10}")
    print(f"  {'-'*22} {'-'*8} {'-'*12} {'-'*12} {'-'*10}")

    for r in current_results:
        key = (r['size'], r['op'])
        if key in base_lookup:
            base_ops = base_lookup[key]['median_ops_sec']
            now_ops = r['median_ops_sec']
            if base_ops > 0:
                pct = ((now_ops - base_ops) / base_ops) * 100
                sign = '+' if pct >= 0 else ''
                print(f"  {r['op']:<22} {r['size']:>8,} {fmt(base_ops):>12} {fmt(now_ops):>12} {sign}{pct:>8.1f}%")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Booklet benchmarks')
    parser.add_argument('--sizes', type=int, nargs='+', default=[1_000, 10_000, 50_000],
                        help='Dataset sizes to test')
    parser.add_argument('--repeats', type=int, default=3,
                        help='Number of repeats per benchmark (median used)')
    parser.add_argument('--save', action='store_true',
                        help='Save results to benchmarks/results.json')
    parser.add_argument('--save-baseline', action='store_true',
                        help='Save results as benchmarks/baseline.json')
    parser.add_argument('--compare', action='store_true',
                        help='Compare against benchmarks/baseline.json')
    args = parser.parse_args()

    print(f"Booklet benchmark suite")
    print(f"Sizes: {args.sizes}, Repeats: {args.repeats}")

    random.seed(42)  # Reproducible dataset generation

    results = run_suite(sizes=args.sizes, repeats=args.repeats)

    if args.save_baseline:
        save_results(results, 'benchmarks/baseline.json')
    elif args.save:
        save_results(results, 'benchmarks/results.json')

    if args.compare:
        compare_results('benchmarks/baseline.json', results)

    print("\nDone.")

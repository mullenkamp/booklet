#!/usr/bin/env python3
"""
mmap investigation for booklet.

Compares current file I/O approach vs mmap for:
  - Random key lookups
  - Sequential iteration
  - Memory (RSS) consumption

Run with:  uv run python benchmarks/mmap_investigation.py
"""
import os
import sys
import time
import random
import string
import tempfile
import mmap

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import booklet
from booklet import utils


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PAGE_SIZE = os.sysconf('SC_PAGE_SIZE') if hasattr(os, 'sysconf') else 4096


def get_rss_kb():
    """Get current RSS in KB (Linux only)."""
    try:
        with open('/proc/self/status') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1])
    except FileNotFoundError:
        pass
    return 0


def random_key(length=12):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def random_value(length=200):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_dataset(n, key_len=12, val_len=200):
    keys = set()
    while len(keys) < n:
        keys.add(random_key(key_len))
    keys = list(keys)
    values = [random_value(val_len) for _ in range(n)]
    return keys, values


class Timer:
    def __init__(self):
        self.elapsed = 0.0
    def __enter__(self):
        self._start = time.perf_counter()
        return self
    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self._start


def fmt_ops(n, elapsed):
    ops = n / elapsed if elapsed > 0 else float('inf')
    if ops >= 1_000_000:
        return f"{ops/1_000_000:.2f}M"
    elif ops >= 1_000:
        return f"{ops/1_000:.1f}K"
    return f"{ops:.0f}"


# ---------------------------------------------------------------------------
# mmap-based read functions (mirroring booklet's utils.py logic)
# ---------------------------------------------------------------------------

def mmap_get_value(mm, key_hash, n_buckets, ts_bytes_len=0, index_offset=200):
    """
    Combined chain traversal + value read using mmap slicing.
    Equivalent to utils.get_value() but uses memory access instead of file I/O.
    """
    key_hash_len = 13
    n_bytes_file = 6
    n_bytes_key = 2
    n_bytes_value = 4
    one_extra = key_hash_len + n_bytes_file  # 19
    header_len = one_extra + n_bytes_key + n_bytes_value  # 25

    # Read bucket pointer
    bucket = int.from_bytes(key_hash, 'little') % n_buckets
    bucket_pos = index_offset + bucket * n_bytes_file
    data_block_pos = int.from_bytes(mm[bucket_pos:bucket_pos + n_bytes_file], 'little')

    if data_block_pos <= 1:
        return False

    while True:
        header = mm[data_block_pos:data_block_pos + header_len]
        next_ptr = int.from_bytes(header[key_hash_len:one_extra], 'little')
        if next_ptr:
            if header[:key_hash_len] == key_hash:
                key_len = int.from_bytes(header[one_extra:one_extra + n_bytes_key], 'little')
                value_len = int.from_bytes(header[one_extra + n_bytes_key:], 'little')
                val_start = data_block_pos + header_len + ts_bytes_len + key_len
                return bytes(mm[val_start:val_start + value_len])
            elif next_ptr == 1:
                return False
        else:
            return False
        data_block_pos = next_ptr

    return False


def mmap_contains(mm, key_hash, n_buckets, index_offset=200):
    """
    Check key existence using mmap.
    """
    key_hash_len = 13
    n_bytes_file = 6
    one_extra = key_hash_len + n_bytes_file  # 19

    bucket = int.from_bytes(key_hash, 'little') % n_buckets
    bucket_pos = index_offset + bucket * n_bytes_file
    data_block_pos = int.from_bytes(mm[bucket_pos:bucket_pos + n_bytes_file], 'little')

    if data_block_pos <= 1:
        return False

    while True:
        data_index = mm[data_block_pos:data_block_pos + one_extra]
        next_ptr = int.from_bytes(data_index[key_hash_len:], 'little')
        if next_ptr:
            if data_index[:key_hash_len] == key_hash:
                return True
            elif next_ptr == 1:
                return False
        else:
            return False
        data_block_pos = next_ptr

    return False


def mmap_iter_keys(mm, n_buckets, ts_bytes_len, index_offset=200, first_data_block_pos=0):
    """
    Iterate keys using mmap.
    """
    key_hash_len = 13
    n_bytes_file = 6
    n_bytes_key = 2
    n_bytes_value = 4
    one_extra = key_hash_len + n_bytes_file
    init_block_len = one_extra + n_bytes_key + n_bytes_value

    if first_data_block_pos == 0:
        first_data_block_pos = 200 + n_buckets * n_bytes_file

    file_end = len(mm)
    pos = first_data_block_pos

    while pos < file_end:
        init = mm[pos:pos + init_block_len]
        next_ptr = int.from_bytes(init[key_hash_len:one_extra], 'little')
        key_len = int.from_bytes(init[one_extra:one_extra + n_bytes_key], 'little')
        value_len = int.from_bytes(init[one_extra + n_bytes_key:], 'little')
        block_len = init_block_len + ts_bytes_len + key_len + value_len

        if next_ptr:  # Not deleted
            key_start = pos + init_block_len + ts_bytes_len
            key = bytes(mm[key_start:key_start + key_len])
            if key != utils.metadata_key_bytes:
                yield key

        pos += block_len


def mmap_iter_items(mm, n_buckets, ts_bytes_len, index_offset=200, first_data_block_pos=0):
    """
    Iterate key-value pairs using mmap.
    """
    key_hash_len = 13
    n_bytes_file = 6
    n_bytes_key = 2
    n_bytes_value = 4
    one_extra = key_hash_len + n_bytes_file
    init_block_len = one_extra + n_bytes_key + n_bytes_value

    if first_data_block_pos == 0:
        first_data_block_pos = 200 + n_buckets * n_bytes_file

    file_end = len(mm)
    pos = first_data_block_pos

    while pos < file_end:
        init = mm[pos:pos + init_block_len]
        next_ptr = int.from_bytes(init[key_hash_len:one_extra], 'little')
        key_len = int.from_bytes(init[one_extra:one_extra + n_bytes_key], 'little')
        value_len = int.from_bytes(init[one_extra + n_bytes_key:], 'little')
        block_len = init_block_len + ts_bytes_len + key_len + value_len

        if next_ptr:
            key_start = pos + init_block_len + ts_bytes_len
            key = bytes(mm[key_start:key_start + key_len])
            if key != utils.metadata_key_bytes:
                value = bytes(mm[key_start + key_len:key_start + key_len + value_len])
                yield key, value

        pos += block_len


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

def run_investigation(n=50_000, repeats=3):
    print(f"mmap investigation: {n:,} entries, {repeats} repeats\n")

    keys, values = generate_dataset(n)

    with tempfile.NamedTemporaryFile(suffix='.blt', delete=False) as f:
        path = f.name

    try:
        # Create test file
        db = booklet.open(path, 'n', key_serializer='str',
                          value_serializer='str', init_timestamps=True)
        for k, v in zip(keys, values):
            db[k] = v
        db.sync()
        n_buckets = db._n_buckets
        ts_bytes_len = db._ts_bytes_len
        index_offset = db._index_offset
        first_data_block_pos = db._first_data_block_pos
        db.close()

        file_size = os.path.getsize(path)
        print(f"File size: {file_size / (1024*1024):.1f} MB")
        print(f"n_buckets: {n_buckets:,}, index_offset: {index_offset}")
        print()

        # Pre-compute key hashes and shuffled keys
        key_hashes = [(k, utils.hash_key(k.encode())) for k in keys]
        shuffled = list(key_hashes)
        random.shuffle(shuffled)
        miss_hashes = [(k, utils.hash_key(('!' + k).encode())) for k in keys]
        random.shuffle(miss_hashes)

        # ---------------------------------------------------------------
        # 1. Random reads: file I/O vs mmap
        # ---------------------------------------------------------------
        print("=" * 65)
        print("  RANDOM READS (hits)")
        print("=" * 65)

        # File I/O (current booklet approach - read only, buffered)
        db = booklet.open(path, 'r', key_serializer='str',
                          value_serializer='str')
        times_fio = []
        for _ in range(repeats):
            with Timer() as t:
                for k, kh in shuffled:
                    utils.get_value(db._file, kh, n_buckets, ts_bytes_len, index_offset)
            times_fio.append(t.elapsed)
        db.close()
        median_fio = sorted(times_fio)[len(times_fio) // 2]

        # mmap (no madvise)
        f = open(path, 'rb')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        rss_before = get_rss_kb()
        times_mm = []
        for _ in range(repeats):
            with Timer() as t:
                for k, kh in shuffled:
                    mmap_get_value(mm, kh, n_buckets, ts_bytes_len, index_offset)
            times_mm.append(t.elapsed)
        rss_after = get_rss_kb()
        mm.close()
        f.close()
        median_mm = sorted(times_mm)[len(times_mm) // 2]

        # mmap + MADV_RANDOM
        f = open(path, 'rb')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        if hasattr(mm, 'madvise') and hasattr(mmap, 'MADV_RANDOM'):
            mm.madvise(mmap.MADV_RANDOM)
        rss_before_r = get_rss_kb()
        times_mm_r = []
        for _ in range(repeats):
            with Timer() as t:
                for k, kh in shuffled:
                    mmap_get_value(mm, kh, n_buckets, ts_bytes_len, index_offset)
            times_mm_r.append(t.elapsed)
        rss_after_r = get_rss_kb()
        mm.close()
        f.close()
        median_mm_r = sorted(times_mm_r)[len(times_mm_r) // 2]

        print(f"  {'Method':<30} {'ops/sec':>12} {'time':>10} {'RSS delta':>10}")
        print(f"  {'-'*30} {'-'*12} {'-'*10} {'-'*10}")
        print(f"  {'file I/O (buffered)':<30} {fmt_ops(n, median_fio):>12} {median_fio:>10.4f}")
        print(f"  {'mmap (default)':<30} {fmt_ops(n, median_mm):>12} {median_mm:>10.4f} {rss_after - rss_before:>+8} KB")
        print(f"  {'mmap + MADV_RANDOM':<30} {fmt_ops(n, median_mm_r):>12} {median_mm_r:>10.4f} {rss_after_r - rss_before_r:>+8} KB")

        # ---------------------------------------------------------------
        # 2. Contains (miss): file I/O vs mmap
        # ---------------------------------------------------------------
        print()
        print("=" * 65)
        print("  CONTAINS (misses)")
        print("=" * 65)

        db = booklet.open(path, 'r', key_serializer='str',
                          value_serializer='str')
        times_fio = []
        for _ in range(repeats):
            with Timer() as t:
                for k, kh in miss_hashes:
                    utils.contains_key(db._file, kh, n_buckets, index_offset)
            times_fio.append(t.elapsed)
        db.close()
        median_fio = sorted(times_fio)[len(times_fio) // 2]

        f = open(path, 'rb')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        if hasattr(mm, 'madvise') and hasattr(mmap, 'MADV_RANDOM'):
            mm.madvise(mmap.MADV_RANDOM)
        times_mm = []
        for _ in range(repeats):
            with Timer() as t:
                for k, kh in miss_hashes:
                    mmap_contains(mm, kh, n_buckets, index_offset)
            times_mm.append(t.elapsed)
        mm.close()
        f.close()
        median_mm = sorted(times_mm)[len(times_mm) // 2]

        print(f"  {'Method':<30} {'ops/sec':>12} {'time':>10}")
        print(f"  {'-'*30} {'-'*12} {'-'*10}")
        print(f"  {'file I/O (buffered)':<30} {fmt_ops(n, median_fio):>12} {median_fio:>10.4f}")
        print(f"  {'mmap + MADV_RANDOM':<30} {fmt_ops(n, median_mm):>12} {median_mm:>10.4f}")

        # ---------------------------------------------------------------
        # 3. Iteration: file I/O vs mmap
        # ---------------------------------------------------------------
        print()
        print("=" * 65)
        print("  ITERATION (keys)")
        print("=" * 65)

        db = booklet.open(path, 'r', key_serializer='str',
                          value_serializer='str')
        times_fio = []
        for _ in range(repeats):
            with Timer() as t:
                for key in utils.iter_keys_values(db._file, n_buckets, True, False, False, ts_bytes_len, index_offset, first_data_block_pos):
                    pass
            times_fio.append(t.elapsed)
        db.close()
        median_fio = sorted(times_fio)[len(times_fio) // 2]

        # mmap default
        f = open(path, 'rb')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        rss_before = get_rss_kb()
        times_mm = []
        for _ in range(repeats):
            with Timer() as t:
                for key in mmap_iter_keys(mm, n_buckets, ts_bytes_len, index_offset, first_data_block_pos):
                    pass
            times_mm.append(t.elapsed)
        rss_after = get_rss_kb()
        mm.close()
        f.close()
        median_mm = sorted(times_mm)[len(times_mm) // 2]

        # mmap + MADV_SEQUENTIAL
        f = open(path, 'rb')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        if hasattr(mm, 'madvise') and hasattr(mmap, 'MADV_SEQUENTIAL'):
            mm.madvise(mmap.MADV_SEQUENTIAL)
        rss_before_s = get_rss_kb()
        times_mm_s = []
        for _ in range(repeats):
            with Timer() as t:
                for key in mmap_iter_keys(mm, n_buckets, ts_bytes_len, index_offset, first_data_block_pos):
                    pass
            times_mm_s.append(t.elapsed)
        rss_after_s = get_rss_kb()

        # Test MADV_DONTNEED to release pages after iteration
        rss_before_dn = get_rss_kb()
        if hasattr(mm, 'madvise') and hasattr(mmap, 'MADV_DONTNEED'):
            mm.madvise(mmap.MADV_DONTNEED)
        rss_after_dn = get_rss_kb()
        mm.close()
        f.close()

        median_mm_s = sorted(times_mm_s)[len(times_mm_s) // 2]

        print(f"  {'Method':<30} {'ops/sec':>12} {'time':>10} {'RSS delta':>10}")
        print(f"  {'-'*30} {'-'*12} {'-'*10} {'-'*10}")
        print(f"  {'file I/O (buffered)':<30} {fmt_ops(n, median_fio):>12} {median_fio:>10.4f}")
        print(f"  {'mmap (default)':<30} {fmt_ops(n, median_mm):>12} {median_mm:>10.4f} {rss_after - rss_before:>+8} KB")
        print(f"  {'mmap + MADV_SEQUENTIAL':<30} {fmt_ops(n, median_mm_s):>12} {median_mm_s:>10.4f} {rss_after_s - rss_before_s:>+8} KB")
        if rss_before_dn > 0:
            print(f"  {'  after MADV_DONTNEED':<30} {'':>12} {'':>10} {rss_after_dn - rss_before_dn:>+8} KB")

        # ---------------------------------------------------------------
        # 4. Iteration (items): file I/O vs mmap
        # ---------------------------------------------------------------
        print()
        print("=" * 65)
        print("  ITERATION (items)")
        print("=" * 65)

        db = booklet.open(path, 'r', key_serializer='str',
                          value_serializer='str')
        times_fio = []
        for _ in range(repeats):
            with Timer() as t:
                for key, value in utils.iter_keys_values(db._file, n_buckets, True, True, False, ts_bytes_len, index_offset, first_data_block_pos):
                    pass
            times_fio.append(t.elapsed)
        db.close()
        median_fio = sorted(times_fio)[len(times_fio) // 2]

        f = open(path, 'rb')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        if hasattr(mm, 'madvise') and hasattr(mmap, 'MADV_SEQUENTIAL'):
            mm.madvise(mmap.MADV_SEQUENTIAL)
        times_mm = []
        for _ in range(repeats):
            with Timer() as t:
                for key, value in mmap_iter_items(mm, n_buckets, ts_bytes_len, index_offset, first_data_block_pos):
                    pass
            times_mm.append(t.elapsed)
        mm.close()
        f.close()
        median_mm = sorted(times_mm)[len(times_mm) // 2]

        print(f"  {'Method':<30} {'ops/sec':>12} {'time':>10}")
        print(f"  {'-'*30} {'-'*12} {'-'*10}")
        print(f"  {'file I/O (buffered)':<30} {fmt_ops(n, median_fio):>12} {median_fio:>10.4f}")
        print(f"  {'mmap + MADV_SEQUENTIAL':<30} {fmt_ops(n, median_mm):>12} {median_mm:>10.4f}")

        # ---------------------------------------------------------------
        # 5. Memory: large file RSS tracking
        # ---------------------------------------------------------------
        print()
        print("=" * 65)
        print("  MEMORY: RSS tracking during random reads")
        print("=" * 65)

        f = open(path, 'rb')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        if hasattr(mm, 'madvise') and hasattr(mmap, 'MADV_RANDOM'):
            mm.madvise(mmap.MADV_RANDOM)

        rss_start = get_rss_kb()
        # Access 10% of keys
        subset = shuffled[:n // 10]
        for k, kh in subset:
            mmap_get_value(mm, kh, n_buckets, ts_bytes_len, index_offset)
        rss_10pct = get_rss_kb()

        # Access 50% of keys
        subset = shuffled[:n // 2]
        for k, kh in subset:
            mmap_get_value(mm, kh, n_buckets, ts_bytes_len, index_offset)
        rss_50pct = get_rss_kb()

        # Access all keys
        for k, kh in shuffled:
            mmap_get_value(mm, kh, n_buckets, ts_bytes_len, index_offset)
        rss_100pct = get_rss_kb()

        # Release with MADV_DONTNEED
        if hasattr(mm, 'madvise') and hasattr(mmap, 'MADV_DONTNEED'):
            mm.madvise(mmap.MADV_DONTNEED)
        rss_released = get_rss_kb()

        mm.close()
        f.close()

        print(f"  File size:           {file_size / 1024:.0f} KB")
        print(f"  RSS before mmap:     {rss_start} KB")
        print(f"  RSS after 10% reads: {rss_10pct} KB  (delta: {rss_10pct - rss_start:+d} KB)")
        print(f"  RSS after 50% reads: {rss_50pct} KB  (delta: {rss_50pct - rss_start:+d} KB)")
        print(f"  RSS after 100% reads:{rss_100pct} KB  (delta: {rss_100pct - rss_start:+d} KB)")
        print(f"  RSS after DONTNEED:  {rss_released} KB  (delta: {rss_released - rss_start:+d} KB)")

    finally:
        os.unlink(path)

    print("\nDone.")


if __name__ == '__main__':
    random.seed(42)
    run_investigation(n=50_000, repeats=3)

"""FixedLengthValue (the ebooklet sidecar class): writes with one reindex, random reads (mmap and file), iteration.
Usage: python benchmarks/bench_fixed.py <dir containing the booklet package to test>   (A/B: run it once per tree,
alternating; see the global CLAUDE.md rule "Benchmark performance-critical packages, as a release step")."""
import os, sys, time, random, tempfile, statistics, json
sys.path.insert(0, sys.argv[1])
import booklet
random.seed(7)
N = 140_000  # one reindex (12,007 -> 144,013) in both versions; 0.12.10 overflows past 144,013
keys = [f'chunk!{i}.{random.randrange(1000)}' for i in range(N)]
val = b'x' * 19
def one():
    d = tempfile.mkdtemp(); p = os.path.join(d, 'f.blt'); r = {}
    t = time.perf_counter()
    with booklet.FixedLengthValue(p, 'n', key_serializer='str', value_len=19, n_buckets=12007) as db:
        for k in keys: db[k] = val
    r['write_140k_with_reindex'] = time.perf_counter() - t           # includes the 12,007 -> 144,013 reindex (index at 200)
    sample = random.sample(keys, 50_000)
    with booklet.FixedLengthValue(p, 'r') as db:
        t = time.perf_counter()
        for k in sample: db[k]
        r['get_50k_mmap'] = time.perf_counter() - t
        t = time.perf_counter()
        for k in sample: k in db
        r['contains_50k'] = time.perf_counter() - t
        t = time.perf_counter(); n = sum(1 for _ in db.keys()); r['iter_keys_140k'] = time.perf_counter() - t
    with booklet.FixedLengthValue(p, 'w') as db:
        t = time.perf_counter()
        for k in sample: db[k]
        r['get_50k_file'] = time.perf_counter() - t
    os.unlink(p); return r
runs = [one() for _ in range(5)]
print(json.dumps({k: statistics.median(r[k] for r in runs) for k in runs[0]}))

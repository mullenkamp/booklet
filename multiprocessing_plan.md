# Implementation Plan: Multiprocessing `.map()` Method for Booklet

## Context

Booklet currently supports thread-safe and process-safe access via `threading.Lock` and `portalocker`, but has no high-level API for parallelizing computation over stored data. The goal is to add a `.map()` method that reads items from a booklet, distributes computation across worker processes, and writes results back to the **same file or a separate output file** — all while streaming (not requiring all reads to finish before writes begin).

The key constraint: the user's processing function is much slower than Booklet I/O, so lock contention between the reader and writer will be minimal.

## Architecture

```
Main Thread (reader)              Worker Pool (N processes)        Writer Thread
────────────────────              ──────────────────────────       ────────────────
Read one (key, value)  ──feed──>  func(key, value) ──results──>   Write result to
  from self (input db)            Pure computation (slow)           output db
  release lock                    returns (new_key, new_value)      acquire lock
Read next item                                                     release lock
  ...                                                              ...
```

- **Main thread**: iterates items with yield-between-locks (lock held only per single block read)
- **Worker pool**: `multiprocessing.Pool.imap_unordered` — workers receive deserialized Python objects via IPC, return results via IPC
- **Writer thread**: drains a thread-safe `queue.Queue` and calls `output_db[new_key] = new_value`

### Data Flow

1. Main thread reads one `(key, value)` from the input file (acquires `_thread_lock` briefly for one block read, then releases)
2. Item is sent to `pool.imap_unordered()` which pickles it and sends to a worker process
3. Worker calls `func(key, value)` — this is the slow, CPU-intensive step
4. Worker returns `(new_key, new_value)` back to main thread via IPC (key may differ from input key)
5. Main thread puts `(new_key, new_value)` on the writer's `queue.Queue`
6. Writer thread picks it up, writes `output_db[new_key] = new_value`
7. Steps 1-6 happen concurrently — reader and writer interleave lock acquisition

### Two modes of operation

**Same-file mode** (`write_db=None`, default): Reader and writer both operate on `self`. The writer thread acquires `self._thread_lock` to write, interleaving with the reader's per-block lock acquisitions. Auto-reindex is deferred until map completes.

**Separate-file mode** (`write_db=<another booklet>`): Reader operates on `self`, writer operates on `write_db`. These have independent `_thread_lock` instances on independent file handles — zero contention. The input booklet can even be open read-only.

---

## Function Signature

```python
def map(self, func, keys=None, write_db=None, n_workers=None):
```

### `func(key, value) → (new_key, new_value)` or `None`

- Receives the original key and value as two arguments
- Returns a `(new_key, new_value)` tuple — the key to write under and the value to write
  - `new_key` can be the same as the input key (write back to same key) or different
  - Return `None` to skip (no write for this item)
- Must be picklable (top-level function or `functools.partial`, not a lambda or closure)

### `keys` (optional)

- If `None`: iterates ALL keys/values via `_iter_items_unlocked()` (sequential scan)
- If provided: an iterable of keys to process. Each key is looked up via `self.get()` individually.

### `write_db` (optional)

- If `None`: writes back to `self` (same-file mode). `self` must be open for writing.
- If provided: an already-open writable Booklet instance. `self` can be read-only. The user controls the output file's serializers, n_buckets, etc.

### `n_workers` (optional)

- Number of worker processes. Defaults to `os.cpu_count()`.

### Returns

- `dict` with `{'processed': int, 'written': int, 'errors': int}`

---

## Files to Create/Modify

### 1. New file: `booklet/parallel.py`

Module-level functions required for `multiprocessing` pickling:

#### `_SENTINEL`
```python
_SENTINEL = object()
```
Poison pill value to signal the writer thread to shut down.

#### `_map_worker(args)`
```python
def _map_worker(args):
    """Worker function for multiprocessing.Pool."""
    func, key, value = args
    result = func(key, value)
    if result is not None:
        return result  # (new_key, new_value) tuple
    return None
```
Unpacks `(func, key, value)`, calls the user's function, returns `(new_key, new_value)` or `None`. Must be at module level to be picklable.

#### `_writer_thread_func(db, result_queue, stats, done_event)`
```python
import queue

def _writer_thread_func(db, result_queue, stats, done_event):
    """Writer thread: drains result_queue and writes to the output booklet."""
    while True:
        try:
            item = result_queue.get(timeout=0.1)
        except queue.Empty:
            if done_event.is_set():
                # Drain any remaining items after pool is done
                while not result_queue.empty():
                    try:
                        item = result_queue.get_nowait()
                        if item is _SENTINEL:
                            return
                        key, value = item
                        try:
                            db[key] = value
                            stats['written'] += 1
                        except Exception:
                            stats['errors'] += 1
                    except queue.Empty:
                        break
                return
            continue

        if item is _SENTINEL:
            return

        key, value = item
        try:
            db[key] = value
            stats['written'] += 1
        except Exception:
            stats['errors'] += 1
```
Runs as a daemon thread in the main process. `db` is whatever booklet results should be written to — either `self` (same-file mode) or the user-provided `write_db` (separate-file mode). Per-item error handling prevents one bad write from killing the entire map operation.

---

### 2. `booklet/main.py`

#### a) `Booklet` base class — add `_iter_items_unlocked()` method

Generator that reads `(key, value)` pairs one at a time, **releasing `_thread_lock` between each item**. This allows the writer thread to interleave writes between reads.

```python
def _iter_items_unlocked(self):
    """
    Yield (key, value) pairs, acquiring/releasing _thread_lock per block.
    Used internally by map() to allow interleaved reads and writes.
    """
    # Sync any pending writes first
    if self._buffer_index_set:
        self.sync()

    # Capture layout parameters under the lock (one-time snapshot)
    with self._thread_lock:
        file_end = self._file.seek(0, 2)
        n_buckets = self._n_buckets
        index_offset = self._index_offset
        first_data_block_pos = self._first_data_block_pos
        ts_bytes_len = self._ts_bytes_len

    # Compute first_data_block_pos if legacy (0 = not set)
    if first_data_block_pos == 0:
        first_data_block_pos = utils.sub_index_init_pos + (n_buckets * utils.n_bytes_file)

    # Determine scan regions (same logic as utils.iter_keys_values)
    if index_offset != utils.sub_index_init_pos:
        # Relocated index: two data regions
        regions = [
            (first_data_block_pos, index_offset),
            (index_offset + n_buckets * utils.n_bytes_file, file_end),
        ]
    else:
        # Standard layout: one region
        regions = [(first_data_block_pos, file_end)]

    # Precompute header sizes
    one_extra_index_bytes_len = utils.key_hash_len + utils.n_bytes_file
    init_data_block_len = one_extra_index_bytes_len + utils.n_bytes_key + utils.n_bytes_value

    for start, end in regions:
        pos = start
        while pos < end:
            # Acquire lock → read one block → release lock
            with self._thread_lock:
                self._file.seek(pos)
                header = self._file.read(init_data_block_len)
                next_ptr = utils.bytes_to_int(
                    header[utils.key_hash_len:one_extra_index_bytes_len]
                )
                key_len = utils.bytes_to_int(
                    header[one_extra_index_bytes_len:one_extra_index_bytes_len + utils.n_bytes_key]
                )
                value_len = utils.bytes_to_int(
                    header[one_extra_index_bytes_len + utils.n_bytes_key:]
                )
                ts_key_value_len = ts_bytes_len + key_len + value_len

                if next_ptr:  # Live block (not tombstoned)
                    payload = self._file.read(ts_key_value_len)
                    key_bytes = payload[ts_bytes_len:ts_bytes_len + key_len]
                    value_bytes = payload[ts_bytes_len + key_len:]
                else:
                    key_bytes = None
                    value_bytes = None

            # Advance position (same for live and deleted blocks)
            pos += init_data_block_len + ts_key_value_len

            # Yield OUTSIDE the lock
            if key_bytes is not None and key_bytes != utils.metadata_key_bytes:
                yield self._post_key(key_bytes), self._post_value(value_bytes)
```

**Key properties:**
- Lock held for ~microseconds per block (one `seek` + `read`) vs entire iteration
- Position tracking via `pos` allows resuming after lock release
- `file_end` captured once at start — new blocks appended by writer are past this boundary (avoids reprocessing)

#### b) `FixedLengthValue` — override `_iter_items_unlocked()`

Same yield-between-locks pattern, adapted for fixed-length block format:
- No `n_bytes_value` field in the header (value length is `self._value_len`)
- No timestamp bytes
- Header: `key_hash(13) + next_ptr(6) + key_len(2)`

```python
def _iter_items_unlocked(self):
    if self._buffer_index_set:
        self.sync()

    with self._thread_lock:
        file_end = self._file.seek(0, 2)
        n_buckets = self._n_buckets
        index_offset = self._index_offset
        first_data_block_pos = self._first_data_block_pos
        value_len = self._value_len

    if first_data_block_pos == 0:
        first_data_block_pos = utils.sub_index_init_pos + (n_buckets * utils.n_bytes_file)

    one_extra_index_bytes_len = utils.key_hash_len + utils.n_bytes_file
    init_data_block_len = one_extra_index_bytes_len + utils.n_bytes_key

    if index_offset != utils.sub_index_init_pos:
        regions = [
            (first_data_block_pos, index_offset),
            (index_offset + n_buckets * utils.n_bytes_file, file_end),
        ]
    else:
        regions = [(first_data_block_pos, file_end)]

    for start, end in regions:
        pos = start
        while pos < end:
            with self._thread_lock:
                self._file.seek(pos)
                header = self._file.read(init_data_block_len)
                next_ptr = utils.bytes_to_int(
                    header[utils.key_hash_len:one_extra_index_bytes_len]
                )
                key_len = utils.bytes_to_int(header[one_extra_index_bytes_len:])

                if next_ptr:
                    kv = self._file.read(key_len + value_len)
                    key_bytes = kv[:key_len]
                    value_bytes = kv[key_len:]
                else:
                    key_bytes = None
                    value_bytes = None

            pos += init_data_block_len + key_len + value_len

            if key_bytes is not None:
                yield self._post_key(key_bytes), self._post_value(value_bytes)
```

#### c) `Booklet` base class — modify `_check_auto_reindex()`

Add early return when `self._defer_reindex` is `True`:

```python
def _check_auto_reindex(self):
    if self._defer_reindex:
        return
    if self._n_keys > self._n_buckets:
        # ... existing reindex logic unchanged
```

Initialize `self._defer_reindex = False` in both `VariableLengthValue.__init__()` and `FixedLengthValue.__init__()`.

**Why defer?** Auto-reindex relocates the bucket index and changes data region boundaries. The sequential scanner's region boundaries were captured at scan start. Deferring until after the map completes avoids invalidating the scan. The final `self.sync()` at the end of `map()` triggers the deferred reindex.

**Note:** Deferral only needed in same-file mode. In separate-file mode, `self` (input) isn't being written to, and `write_db` (output) can reindex freely since the scanner doesn't touch it.

#### d) `Booklet` base class — add `map()` method

```python
def map(self, func, keys=None, write_db=None, n_workers=None):
    """
    Apply func to items in parallel using multiprocessing, writing results
    to this booklet or a separate output booklet.

    Parameters
    ----------
    func : callable
        A picklable function: func(key, value) -> (new_key, new_value) or None.
        Return a (key, value) tuple to write the result. The output key can
        differ from the input key. Return None to skip (no write for this item).
        Must be a top-level function (not a lambda or closure).
    keys : iterable, optional
        Specific keys to process. If None, iterates all keys in the booklet.
    write_db : Booklet, optional
        A separate writable Booklet to write results to. If None, writes
        back to this booklet (which must be open for writing).
    n_workers : int, optional
        Number of worker processes. Defaults to os.cpu_count().

    Returns
    -------
    dict
        Statistics: {'processed': int, 'written': int, 'errors': int}
    """
    import multiprocessing
    import threading
    import queue as queue_mod

    from .parallel import _map_worker, _writer_thread_func, _SENTINEL

    # Determine the output target
    output_db = write_db if write_db is not None else self
    same_file = write_db is None

    if same_file and not self.writable:
        raise ValueError('File is open for read only. Pass a writable write_db or open in write mode.')

    if write_db is not None and not write_db.writable:
        raise ValueError('write_db is open for read only.')

    if self._buffer_index_set:
        self.sync()

    if n_workers is None:
        n_workers = os.cpu_count() or 4

    # Defer auto-reindex on input db only when writing to same file
    if same_file:
        self._defer_reindex = True

    # Set up writer thread
    result_queue = queue_mod.Queue(maxsize=n_workers * 4)
    done_event = threading.Event()
    stats = {'processed': 0, 'written': 0, 'errors': 0}

    writer = threading.Thread(
        target=_writer_thread_func,
        args=(output_db, result_queue, stats, done_event),
        daemon=True,
    )
    writer.start()

    # Build item iterator
    if keys is not None:
        item_iter = ((k, self.get(k)) for k in keys)
    else:
        item_iter = self._iter_items_unlocked()

    # Feed pool and collect results
    try:
        with multiprocessing.Pool(processes=n_workers) as pool:
            work_iter = ((func, k, v) for k, v in item_iter if v is not None)
            for result in pool.imap_unordered(_map_worker, work_iter):
                stats['processed'] += 1
                if result is not None:
                    result_queue.put(result)
    finally:
        # Shut down writer thread
        done_event.set()
        result_queue.put(_SENTINEL)
        writer.join(timeout=60)

        # Re-enable auto-reindex and sync
        if same_file:
            self._defer_reindex = False
        output_db.sync()

    return stats
```

**Backpressure:** `result_queue` has `maxsize=n_workers * 4`. If the writer falls behind, `put()` blocks, which slows down pool result consumption, which naturally throttles feeding new work.

---

### 3. No changes needed

- **`booklet/utils.py`** — all existing functions used as-is
- **`booklet/__init__.py`** — `map()` is a method on existing classes, no new exports

---

## Safety Analysis

### Same-file mode: sequential scan vs concurrent writes

| Concern | Why it's safe |
|---------|---------------|
| Double-processing | Scanner captures `file_end` at start; writer appends new blocks past this boundary → never seen by scanner |
| Tombstoned blocks | Old blocks get `next_ptr = 0` when overwritten; scanner skips them → correct |
| Bucket index mutations | `update_index()` modifies chain pointers; scanner walks data blocks by position, never reads bucket index |
| Auto-reindex | Deferred until after map completes → no relocation during scan |
| Buffer flush by writer | Writes go to EOF past scanner's captured `file_end` → invisible to scanner |

### Separate-file mode

- Reader and writer operate on different files with independent `_thread_lock` instances — zero contention
- Input booklet can be read-only (`flag='r'`) with mmap for maximum read performance
- No need to defer auto-reindex on the input file (it's not being written to)

### Thread lock contention (same-file mode)

- Each lock hold is ~microseconds (one block read or one buffered write)
- Workers spend orders of magnitude longer on computation
- Lock contention is negligible

---

## Usage Examples

### Same file, same key (transform values in-place)
```python
def double_value(key, value):
    return (key, value * 2)  # write back to same key

with booklet.open('data.blt', 'w') as db:
    stats = db.map(double_value, n_workers=4)
```

### Same file, different keys (derive new keys from old)
```python
def process(key, value):
    new_key = f"processed_{key}"
    result = expensive_computation(value)
    return (new_key, result)

with booklet.open('data.blt', 'w') as db:
    stats = db.map(process, n_workers=8)
```

### Separate output file
```python
def transform(key, value):
    return (key, expensive_computation(value))

with booklet.open('input.blt', 'r') as input_db:
    with booklet.open('output.blt', 'n', value_serializer='pickle') as output_db:
        stats = input_db.map(transform, write_db=output_db, n_workers=8)
```

### Specific keys only
```python
keys_to_process = [1, 5, 10, 42]

with booklet.open('data.blt', 'w') as db:
    stats = db.map(my_func, keys=keys_to_process, n_workers=4)
```

### Skip certain items
```python
def selective_process(key, value):
    if value > threshold:
        return (key, expensive_computation(value))
    return None  # skip this item

with booklet.open('data.blt', 'w') as db:
    stats = db.map(selective_process)
```

---

## Tests

Add `booklet/tests/test_map.py`:

| Test | What it verifies |
|------|-----------------|
| `test_map_all_keys` | Map over all items with `keys=None`, verify all values transformed |
| `test_map_specific_keys` | Map over a subset via `keys=[...]`, verify only those changed |
| `test_map_different_output_keys` | `func` returns different keys, verify new keys written |
| `test_map_skip_none` | `func` returns `None` for some keys → those not written |
| `test_map_separate_write_db` | Write to a different booklet, verify input unchanged and output correct |
| `test_map_read_only_input` | Input open as `'r'` with `write_db` provided — works correctly |
| `test_map_read_only_no_write_db_raises` | `self` is read-only without `write_db` → raises `ValueError` |
| `test_map_fixed_length` | Map on `FixedLengthValue` booklet works correctly |
| `test_map_stats` | Return dict has correct `processed`/`written` counts |

## Verification

```bash
uv run pytest booklet/tests/test_map.py -xvs   # new tests
uv run pytest -xvs                               # full suite, no regressions
```

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Booklet is a pure Python key-value file database (`.blt` files). It implements `collections.abc.MutableMapping` (dict-like API) and is both thread-safe (thread locks) and multiprocessing-safe (file locks via `portalocker`). It serves as a faster alternative to `shelve`/`dbm`/`sqlitedict`.

## Build & Development Commands

All commands use [uv](https://docs.astral.sh/uv/) as the build/environment manager:

```bash
uv build                  # Build distribution packages
uv run test               # Run pytest
uv run cov                # Run tests with coverage report
uv run lint:style         # Check style (ruff + black)
uv run lint:typing        # Run mypy type checking
uv run lint:fmt           # Auto-format code
uv run lint:all           # All lint checks
uv run docs-serve         # Local docs server (mkdocs)
```

The version is defined in `booklet/__init__.py` and read by uv dynamically.

## Git Workflow

- `main` branch is the release branch
- `dev` branch is the active development branch
- CI runs on push to `dev` and PRs to `main`, testing Python 3.10, 3.11, 3.12

## Architecture

### Module Structure

- **`booklet/__init__.py`** — Public API exports: `open`, `VariableLengthValue`, `FixedLengthValue`, `available_serializers`, `make_timestamp_int`
- **`booklet/main.py`** — Core classes and the `open()` entry point
- **`booklet/utils.py`** — All low-level file I/O, hashing, binary format logic, buffer management
- **`booklet/serializers.py`** — Serializer classes (each with static `dumps`/`loads` methods) and the serializer registry dicts

### Class Hierarchy

`Booklet` (base, extends `MutableMapping`) is the shared base class with dict operations, sync, close, metadata, reserved slots, timestamps, and prune logic. Two subclasses:

- **`VariableLengthValue`** — Default. Variable-length keys and values. Supports timestamps. Created via `booklet.open()`.
- **`FixedLengthValue`** — Fixed-length values (overwriting same key writes in-place instead of appending). No timestamps. Requires `value_len` parameter.

### Binary File Format

The `.blt` file has three regions:
1. **Sub-index header** (first 200 bytes) — UUID identifying file type (variable vs fixed), version, serializer codes, n_buckets, n_keys, timestamps flag, file UUID, and the layout fields `index_offset` / `first_data_block_pos` (see "Layout" below)
2. **Bucket index** — `n_buckets` entries of 6 bytes each, pointing to first data block for that bucket
3. **Data blocks** — Sequential blocks containing: key hash (13 bytes, blake2s), next block pointer (6 bytes), key length (2 bytes), value length (4 bytes, variable only), timestamp (7 bytes if enabled), key bytes, value bytes

Key lookup: hash key → modulus n_buckets → read bucket → follow chain of data blocks comparing key hashes. Deletes set next-block-pointer to 0 (tombstone). Overwrites append new block and tombstone old one.

**Layout — standard vs relocated index.** The bucket index is *not* always in region 2. When the live key count outgrows `n_buckets` (auto-reindex) or after `prune()`, the index is rewritten *after* the data and its offset recorded in the header. Both states are fully supported and selected at read time from the header:
- **Standard** (`index_offset == 200`, `first_data_block_pos == 0` sentinels): index in region 2, a single data region after it.
- **Relocated** (`index_offset > 200`): data starts at byte 200 and the index sits at `index_offset`; readers scan **two** data regions — `[first_data_block_pos, index_offset)` and `[index_offset + n_buckets*6, EOF)`.

### Prune / compaction

`prune()` reclaims tombstoned/overwritten (and optionally old-timestamp) blocks. It compacts **in place and streaming**: live blocks are moved down toward byte 200 while the file is read forward, so peak memory is bounded by `write_buffer_size` (+ one value), never the file size — pruning a 24 GB file peaks at ~150 MB RSS. The rebuilt bucket index is written *after* the compacted data, so **a non-empty prune's normal output is the relocated layout above** (an all-empty result resets to the standard cleared layout). `Booklet.prune()` returns the count of removed items. Since 0.12.7 `prune(timestamp=..., keep_keys=[...])` exempts the given keys from the timestamp eviction (their live entries survive regardless of age) — ebooklet passes its journal's pending writes.

**Reserved slots (0.12.7).** `set_reserved(slot, data: bytes, timestamp=None)` / `get_reserved(slot, include_timestamp=False)` store app-owned hidden entries (slots 1 and 2; ebooklet's journal + remote-state cache are the consumers). They generalize the metadata-key mechanism: magic key bytes in `utils.reserved_slot_key_bytes`, invisible to every enumeration path via the `reserved_key_bytes` frozenset guards (file iter `utils.iter_keys_value_from_start_end_pos`, mmap iter `_mmap_iter_keys_values_region`, the header-only location iters `iter_locations_from_start_end_pos`/`_mmap_iter_locations_region`, map scan `_iter_items_unlocked`), never counted in `_n_keys` (writes discard the count delta like `set_metadata`; deletes of reserved/metadata keys must NOT decrement — the 0.12.7 count-skew fix), preserved by `prune` (reserved-hash counter compensation), destroyed by `clear()` (ftruncate). Invariants when touching this code: any new enumeration path needs the frozenset guard; `set_reserved` keeps the mandatory pre-`sync()` so a reserved key never sits in the shared write buffer; fixed-length booklets raise `NotImplementedError` (variable framing corrupts fixed-stride iteration). Slots are iteration-hidden, not lookup-hidden (reachable via `get`/`in` with the raw magic key).

### Serializer System

Serializers are classes with static `dumps(obj) -> bytes` and `loads(bytes) -> obj` methods. Built-in serializers are registered in `serial_dict` (name→class), `serial_name_dict` (name→int code), and `serial_int_dict` (int code→class). The int code is stored in the file header so built-in serializers are auto-detected on reopen. **New serializers must be appended to the end of `serial_dict`** — order determines the int codes.

### Concurrency Model

- Thread safety: `threading.Lock` (`self._thread_lock`) guards all file reads/writes
- Process safety: `portalocker` file locks — `LOCK_SH` for read, `LOCK_EX` for write
- Lock acquisition (0.12.9+): all acquires go through `utils._acquire_lock(file, flags, timeout, path_repr)` — a non-blocking fast attempt, then on contention: `timeout=None` (default) keeps the fair, zero-CPU blocking `portalocker.lock` but logs ONE warning naming the file past `_LOCK_WARN_AFTER` via a background `threading.Timer` (do NOT reintroduce a poll loop here — userspace polling defeats the kernel's FIFO queue for concurrent ingestion writers); a finite `timeout` polls (retrying only on `AlreadyLocked`; other `LockException` propagates) and raises `LockTimeoutError`. The helper never closes `file` on a raise — the caller wraps each acquire and closes on any failure (`reopen()` also sets `writable=False`). Create paths open WITHOUT truncating (`os.open(O_CREAT|O_RDWR)`), lock, THEN truncate for `'n'` — never truncate before the lock is held. BytesIO inputs skip locking entirely (guarded by `is_file`).
- Write buffering: writes accumulate in `_buffer_data`/`_buffer_index` bytearrays, flushed when buffer exceeds `write_buffer_size` or on `sync()`/`close()`
- `weakref.finalize` ensures cleanup (unlock + close) even if user forgets to close; `close()`/`sync()` tolerate an already-closed file so a defunct object is always safely closeable

### Iteration Contract (0.12.6+)

`keys()`/`items()`/`values()`/`timestamps()`/`locations()` use **per-step locking** via `Booklet._iter_locked`: each step's seek+read runs under `_thread_lock`, the lock is released before every yield, so interleaved same-instance reads (`get`, `[]`, `in`, nested iterators) are safe during iteration (pre-0.12.6 they deadlocked — the generators held the lock across yields). A per-instance `_mutation_count` (bumped by every layout-mutating op: set/update/del/set_metadata/set_reserved/prune/clear/auto-reindex) is snapshotted at iterator start and checked each step — any mutation mid-iteration raises `RuntimeError('booklet mutated during iteration')`. This *includes* overwriting an existing key (an overwrite appends a block that the scan walks); `set_timestamp` is the one write allowed during iteration (in-place, no layout change). Invariants when touching this code: every bump is a *bare* `+= 1` inside the same lock block as the layout write (never its own `with` — reindex bumps run with the lock already held); underlying `utils` region iterators must keep their cursor in **local state** and re-seek (or slice the mmap positionlessly) every step — never carry state in the shared file position across yields. `map()`/`_iter_items_unlocked` sit outside this contract: plain writes are allowed while a map runs (`_defer_reindex`); only `prune()`/`clear()` invalidate it (a separate `_compaction_count` raises `RuntimeError`). Two `Booklet` instances sharing one `BytesIO` bypass portalocker and have independent locks/counters — unsupported.

**`locations()` and the offset-validity contract (0.12.8).** `locations()` yields `(key, ts, value_offset, value_len)` header-only (its region iterators never read value bytes — that is what keeps a multi-GB sweep to minutes); external holders read the value bytes through their OWN handle, outside booklet's locks (ebooklet's pipelined push is the consumer). The contract this rests on: **value blocks are append-only** — overwrite appends + repoints the chain (only zeroing the OLD block's next_ptr), delete zeroes a next_ptr, auto-reindex rewrites only next_ptrs, `set_timestamp` rewrites 7 bytes in place, `set_reserved`/`set_metadata` go through the ordinary append path — so captured offsets are invalidated ONLY by `prune()`/`clear()`, both of which bump `_compaction_count` (public `compaction_count` property; holders snapshot + re-check). Do NOT add any code path that moves or overwrites stored value bytes outside prune/clear without bumping the counter; do NOT drop the sync-first behavior (`_iter_locked` pre-syncs — buffered writes hold *predicted* offsets that are not yet on disk). `FixedLengthValue.locations()` deliberately raises (different framing).

### File Open Flags

Standard dbm convention: `'r'` (read-only, default), `'w'` (read-write existing), `'c'` (read-write, create if missing), `'n'` (always create new).

### Dependencies

Runtime: `portalocker`, `orjson`, `uuid6`. Many serializers have optional dependencies (zstandard, numpy, pandas, geopandas, msgpack, shapely, pyarrow) that are imported at module load with try/except.

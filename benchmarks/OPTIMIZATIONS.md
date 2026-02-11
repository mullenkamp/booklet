# Booklet Performance Optimizations

## Baseline (pre-optimization)

| Operation | 1K ops/s | 10K ops/s | 50K ops/s |
|---|---|---|---|
| seq_write | 237K | 252K | 104K |
| bulk_update | 259K | 272K | 108K |
| random_read | 249K | 228K | 234K |
| random_read_miss | 619K | 446K | 547K |
| contains | 501K | 398K | 442K |
| iter_keys | 614K | 622K | 619K |
| iter_values | 597K | 601K | 604K |
| iter_items | 549K | 556K | 559K |
| overwrite | 194K | 172K | 182K |
| delete | 197K | 195K | 197K |
| mixed_rw_80r | 262K | 244K | 245K |
| prune | 155K | 222K | 221K |

---

## 1. Buffered I/O for Read-Only Mode [DONE]

**Change:** Remove `buffering=0` from read-only file opens (`'rb'`), keeping `buffering=0` for write modes (`'r+b'`, `'w+b'`).

**Why:** With `buffering=0`, every `file.read()` is a direct syscall. Python's default `BufferedReader` (~8KB buffer) prefetches surrounding data, which dramatically benefits sequential reads (iteration) where blocks are adjacent in the file. Write modes keep `buffering=0` to avoid the overhead of buffer invalidation on the many small seek+write patterns in `update_index` and chain traversal.

**Results vs baseline:**

| Operation | Change | Notes |
|---|---|---|
| iter_keys | +145-154% | 2.5x faster |
| iter_values | +133-145% | 2.5x faster |
| iter_items | +120-134% | 2.3x faster |
| random_read | +3-8% | Small improvement |
| all writes | ~0% | No regression |
| contains | -23-26% | Regression (buffer overhead on short random lookups) |
| random_read_miss | -20-26% | Regression (same cause) |

**Tradeoff:** The contains/miss regression occurs because these operations do very short chain lookups (often just 1 read of 6 bytes). Python's `BufferedReader` reads 8KB to serve 6 bytes, then discards the buffer on the next seek to a different bucket. The buffer management overhead outweighs the prefetch benefit for these random-access patterns.

---

## 2. Merged Lookup + Value Read [DONE]

**Change:** Inline the chain traversal into `get_value()`, `get_value_ts()`, and `get_value_fixed()`. Read the full block header (hash + next_ptr + key_len + value_len = 25 bytes) in one read per chain hop instead of reading 19 bytes during traversal, then seeking back to re-read 6 bytes after finding the match.

**Why:** The original code called `get_last_data_block_pos()` (which reads 19 bytes per block), discarded the file position, then re-sought to the same block to read key_len/value_len. The merged version eliminates 1 seek + 1 read per successful lookup, and reading 25 bytes instead of 19 costs the same (single syscall either way).

**Results vs baseline (cumulative with optimization 1):**

| Operation | Change | Notes |
|---|---|---|
| iter_keys/values/items | +120-150% | Unchanged (different code path) |
| random_read | +8-13% | Improved from +3-8% |
| mixed_rw_80r | +15-16% | New win (helps write-mode reads too) |
| all writes | ~0% | No regression |

---

## 3. mmap-Based Read Path [PROPOSED]

**Change:** Use `mmap` instead of file I/O for all read operations. The mmap object replaces `file.seek()` + `file.read()` with direct memory slicing (`mm[offset:offset+n]`), eliminating syscall overhead entirely.

### Benchmark results (50K entries, 12.5 MB file)

| Operation | File I/O (current) | mmap | Speedup |
|---|---|---|---|
| Random read (hit) | 300K ops/s | 1.34M ops/s | **4.5x** |
| Contains (miss) | 503K ops/s | 2.95M ops/s | **5.9x** |
| Iter keys | 1.81M ops/s | 2.60M ops/s | **1.4x** |
| Iter items | 1.64M ops/s | 2.02M ops/s | **1.2x** |

### Memory behavior

RSS grows only for pages actually accessed, not the full file mapping:

| State | RSS delta |
|---|---|
| After mmap (before access) | +0 KB |
| After 10% random reads | +12,672 KB |
| After 50% random reads | +12,672 KB (same — pages reused) |
| After 100% random reads | +12,672 KB (same) |
| After `MADV_DONTNEED` | +0 KB (fully released) |

For this 12.5 MB file, the full file is paged in after any substantial access because the file fits within a small number of OS pages. For larger files (GBs), only the working set of accessed pages would be resident. The OS automatically evicts mmap'd pages under memory pressure since they're file-backed (no swap needed).

VIRT (virtual address space) increases by the full file size immediately, but this is just address space reservation and costs zero physical RAM. On 64-bit systems, virtual address space is effectively unlimited (128 TB on Linux).

### madvise strategy

| Region | Advice | Effect |
|---|---|---|
| Bucket index | `MADV_RANDOM` | Disables read-ahead for hash-based lookups |
| Data blocks (iteration) | `MADV_SEQUENTIAL` | Aggressive read-ahead for sequential scans |
| After iteration/prune | `MADV_DONTNEED` | Releases physical pages back to OS |

`madvise` is available on Linux and macOS (Python 3.8+). On Windows, `madvise` is not available — calls should be guarded with `hasattr` checks and silently skipped.

### Implementation plan

**Read-only mode (`flag='r'`):**

1. Open file normally, read the 200-byte header via file I/O
2. Create `mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)`
3. Replace all read operations to use mmap slicing instead of `file.seek()` + `file.read()`:
   - `get_value()` / `get_value_ts()` / `get_value_fixed()` — chain traversal + value read
   - `contains_key()` — chain traversal
   - `get_last_data_block_pos()` — chain traversal (used by `set_timestamp`, `assign_delete_flag`)
   - `iter_keys_values()` / `iter_keys_values_fixed()` — sequential scan
4. Apply `MADV_RANDOM` to the bucket index region, `MADV_SEQUENTIAL` to data region (when iterating)
5. On `close()`, call `mm.close()`

**Write mode (`flag='w'`, `'c'`, `'n'`):**

Two options, in order of preference:

- **Option A — mmap for reads only:** Keep file I/O (`buffering=0`) for all writes (appending data blocks, updating index pointers, etc.). Use mmap for read operations (lookups during write mode, e.g., checking if a key exists before overwriting). The mmap must be remapped after writes that extend the file (after `sync()`, `reindex()`), or after `prune()` which truncates and restructures the file. This is the simplest approach.

- **Option B — no mmap in write mode:** Keep current file I/O for everything in write mode. The merged lookup+read optimization already gives +15% for write-mode reads. Simpler, no remapping complexity.

**Key implementation details:**

- The mmap replaces the `file` parameter in utils functions. Functions need to accept either a file object or mmap. The simplest approach: mmap supports slicing (`mm[a:b]`) which returns `bytes`, same as `file.read()`. Create mmap-specific versions of the hot-path functions that use slicing instead of seek+read.
- For write mode Option A: after any operation that changes file size (flush buffer, reindex, prune, clear), the mmap must be closed and recreated: `mm.close(); mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)`. This is cheap (~microseconds).
- `mmap.mmap(f.fileno(), 0)` maps the entire file. The `0` means "use current file size". For write mode, this means the mmap only covers data written so far — pending buffer data is not visible through the mmap, which is correct because booklet already syncs buffers before reads.
- The `reopen()` method needs to create/destroy the mmap when switching between modes.
- Thread safety: mmap reads are thread-safe (read-only memory). The existing `_thread_lock` still protects write operations.

**Files to modify:**

- `utils.py`: Add mmap versions of `get_value`, `get_value_ts`, `get_value_fixed`, `contains_key`, `get_last_data_block_pos`, `iter_keys_values`, `iter_keys_values_fixed`. These replace `file.seek(pos); file.read(n)` with `mm[pos:pos+n]`.
- `main.py`: In `Booklet.__init__` / init functions, create the mmap after opening the file. Store as `self._mmap`. Update `get()`, `__getitem__()`, `__contains__()`, `keys()`, `values()`, `items()`, `timestamps()` to pass `self._mmap` to the mmap-based utils functions. Update `close()` and `reopen()` to manage mmap lifecycle. For write mode Option A, add `_remap_mmap()` helper called after sync/reindex/prune.

**Cross-platform notes:**

- `mmap.mmap()` works on Linux, macOS, and Windows
- `madvise()` requires Python 3.8+ and is not available on Windows
- Guard all `madvise` calls: `if hasattr(self._mmap, 'madvise') and hasattr(mmap, 'MADV_RANDOM'): ...`
- On Windows, mmap still provides the core benefit (memory-mapped access instead of syscalls), just without the fine-grained memory advice

---

## 4. Bulk Read for Iteration [PROPOSED]

**Change:** In `iter_keys_value_from_start_end_pos()`, instead of reading each block individually with separate seek+read calls, read a large chunk (e.g., 64KB-256KB) into memory and parse blocks from the in-memory buffer. Only issue new file reads when the buffer is exhausted.

**Why:** Data blocks are stored sequentially, so sequential reads can be coalesced. Currently each block requires a `file.seek()` + `file.read()` of ~25 bytes. With bulk reads, one syscall serves many blocks.

**Expected impact:** This would primarily benefit write-mode iteration (where `buffering=0` means each read is a syscall). Read-mode iteration already benefits from Python's `BufferedReader`, but a larger explicit buffer (64-256KB vs 8KB) could further improve throughput. Estimated 20-50% improvement on iteration in write mode. If mmap (optimization 3) is implemented, this optimization becomes less important since mmap handles read coalescing at the OS level.

**Risk:** Low. The iteration code already processes blocks linearly. The change replaces per-block I/O with chunk-based I/O over the same sequential data.

---

## 5. `write_init_bucket_indexes` Optimization [PROPOSED]

**Change:** Replace the Python loop that extends a bytearray one 6-byte entry at a time:
```python
for i in range(n_buckets):
    temp_bytes.extend(init_end_pos_bytes)
```
with bytes multiplication:
```python
file.write(init_end_pos_bytes * n_buckets)
```

**Why:** Bytes multiplication (`b'\x01\x00...' * n`) runs in C and is orders of magnitude faster than a Python loop with `extend()`. This function is called during file creation, `clear()`, `prune()`, and `reindex()`.

**Expected impact:** Negligible at the default 12,007 buckets (already fast). Meaningful at higher bucket counts after reindexing: 144,013 or 1,728,017 buckets. Would speed up `prune()` and `clear()` on large databases.

**Risk:** Very low. Pure simplification.

---

## 6. `blake2b` Instead of `blake2s` [PROPOSED — BREAKS COMPATIBILITY]

**Change:** Switch `hash_key()` from `blake2s` to `blake2b`.

**Why:** On 64-bit platforms, `blake2b` processes 128-byte blocks vs 64-byte blocks for `blake2s`, making it faster for the same digest size. Both support the 13-byte digest used here.

**Expected impact:** Potentially 20-40% faster hashing, which affects every read and write operation. Needs benchmarking with typical key sizes to confirm.

**Risk:** **Breaks backward compatibility.** Different hash function produces different digests, so existing `.blt` files would be unreadable. Would require a file version bump and migration tooling, or a header flag to select the hash algorithm. Not recommended unless the speed gain is substantial enough to justify a breaking change.

---

## Summary

| # | Optimization | Status | Read Impact | Write Impact | Complexity |
|---|---|---|---|---|---|
| 1 | Buffered I/O (read-only) | Done | Iteration +150%, contains -25% | None | Low |
| 2 | Merged lookup+read | Done | +8-16% random/mixed reads | None | Low |
| 3 | mmap read path | Proposed | 4-6x random reads, 1.2-1.4x iteration | None | Medium |
| 4 | Bulk iteration reads | Proposed | +20-50% write-mode iteration | None | Medium |
| 5 | Bucket init optimization | Proposed | None | Faster prune/clear/create | Very low |
| 6 | blake2b hash | Proposed | +20-40% all ops (estimated) | +20-40% all ops (estimated) | High (breaking) |

Note: If optimization 3 (mmap) is implemented, it subsumes optimization 1 (buffered I/O) for read-only mode and largely replaces optimization 4 (bulk iteration reads). The contains/miss regression from optimization 1 would also be eliminated since mmap is 5.9x faster for contains/miss lookups.

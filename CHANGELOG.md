# Changelog

Notable changes to booklet. The format loosely follows [Keep a Changelog](https://keepachangelog.com/);
booklet does not promise SemVer — minor versions may change behavior.
Entries for 0.12.2 and earlier were reconstructed from commit history after the fact.

## 0.12.9 (2026-07-21)

### Fixed
- **Opening a locked file no longer hangs silently.** Every OS-lock acquire went
  through a bare blocking `portalocker.lock(...)`, so opening a file whose flock
  another handle held waited forever with no message — including a second open of
  the *same* file in the *same* process (POSIX `flock` is per open-file-description).
  Now: a non-blocking fast attempt first; on contention the default keeps the fair,
  zero-CPU kernel wait but logs ONE warning naming the file if the wait exceeds a
  grace window (via a background timer — never busy-polls, stays silent for brief
  legitimate contention). Retries only on `AlreadyLocked`; any other `LockException`
  propagates. See `open(..., timeout=...)` below to fail fast instead.
- **Create-path data-loss race — lock BEFORE truncate.** A `flag='n'` (and
  `flag='c'` create) open did `io.open(fp, 'w+b')`, which truncates the file to
  zero *before* the lock is acquired — so creating over a file another writer holds
  destroyed that writer's data without waiting for the lock. The create path now
  opens without truncating (`os.open(O_CREAT|O_RDWR)`), acquires the lock, and only
  then truncates (for `'n'`). *Known limitation:* the narrower `'c'` create-vs-open
  branch-selection TOCTOU under a race is unchanged.
- `close()`/`sync()` now tolerate an already-closed file, so a defunct object (after
  a failed `reopen()`, or a double `close()`) is always safely closeable instead of
  raising `ValueError: I/O operation on closed file`.

### Added
- **`timeout=` parameter** on `open()`, `VariableLengthValue`, and `FixedLengthValue`.
  `None` (default) waits indefinitely (warning if the wait is long); a number raises
  the new **`LockTimeoutError`** (subclass of `TimeoutError`, exported) if the lock
  isn't acquired in time. `timeout=0` fails fast on any contention. A finite timeout
  is reused by `reopen()`; a timed-out `reopen()` leaves the object safely closeable.

## 0.12.8 (2026-07-15)

### Added
- **`locations()`**: a header-only iterator of `(key, timestamp, value_offset, value_len)`
  for every live user key — the physical position of each value's raw (serialized) bytes
  inside the file, resolved without reading them. Callers may then read the bytes through
  their OWN file handle, outside booklet's locks (ebooklet's pipelined push is the first
  consumer). Validity contract: value blocks are append-only, so captured offsets survive
  `set`/overwrite/`del`/auto-reindex and are invalidated ONLY by `prune()`/`clear()`; an
  overwrite leaves the old offset addressing the OLD bytes. Buffered writes are flushed
  first, so offsets are on-disk positions. Yields `timestamp=None` on files created with
  `init_timestamps=False` (deliberate divergence from `timestamps()`, which raises there).
  Same iteration semantics as `keys()` (mutation during iteration raises; `set_timestamp`
  allowed). Not implemented for fixed-length booklets (different framing; raises
  NotImplementedError). Windows caveat: a write-mode booklet's OS lock is mandatory over
  roughly the first 4.29 GB — reads through a second handle in that range may be denied
  (PermissionError); fall back to `get()`/`get_timestamp()`.
- **`compaction_count`** (read-only property): monotonic count of compactions
  (`prune()`/`clear()`) performed through this handle since open. External holders of
  `locations()` offsets snapshot it before capturing and re-check after reading to detect
  invalidation.

## 0.12.7 (2026-07-12)

### Added
- **Application-reserved internal slots**: `set_reserved(slot, data: bytes, timestamp=None)` /
  `get_reserved(slot, include_timestamp=False)` on variable-length booklets — hidden
  bookkeeping entries for libraries built on booklet (ebooklet's pending-change journal
  is the first consumer). Like the metadata key they are invisible to
  `keys()`/`items()`/`values()`/`timestamps()`/`map()` and `len()`, survive `prune()`,
  and are destroyed by `clear()`. Values are raw bytes — the caller owns serialization.
  Not supported on fixed-length booklets (`set_reserved` raises NotImplementedError,
  same reason as `set_metadata` since 0.12.5). Note: like metadata, slots are hidden
  from iteration but remain reachable via `get`/`in` with the raw internal key.
  Files containing reserved slots read by booklet < 0.12.7 leak them into iteration —
  upgrade readers first.
- **`prune(timestamp=..., keep_keys=[...])`**: keys exempt from the timestamp eviction
  (their live entries are kept regardless of age); overwritten/deleted blocks are still
  compacted. Lets a caller protect unpushed/pending entries while evicting old cache.

### Fixed
- Deleting the metadata key (or a reserved slot) via `del` decremented the key count
  that was never incremented for it, silently skewing `len()` down by one.

## 0.12.6 (2026-07-09)

The iteration-contract release: iteration now follows python dict semantics.

### Fixed
- **`keys()`/`items()`/`values()`/`timestamps()` no longer deadlock on interleaved reads.**
  The generators used to hold the thread lock across yields, so the natural idiom
  `for k in db.keys(): db[k]` hung forever, single-threaded (as did `in`, `get`,
  nested iterators, and `MutableMapping.popitem()`). Iteration now locks per step
  and releases the lock before every yield; an abandoned half-consumed iterator no
  longer holds the lock.
- The fixed-length file iterator kept its cursor in the shared file position across
  yields — under the new per-step locking an interleaved read would have corrupted or
  livelocked the scan. Rewritten to local-position form (seek per step), matching the
  mmap iterators.
- `FixedLengthValue.set()` inherited the variable-length block writer, whose framing
  fixed-stride iteration cannot parse — one call corrupted iteration *and* point reads
  for the whole file. It now writes fixed framing; `timestamp` must be None (fixed
  files have no timestamp support).
- Values of the wrong length on fixed booklets (`__setitem__`/`set`/`update`) were
  accepted and silently corrupted the stride; they now raise ValueError.
- `set_timestamp()` crashed with AttributeError for the documented str/datetime
  timestamp forms; they are now normalized like `prune()`'s (0.12.4).
- `_set_file_timestamp()` moved the shared file position without holding the thread
  lock; it now locks.

### Changed
- **Mutating a booklet while iterating it now raises
  `RuntimeError('booklet mutated during iteration')`** at the iterator's next step
  (previously it deadlocked): set/update/del/set_metadata/prune/clear, or an
  auto-reindex they trigger. This *includes* overwriting an existing key, which a
  plain dict allows — an overwrite appends a data block that the scan walks.
  `set_timestamp` is the one write allowed during iteration (in-place, no layout
  change). Collect keys into a list first if you need to write while walking.
- `prune()`/`clear()` during a running `map()` now raise RuntimeError instead of
  returning garbage from the relocated scan. Plain writes during `map()` remain
  allowed (its documented contract; auto-reindex stays deferred for its duration).
- Full-scan iteration costs ~0.3 µs/key more (per-step lock acquisition; ~1.1–1.2×
  on a 100k-key scan).

## 0.12.5 (2026-07-05)

### Fixed
- `set_metadata()` on a `FixedLengthValue` now raises NotImplementedError. The base
  metadata write path uses variable-length framing that silently corrupts
  fixed-stride iteration (`keys()` returned garbage after one metadata write).
  `get_metadata()` stays inherited and harmlessly returns None.

## 0.12.4 (2026-07-02)

### Fixed
- `prune(timestamp=...)` accepts the documented str/datetime timestamp forms again —
  the compaction compared the raw value against int-microsecond timestamps and raised
  TypeError for anything but int. None still means "no timestamp filter" (it is never
  interpreted as "now").

## 0.12.3 (2026-07-02)

### Fixed
- `prune()` post-compaction layout handling: the in-memory index offset and first
  data block position now mirror what the compaction wrote (relocated index for a
  non-empty result, standard cleared layout for an empty one), so reads and iteration
  immediately after a prune see the right regions.

## 0.12.2 (2026-04-07)

### Fixed
- Initializing a very large booklet (huge `n_buckets`) no longer fails.

## 0.12.1 (2026-03-01)

### Changed
- `map()` output reworked: the worker function returns `(key, value)` (or None to
  skip) and results are yielded as they complete.

## 0.12.0 (2026-02-22)

### Added
- `map()`: apply a function to items in parallel via multiprocessing.

## 0.11.0 (2026-02-14)

### Added
- Read-only opens (`flag='r'`) use mmap for faster reads.

## 0.10.2 (2026-02-12)

### Changed
- Performance improvements.

## 0.10.1 (2026-02-11)

### Fixed
- Backwards compatibility with files written by earlier versions.

## 0.10.0 (2026-02-10)

### Added
- Automatic reindexing: when the load factor exceeds 1.0 the bucket index grows and
  relocates on sync, so `n_buckets` no longer needs to be sized up front.

## 0.9.3 (2026-02-09)

### Fixed
- Various bug fixes.

## 0.9.2 (2025-07-15)

### Added
- Thread locks around all file reads/writes (thread safety).

## 0.9.0 / 0.9.1 (2025-06-16)

### Added
- `io.BytesIO` accepted as the file input (0.9.0), with compatibility improvements
  (0.9.1).

## 0.8.0 and earlier

Pre-changelog history (0.5.x–0.8.0, 2024–2025): serializers, timestamps, the fixed-
and variable-length classes, `reopen()`, and the on-disk format itself. See
`git log` for details.

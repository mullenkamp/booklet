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

`Booklet` (base, extends `MutableMapping`) is the shared base class with dict operations, sync, close, metadata, timestamps, and prune logic. Two subclasses:

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

`prune()` reclaims tombstoned/overwritten (and optionally old-timestamp) blocks. It compacts **in place and streaming**: live blocks are moved down toward byte 200 while the file is read forward, so peak memory is bounded by `write_buffer_size` (+ one value), never the file size — pruning a 24 GB file peaks at ~150 MB RSS. The rebuilt bucket index is written *after* the compacted data, so **a non-empty prune's normal output is the relocated layout above** (an all-empty result resets to the standard cleared layout). `Booklet.prune()` returns the count of removed items.

### Serializer System

Serializers are classes with static `dumps(obj) -> bytes` and `loads(bytes) -> obj` methods. Built-in serializers are registered in `serial_dict` (name→class), `serial_name_dict` (name→int code), and `serial_int_dict` (int code→class). The int code is stored in the file header so built-in serializers are auto-detected on reopen. **New serializers must be appended to the end of `serial_dict`** — order determines the int codes.

### Concurrency Model

- Thread safety: `threading.Lock` (`self._thread_lock`) guards all file reads/writes
- Process safety: `portalocker` file locks — `LOCK_SH` for read, `LOCK_EX` for write
- Write buffering: writes accumulate in `_buffer_data`/`_buffer_index` bytearrays, flushed when buffer exceeds `write_buffer_size` or on `sync()`/`close()`
- `weakref.finalize` ensures cleanup (unlock + close) even if user forgets to close

### File Open Flags

Standard dbm convention: `'r'` (read-only, default), `'w'` (read-write existing), `'c'` (read-write, create if missing), `'n'` (always create new).

### Dependencies

Runtime: `portalocker`, `orjson`, `uuid6`. Many serializers have optional dependencies (zstandard, numpy, pandas, geopandas, msgpack, shapely, pyarrow) that are imported at module load with try/except.

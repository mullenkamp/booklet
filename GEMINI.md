# Booklet

## Project Overview

**Booklet** is a pure Python key-value file database (utilizing `.blt` files) designed for performance and concurrency. It implements the `collections.abc.MutableMapping` API (dictionary-like) and serves as a faster, more robust alternative to standard Python libraries like `shelve`, `dbm`, or `sqlitedict`.

### Key Features
*   **Thread & Process Safety:** Uses thread locks (`threading.Lock`) and file locks (`portalocker`) to ensure safe concurrent access. Lock acquisition (0.12.9+) goes through `utils._acquire_lock`: a non-blocking fast attempt, then either a fair blocking wait that logs one warning naming the file if it's long (`timeout=None`, default) or a polling wait that raises `LockTimeoutError` (finite `timeout`). Create paths lock BEFORE truncating so a `flag='n'` open never destroys a file another writer holds.
*   **Serialization:** Supports multiple built-in serializers (e.g., `orjson`, `msgpack`, `pickle`) and allows custom serializer implementations.
*   **Storage Modes:**
    *   **VariableLengthValue:** (Default) Variable-length keys/values, append-only writes (overwrites append new data), supports timestamps.
    *   **FixedLengthValue:** Fixed-length values, allows in-place overwrites for performance optimization.
*   **Metadata:** Supports file-level metadata storage independent of key-value pairs.

## Building and Running

The project uses **[uv](https://docs.astral.sh/uv/)** for dependency management and running development commands.

### Setup
Ensure you have Python 3.10+ installed.

```bash
# Install dependencies and create the environment
uv sync
```

### Testing
Run the test suite with uv:

```bash
uv run test          # Run pytest
uv run cov           # Run tests with a coverage report
```

### Building
The project uses `hatchling` as the build backend.

```bash
# Build distribution packages
uv build
```

## Development Conventions

### Code Style & Linting
The project enforces strict code style and type checking using `ruff`, `black`, and `mypy`.

*   **Formatting:** `black`
*   **Linting:** `ruff`
*   **Type Checking:** `mypy`

**Linting Commands (via uv):**
```bash
uv run lint:style         # Check style (ruff + black)
uv run lint:typing        # Run mypy type checking
uv run lint:fmt           # Auto-format code
uv run lint:all           # Run all lint checks
```

### Git Workflow
*   **`main`**: The release branch.
*   **`dev`**: The active development branch.
*   **CI**: Runs on pushes to `dev` and PRs to `main`, testing across Python 3.10, 3.11, and 3.12.

### Architecture Notes
*   **Entry Point:** `booklet.open()` in `booklet/main.py` is the primary factory function.
*   **Core Logic:** `booklet/utils.py` handles low-level file I/O, hashing (Blake2s), and binary format management.
*   **Serializers:** Defined in `booklet/serializers.py`. New built-in serializers must be appended to the end of the registry to maintain integer code compatibility.
*   **File Format:** `.blt` files consist of a 200-byte Header (metadata/params, including the `index_offset` / `first_data_block_pos` layout fields), a Bucket Index (hash table of chain heads), and Data Blocks (per-bucket linked lists of entries; deletes/overwrites tombstone the old block). The index has two supported layouts, chosen at read time from the header: **standard** (index before the data, `index_offset == 200`) and **relocated** (index written *after* the data, `index_offset > 200`, with two data regions) — the relocated form is produced by auto-reindex and by `prune()`.
*   **Prune / Compaction:** `prune()` reclaims tombstoned/overwritten (and optionally old-timestamp) blocks by compacting the file **in place, streaming** live blocks toward byte 200 — peak memory is bounded by `write_buffer_size`, not the file size (a 24 GB file prunes at ~150 MB RSS). Its normal (non-empty) output is the relocated layout.

# Booklet

## Project Overview

**Booklet** is a pure Python key-value file database (utilizing `.blt` files) designed for performance and concurrency. It implements the `collections.abc.MutableMapping` API (dictionary-like) and serves as a faster, more robust alternative to standard Python libraries like `shelve`, `dbm`, or `sqlitedict`.

### Key Features
*   **Thread & Process Safety:** Uses thread locks (`threading.Lock`) and file locks (`portalocker`) to ensure safe concurrent access.
*   **Serialization:** Supports multiple built-in serializers (e.g., `orjson`, `msgpack`, `pickle`) and allows custom serializer implementations.
*   **Storage Modes:**
    *   **VariableLengthValue:** (Default) Variable-length keys/values, append-only writes (overwrites append new data), supports timestamps.
    *   **FixedLengthValue:** Fixed-length values, allows in-place overwrites for performance optimization.
*   **Metadata:** Supports file-level metadata storage independent of key-value pairs.

## Building and Running

The project recommends using **[uv](https://docs.astral.sh/uv/)** for dependency management and running development commands, though standard `pip` and `pytest` are also supported (as seen in CI).

### Setup
Ensure you have Python 3.10+ installed.

```bash
# Install dependencies
pip install -r requirements.txt
# OR using uv
uv sync
```

### Testing
Run the test suite using `uv run pytest`:

```bash
# Standard
uv run pytest

# With coverage
coverage run -m pytest tests
coverage report

# Using uv
uv run pytest
uv run cov
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
*   **File Format:** `.blt` files consist of a Header (metadata/params), Bucket Index (hash table), and Data Blocks (linked list of entries).

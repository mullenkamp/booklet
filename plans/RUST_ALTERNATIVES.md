# Rust Alternatives to Booklet

There is no direct "clone" of Booklet in Rust (i.e., a crate that reads `.blt` files or copies its exact API), but the Rust ecosystem has extremely strong, high-performance **embedded key-value stores** that serve the exact same purpose.

The closest equivalent in spirit—modern, pure Rust, safe, and easy to use—is **[redb](https://github.com/cberner/redb)**.

## Top Recommendation: `redb`

**Why it's the best fit:**
Like Booklet, `redb` is a pure embedded key-value store. It is ACID-compliant, thread-safe, and provides a "dictionary-like" experience but with strong typing.

*   **Pure Rust:** No external C libraries (unlike RocksDB or LMDB).
*   **Concurrency:** MVCC (Multi-Version Concurrency Control) allowing **1 writer and multiple concurrent readers**.
*   **Safety:** ACID transactions guarantee data integrity even during crashes (Booklet relies on file flushing/locking).
*   **API:** It uses strictly typed tables (e.g., `Table<u64, str>`), whereas Booklet is more flexible/dynamic (stores any pickled object).

## Comparison of Options

| Feature | **Booklet** (Python) | **redb** (Rust) | **sled** (Rust) | **persy** (Rust) |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Goal** | Ease of use, `dict` API | Safety, ACID, Performance | High-performance, Beta | Transactional Storage |
| **Backend** | Custom `.blt` format | B-tree (Cow) | Log-Structured (LSM) | Page-based |
| **Concurrency** | Locks (Thread/Process) | MVCC (Read/Write split) | Lock-free | MVCC |
| **Value Type** | Any Python Object | Byte arrays or Typed traits | Byte arrays (`[u8]`) | Byte arrays |
| **Status** | Stable | **Stable (v2.0+)** | Beta (Rewrite in progress) | Stable |

## Other Notable Mentions

### 1. [sled](https://github.com/spacejam/sled)
*   Historically the most popular "pure Rust" KV store.
*   **Pros:** Extremely fast, advanced features (prefix scan, merge operators).
*   **Cons:** Currently in a long beta/rewrite phase; on-disk format has changed frequently.

### 2. [pickledb-rs](https://github.com/seladb/pickledb-rs)
*   **Direct equivalent to Python's simple K/V usage.**
*   Inspired directly by Python's `pickledb`.
*   **Pros:** Dead simple, dumps generic JSON/CBOR to a file.
*   **Cons:** Not efficient for large datasets (loads entire DB into memory), unlike Booklet/redb which access disk pages on demand.

### 3. [heed](https://github.com/meilisearch/heed) (LMDB wrapper)
*   If you need **multi-process** concurrency (e.g., multiple different binaries writing to the DB at once), `heed` (wrapping LMDB) is often the standard choice in Rust, as pure Rust options like `redb` typically lock the file to a single writer process.

## Summary

If you are porting Booklet logic to Rust or looking for a Rust alternative:

*   **Use `redb`** for a robust, production-grade embedded database.
*   **Use `pickledb`** if you just want a quick "save this HashMap to a file" for config/small data (similar to `json.dump`).

## New findings

Fjall also seems really good:
https://github.com/fjall-rs/fjall

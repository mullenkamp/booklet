# Booklet Automatic Resizing Options

This document outlines two strategies for implementing automatic bucket resizing in `booklet` while maintaining its single-file architecture (`.blt`).

## Current Architecture & Problem
Currently, Booklet uses a **Static Hash Table** layout:
`[Header (0-200)] [Fixed Index] [Data Blocks...]`

*   **Constraint:** The Index is trapped between the Header and the Data Blocks.
*   **Problem:** Increasing the Index size (`n_buckets`) requires shifting all Data Blocks further down the file, which necessitates a full file rewrite (currently done via `prune(reindex=True)`). This is too slow for automatic resizing during runtime.

---

## Option 2: The Relocatable Index (Recommended)

This strategy treats the Index as a movable object. Instead of being fixed at byte 200, the Index can exist anywhere in the file. When it needs to grow, we simply append a larger Index to the end of the file and point to it.

### 1. File Structure Changes
We modify the **Header** (first 200 bytes) to track the Index's location.

*   **New Field:** `index_offset` (8 bytes).
    *   *Default/Legacy:* `200` (points to the space immediately after the header).
    *   *After Resize:* Points to the file offset where the new, larger Index begins.

**Visual Layout After Resize:**
```text
[Header (index_offset=9000)] ... [Old Dead Index] ... [Data Blocks] ... [New Larger Index @9000]
```

### 2. The Resize Algorithm ("Expand")
Triggered when `n_keys > n_buckets * LOAD_FACTOR` (e.g., 0.8).

**Phase A: Allocation**
1.  **Calculate Size:** Select a new `n_buckets` (e.g., next prime or double).
2.  **Append:** Seek to the end of the file (`0, 2`) and write `new_n_buckets * 6` bytes of zeros. This reserves space for the new Index.
3.  **Track:** Store this start position as `new_index_offset`.

**Phase B: Rewiring (In-Place Updates)**
We must re-hash all existing items into the new Index. **Crucially, we do NOT move the Key/Value data.** We only update the "Next Pointer" links.

1.  **Scan:** Iterate linearly through all Data Blocks in the file.
2.  **For each Block:**
    *   Read `key_hash` (13 bytes).
    *   Calculate `new_bucket = hash % new_n_buckets`.
    *   **Read** the current pointer at `NewIndex[new_bucket]`.
    *   **Write** that pointer into the *Data Block's* `next_block_ptr` field. (This inserts the block at the head of the new bucket's chain).
    *   **Update** `NewIndex[new_bucket]` to point to the current Data Block.

**Phase C: The Switch**
1.  **Header Update:** Write `new_index_offset` and `new_n_buckets` to the file header.
2.  **Flush:** Force sync to disk.
3.  **Cleanup:** The old Index space (at byte 200) is now "dead space" (approx. 72KB for 12k buckets). It will be reclaimed naturally next time the user runs `prune()`.

### Pros & Cons
*   **Pros:**
    *   **Fast:** Avoids rewriting the heavy Key/Value data. Only reads headers and writes pointers.
    *   **Simple:** Retains the core "Modulo Hashing" logic.
    *   **Safe:** If the process crashes during Phase B, the Header still points to the old valid Index.
*   **Cons:**
    *   **Fragmentation:** Leaves small pockets of dead space (old indices) until `prune()` is called.
    *   **Random I/O:** Phase B involves jumping between Data Blocks and the New Index (though OS caching helps significantly).

---

## Option 3: Extendible Hashing (Dynamic Directory)

This strategy replaces the single monolithic Index with a **Directory of Pointers** to smaller **Index Pages**. This allows the index to grow incrementally (one page at a time) rather than all at once.

### 1. File Structure Changes
The structure changes from a "Hash Table" to a "Directory System."

*   **Header:** Adds `global_depth` and `directory_offset`.
*   **Directory:** A variable-sized table at `directory_offset` containing pointers to **Index Pages**. Size = $2^{global\_depth}$.
*   **Index Pages:** Fixed-size blocks (e.g., 4KB) scattered throughout the file. These contain the actual bucket pointers.

**Visual Layout:**
```text
[Header] [Directory] [Page 0] [Data...] [Page 1] [Data...] [New Directory] ...
```

### 2. Logic Changes

**Lookup (Read)**
Instead of `hash % n`, we use bitmasking:
1.  **Mask:** `dir_idx = hash & ((1 << global_depth) - 1)`
2.  **Directory:** Read pointer at `Directory[dir_idx]` -> find `PageOffset`.
3.  **Page:** Read `Page[PageOffset]` to find the specific bucket.

**Resize (Write) - "Split"**
When an insertion targets a full Index Page:
1.  **No Full Rewrite:** We only touch *that specific page*.
2.  **Split:** Allocate 2 new Index Pages at the end of the file.
3.  **Redistribute:** Move items from the old full page into the 2 new pages based on their `(global_depth + 1)` bit.
4.  **Update Directory:** Point the relevant directory slots to the new pages.
5.  **Expand Directory:** If the Directory itself is too small (Local Depth > Global Depth), create a new 2x larger Directory at the end of the file.

### Pros & Cons
*   **Pros:**
    *   **Smooth Performance:** No "stop-the-world" pauses. Resizing cost is amortized across many writes.
    *   **Scalability:** Can grow from KB to TB seamlessly.
*   **Cons:**
    *   **Complexity:** significantly harder to implement and debug (managing depths, page splits, directory expansion).
    *   **Slower Lookups:** Requires an extra seek (Directory -> Page -> Data) compared to Option 2 (Index -> Data).
    *   **Space:** Higher overhead from partially filled Pages and abandoned Directories.

---

## Recommendation
**Option 2 (Relocatable Index)** is recommended for Booklet.

1.  **Fit:** It aligns perfectly with Booklet's current "simple, fast, append-only" philosophy.
2.  **Effort:** It requires modifying ~4 functions in `utils.py` vs. rewriting the entire lookup engine for Option 3.
3.  **Performance:** The "stop-the-world" pause for Option 2 is significantly faster than the current method and likely acceptable for 99% of use cases (sub-second for <1GB files).

# Relocatable Index with Auto-Reindex — Implementation Summary

## Overview

Booklet's bucket index was previously fixed at byte 200, requiring a full file rewrite (via `prune(reindex=True)`) to grow it. This implementation allows the index to be appended at the end of the file and pointed to from the header, avoiding data movement. Auto-reindexing triggers when `n_keys > n_buckets`, using a predefined chain for new sizes.

## Design

- **Trigger**: reindex when `n_keys > n_buckets` (load factor 1.0)
- **New n_buckets chain**: 12007 → 144013 → 1728017 → 20736017 → None (stop)
- **Non-mapped values**: jump to the smallest chain value larger than current
- **File version**: bumped from 4 to 5 (version 4 files remain backward compatible)

### Header Changes

Two new 6-byte fields in previously unused header space:

| Bytes | Field | Description |
|-------|-------|-------------|
| 65-70 | `index_offset` | Position of the bucket index (0 = legacy, means byte 200) |
| 71-76 | `first_data_block_pos` | Start of data blocks (0 = legacy, means `200 + n_buckets * 6`) |

### File Layout After Reindex

```
[Header 200] [Dead original index] [Data blocks A] [New index @ index_offset] [Data blocks B (new writes)]
```

Iteration scans two regions: `[first_data_block_pos, index_offset)` and `[index_offset + n_buckets*6, EOF)`.
If the index hasn't been relocated (default), one region: `[first_data_block_pos, EOF)`.

### Reindex Algorithm (Chain-Following)

1. **Allocate**: Append new bucket index (initialized with 1s) at EOF
2. **Rewire**: For each bucket in the current index, follow the chain. For each live block, compute `new_bucket = hash % new_n_buckets`, insert at head of new bucket's chain
3. **Skip block**: If old index was relocated (not at byte 200), write a skip block over it so sequential iteration can skip past it
4. **Header update**: Write new `n_buckets`, `index_offset` to header. Flush.

## Changes

### `booklet/utils.py`

**New constants:**
- `index_offset_pos = 65`
- `first_data_block_pos_pos = 71`
- `n_buckets_chain` — sorted list of all target n_buckets values
- `current_version` bumped from 4 to 5

**New functions:**
- `get_new_n_buckets(n_buckets)` — returns the next n_buckets from the chain, or None if at max
- `write_skip_block_variable(file, offset, dead_size, ts_bytes_len)` — writes a skip block over a dead region for variable-length files
- `write_skip_block_fixed(file, offset, dead_size, value_len)` — writes skip block(s) over a dead region for fixed-length files
- `reindex(file, n_buckets, new_n_buckets, index_offset, first_data_block_pos, write_buffer_size, ts_bytes_len, fixed_value_len)` — the chain-following reindex algorithm

**Modified functions (added `index_offset` parameter):**
- `get_bucket_index_pos`
- `get_last_data_block_pos`
- `contains_key`
- `set_timestamp`
- `get_value`
- `get_value_ts`
- `assign_delete_flag`
- `update_index`
- `get_value_fixed`
- `write_data_blocks`
- `write_data_blocks_fixed`

**Modified iteration functions (added `index_offset` and `first_data_block_pos`):**
- `iter_keys_values` — scans two regions if index is relocated
- `iter_keys_values_fixed` — refactored into `_iter_keys_values_fixed_region` helper + wrapper that scans two regions if needed

**Modified prune functions:**
- `prune_file` — removed `reindex` param; uses two-phase approach (read all live blocks into memory, then write) to avoid overlap when n_buckets has grown; always writes index at byte 200; resets `index_offset` and `first_data_block_pos` in header; returns `(n_keys, removed_count)` instead of `(n_keys, removed_count, n_buckets)`
- `prune_file_fixed` — same changes

**Modified init/read functions:**
- `read_base_params_variable` — reads `index_offset` and `first_data_block_pos` from header (defaults to computed legacy values for version 4)
- `read_base_params_fixed` — same
- `init_files_variable` — sets `self._index_offset` and `self._first_data_block_pos` on new file creation
- `init_files_fixed` — same

**Modified `clear`:**
- Resets `index_offset` and `first_data_block_pos` to 0 in header

### `booklet/main.py`

**New instance attributes:**
- `self._index_offset` — current index position
- `self._first_data_block_pos` — start of data blocks

**New method:**
- `_check_auto_reindex()` — checks if `n_keys > n_buckets` and calls `reindex()` if needed

**Modified `Booklet._sync_index()`:**
- Passes `self._index_offset` to `update_index`
- Calls `_check_auto_reindex()` after updating

**Modified `Booklet.sync()`:**
- Also checks for auto-reindex when buffer is empty (handles case where keys were flushed during `write_data_blocks`)

**Modified `Booklet.set()`:**
- Checks for auto-reindex after internal buffer flushes report new keys

**Modified `Booklet.prune()`:**
- Removed `reindex` parameter
- Resets `self._index_offset` and `self._first_data_block_pos` after prune

**Modified `FixedLengthValue.prune()`:**
- Same: removed `reindex` parameter, resets layout fields

**Modified `Booklet.clear()`:**
- Resets `self._index_offset` and `self._first_data_block_pos`

**All methods that call utils functions:**
- Pass `self._index_offset` to all bucket-access utils calls
- Pass `self._index_offset` and `self._first_data_block_pos` to all iteration utils calls

### Tests

**`booklet/tests/test_auto_reindex.py`** — 9 tests:
- `test_auto_reindex_variable` — variable-length auto-reindex with reopen verification
- `test_auto_reindex_fixed` — fixed-length auto-reindex with reopen verification
- `test_prune_resets_index_layout` — prune resets index back to byte 200
- `test_reopen_after_reindex` — reopened file reads relocated index correctly
- `test_non_mapped_n_buckets_jumps` — non-mapped n_buckets jumps to nearest chain value
- `test_writes_after_reindex` — writes after reindex are appended and readable
- `test_delete_after_reindex` — deletes work correctly with new index
- `test_iteration_after_reindex` — iteration scans both data regions
- `test_clear_after_reindex` — clear resets the index layout

**`booklet/tests/test_booklet.py`:**
- `test_prune` — removed `reindex=True` test, replaced with prune-again test
- `test_prune_fixed` — same change

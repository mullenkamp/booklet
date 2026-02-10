# Plan: Implement Option 2 — Relocatable Index with Auto-Reindex

## Context
Booklet's bucket index is fixed at byte 200, so growing it requires a full file rewrite (via `prune(reindex=True)`). Option 2 allows the index to be appended at the end of the file and pointed to from the header, avoiding data movement. Auto-reindexing triggers when `n_keys > n_buckets`, using the `n_buckets_reindex` mapping for new sizes. The `reindex` parameter is removed from `prune`.

## Design Decisions (confirmed with user)
- **Trigger**: reindex when `n_keys > n_buckets` (load factor 1.0)
- **New n_buckets**: use `n_buckets_reindex` mapping; for non-mapped values, jump to the smallest mapped value larger than current
- **Max**: stop reindexing at 20,736,017 (mapping returns None)

## Header Changes

Add two 6-byte fields in unused header space (bytes 65-199):
- **Bytes 65-70**: `index_offset` — position of the bucket index (0 = legacy, means 200)
- **Bytes 71-76**: `first_data_block_pos` — start of data blocks (0 = legacy, means `200 + n_buckets * 6`)

Bump file version from 4 to 5 for new files. Version 4 files remain readable (default values for new fields).

## File Layout After Reindex

```
[Header 200] [Dead original index] [Data blocks A] [New index @ index_offset] [Data blocks B (new writes)]
```

Iteration scans two regions: `[first_data_block_pos, index_offset)` and `[index_offset + n_buckets*6, EOF)`.
If index hasn't been relocated (default), one region: `[first_data_block_pos, EOF)`.

## Reindex Algorithm (chain-following)

The reindex uses **chain-following** (not sequential scan) to build the new index. This is always correct regardless of file layout:

1. **Allocate**: Append new bucket index (initialized with 1s) at EOF
2. **Rewire**: For each bucket in the *current* index, follow the chain. For each live block, compute `new_bucket = hash % new_n_buckets`, insert at head of new bucket's chain by swapping next_ptr
3. **Skip block**: If old index was relocated (not at byte 200), write a "skip block" over it so sequential iteration can skip past it (one fake deleted block for variable-length; loop of blocks for fixed-length)
4. **Header update**: Write new `n_buckets`, `index_offset` to header. Flush.

## Changes to `utils.py`

### New constants
- `index_offset_pos = 65`
- `first_data_block_pos_pos = 71`
- `current_version = 5`
- `n_buckets_chain` — sorted list of all target n_buckets values: `[12007, 144013, 1728017, 20736017]`

### New functions
- `get_new_n_buckets(n_buckets)` — returns the next n_buckets from the chain, or None if at max
- `reindex(file, n_buckets, index_offset, first_data_block_pos, write_buffer_size, ts_bytes_len, fixed_value_len)` — the Option 2 reindex algorithm (chain-following, handles both variable and fixed)
- `write_skip_block(file, offset, dead_size, ts_bytes_len, fixed_value_len)` — writes skip block(s) over dead index area

### Modified functions (add `index_offset` parameter)
All functions that access the bucket index need `index_offset` instead of hardcoded `sub_index_init_pos`:
- `get_bucket_index_pos(index_bucket, index_offset)`
- `get_last_data_block_pos(file, key_hash, n_buckets, index_offset)`
- `contains_key(file, key_hash, n_buckets, index_offset)`
- `set_timestamp(file, key_hash, n_buckets, timestamp, index_offset)`
- `get_value(file, key_hash, n_buckets, ts_bytes_len, index_offset)`
- `get_value_ts(file, key_hash, n_buckets, ..., index_offset)`
- `assign_delete_flag(file, key_hash, n_buckets, index_offset)`
- `update_index(file, buffer_index, buffer_index_set, n_buckets, index_offset)`
- `get_value_fixed(file, key_hash, n_buckets, value_len, index_offset)`
- `write_data_blocks(...)` and `write_data_blocks_fixed(...)` — pass `index_offset` to internal `update_index` calls

### Modified iteration functions
- `iter_keys_values(file, n_buckets, ..., index_offset, first_data_block_pos)` — scan two regions if index relocated
- `iter_keys_values_fixed(file, n_buckets, ..., index_offset, first_data_block_pos)` — same

### Modified prune functions
- `prune_file(...)` — remove `reindex` param; scan two regions for reading; always write index at byte 200; update `index_offset` and `first_data_block_pos` in header; return `(n_keys, removed_count)` instead of `(n_keys, removed_count, n_buckets)`
- `prune_file_fixed(...)` — same changes

### Modified init/read functions
- `read_base_params_variable(self, ...)` — read `index_offset` and `first_data_block_pos` from header (default to computed values for version 4)
- `read_base_params_fixed(self, ...)` — same
- `init_base_params_variable(self, ...)` — write new fields to header, use version 5
- `init_base_params_fixed(self, ...)` — same

### Modified `clear`
- Reset `index_offset` and `first_data_block_pos` in header

## Changes to `main.py`

### New instance attributes
- `self._index_offset` — current index position
- `self._first_data_block_pos` — start of data blocks

### Modified `Booklet._sync_index()`
After `update_index`, check if `self._n_keys > self._n_buckets` and call `reindex()`. Update `self._n_buckets`, `self._index_offset` on the instance. Also write updated `n_keys` to header.

### Modified `Booklet.prune()`
- Remove `reindex` parameter
- After prune, reset `self._index_offset = sub_index_init_pos` and `self._first_data_block_pos`
- `n_buckets` stays the same (keeps the post-auto-reindex value)

### Modified `FixedLengthValue.prune()`
- Same: remove `reindex` parameter

### All methods that call utils functions
Pass `self._index_offset` (and `self._first_data_block_pos` for iteration) to all utils function calls. This is mechanical — every call to `contains_key`, `get_value`, `get_value_ts`, `get_value_fixed`, `assign_delete_flag`, `update_index`, `write_data_blocks`, `write_data_blocks_fixed`, `iter_keys_values`, `iter_keys_values_fixed`, `set_timestamp` gets `self._index_offset` added.

### Modified `Booklet.clear()`
Reset `self._index_offset` and `self._first_data_block_pos`

## Test Updates

Modify `booklet/tests/test_auto_reindex.py`:
- The existing tests should pass as-is (they test auto-reindex with small n_buckets)
- Add test for prune resetting the index layout
- Add test for reopen after reindex
- Add test verifying non-mapped n_buckets jumps to nearest chain value

## Verification
1. `uv run test` — run all tests
2. Specifically verify test_auto_reindex tests pass
3. Verify existing test_booklet tests still pass (backward compat)

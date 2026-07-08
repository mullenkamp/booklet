#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import os
import io
import mmap
import pathlib
# import inspect
from collections.abc import MutableMapping
from typing import Union, Any, Optional, Iterator, Iterable, Tuple
from datetime import datetime
# from threading import Lock
import portalocker
# from itertools import count
# from collections import Counter, defaultdict, deque
import orjson
import weakref
import multiprocessing

# try:
#     import fcntl
#     fcntl_import = True
# except ImportError:
#     fcntl_import = False


# import utils
from . import utils
from .parallel import _map_worker

# import serializers
# from . import serializers


# page_size = mmap.ALLOCATIONGRANULARITY

# n_keys_pos = 25



#######################################################
### Helper functions


#######################################################
### Generic class



class Booklet(MutableMapping):
    """
    Base class
    """
    def _set_file_timestamp(self, timestamp: Optional[Union[int, str, datetime]] = None):
        """
        Set the timestamp on the file.
        Accessed by self._file_timestamp
        """
        ts_int = utils.make_timestamp_int(timestamp)
        ts_int_bytes = utils.int_to_bytes(ts_int, utils.timestamp_bytes_len)

        # Moves the shared file position, so it must hold the lock like every
        # other position-mover (no mutation bump - layout-safe header write).
        with self._thread_lock:
            self._file.seek(utils.file_timestamp_pos)
            self._file.write(ts_int_bytes)

        self._file_timestamp = ts_int


    # def _get_file_timestamp(self):
    #     """
    #     Get the timestamp of the file.
    #     """
    #     self._file.seek(utils.file_timestamp_pos)
    #     ts_int_bytes = self._file


    def set_metadata(self, data: Any, timestamp: Optional[Union[int, str, datetime]] = None):
        """
        Set the metadata for the booklet.

        Parameters
        ----------
        data : any
            A JSON serializable object to be stored as metadata.
        timestamp : int, str, datetime, or None, optional
            A specific timestamp to associate with the metadata. 
            If None (default), the current time is used.
        """
        if self.writable:
            self.sync()
            with self._thread_lock:
                self._mutation_count += 1
                _ = utils.write_data_blocks(self._file,  utils.metadata_key_bytes, utils.encode_metadata(data), self._n_buckets, self._buffer_data, self._buffer_index, self._buffer_index_set, self._write_buffer_size, timestamp, self._ts_bytes_len, self._index_offset)
                if self._buffer_index:
                    utils.flush_data_buffer(self._file, self._buffer_data, self._file.seek(0, 2))
                _ = utils.update_index(self._file, self._buffer_index, self._buffer_index_set, self._n_buckets, self._index_offset)
                self._file.flush()
        else:
            raise ValueError('File is open for read only.')

    def get_metadata(self, include_timestamp: bool = False) -> Optional[Union[Any, Tuple[Any, int]]]:
        """
        Retrieve the metadata for the booklet.

        Parameters
        ----------
        include_timestamp : bool, optional
            Whether to include the timestamp in the returned output. 
            Defaults to False.

        Returns
        -------
        any or tuple or None
            The metadata object. If include_timestamp is True, returns (metadata, timestamp). 
            Returns None if no metadata is set.
        """
        if self._mmap is not None:
            output = utils.mmap_get_value_ts(self._mmap, utils.metadata_key_hash, self._n_buckets, True, include_timestamp, self._ts_bytes_len, self._index_offset)
        else:
            output = utils.get_value_ts(self._file, utils.metadata_key_hash, self._n_buckets, True, include_timestamp, self._ts_bytes_len, self._index_offset)

        if output:
            value, ts_int = output
            if ts_int is not None:
                return orjson.loads(value), ts_int
            else:
                return orjson.loads(value)
        else:
            return None

    def _pre_key(self, key: Any) -> bytes:

        ## Serialize to bytes
        try:
            key = self._key_serializer.dumps(key)
        except Exception as error:
            raise error

        return key

    def _post_key(self, key: bytes) -> Any:

        ## Serialize from bytes
        key = self._key_serializer.loads(key)

        return key

    def _pre_value(self, value: Any) -> bytes:

        ## Serialize to bytes
        try:
            value = self._value_serializer.dumps(value)
        except Exception as error:
            raise error

        return value

    def _post_value(self, value: bytes) -> Any:

        ## Serialize from bytes
        value = self._value_serializer.loads(value)

        return value

    def _iter_locked(self, make_iter) -> Iterator[Any]:
        """
        Advance a utils iterator one step at a time under _thread_lock,
        releasing the lock before every yield.

        This is what makes interleaved same-instance reads (get, [], in,
        nested iterators) safe during iteration: each step's seek+read runs
        under the lock, but the lock is never held while control is with the
        caller. The underlying iterators keep their cursor in local state and
        re-seek (or slice positionlessly) every step, so other lock-holders
        may move the shared file position between steps.

        A snapshot of _mutation_count guards the scan: any layout mutation
        while iteration is in progress raises RuntimeError at the next step
        instead of silently corrupting it. make_iter is called under the lock
        (after the pre-sync), so it captures post-sync/post-reindex layout
        offsets.

        Note: two Booklet instances sharing one BytesIO buffer bypass
        portalocker and have independent locks/counters - that configuration
        is unsupported.
        """
        if self._buffer_index_set:
            self.sync()

        with self._thread_lock:
            mut0 = self._mutation_count
            it = make_iter()

        while True:
            with self._thread_lock:
                if self._mutation_count != mut0:
                    raise RuntimeError('booklet mutated during iteration')
                try:
                    item = next(it)
                except StopIteration:
                    return
            yield item

    def keys(self) -> Iterator[Any]:
        """
        Return an iterator over the booklet's keys.

        Reads (get, [], in, nested iterators) are allowed while iterating.
        Any mutation (set/update/del/set_metadata/prune/clear, or an
        auto-reindex they trigger) invalidates open iterators, which raise
        RuntimeError at their next step. Unlike a plain dict this includes
        overwriting an EXISTING key (an overwrite appends a new data block
        that the scan walks). set_timestamp is the only write allowed during
        iteration; otherwise collect the keys into a list first. map() sits
        outside this guarantee (see its docstring).
        """
        if self._mmap is not None:
            def make_iter():
                return utils.mmap_iter_keys_values(self._mmap, self._n_buckets, True, False, False, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)
        else:
            def make_iter():
                return utils.iter_keys_values(self._file, self._n_buckets, True, False, False, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)

        for key in self._iter_locked(make_iter):
            yield self._post_key(key)

    def items(self) -> Iterator[Tuple[Any, Any]]:
        """
        Return an iterator over the booklet's (key, value) pairs.

        Same iteration semantics as keys(): interleaved reads are allowed,
        any mutation raises RuntimeError at the next step.
        """
        if self._mmap is not None:
            def make_iter():
                return utils.mmap_iter_keys_values(self._mmap, self._n_buckets, True, True, False, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)
        else:
            def make_iter():
                return utils.iter_keys_values(self._file, self._n_buckets, True, True, False, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)

        for key, value in self._iter_locked(make_iter):
            yield self._post_key(key), self._post_value(value)

    def values(self) -> Iterator[Any]:
        """
        Return an iterator over the booklet's values.

        Same iteration semantics as keys(): interleaved reads are allowed,
        any mutation raises RuntimeError at the next step.
        """
        if self._mmap is not None:
            def make_iter():
                return utils.mmap_iter_keys_values(self._mmap, self._n_buckets, False, True, False, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)
        else:
            def make_iter():
                return utils.iter_keys_values(self._file, self._n_buckets, False, True, False, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)

        for value in self._iter_locked(make_iter):
            yield self._post_value(value)

    def timestamps(self, include_value: bool = False, decode_value: bool = True) -> Iterator[Union[Tuple[Any, int], Tuple[Any, int, Any]]]:
        """
        Return an iterator for timestamps for all keys.

        Parameters
        ----------
        include_value : bool, optional
            Whether to include the value in the iterator. Defaults to False.
        decode_value : bool, optional
            Whether to decode the value using the value_serializer.
            Only relevant if include_value is True. Defaults to True.

        Yields
        ------
        tuple
            If include_value is False: (key, timestamp)
            If include_value is True: (key, timestamp, value)

        Notes
        -----
        Same iteration semantics as keys(), with one addition: set_timestamp
        writes in place, so `for key, ts in b.timestamps(): b.set_timestamp(...)`
        is a supported pattern.
        """
        if self._init_timestamps:
            if self._mmap is not None:
                def make_iter():
                    return utils.mmap_iter_keys_values(self._mmap, self._n_buckets, True, include_value, True, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)
            else:
                def make_iter():
                    return utils.iter_keys_values(self._file, self._n_buckets, True, include_value, True, self._ts_bytes_len, self._index_offset, self._first_data_block_pos)

            if include_value:
                for key, ts_int, value in self._iter_locked(make_iter):
                    if decode_value:
                        value = self._post_value(value)
                    yield self._post_key(key), ts_int, value
            else:
                for key, ts_int in self._iter_locked(make_iter):
                    yield self._post_key(key), ts_int
        else:
            raise ValueError('timestamps were not initialized with this file.')

    def __iter__(self) -> Iterator[Any]:
        return self.keys()

    def __len__(self) -> int:
        """
        Return the number of keys in the booklet.
        """
        return self._n_keys

    def __contains__(self, key: Any) -> bool:
        """
        Check if key is in the booklet.
        """
        bytes_key = self._pre_key(key)
        key_hash = utils.hash_key(bytes_key)

        if key_hash in self._buffer_index_set:
            return True

        with self._thread_lock:
            if self._mmap is not None:
                check = utils.mmap_contains_key(self._mmap, key_hash, self._n_buckets, self._index_offset)
            else:
                check = utils.contains_key(self._file, key_hash, self._n_buckets, self._index_offset)
        return check

    def get(self, key: Any, default: Any = None) -> Any:
        """
        Return the value for key if key is in the booklet, else default.

        Parameters
        ----------
        key : any
            The key to look up.
        default : any, optional
            The value to return if the key is not found. Defaults to None.

        Returns
        -------
        any
            The value associated with the key, or the default value.
        """
        key_bytes = self._pre_key(key)
        key_hash = utils.hash_key(key_bytes)

        if key_hash in self._buffer_index_set:
            self.sync()

        with self._thread_lock:
            if self._mmap is not None:
                value = utils.mmap_get_value(self._mmap, key_hash, self._n_buckets, self._ts_bytes_len, self._index_offset)
            else:
                value = utils.get_value(self._file, key_hash, self._n_buckets, self._ts_bytes_len, self._index_offset)

        if isinstance(value, bytes):
            return self._post_value(value)
        else:
            return default

    def get_items(self, keys: Iterable[Any], default: Any = None) -> Iterator[Tuple[Any, Any]]:
        """
        Return an iterator of (key, value) pairs for the given keys.

        Parameters
        ----------
        keys : iterable
            The keys to retrieve.
        default : any, optional
            The value to return for any missing keys. Defaults to None.

        Yields
        ------
        tuple
            (key, value) pairs.
        """
        for key in keys:
            value = self.get(key, default=default)
            yield key, value

    def get_timestamp(self, key: Any, include_value: bool = False, decode_value: bool = True, default: Any = None) -> Union[int, Tuple[int, Any], Any]:
        """
        Get the timestamp associated with a key.

        Parameters
        ----------
        key : any
            The key to look up.
        include_value : bool, optional
            Whether to also return the value. Defaults to False.
        decode_value : bool, optional
            Whether to decode the value. Defaults to True.
        default : any, optional
            The value to return if the key is not found. Defaults to None.

        Returns
        -------
        int or tuple or any
            The timestamp (int) if include_value is False.
            A tuple (timestamp, value) if include_value is True.
            The default value if the key is not found.
        """
        if self._init_timestamps:
            key_bytes = self._pre_key(key)
            key_hash = utils.hash_key(key_bytes)

            if key_hash in self._buffer_index_set:
                self.sync()

            with self._thread_lock:
                if self._mmap is not None:
                    output = utils.mmap_get_value_ts(self._mmap, key_hash, self._n_buckets, include_value, True, self._ts_bytes_len, self._index_offset)
                else:
                    output = utils.get_value_ts(self._file, key_hash, self._n_buckets, include_value, True, self._ts_bytes_len, self._index_offset)

            if output:
                value, ts_int = output

                if include_value:
                    if decode_value:
                        value = self._post_value(value)

                    return ts_int, value
                else:
                    return ts_int
            else:
                return default
        else:
            raise ValueError('timestamps were not initialized with this file.')

    def set_timestamp(self, key: Any, timestamp: Union[int, str, datetime]):
        """
        Set a timestamp for a specific key.

        Parameters
        ----------
        key : any
            The key to update.
        timestamp : int, str, or datetime
            The timestamp to assign. It must be either an int of the number of 
            microseconds in POSIX UTC time, an ISO 8601 datetime string with 
            timezone, or a datetime object with timezone.
        """
        if self._init_timestamps:
            if self.writable:
                key_bytes = self._pre_key(key)
                key_hash = utils.hash_key(key_bytes)

                ## Normalize to int microseconds - utils.set_timestamp writes the
                ## raw int, so the documented str/datetime forms must be converted
                ## here (same boundary as prune()).
                timestamp = utils.make_timestamp_int(timestamp)

                with self._thread_lock:
                    success = utils.set_timestamp(self._file, key_hash, self._n_buckets, timestamp, self._index_offset)

                if not success:
                    raise KeyError(key)
            else:
                raise ValueError('File is open for read only.')
        else:
            raise ValueError('timestamps were not initialized with this file.')


    def set(self, key: Any, value: Any, timestamp: Optional[Union[int, str, datetime]] = None, encode_value: bool = True):
        """
        Set a key/value pair.

        Parameters
        ----------
        key : any
            The key to set.
        value : any
            The value to set.
        timestamp : int, str, datetime, or None, optional
            A specific timestamp to associate with the key/value pair. 
            If None (default), the current time is used.
        encode_value : bool, optional
            Whether to encode the value using the value_serializer. 
            Defaults to True. If False, value must be bytes.
        """
        if self.writable:
            if encode_value:
                value = self._pre_value(value)
            elif not isinstance(value, bytes):
                raise TypeError('If encode_value is False, then value must be a bytes object.')
            with self._thread_lock:
                self._mutation_count += 1
                n_extra_keys = utils.write_data_blocks(self._file,  self._pre_key(key), value, self._n_buckets, self._buffer_data, self._buffer_index, self._buffer_index_set, self._write_buffer_size, timestamp, self._ts_bytes_len, self._index_offset)
                self._n_keys += n_extra_keys
                # self._check_auto_reindex()
        else:
            raise ValueError('File is open for read only.')


    def update(self, key_value: MutableMapping):
        """
        Update the booklet with key/value pairs from another mapping.

        Parameters
        ----------
        key_value : MutableMapping
            A mapping of key/value pairs to add to the booklet.
        """
        if self.writable:
            with self._thread_lock:
                self._mutation_count += 1
                for key, value in key_value.items():
                    n_extra_keys = utils.write_data_blocks(self._file, self._pre_key(key), self._pre_value(value), self._n_buckets, self._buffer_data, self._buffer_index, self._buffer_index_set, self._write_buffer_size, None, self._ts_bytes_len, self._index_offset)
                    self._n_keys += n_extra_keys

                # self._check_auto_reindex()

        else:
            raise ValueError('File is open for read only.')


    def prune(self, timestamp: Optional[Union[int, str, datetime]] = None) -> int:
        """
        Prune old keys and values from the booklet.

        This method removes overwritten or deleted entries, potentially reclaiming 
        disk space and improving performance. It can also remove entries older 
        than a specified timestamp.

        Parameters
        ----------
        timestamp : int, str, datetime, or None, optional
            If provided, entries older than this timestamp will be removed.

        Returns
        -------
        int
            The number of removed items.
        """
        self.sync()

        if self.writable:

            ## Normalize to int microseconds - prune_file compares raw int
            ## timestamps, so the documented str/datetime forms must be converted
            ## here. None means "no timestamp filter" and must stay None (a
            ## make_timestamp_int(None) would mean "now" and evict everything).
            if timestamp is not None:
                timestamp = utils.make_timestamp_int(timestamp)

            with self._thread_lock:
                self._mutation_count += 1
                self._compaction_count += 1
                n_keys, removed_count, new_index_offset = utils.prune_file(self._file, timestamp, self._n_buckets, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._write_buffer_size, self._ts_bytes_len, self._buffer_data, self._buffer_index, self._buffer_index_set, self._index_offset, self._first_data_block_pos)
                self._n_keys = n_keys
                self._file.seek(self._n_keys_pos)
                self._file.write(utils.int_to_bytes(self._n_keys, 4))

                # Mirror the post-prune layout written by prune_file: non-empty -> relocated index (data
                # at byte 200, index at new_index_offset); empty -> standard cleared layout.
                if new_index_offset:
                    self._index_offset = new_index_offset
                    self._first_data_block_pos = utils.sub_index_init_pos
                else:
                    self._index_offset = utils.sub_index_init_pos
                    self._first_data_block_pos = utils.sub_index_init_pos + (self._n_buckets * utils.n_bytes_file)

                self._file.flush()

            return removed_count
        else:
            raise ValueError('File is open for read only.')


    def __getitem__(self, key: Any) -> Any:
        """
        Return the value for key. Raises KeyError if not found.
        """
        value = self.get(key)

        if value is None:
            raise KeyError(key)
        else:
            return value


    def __setitem__(self, key: Any, value: Any):
        """
        Set key to value.
        """
        self.set(key, value)


    def __delitem__(self, key: Any):
        """
        Remove key from the booklet. Raises KeyError if not found.

        Delete flags are written immediately to ensure data integrity.
        """
        if self.writable:
            if self._buffer_index_set:
                self.sync()

            key_bytes = self._pre_key(key)
            key_hash = utils.hash_key(key_bytes)

            with self._thread_lock:
                del_bool = utils.assign_delete_flag(self._file, key_hash, self._n_buckets, self._index_offset)
                if del_bool:
                    self._mutation_count += 1
                    self._n_keys -= 1
                    self._file.seek(self._n_keys_pos)
                    self._file.write(utils.int_to_bytes(self._n_keys, 4))
                else:
                    raise KeyError(key)
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self) -> 'Booklet':
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        """
        Remove all keys and values from the booklet.
        """
        if self.writable:
            with self._thread_lock:
                self._mutation_count += 1
                self._compaction_count += 1
                utils.clear(self._file, self._n_buckets, self._n_keys_pos, self._write_buffer_size)
                self._n_keys = 0
                self._index_offset = utils.sub_index_init_pos
                self._first_data_block_pos = utils.sub_index_init_pos + (self._n_buckets * utils.n_bytes_file)
                self._file.seek(self._n_keys_pos)
                self._file.write(utils.int_to_bytes(self._n_keys, 4))
        else:
            raise ValueError('File is open for read only.')

    def close(self):
        """
        Sync and close the booklet file.
        """
        self.sync()
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        # self._finalizer()
        try:
            portalocker.lock(self._file, portalocker.LOCK_UN)
        except portalocker.exceptions.LockException:
            pass
        except io.UnsupportedOperation:
            pass
        self._file.close()
        self._finalizer.detach()

    # def __del__(self):
    #     self.close()
    #     self._file_path.unlink()


    def reopen(self, flag: str):
        """
        Reopen the booklet file.

        Parameters
        ----------
        flag : str
            The mode to open the file in. Must be either 'r' (read-only)
            or 'w' (read-write).
        """
        self.close()
        if flag == 'w':
            self._file = io.open(self._file_path, 'r+b', buffering=0)
            portalocker.lock(self._file, portalocker.LOCK_EX)
            self.writable = True
            self._mmap = None
        elif flag == 'r':
            self._file = io.open(self._file_path, 'rb')
            portalocker.lock(self._file, portalocker.LOCK_SH)
            self.writable = False
            self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            if hasattr(self._mmap, 'madvise') and hasattr(mmap, 'MADV_RANDOM'):
                self._mmap.madvise(mmap.MADV_RANDOM)
        else:
            raise ValueError("flag must be either 'r' or 'w'.")

        self._buffer_data = bytearray()
        self._buffer_index = bytearray()
        self._buffer_index_set = set()

        self._finalizer = weakref.finalize(self, utils.close_files, self._file, utils.n_keys_crash, self._n_keys_pos, self.writable, self._mmap)


    def sync(self):
        """
        Sync the data buffers to disk, ensuring all changes are persisted.
        """
        if self.writable:
            with self._thread_lock:
                if self._buffer_index:
                    utils.flush_data_buffer(self._file, self._buffer_data, self._file.seek(0, 2))
                    self._sync_index()
                    self._file.seek(self._n_keys_pos)
                    self._file.write(utils.int_to_bytes(self._n_keys, 4))

                # Check for auto-reindex even when buffer is empty
                # (keys may have been flushed during write_data_blocks)
                self._check_auto_reindex()
                self._file.flush()

    def _sync_index(self):
        n_extra_keys = utils.update_index(self._file, self._buffer_index, self._buffer_index_set, self._n_buckets, self._index_offset)
        self._n_keys += n_extra_keys

        self._check_auto_reindex()

    def _check_auto_reindex(self):
        if self._defer_reindex:
            return
        # Auto-reindex when load factor > 1.0
        if self._n_keys > self._n_buckets:
            new_n_buckets = utils.get_new_n_buckets(self._n_buckets)
            if new_n_buckets is not None:
                # Bare increment - the caller (sync/_sync_index) already holds
                # the non-reentrant lock; taking it again would self-deadlock.
                self._mutation_count += 1
                fixed_value_len = getattr(self, '_value_len', None)
                new_index_offset, new_first_data_block_pos = utils.reindex(
                    self._file, self._n_buckets, new_n_buckets,
                    self._index_offset, self._first_data_block_pos,
                    self._write_buffer_size, self._ts_bytes_len,
                    fixed_value_len
                )
                self._n_buckets = new_n_buckets
                self._index_offset = new_index_offset
                self._first_data_block_pos = new_first_data_block_pos

    def _iter_items_unlocked(self):
        """
        Yield (key, value) pairs, acquiring/releasing _thread_lock per block.
        Used internally by map() to allow interleaved reads and writes.
        """
        if self._buffer_index_set:
            self.sync()

        with self._thread_lock:
            comp0 = self._compaction_count
            file_end = self._file.seek(0, 2)
            n_buckets = self._n_buckets
            index_offset = self._index_offset
            first_data_block_pos = self._first_data_block_pos
            ts_bytes_len = self._ts_bytes_len

        if first_data_block_pos == 0:
            first_data_block_pos = utils.sub_index_init_pos + (n_buckets * utils.n_bytes_file)

        if index_offset != utils.sub_index_init_pos:
            regions = [
                (first_data_block_pos, index_offset),
                (index_offset + n_buckets * utils.n_bytes_file, file_end),
            ]
        else:
            regions = [(first_data_block_pos, file_end)]

        one_extra_index_bytes_len = utils.key_hash_len + utils.n_bytes_file
        init_data_block_len = one_extra_index_bytes_len + utils.n_bytes_key + utils.n_bytes_value

        for start, end in regions:
            pos = start
            while pos < end:
                with self._thread_lock:
                    if self._compaction_count != comp0:
                        raise RuntimeError('booklet compacted (prune/clear) during map() iteration')
                    self._file.seek(pos)
                    header = self._file.read(init_data_block_len)
                    next_ptr = utils.bytes_to_int(
                        header[utils.key_hash_len:one_extra_index_bytes_len]
                    )
                    key_len = utils.bytes_to_int(
                        header[one_extra_index_bytes_len:one_extra_index_bytes_len + utils.n_bytes_key]
                    )
                    value_len = utils.bytes_to_int(
                        header[one_extra_index_bytes_len + utils.n_bytes_key:]
                    )
                    ts_key_value_len = ts_bytes_len + key_len + value_len

                    if next_ptr:
                        payload = self._file.read(ts_key_value_len)
                        key_bytes = payload[ts_bytes_len:ts_bytes_len + key_len]
                        value_bytes = payload[ts_bytes_len + key_len:]
                    else:
                        key_bytes = None
                        value_bytes = None

                pos += init_data_block_len + ts_key_value_len

                if key_bytes is not None and key_bytes != utils.metadata_key_bytes:
                    yield self._post_key(key_bytes), self._post_value(value_bytes)

    def map(self, func, keys=None, n_workers=None):
        """
        Apply func to items in parallel using multiprocessing.

        Parameters
        ----------
        func : callable
            A picklable function: func(key, value) -> (new_key, new_value) or None.
            Return a (key, value) tuple to yield the result. The output key can
            differ from the input key. Return None to skip (item not yielded).
            Must be a top-level function (not a lambda or closure).
        keys : iterable, optional
            Specific keys to process. If None, iterates all keys in the booklet.
        n_workers : int, optional
            Number of worker processes. Defaults to os.cpu_count().

        Yields
        ------
        tuple
            (key, value) pairs produced by func, as they complete.

        Notes
        -----
        map() sits outside the mutation-during-iteration guarantee that
        keys()/items()/values() provide: plain writes (set/del) ARE allowed
        while a map() is running (auto-reindex is deferred for its duration).
        Only prune()/clear() invalidate a running map(), raising RuntimeError.
        """
        if self._buffer_index_set:
            self.sync()

        if n_workers is None:
            n_workers = os.cpu_count() or 4

        self._defer_reindex = True
        try:
            if keys is not None:
                item_iter = ((k, self.get(k)) for k in keys)
            else:
                item_iter = self._iter_items_unlocked()

            with multiprocessing.Pool(processes=n_workers) as pool:
                work_iter = ((func, k, v) for k, v in item_iter if v is not None)
                for result in pool.imap_unordered(_map_worker, work_iter):
                    if result is not None:
                        yield result
        finally:
            self._defer_reindex = False


#######################################################
### Variable length value Booklet


class VariableLengthValue(Booklet):
    """
    Open a persistent dictionary for reading and writing. This class allows for variable length values (and keys). On creation of the file, the serializers will be written to the file. Any subsequent reads and writes do not need to be opened with any parameters other than file_path and flag (unless a custom serializer is passed).

    Parameters
    -----------
    file_path : str or pathlib.Path
        It must be a path to a local file location. If you want to use a tempfile, then use the name from the NamedTemporaryFile initialized class.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    key_serializer : str, class, or None
        The serializer to use to convert the input value to bytes. Run the booklet.available_serializers to determine the internal serializers that are available. None will require bytes as input. A custom serializer class can also be used. If the objects can be serialized to json, then use orjson or msgpack. They are super fast and you won't have the pickle issues.
        If a custom class is passed, then it must have dumps and loads methods.

    value_serializer : str, class, or None
        Similar to the key_serializer, except for the values.

    n_buckets : int
        The number of hash buckets to using in the indexing. Generally use the same number of buckets as you expect for the total number of keys.

    buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    Returns
    -------
    Booklet

    The optional *flag* argument can be:

    +---------+-------------------------------------------+
    | Value   | Meaning                                   |
    +=========+===========================================+
    | ``'r'`` | Open existing database for reading only   |
    |         | (default)                                 |
    +---------+-------------------------------------------+
    | ``'w'`` | Open existing database for reading and    |
    |         | writing                                   |
    +---------+-------------------------------------------+
    | ``'c'`` | Open database for reading and writing,    |
    |         | creating it if it doesn't exist           |
    +---------+-------------------------------------------+
    | ``'n'`` | Always create a new, empty database, open |
    |         | for reading and writing                   |
    +---------+-------------------------------------------+

    """
    def __init__(self, file_path: Union[str, pathlib.Path, io.BytesIO], flag: str = "r", key_serializer: Optional[Union[str, Any]] = None, value_serializer: Optional[Union[str, Any]] = None, n_buckets: int=12007, buffer_size: int = 2**22, init_timestamps: bool = True, init_bytes: Optional[bytes] = None):
        """
        Initialize a VariableLengthValue booklet.

        Parameters
        ----------
        file_path : str, pathlib.Path, or io.BytesIO
            Path to the booklet file or a BytesIO object.
        flag : str, optional
            Mode to open the file ('r', 'w', 'c', 'n'). Defaults to 'r'.
        key_serializer : str, class, or None, optional
            Serializer for keys. Defaults to None (bytes).
        value_serializer : str, class, or None, optional
            Serializer for values. Defaults to None (bytes).
        n_buckets : int, optional
            Initial number of hash buckets. Defaults to 12007.
        buffer_size : int, optional
            Write buffer size in bytes. Defaults to 4MB (2**22).
        init_timestamps : bool, optional
            Whether to enable timestamp support. Defaults to True.
        init_bytes : bytes, optional
            Initial bytes to write to a new file. Defaults to None.
        """
        self._defer_reindex = False
        utils.init_files_variable(self, file_path, flag, key_serializer, value_serializer, n_buckets, buffer_size, init_timestamps, init_bytes)


### Alias
# VariableValue = Booklet


#######################################################
### Fixed length value Booklet


class FixedLengthValue(Booklet):
    """
    Open a persistent dictionary for reading and writing. This class required a globally fixed value length. For example, this can be used for fixed length hashes or timestamps. On creation of the file, the serializers will be written to the file. Any subsequent reads and writes do not need to be opened with any parameters other than file_path and flag.

    Parameters
    -----------
    file_path : str or pathlib.Path
        It must be a path to a local file location. If you want to use a tempfile, then use the name from the NamedTemporaryFile initialized class.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    key_serializer : str, class, or None
        The serializer to use to convert the input value to bytes. Run the booklet.available_serializers to determine the internal serializers that are available. None will require bytes as input. A custom serializer class can also be used. If the objects can be serialized to json, then use orjson or msgpack. They are super fast and you won't have the pickle issues.
        If a custom class is passed, then it must have dumps and loads methods.

    value_len : int
        The number of bytes that all values will have.

    buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    Returns
    -------
    Booklet

    The optional *flag* argument can be:

    +---------+-------------------------------------------+
    | Value   | Meaning                                   |
    +=========+===========================================+
    | ``'r'`` | Open existing database for reading only   |
    |         | (default)                                 |
    +---------+-------------------------------------------+
    | ``'w'`` | Open existing database for reading and    |
    |         | writing                                   |
    +---------+-------------------------------------------+
    | ``'c'`` | Open database for reading and writing,    |
    |         | creating it if it doesn't exist           |
    +---------+-------------------------------------------+
    | ``'n'`` | Always create a new, empty database, open |
    |         | for reading and writing                   |
    +---------+-------------------------------------------+

    """
    def __init__(self, file_path: Union[str, pathlib.Path, io.BytesIO], flag: str = "r", key_serializer: Optional[Union[str, Any]] = None, value_len: Optional[int] = None, n_buckets: int=12007, buffer_size: int = 2**22, init_bytes: Optional[bytes] = None):
        """
        Initialize a FixedLengthValue booklet.

        Parameters
        ----------
        file_path : str, pathlib.Path, or io.BytesIO
            Path to the booklet file or a BytesIO object.
        flag : str, optional
            Mode to open the file ('r', 'w', 'c', 'n'). Defaults to 'r'.
        key_serializer : str, class, or None, optional
            Serializer for keys. Defaults to None (bytes).
        value_len : int, optional
            Fixed length of values in bytes. Required for new files.
        n_buckets : int, optional
            Initial number of hash buckets. Defaults to 12007.
        buffer_size : int, optional
            Write buffer size in bytes. Defaults to 4MB (2**22).
        init_bytes : bytes, optional
            Initial bytes to write to a new file. Defaults to None.
        """
        self._defer_reindex = False
        utils.init_files_fixed(self, file_path, flag, key_serializer, value_len, n_buckets, buffer_size, init_bytes)


    def set_metadata(self, data: Any, timestamp: Optional[Union[int, str, datetime]] = None):
        """
        Not supported on fixed-length booklets.
        """
        # The base-class metadata write goes through the variable-length block
        # writer, whose framing (per-entry value length) fixed-stride iteration
        # cannot parse - a single metadata write silently corrupts keys()/items()
        # for the whole file. Fail loud instead.
        raise NotImplementedError(
            'Metadata is not supported on fixed-length booklets: the metadata write '
            'path would corrupt fixed-stride iteration. Use a variable-length booklet '
            'if you need metadata.'
        )


    def _pre_value(self, value: Any) -> bytes:
        # Fixed-stride iteration derives every block boundary from value_len -
        # a value of any other length silently corrupts the whole scan, so
        # validate centrally for every write entry point that serializes.
        value = super()._pre_value(value)
        if len(value) != self._value_len:
            raise ValueError(f'Value must serialize to exactly {self._value_len} bytes, got {len(value)}.')
        return value


    def set(self, key: Any, value: Any, timestamp: Optional[Union[int, str, datetime]] = None, encode_value: bool = True):
        """
        Set a key/value pair.

        Parameters
        ----------
        key : any
            The key to set.
        value : any
            The value to set. It must serialize to exactly value_len bytes.
        timestamp : None
            Fixed-length booklets do not support per-key timestamps; anything
            other than None raises ValueError.
        encode_value : bool, optional
            Whether to encode the value using the value_serializer.
            Defaults to True. If False, value must be bytes.
        """
        # The inherited Booklet.set writes variable-length framing (a value_len
        # field + optional timestamp bytes) that fixed-stride iteration cannot
        # parse - it would silently corrupt the file (same family as the
        # set_metadata guard above).
        if self.writable:
            if timestamp is not None:
                raise ValueError('Fixed-length booklets do not support timestamps.')
            if encode_value:
                value = self._pre_value(value)
            else:
                if not isinstance(value, bytes):
                    raise TypeError('If encode_value is False, then value must be a bytes object.')
                if len(value) != self._value_len:
                    raise ValueError(f'Value must be exactly {self._value_len} bytes, got {len(value)}.')
            with self._thread_lock:
                self._mutation_count += 1
                n_extra_keys = utils.write_data_blocks_fixed(self._file, self._pre_key(key), value, self._n_buckets, self._buffer_data, self._buffer_index, self._buffer_index_set, self._write_buffer_size, self._index_offset)
                self._n_keys += n_extra_keys
        else:
            raise ValueError('File is open for read only.')


    def keys(self) -> Iterator[Any]:
        """
        Return an iterator over the booklet's keys.

        Same iteration semantics as the variable-length class: interleaved
        reads are allowed, any mutation raises RuntimeError at the next step.
        """
        if self._mmap is not None:
            def make_iter():
                return utils.mmap_iter_keys_values_fixed(self._mmap, self._n_buckets, True, False, self._value_len, self._index_offset, self._first_data_block_pos)
        else:
            def make_iter():
                return utils.iter_keys_values_fixed(self._file, self._n_buckets, True, False, self._value_len, self._index_offset, self._first_data_block_pos)

        for key in self._iter_locked(make_iter):
            yield self._post_key(key)

    def items(self) -> Iterator[Tuple[Any, Any]]:
        """
        Return an iterator over the booklet's (key, value) pairs.

        Same iteration semantics as the variable-length class: interleaved
        reads are allowed, any mutation raises RuntimeError at the next step.
        """
        if self._mmap is not None:
            def make_iter():
                return utils.mmap_iter_keys_values_fixed(self._mmap, self._n_buckets, True, True, self._value_len, self._index_offset, self._first_data_block_pos)
        else:
            def make_iter():
                return utils.iter_keys_values_fixed(self._file, self._n_buckets, True, True, self._value_len, self._index_offset, self._first_data_block_pos)

        for key, value in self._iter_locked(make_iter):
            yield self._post_key(key), self._post_value(value)

    def values(self) -> Iterator[Any]:
        """
        Return an iterator over the booklet's values.

        Same iteration semantics as the variable-length class: interleaved
        reads are allowed, any mutation raises RuntimeError at the next step.
        """
        if self._mmap is not None:
            def make_iter():
                return utils.mmap_iter_keys_values_fixed(self._mmap, self._n_buckets, False, True, self._value_len, self._index_offset, self._first_data_block_pos)
        else:
            def make_iter():
                return utils.iter_keys_values_fixed(self._file, self._n_buckets, False, True, self._value_len, self._index_offset, self._first_data_block_pos)

        for value in self._iter_locked(make_iter):
            yield self._post_value(value)

    def _iter_items_unlocked(self):
        if self._buffer_index_set:
            self.sync()

        with self._thread_lock:
            comp0 = self._compaction_count
            file_end = self._file.seek(0, 2)
            n_buckets = self._n_buckets
            index_offset = self._index_offset
            first_data_block_pos = self._first_data_block_pos
            value_len = self._value_len

        if first_data_block_pos == 0:
            first_data_block_pos = utils.sub_index_init_pos + (n_buckets * utils.n_bytes_file)

        one_extra_index_bytes_len = utils.key_hash_len + utils.n_bytes_file
        init_data_block_len = one_extra_index_bytes_len + utils.n_bytes_key

        if index_offset != utils.sub_index_init_pos:
            regions = [
                (first_data_block_pos, index_offset),
                (index_offset + n_buckets * utils.n_bytes_file, file_end),
            ]
        else:
            regions = [(first_data_block_pos, file_end)]

        for start, end in regions:
            pos = start
            while pos < end:
                with self._thread_lock:
                    if self._compaction_count != comp0:
                        raise RuntimeError('booklet compacted (prune/clear) during map() iteration')
                    self._file.seek(pos)
                    header = self._file.read(init_data_block_len)
                    next_ptr = utils.bytes_to_int(
                        header[utils.key_hash_len:one_extra_index_bytes_len]
                    )
                    key_len = utils.bytes_to_int(header[one_extra_index_bytes_len:])

                    if next_ptr:
                        kv = self._file.read(key_len + value_len)
                        key_bytes = kv[:key_len]
                        value_bytes = kv[key_len:]
                    else:
                        key_bytes = None
                        value_bytes = None

                pos += init_data_block_len + key_len + value_len

                if key_bytes is not None:
                    yield self._post_key(key_bytes), self._post_value(value_bytes)

    def get(self, key: Any, default: Any = None) -> Any:
        """
        Return the value for key if key is in the booklet, else default.

        Parameters
        ----------
        key : any
            The key to look up.
        default : any, optional
            The value to return if the key is not found. Defaults to None.

        Returns
        -------
        any
            The value associated with the key, or the default value.
        """
        key_bytes = self._pre_key(key)
        key_hash = utils.hash_key(key_bytes)

        if key_hash in self._buffer_index_set:
            self.sync()

        with self._thread_lock:
            if self._mmap is not None:
                value = utils.mmap_get_value_fixed(self._mmap, key_hash, self._n_buckets, self._value_len, self._index_offset)
            else:
                value = utils.get_value_fixed(self._file, key_hash, self._n_buckets, self._value_len, self._index_offset)

        if isinstance(value, bytes):
            return self._post_value(value)
        else:
            return default

    # def __len__(self):
    #     return self._n_keys

    def update(self, key_value_dict: MutableMapping):
        """
        Update the booklet with key/value pairs from another mapping.

        Parameters
        ----------
        key_value_dict : MutableMapping
            A mapping of key/value pairs to add to the booklet.
        """
        if self.writable:
            with self._thread_lock:
                self._mutation_count += 1
                for key, value in key_value_dict.items():
                    n_extra_keys = utils.write_data_blocks_fixed(self._file, self._pre_key(key), self._pre_value(value), self._n_buckets, self._buffer_data, self._buffer_index, self._buffer_index_set, self._write_buffer_size, self._index_offset)
                    self._n_keys += n_extra_keys

        else:
            raise ValueError('File is open for read only.')


    def prune(self) -> int:
        """
        Prune old keys and values from the booklet.

        This method removes overwritten or deleted entries, potentially reclaiming 
        disk space and improving performance.

        Returns
        -------
        int
            The number of removed items.
        """
        self.sync()

        if self.writable:
            with self._thread_lock:
                self._mutation_count += 1
                self._compaction_count += 1
                n_keys, removed_count, new_index_offset = utils.prune_file_fixed(self._file, self._n_buckets, self._n_bytes_file, self._n_bytes_key, self._value_len, self._write_buffer_size, self._buffer_data, self._buffer_index, self._buffer_index_set, self._index_offset, self._first_data_block_pos)
                self._n_keys = n_keys
                self._file.seek(self._n_keys_pos)
                self._file.write(utils.int_to_bytes(self._n_keys, 4))

                # Mirror the post-prune layout written by prune_file_fixed: non-empty -> relocated index
                # (data at byte 200, index at new_index_offset); empty -> standard cleared layout.
                if new_index_offset:
                    self._index_offset = new_index_offset
                    self._first_data_block_pos = utils.sub_index_init_pos
                else:
                    self._index_offset = utils.sub_index_init_pos
                    self._first_data_block_pos = utils.sub_index_init_pos + (self._n_buckets * utils.n_bytes_file)

                self._file.flush()

                return removed_count
        else:
            raise ValueError('File is open for read only.')


    def __getitem__(self, key: Any) -> Any:
        """
        Return the value for key. Raises KeyError if not found.
        """
        value = self.get(key)

        if value is None:
            raise KeyError(key)
        else:
            return value


    def __setitem__(self, key: Any, value: Any):
        """
        Set key to value.
        """
        # Delegates to the fixed-framing set() above (which validates the
        # value length and bumps the mutation counter).
        self.set(key, value)


#####################################################
### Default "open" should be the variable value class


def open(
    file_path: Union[str, pathlib.Path, io.BytesIO], flag: str = "r", key_serializer: Optional[Union[str, Any]] = None, value_serializer: Optional[Union[str, Any]] = None, n_buckets: int=12007, buffer_size: int = 2**22, init_timestamps: bool = True, init_bytes: Optional[bytes] = None) -> VariableLengthValue:
    """
    Open a persistent dictionary for reading and writing.

    On creation of the file, the serializers will be written to the file. 
    Any subsequent reads and writes do not need to be opened with any 
    parameters other than file_path and flag.

    Parameters
    ----------
    file_path : str, pathlib.Path, or io.BytesIO
        Path to the booklet file or a BytesIO object.
    flag : str, optional
        Flag associated with how the file is opened:
        'r': Open existing database for reading only (default).
        'w': Open existing database for reading and writing.
        'c': Open database for reading and writing, creating it if it 
             doesn't exist.
        'n': Always create a new, empty database, open for reading 
             and writing.
    key_serializer : str, class, or None, optional
        The serializer to use to convert the input key to bytes. 
        None (default) will require bytes as input. 
        Supported built-in serializers: 'str', 'pickle', 'json', 'orjson', 
        'uint1', 'int1', etc.
    value_serializer : str, class, or None, optional
        Similar to the key_serializer, except for the values.
    n_buckets : int, optional
        The number of hash buckets to use in the indexing. 
        Defaults to 12007.
    buffer_size : int, optional
        The write buffer size in bytes. Defaults to 4MB (2**22).
    init_timestamps : bool, optional
        Whether to enable timestamp support for keys. Defaults to True.
    init_bytes : bytes, optional
        Initial bytes to write to a new file. Defaults to None.

    Returns
    -------
    Booklet
        A Booklet object (specifically a VariableLengthValue instance).
    """
    return VariableLengthValue(file_path, flag, key_serializer, value_serializer, n_buckets, buffer_size, init_timestamps, init_bytes)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
# import io
# import mmap
import pathlib
# import inspect
from collections.abc import MutableMapping
from typing import Union
# from threading import Lock
import portalocker
from itertools import count
from collections import Counter, defaultdict, deque
import orjson
import datetime
import time
# import weakref
# from multiprocessing import Manager, shared_memory

# try:
#     import fcntl
#     fcntl_import = True
# except ImportError:
#     fcntl_import = False


# import utils
from . import utils

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
    def set_metadata(self, data, timestamp=None):
        """
        Sets the metadata for the booklet. The data input must be a json serializable object.
        """
        if self.writable:
            self.sync()
            with self._thread_lock:
                _ = utils.write_data_blocks(self._file,  utils.metadata_key_bytes, utils.encode_metadata(data), self._n_buckets, self._buffer_data, self._buffer_index, self._write_buffer_size, timestamp, self._ts_bytes_len)
                if self._buffer_index:
                    utils.flush_data_buffer(self._file, self._buffer_data, self._file.seek(0, 2))
                _ = utils.update_index(self._file, self._buffer_index, self._n_buckets)
                self._file.flush()
        else:
            raise ValueError('File is open for read only.')

    def get_metadata(self, include_timestamp=False):
        """

        """
        output = utils.get_value_ts(self._file, utils.metadata_key_bytes, self._n_buckets, True, include_timestamp, self._ts_bytes_len)

        if output:
            value, ts_int = output
            if value and ts_int:
                return orjson.loads(value), ts_int
            else:
                return orjson.loads(value)
        else:
            return None

    def _pre_key(self, key) -> bytes:

        ## Serialize to bytes
        try:
            key = self._key_serializer.dumps(key)
        except Exception as error:
            raise error

        return key

    def _post_key(self, key: bytes):

        ## Serialize from bytes
        key = self._key_serializer.loads(key)

        return key

    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        try:
            value = self._value_serializer.dumps(value)
        except Exception as error:
            raise error

        return value

    def _post_value(self, value: bytes):

        ## Serialize from bytes
        value = self._value_serializer.loads(value)

        return value

    def keys(self):
        for key in utils.iter_keys_values(self._file, self._n_buckets, True, False, False, self._ts_bytes_len):
            yield self._post_key(key)

    def items(self):
        for key, value in utils.iter_keys_values(self._file, self._n_buckets, True, True, False, self._ts_bytes_len):
            yield self._post_key(key), self._post_value(value)

    def values(self):
        for value in utils.iter_keys_values(self._file, self._n_buckets, False, True, False, self._ts_bytes_len):
            yield self._post_value(value)

    def timestamps(self, include_value=False):
        if self._init_timestamps:
            if include_value:
                for key, ts_int, value in utils.iter_keys_values(self._file, self._n_buckets, True, True, True, self._ts_bytes_len):
                    yield self._post_key(key), ts_int, self._post_value(value)
            else:
                for key, ts_int in utils.iter_keys_values(self._file, self._n_buckets, True, False, True, self._ts_bytes_len):
                    yield self._post_key(key), ts_int
        else:
            raise ValueError('timestamps were not initialized with this file.')

    def __iter__(self):
        return self.keys()

    def __len__(self):
        # counter = count()
        # deque(zip(self.keys(), counter), maxlen=0)

        # return next(counter)

        # len1 = (len(self._index_mmap) - self._index_n_bytes_skip - (self._n_buckets*utils.n_bytes_index))/(utils.n_bytes_file + utils.key_hash_len)

        # return int(len1 - self._n_deletes)

        return self._n_keys

    def __contains__(self, key):
        bytes_key = self._pre_key(key)
        hash_key = utils.hash_key(bytes_key)

        return utils.contains_key(self._file, hash_key, self._n_buckets)

    def get(self, key, default=None):
        value = utils.get_value(self._file, self._pre_key(key), self._n_buckets, self._ts_bytes_len)

        if value:
            return self._post_value(value)
        else:
            return default

    def get_timestamp(self, key, include_value=False, default=None):
        """

        """
        if self._init_timestamps:
            output = utils.get_value_ts(self._file, self._pre_key(key), self._n_buckets, include_value, True, self._ts_bytes_len)

            if output:
                value, ts_int = output
                if value:
                    return ts_int, self._post_value(value)
                else:
                    return ts_int
            else:
                return default
        else:
            raise ValueError('timestamps were not initialized with this file.')

    def set_timestamp(self, key, timestamp):
        """
        timestamp should be an int of the number of microseconds from UTC unix time.
        """
        if self._init_timestamps:
            if self.writable:
                success = utils.set_timestamp(self._file, self._pre_key(key), self._n_buckets, timestamp)

                if not success:
                    raise KeyError(key)
            else:
                raise ValueError('File is open for read only.')
        else:
            raise ValueError('timestamps were not initialized with this file.')


    def set(self, key, value, timestamp=None, encode_value=True):
        """
        timestamp should be an int of the number of microseconds from UTC unix time.
        """
        if self.writable:
            with self._thread_lock:
                if encode_value:
                    value = self._pre_value(value)
                n_extra_keys = utils.write_data_blocks(self._file,  self._pre_key(key), value, self._n_buckets, self._buffer_data, self._buffer_index, self._write_buffer_size, timestamp, self._ts_bytes_len)
                self._n_keys += n_extra_keys
        else:
            raise ValueError('File is open for read only.')


    def update(self, key_value_dict):
        """

        """
        if self.writable:
            with self._thread_lock:
                for key, value in key_value_dict.items():
                    n_extra_keys = utils.write_data_blocks(self._file, self._pre_key(key), self._pre_value(value), self._n_buckets, self._buffer_data, self._buffer_index, self._write_buffer_size, None, self._ts_bytes_len)
                    self._n_keys += n_extra_keys

        else:
            raise ValueError('File is open for read only.')


    def prune(self, timestamp: int=None, reindex=False):
        """
        Prunes the old keys and associated values. Returns the number of removed items.
        """
        if self.writable:
            with self._thread_lock:
                n_keys, removed_count, n_buckets = utils.prune_file(self._file, timestamp, reindex, self._n_buckets, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._write_buffer_size, self._ts_bytes_len, self._buffer_data, self._buffer_index)
                self._n_keys = n_keys

                if n_buckets != self._n_buckets:
                    self._n_buckets = n_buckets
                    self._file.seek(21)
                    self._file.write(utils.int_to_bytes(n_buckets, 4))
                    self._file.flush()

                return removed_count
        else:
            raise ValueError('File is open for read only.')


    def __getitem__(self, key):
        value = self.get(key)

        if value is None:
            raise KeyError(key)
        else:
            return value


    def __setitem__(self, key, value):
        self.set(key, value)


    def __delitem__(self, key):
        """
        Delete flags are written immediately as are the number of total deletes. This ensures that there are no sync issues. Deletes are generally rare, so this shouldn't impact most use cases.
        """
        if self.writable:
            self.sync()
            with self._thread_lock:
                del_bool = utils.assign_delete_flag(self._file, self._pre_key(key), self._n_buckets)
                if del_bool:
                    self._n_keys -= 1
                    # self._file.seek(self._n_deletes_pos)
                    # self._file.write(utils.int_to_bytes(self._n_deletes, 4))
                else:
                    raise KeyError(key)
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        if self.writable:
            with self._thread_lock:
                utils.clear(self._file, self._n_buckets, self._n_keys_pos, self._write_buffer_size)
                self._n_keys = 0

        else:
            raise ValueError('File is open for read only.')

    def close(self):
        self.sync()
        portalocker.lock(self._file, portalocker.LOCK_UN)
        self._file.close()
        self._finalizer.detach()

    # def __del__(self):
    #     self.close()
    #     self._file_path.unlink()


    def sync(self):
        if self.writable:
            with self._thread_lock:
                if self._buffer_index:
                    utils.flush_data_buffer(self._file, self._buffer_data, self._file.seek(0, 2))
                self._sync_index()
                self._file.seek(self._n_keys_pos)
                self._file.write(utils.int_to_bytes(self._n_keys, 4))
                self._file.flush()

    def _sync_index(self):
        n_extra_keys = utils.update_index(self._file, self._buffer_index, self._n_buckets)
        self._n_keys += n_extra_keys
        # self._index_mmap.flush()

        # n_keys = len(self)
        # if n_keys > self._n_buckets*10:
        #     self._reindex()

    # def _reindex(self):
    #     """

    #     """
    #     self._n_buckets = utils.reindex(self._index_mmap, self._n_bytes_index, self._n_bytes_file, self._n_buckets, len(self))
    #     self._n_deletes = 0
    #     self._file.seek(21)
    #     self._file.write(utils.int_to_bytes(self._n_buckets, 4))





#######################################################
### Variable length value Booklet


class VariableValue(Booklet):
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
    def __init__(self, file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, value_serializer: str = None, n_buckets: int=12007, buffer_size: int = 2**22, init_timestamps=False):
        """

        """
        utils.init_files_variable(self, file_path, flag, key_serializer, value_serializer, n_buckets, buffer_size, init_timestamps, utils.tz_offset)


### Alias
# VariableValue = Booklet


#######################################################
### Fixed length value Booklet


class FixedValue(Booklet):
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
    def __init__(self, file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, value_len: int=None, n_buckets: int=12007, buffer_size: int = 2**22):
        """

        """
        utils.init_files_fixed(self, file_path, flag, key_serializer, value_len, n_buckets, buffer_size, utils.tz_offset)


    def keys(self):
        for key in utils.iter_keys_values_fixed(self._file, self._n_buckets, True, False, self._value_len):
            yield self._post_key(key)

    def items(self):
        for key, value in utils.iter_keys_values_fixed(self._file, self._n_buckets, True, True, self._value_len):
            yield self._post_key(key), self._post_value(value)

    def values(self):
        for value in utils.iter_keys_values_fixed(self._file, self._n_buckets, False, True, self._value_len):
            yield self._post_value(value)

    def get(self, key, default=None):
        value = utils.get_value_fixed(self._file, self._pre_key(key), self._n_buckets, self._value_len)

        if not value:
            return default
        else:
            return self._post_value(value)

    # def __len__(self):
    #     return self._n_keys

    def update(self, key_value_dict):
        """

        """
        if self.writable:
            with self._thread_lock:
                for key, value in key_value_dict.items():
                    n_extra_keys = utils.write_data_blocks_fixed(self._file, self._pre_key(key), self._pre_value(value), self._n_buckets, self._buffer_data, self._buffer_index, self._write_buffer_size)
                    self._n_keys += n_extra_keys

        else:
            raise ValueError('File is open for read only.')


    # def prune(self):
    #     """
    #     Prunes the old keys and associated values. Returns the recovered space in bytes.
    #     """
    #     if self.writable:
    #         with self._thread_lock:
    #             recovered_space = utils.prune_file_fixed(self._file, self._index_mmap, self._n_buckets, self._n_bytes_index, self._n_bytes_file, self._n_bytes_key, self._value_len, self._write_buffer_size, self._index_n_bytes_skip)
    #     else:
    #         raise ValueError('File is open for read only.')

    #     return recovered_space


    def __getitem__(self, key):
        value = utils.get_value_fixed(self._file, self._pre_key(key), self._n_buckets, self._value_len)

        if not value:
            raise KeyError(key)
        else:
            return self._post_value(value)


    def __setitem__(self, key, value):
        if self.writable:
            with self._thread_lock:
                n_extra_keys = utils.write_data_blocks_fixed(self._file, self._pre_key(key), self._pre_value(value), self._n_buckets, self._buffer_data, self._buffer_index, self._write_buffer_size)
                self._n_keys += n_extra_keys

        else:
            raise ValueError('File is open for read only.')





#####################################################
### Default "open" should be the variable length class


def open(
    file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, value_serializer: str = None, n_buckets: int=12007, buffer_size: int = 2**22):
    """
    Open a persistent dictionary for reading and writing. On creation of the file, the serializers will be written to the file. Any subsequent reads and writes do not need to be opened with any parameters other than file_path and flag.

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
    return VariableValue(file_path, flag, key_serializer, value_serializer, n_buckets, buffer_size)

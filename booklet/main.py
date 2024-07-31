#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import io
import mmap
import pathlib
import inspect
from collections.abc import MutableMapping
from typing import Any, Generic, Iterator, Union
from threading import Lock
import portalocker
from itertools import count
from collections import Counter, defaultdict, deque
import weakref
# from multiprocessing import Manager, shared_memory

# try:
#     import fcntl
#     fcntl_import = True
# except ImportError:
#     fcntl_import = False


import utils
# from . import utils

# import serializers
# from . import serializers


# page_size = mmap.ALLOCATIONGRANULARITY

# n_keys_pos = 25


#######################################################
### Generic class



class Booklet(MutableMapping):
    """
    Base class
    """
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
        for key in utils.iter_keys_values(self._file, self._data_end_pos, self._write, self._n_buckets, True, False, self._n_bytes_key, self._n_bytes_value):
            yield self._post_key(key)

    def items(self):
        for key, value in utils.iter_keys_values(self._file, self._data_end_pos, self._write, self._n_buckets, True, True, self._n_bytes_key, self._n_bytes_value):
            yield self._post_key(key), self._post_value(value)

    def values(self):
        for value in utils.iter_keys_values(self._file, self._data_end_pos, self._write, self._n_buckets, False, True, self._n_bytes_key, self._n_bytes_value):
            yield self._post_value(value)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        # counter = count()
        # deque(zip(self.keys(), counter), maxlen=0)

        # return next(counter)

        # if self._write:
        #     self._file.seek(0, 2)
        #     data_pos = self._file.tell()
        # else:
        #     data_pos = self._data_end_pos

        len1 = (len(self._index_mmap) - self._index_n_bytes_skip - (self._n_buckets*utils.n_bytes_index))/(utils.n_bytes_file + utils.key_hash_len)

        return int(len1 - self._n_deletes)

    def __contains__(self, key):
        bytes_key = self._pre_key(key)
        hash_key = utils.hash_key(bytes_key)

        return utils.contains_key(self._index_mmap, hash_key, self._n_bytes_index, self._n_bytes_file, self._n_buckets, self._index_n_bytes_skip)

    def get(self, key, default=None):
        value = utils.get_value(self._index_mmap, self._file, self._pre_key(key), self._n_bytes_index, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets, self._index_n_bytes_skip)

        if not value:
            return default
        else:
            return self._post_value(value)

    def update(self, key_value_dict):
        """

        """
        if self._write:
            with self._thread_lock:
                for key, value in key_value_dict.items():
                    n_deletes = utils.write_data_blocks(self._file, self._index_mmap, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._n_bytes_value, self._n_buckets, self._index_n_bytes_skip, self._buffer_index, self._write_buffer, self._write_buffer_size)
                    self._n_deletes += n_deletes

        else:
            raise ValueError('File is open for read only.')


    def prune(self):
        """
        Prunes the old keys and associated values. Returns the recovered space in bytes.
        """
        if self._write:
            with self._thread_lock:
                recovered_space = utils.prune_file(self._mm, self._n_buckets, self._n_bytes_index, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._data_pos)
        else:
            raise ValueError('File is open for read only.')

        return recovered_space


    def __getitem__(self, key):
        value = utils.get_value(self._index_mmap, self._file, self._pre_key(key), self._n_bytes_index, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets, self._index_n_bytes_skip)

        if not value:
            raise KeyError(key)
        else:
            return self._post_value(value)


    def __setitem__(self, key, value):
        if self._write:
            with self._thread_lock:
                n_deletes = utils.write_data_blocks(self._file, self._index_mmap, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._n_bytes_value, self._n_buckets, self._index_n_bytes_skip, self._buffer_index, self._write_buffer, self._write_buffer_size)
                self._n_deletes += n_deletes

        else:
            raise ValueError('File is open for read only.')


    def __delitem__(self, key):
        """
        Delete flags are written immediately as are the number of total deletes. This ensures that there are no sync issues. Deletes are generally rare, so this shouldn't impact most use cases.
        """
        if self._write:
            self.sync()
            with self._thread_lock:
                del_bool = utils.assign_delete_flags(self._index_mmap, self._file, self._pre_key(key), self._n_buckets, self._n_bytes_index, self._n_bytes_file, self._index_n_bytes_skip)
                if del_bool:
                    self._n_deletes += 1
                    self._file.seek(self._n_deletes_pos)
                    self._file.write(utils.int_to_bytes(self._n_deletes, 4))
                else:
                    raise KeyError(key)
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        if self._write:
            with self._thread_lock:
                utils.clear(self._mm, self._n_buckets, self._n_bytes_index, self._n_deletes_pos)
                # self._data_pos = len(self._mm)
        else:
            raise ValueError('File is open for read only.')

    def close(self):
        if not self._index_mmap.closed:
            self.sync()
            # if self._write:
            #     self._write_buffer.close()
        self._finalizer()

    # def __del__(self):
    #     self.close()
    #     self._file_path.unlink()


    def sync(self):
        if self._write:
            with self._thread_lock:
                if self._buffer_index:
                    utils.flush_write_buffer(self._file, self._write_buffer)
                self._sync_index()
                self._file.flush()

    def _sync_index(self):
        n_deletes = utils.update_index(self._file, self._buffer_index, self._index_mmap, self._n_bytes_index, self._n_bytes_file, self._n_buckets, self._index_n_bytes_skip)
        self._n_deletes += n_deletes
        self._index_mmap.flush()

        # n_keys = len(self)
        # if n_keys > self._n_buckets*10:
        #     self._data_pos, self._n_buckets = utils.reindex(self._mm, self._data_pos, self._n_bytes_index, self._n_bytes_file, self._n_buckets, n_keys)



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

    write_buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    key_serializer : str, class, or None
        The serializer to use to convert the input value to bytes. Run the booklet.available_serializers to determine the internal serializers that are available. None will require bytes as input. A custom serializer class can also be used. If the objects can be serialized to json, then use orjson or msgpack. They are super fast and you won't have the pickle issues.
        If a custom class is passed, then it must have dumps and loads methods.

    value_serializer : str, class, or None
        Similar to the key_serializer, except for the values.

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
    def __init__(self, file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, value_serializer: str = None, write_buffer_size: int = 2**22):
        """

        """
        utils.init_files_variable(self, file_path, flag, key_serializer, value_serializer, write_buffer_size)


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

    write_buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    key_serializer : str, class, or None
        The serializer to use to convert the input value to bytes. Run the booklet.available_serializers to determine the internal serializers that are available. None will require bytes as input. A custom serializer class can also be used. If the objects can be serialized to json, then use orjson or msgpack. They are super fast and you won't have the pickle issues.
        If a custom class is passed, then it must have dumps and loads methods.

    value_serializer : str, class, or None
        Similar to the key_serializer, except for the values.

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
    def __init__(self, file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, write_buffer_size: int = 5000000, value_len: int=None):
        """

        """
        fp = pathlib.Path(file_path)

        if flag == "r":  # Open existing database for reading only (default)
            write = False
            fp_exists = True
        elif flag == "w":  # Open existing database for reading and writing
            write = True
            fp_exists = True
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            fp_exists = fp.exists()
            write = True
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            write = True
            fp_exists = False
        else:
            raise ValueError("Invalid flag")

        self._write = write
        self._write_buffer_size = write_buffer_size
        self._write_buffer_pos = 0
        self._file_path = fp
        n_buckets = 10007
        # self._n_keys_pos = utils.n_keys_pos_dict['fixed']

        ## Load or assign encodings and attributes
        if fp_exists:
            if write:
                self._file = io.open(file_path, 'r+b')

                ## Locks
                # if fcntl_import:
                #     fcntl.flock(self._file, fcntl.LOCK_EX)
                portalocker.lock(self._file, portalocker.LOCK_EX)

                self._thread_lock = Lock()

                ## Write buffers
                self._mm = mmap.mmap(self._file.fileno(), 0)
                self._write_buffer = mmap.mmap(-1, write_buffer_size)
                self._buffer_index = []
            else:
                self._file = io.open(file_path, 'rb')
                # if fcntl_import:
                #     fcntl.flock(self._file, fcntl.LOCK_SH)
                portalocker.lock(self._file, portalocker.LOCK_SH)
                self._mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)

            self._finalizer = weakref.finalize(self, utils.close_file, self._mm, self._file)

            ## Pull out base parameters
            base_param_bytes = self._mm.read(utils.sub_index_init_pos)

            # TODO: Run uuid and version check
            sys_uuid = base_param_bytes[:16]
            if sys_uuid != utils.uuid_fixed_blt:
                portalocker.lock(self._file, portalocker.LOCK_UN)
                raise TypeError('This is not the correct file type.')

            version = utils.bytes_to_int(base_param_bytes[16:18])
            if version < utils.version:
                raise ValueError('File is an older version.')

            ## Init for existing file
            utils.init_existing_fixed_booklet(self, base_param_bytes, key_serializer)

        else:
            if value_len is None:
                raise ValueError('value_len must be an int.')

            ## Init to create a new file
            utils.init_new_fixed_booklet(self, key_serializer, utils.n_bytes_file, utils.n_bytes_key, value_len, n_buckets, file_path, write_buffer_size)


    def keys(self):
        for key in utils.iter_keys_values_fixed(self._mm, self._n_buckets, self._data_pos, True, False, self._n_bytes_key, self._value_len):
            yield self._post_key(key)

    def items(self):
        for key, value in utils.iter_keys_values_fixed(self._mm, self._n_buckets, self._data_pos, True, True, self._n_bytes_key, self._value_len):
            yield self._post_key(key), self._post_value(value)

    def values(self):
        for key, value in utils.iter_keys_values_fixed(self._mm, self._n_buckets, self._data_pos, True, True, self._n_bytes_key, self._value_len):
            yield self._post_value(value)

    def get(self, key, default=None):
        value = utils.get_value_fixed(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_index, self._n_bytes_file, self._n_bytes_key, self._value_len, self._n_buckets)

        if not value:
            return default
        else:
            return self._post_value(value)

    # def __len__(self):
    #     return self._n_keys

    def update(self, key_value_dict):
        """

        """
        if self._write:
            with self._thread_lock:
                for key, value in key_value_dict.items():
                    utils.write_data_blocks_fixed(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_index, self._n_bytes_key, self._n_bytes_file, self._n_buckets)

        else:
            raise ValueError('File is open for read only.')


    def prune(self):
        """
        Prunes the old keys and associated values. Returns the recovered space in bytes.
        """
        if self._write:
            with self._thread_lock:
                recovered_space = utils.prune_file_fixed(self._mm, self._n_buckets, self._n_bytes_index, self._n_bytes_file, self._n_bytes_key, self._value_len, self._data_pos)
        else:
            raise ValueError('File is open for read only.')

        return recovered_space


    def __getitem__(self, key):
        value = utils.get_value_fixed(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_index, self._n_bytes_file, self._n_bytes_key, self._value_len, self._n_buckets)

        if not value:
            raise KeyError(str(key))
        else:
            return self._post_value(value)


    def __setitem__(self, key, value):
        if self._write:
            with self._thread_lock:
                utils.write_data_blocks_fixed(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_index, self._n_bytes_key, self._n_bytes_file, self._n_buckets)

        else:
            raise ValueError('File is open for read only.')





#####################################################
### Default "open" should be the variable length class


def open(
    file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, value_serializer: str = None, write_buffer_size: int = 5000000):
    """
    Open a persistent dictionary for reading and writing. On creation of the file, the serializers will be written to the file. Any subsequent reads and writes do not need to be opened with any parameters other than file_path and flag.

    Parameters
    -----------
    file_path : str or pathlib.Path
        It must be a path to a local file location. If you want to use a tempfile, then use the name from the NamedTemporaryFile initialized class.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    write_buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    key_serializer : str, class, or None
        The serializer to use to convert the input value to bytes. Run the booklet.available_serializers to determine the internal serializers that are available. None will require bytes as input. A custom serializer class can also be used. If the objects can be serialized to json, then use orjson or msgpack. They are super fast and you won't have the pickle issues.
        If a custom class is passed, then it must have dumps and loads methods.

    value_serializer : str, class, or None
        Similar to the key_serializer, except for the values.

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
    return VariableValue(file_path, flag, key_serializer, value_serializer, write_buffer_size)

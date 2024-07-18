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


# import utils
from . import utils

# import serializers
# from . import serializers


# page_size = mmap.ALLOCATIONGRANULARITY

# n_keys_pos = 25


#######################################################
### Generic class



class EmptyBooklet(MutableMapping):
    """
    Base class
    """


    def _pre_key(self, key) -> bytes:

        ## Serialize to bytes
        try:
            key = self._key_serializer.dumps(key)
        except Exception as exc:
            raise utils.SerializeError(exc, self)

        return key

    def _post_key(self, key: bytes):

        ## Serialize from bytes
        key = self._key_serializer.loads(key)

        return key

    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        try:
            value = self._value_serializer.dumps(value)
        except Exception as exc:
            raise utils.SerializeError(exc, self)

        return value

    def _post_value(self, value: bytes):

        ## Serialize from bytes
        value = self._value_serializer.loads(value)

        return value

    def keys(self):
        for key in utils.iter_keys_values(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, False, self._n_bytes_key, self._n_bytes_value):
            yield self._post_key(key)

    def items(self):
        for key, value in utils.iter_keys_values(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, True, self._n_bytes_key, self._n_bytes_value):
            yield self._post_key(key), self._post_value(value)

    def values(self):
        for key, value in utils.iter_keys_values(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, True, self._n_bytes_key, self._n_bytes_value):
            yield self._post_value(value)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        counter = count()
        deque(zip(self.keys(), counter), maxlen=0)

        return next(counter)

    def __contains__(self, key):
        bytes_key = self._pre_key(key)
        hash_key = utils.hash_key(bytes_key)
        return utils.contains_key(self._mm, hash_key, self._n_bytes_file, self._n_buckets)

    def get(self, key, default=None):
        value = utils.get_value(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets)

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
                    utils.write_data_blocks(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._n_bytes_value)
                    # self._n_keys += 1

        else:
            raise utils.ValueError('File is open for read only.', self)


    def prune(self):
        """
        Prunes the old keys and associated values. Returns the recovered space in bytes.
        """
        if self._write:
            with self._thread_lock:
                self._data_pos, recovered_space = utils.prune_file(self._mm, self._n_buckets, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value)
        else:
            raise utils.ValueError('File is open for read only.', self)

        return recovered_space


    def __getitem__(self, key):
        value = utils.get_value(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets)

        if not value:
            raise utils.KeyError(key, self)
        else:
            return self._post_value(value)


    def __setitem__(self, key, value):
        if self._write:
            with self._thread_lock:
                utils.write_data_blocks(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._n_bytes_value)
                # self._n_keys += 1

        else:
            raise utils.ValueError('File is open for read only.', self)


    def __delitem__(self, key):
        if self._write:
            if key not in self:
                raise utils.KeyError(key, self)

            delete_key_hash = utils.hash_key(self._pre_key(key))
            with self._thread_lock:
                self._buffer_index[delete_key_hash] = 0
                # self._n_keys -= 1
        else:
            raise utils.ValueError('File is open for read only.', self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        if self._write:
            with self._thread_lock:
                for key in self.keys():
                    delete_key_hash = utils.hash_key(self._pre_key(key))
                    self._buffer_index[delete_key_hash] = 0
                    # self._n_keys -= 1
            self.sync()
        else:
            raise utils.ValueError('File is open for read only.', self)

    def close(self):
        if not self._mm.closed:
            self.sync()
            if self._write:
                self._write_buffer.close()
            # if fcntl_import:
            #     fcntl.flock(self._file, fcntl.LOCK_UN)
            # portalocker.lock(self._file, portalocker.LOCK_UN)
        self._finalizer()

    # def __del__(self):
    #     self.close()
    #     self._file_path.unlink()


    def sync(self):
        if self._write:
            with self._thread_lock:
                if self._buffer_index:
                    utils.flush_write_buffer(self._mm, self._write_buffer)
                    self._sync_index()
                # self._mm.seek(self._n_keys_pos)
                # self._mm.write(utils.int_to_bytes(self._n_keys, 4))

                self._mm.flush()
                self._file.flush()

    def _sync_index(self):
        self._data_pos = utils.update_index(self._mm, self._buffer_index, self._data_pos, self._n_bytes_file, self._n_buckets)
        self._buffer_index = {}



#######################################################
### Variable length value Booklet


class Booklet(EmptyBooklet):
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

    n_bytes_file : int
        The number of bytes to represent an integer of the max size of the file. For example, the default of 4 can allow for a file size of ~4.3 GB. A value of 5 can allow for a file size of 1.1 TB. You shouldn't need a bigger value than 5...

    n_bytes_key : int
        The number of bytes to represent an integer of the max length of each key.

    n_bytes_value : int
        The number of bytes to represent an integer of the max length of each value.

    n_buckets : int
        The number of hash buckets to put all of the kay hashes for the "hash table". This number should be at least ~2 magnitudes under the max number of keys expected to be in the db. Below ~3 magnitudes then you'll get poorer read performance. Just keep the number of buckets at approximately the number of keys you expect to have.

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
    def __init__(self, file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, value_serializer: str = None, write_buffer_size: int = 5000000, n_bytes_file: int=4, n_bytes_key: int=1, n_bytes_value: int=4, n_buckets:int =10007):
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
            raise utils.ValueError("Invalid flag")

        self._write = write
        self._write_buffer_size = write_buffer_size
        self._write_buffer_pos = 0
        self._file_path = fp
        self._n_keys_pos = utils.n_keys_pos_dict['variable']

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
                self._buffer_index = {}
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
            if sys_uuid != utils.uuid_variable_blt:
                portalocker.lock(self._file, portalocker.LOCK_UN)
                raise utils.TypeError('This is not the correct file type.', self)

            version = utils.bytes_to_int(base_param_bytes[16:18])
            if version < utils.version:
                raise ValueError('File is an older version.')

            ## Init for existing file
            utils.init_existing_variable_booklet(self, base_param_bytes, key_serializer, value_serializer, self._n_keys_pos)

        else:
            ## Init to create a new file
            utils.init_new_variable_booklet(self, key_serializer, value_serializer, self._n_keys_pos, n_bytes_file, n_bytes_key, n_bytes_value, n_buckets, file_path, write_buffer_size)


### Alias
VariableValue = Booklet


#######################################################
### Fixed length value Booklet


class FixedValue(EmptyBooklet):
    """
    Open a persistent dictionary for reading and writing. This class required a globally fixed value length. For example, for fixed length hashes. On creation of the file, the serializers will be written to the file. Any subsequent reads and writes do not need to be opened with any parameters other than file_path and flag (unless a custom serializer is passed).

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

    n_bytes_file : int
        The number of bytes to represent an integer of the max size of the file. For example, the default of 4 can allow for a file size of ~4.3 GB. A value of 5 can allow for a file size of 1.1 TB. You shouldn't need a bigger value than 5...

    n_bytes_key : int
        The number of bytes to represent an integer of the max length of each key.

    n_bytes_value : int
        The number of bytes to represent an integer of the max length of each value.

    n_buckets : int
        The number of hash buckets to put all of the kay hashes for the "hash table". This number should be at least ~2 magnitudes under the max number of keys expected to be in the db. Below ~3 magnitudes then you'll get poorer read performance. Just keep the number of buckets at approximately the number of keys you expect to have.

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
    def __init__(self, file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, write_buffer_size: int = 5000000, n_bytes_file: int=4, n_bytes_key: int=1, value_len: int=None, n_buckets:int =10007):
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
            raise utils.ValueError("Invalid flag")

        self._write = write
        self._write_buffer_size = write_buffer_size
        self._write_buffer_pos = 0
        self._file_path = fp
        self._n_keys_pos = utils.n_keys_pos_dict['fixed']

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
                self._buffer_index = {}
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
                raise utils.TypeError('This is not the correct file type.', self)

            version = utils.bytes_to_int(base_param_bytes[16:18])
            if version < utils.version:
                raise ValueError('File is an older version.')

            ## Init for existing file
            utils.init_existing_fixed_booklet(self, base_param_bytes, key_serializer, self._n_keys_pos)

        else:
            if value_len is None:
                raise utils.ValueError('value_len must be an int.', self)

            ## Init to create a new file
            utils.init_new_fixed_booklet(self, key_serializer, self._n_keys_pos, n_bytes_file, n_bytes_key, value_len, n_buckets, file_path, write_buffer_size)


    def keys(self):
        for key in utils.iter_keys_values_fixed(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, False, self._n_bytes_key, self._value_len):
            yield self._post_key(key)
    
    def items(self):
        for key, value in utils.iter_keys_values_fixed(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, True, self._n_bytes_key, self._value_len):
            yield self._post_key(key), self._post_value(value)
    
    def values(self):
        for key, value in utils.iter_keys_values_fixed(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, True, self._n_bytes_key, self._value_len):
            yield self._post_value(value)

    def get(self, key, default=None):
        value = utils.get_value_fixed(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_file, self._n_bytes_key, self._value_len, self._n_buckets)

        if not value:
            return default
        else:
            return self._post_value(value)

    def __len__(self):
        return self._n_keys

    def update(self, key_value_dict):
        """

        """
        if self._write:
            with self._thread_lock:
                for key, value in key_value_dict.items():
                    n_new_keys = utils.write_data_blocks_fixed(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._value_len, self._n_bytes_file, self._n_buckets)
                    self._n_keys += n_new_keys

        else:
            raise utils.ValueError('File is open for read only.', self)


    def prune(self):
        """
        Prunes the old keys and associated values. Returns the recovered space in bytes.
        """
        if self._write:
            with self._thread_lock:
                self._data_pos, recovered_space = utils.prune_file_fixed(self._mm, self._n_buckets, self._n_bytes_file, self._n_bytes_key, self._value_len)
        else:
            raise utils.ValueError('File is open for read only.', self)

        return recovered_space


    def __getitem__(self, key):
        value = utils.get_value_fixed(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_file, self._n_bytes_key, self._value_len, self._n_buckets)

        if not value:
            raise utils.KeyError(key, self)
        else:
            return self._post_value(value)


    def __setitem__(self, key, value):
        if self._write:
            with self._thread_lock:
                n_new_keys = utils.write_data_blocks_fixed(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._value_len, self._n_bytes_file, self._n_buckets)
                self._n_keys += n_new_keys

        else:
            raise utils.ValueError('File is open for read only.', self)


    def __delitem__(self, key):
        if self._write:
            if key not in self:
                raise utils.KeyError(key, self)

            delete_key_hash = utils.hash_key(self._pre_key(key))
            with self._thread_lock:
                self._buffer_index[delete_key_hash] = 0
                self._n_keys -= 1
        else:
            raise utils.ValueError('File is open for read only.', self)


    def clear(self):
        if self._write:
            with self._thread_lock:
                for key in self.keys():
                    delete_key_hash = utils.hash_key(self._pre_key(key))
                    self._buffer_index[delete_key_hash] = 0
                    self._n_keys -= 1
            self.sync()
        else:
            raise utils.ValueError('File is open for read only.', self)

    def sync(self):
        if self._write:
            with self._thread_lock:
                if self._buffer_index:
                    utils.flush_write_buffer(self._mm, self._write_buffer)
                    self._sync_index()
                self._mm.seek(self._n_keys_pos)
                self._mm.write(utils.int_to_bytes(self._n_keys, 4))
                self._mm.flush()
                self._file.flush()



#####################################################
### Default "open" should be the variable length class


def open(
    file_path: Union[str, pathlib.Path], flag: str = "r", key_serializer: str = None, value_serializer: str = None, write_buffer_size: int = 5000000, n_bytes_file: int=4, n_bytes_key: int=1, n_bytes_value: int=4, n_buckets:int =10007):
    """
    Open a persistent dictionary for reading and writing. On creation of the file, the serializers will be written to the file. Any subsequent reads and writes do not need to be opened with any parameters other than file_path and flag (unless a custom serializer is passed).

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

    n_bytes_file : int
        The number of bytes to represent an integer of the max size of the file. For example, the default of 4 can allow for a file size of ~4.3 GB. A value of 5 can allow for a file size of 1.1 TB. You shouldn't need a bigger value than 5...

    n_bytes_key : int
        The number of bytes to represent an integer of the max length of each key.

    n_bytes_value : int
        The number of bytes to represent an integer of the max length of each value.

    n_buckets : int
        The number of hash buckets to put all of the kay hashes for the "hash table". This number should be at least ~2 magnitudes under the max number of keys expected to be in the db. Below ~3 magnitudes then you'll get poorer read performance. Just keep the number of buckets at approximately the number of keys you expect to have.

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

    return Booklet(file_path, flag, key_serializer, value_serializer, write_buffer_size, n_bytes_file, n_bytes_key, n_bytes_value, n_buckets)

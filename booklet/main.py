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
# from multiprocessing import Manager, shared_memory
import portalocker

# try:
#     import fcntl
#     fcntl_import = True
# except ImportError:
#     fcntl_import = False


# import utils
from . import utils

# import serializers
from . import serializers

uuid_blt = b'O~\x8a?\xe7\\GP\xadC\nr\x8f\xe3\x1c\xfe'
version = 1
version_bytes = version.to_bytes(2, 'little', signed=False)

# page_size = mmap.ALLOCATIONGRANULARITY

n_keys_pos = 25

#######################################################
### Classes


class Booklet(MutableMapping):
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
            raise ValueError("Invalid flag")

        self._write = write
        self._write_buffer_size = write_buffer_size
        self._write_buffer_pos = 0
        self._file_path = fp

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
                self._data_block_rel_pos_delete_bytes = utils.int_to_bytes(0, n_bytes_file)
            else:
                self._file = io.open(file_path, 'rb')
                # if fcntl_import:
                #     fcntl.flock(self._file, fcntl.LOCK_SH)
                portalocker.lock(self._file, portalocker.LOCK_SH)
                self._mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)

            ## Pull out base parameters
            base_param_bytes = self._mm.read(utils.sub_index_init_pos)

            # TODO: Run uuid and version check
            sys_uuid = base_param_bytes[:16]
            version = utils.bytes_to_int(base_param_bytes[16:18])

            self._n_bytes_file = utils.bytes_to_int(base_param_bytes[18:19])
            self._n_bytes_key = utils.bytes_to_int(base_param_bytes[19:20])
            self._n_bytes_value = utils.bytes_to_int(base_param_bytes[20:21])
            self._n_buckets = utils.bytes_to_int(base_param_bytes[21:25])
            self._n_keys = utils.bytes_to_int(base_param_bytes[n_keys_pos:29])
            saved_value_serializer = utils.bytes_to_int(base_param_bytes[29:31])
            saved_key_serializer = utils.bytes_to_int(base_param_bytes[31:33])

            data_index_pos = utils.get_data_index_pos(self._n_buckets, self._n_bytes_file)
            self._data_pos = utils.get_data_pos(self._mm, data_index_pos, self._n_bytes_file)

            ## Pull out the serializers
            if saved_value_serializer > 0:
                self._value_serializer = serializers.serial_int_dict[saved_value_serializer]
            # elif value_serializer is None:
            #     raise ValueError('value serializer must be a serializer class with dumps and loads methods.')
            elif inspect.isclass(value_serializer):
                class_methods = dir(value_serializer)
                if ('dumps' in class_methods) and ('loads' in class_methods):
                    self._value_serializer = value_serializer
                else:
                    raise ValueError('If a custom class is passed for a serializer, then it must have dumps and loads methods.')
            else:
                raise ValueError('How did you mess up value_serializer so bad?!')

            if saved_key_serializer > 0:
                self._key_serializer = serializers.serial_int_dict[saved_key_serializer]
            # elif key_serializer is None:
            #     raise ValueError('key serializer must be a serializer class with dumps and loads methods.')
            elif inspect.isclass(key_serializer):
                class_methods = dir(key_serializer)
                if ('dumps' in class_methods) and ('loads' in class_methods):
                    self._key_serializer = key_serializer
                else:
                    raise ValueError('If a custom class is passed for a serializer, then it must have dumps and loads methods.')
            else:
                raise ValueError('How did you mess up key_serializer so bad?!')

        else:
            ## Value Serializer
            if value_serializer in serializers.serial_name_dict:
                value_serializer_code = serializers.serial_name_dict[value_serializer]
                self._value_serializer = serializers.serial_int_dict[value_serializer_code]
            elif inspect.isclass(value_serializer):
                class_methods = dir(value_serializer)
                if ('dumps' in class_methods) and ('loads' in class_methods):
                    self._value_serializer = value_serializer
                    value_serializer_code = 0
                else:
                    raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.')
            else:
                raise ValueError('value serializer must be one of None, {}, or a serializer class with dumps and loads methods.'.format(', '.join(serializers.serial_name_dict.keys())))

            ## Key Serializer
            if key_serializer in serializers.serial_name_dict:
                key_serializer_code = serializers.serial_name_dict[key_serializer]
                self._key_serializer = serializers.serial_int_dict[key_serializer_code]
            elif inspect.isclass(key_serializer):
                class_methods = dir(key_serializer)
                if ('dumps' in class_methods) and ('loads' in class_methods):
                    self._key_serializer = key_serializer
                    key_serializer_code = 0
                else:
                    raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.')
            else:
                raise ValueError('key serializer must be one of None, {}, or a serializer class with dumps and loads methods.'.format(', '.join(serializers.serial_name_dict.keys())))

            ## Write uuid, version, and other parameters and save encodings to new file
            self._n_bytes_file = n_bytes_file
            self._n_bytes_key = n_bytes_key
            self._n_bytes_value = n_bytes_value
            self._n_buckets = n_buckets
            self._n_keys = 0
            self._data_block_rel_pos_delete_bytes = utils.int_to_bytes(0, n_bytes_file)

            n_bytes_file_bytes = utils.int_to_bytes(n_bytes_file, 1)
            n_bytes_key_bytes = utils.int_to_bytes(n_bytes_key, 1)
            n_bytes_value_bytes = utils.int_to_bytes(n_bytes_value, 1)
            n_buckets_bytes = utils.int_to_bytes(n_buckets, 4)
            n_keys_bytes = utils.int_to_bytes(self._n_keys, 4)
            saved_value_serializer_bytes = utils.int_to_bytes(value_serializer_code, 2)
            saved_key_serializer_bytes = utils.int_to_bytes(key_serializer_code, 2)

            bucket_bytes = utils.create_initial_bucket_indexes(n_buckets, n_bytes_file)

            self._file = io.open(file_path, 'w+b')

            ## Locks
            # if fcntl_import:
            #     fcntl.flock(self._file, fcntl.LOCK_EX)
            portalocker.lock(self._file, portalocker.LOCK_EX)
            self._thread_lock = Lock()

            with self._thread_lock:
                _ = self._file.write(uuid_blt + version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + n_bytes_value_bytes + n_buckets_bytes + n_keys_bytes +  saved_value_serializer_bytes + saved_key_serializer_bytes + bucket_bytes)
                self._file.flush()

                self._write_buffer = mmap.mmap(-1, write_buffer_size)
                self._buffer_index = {}

                self._mm = mmap.mmap(self._file.fileno(), 0)
                self._data_pos = len(self._mm)


    def _pre_key(self, key) -> bytes:

        ## Serialize to bytes
        key = self._key_serializer.dumps(key)

        return key

    def _post_key(self, key: bytes):

        ## Serialize from bytes
        key = self._key_serializer.loads(key)

        return key

    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        value = self._value_serializer.dumps(value)

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
        return self._n_keys

    def __contains__(self, key):
        return utils.contains_key(self._mm, self._pre_key(key), self._n_bytes_file, self._n_buckets)

    def get(self, key, default=None):
        value = utils.get_value(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets)

        if value is None:
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
                    self._n_keys += 1

        else:
            raise ValueError('File is open for read only.')


    def prune(self):
        """
        Prunes the old keys and associated values. Returns the recovered space in bytes.
        """
        if self._write:
            with self._thread_lock:
                self._data_pos, recovered_space = utils.prune_file(self._mm, self._n_buckets, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value)
        else:
            raise ValueError('File is open for read only.')

        return recovered_space


    def __getitem__(self, key):
        value = utils.get_value(self._mm, self._pre_key(key), self._data_pos, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets)

        if value is None:
            raise KeyError(key)
        else:
            return self._post_value(value)


    def __setitem__(self, key, value):
        if self._write:
            with self._thread_lock:
                utils.write_data_blocks(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._n_bytes_value)
                self._n_keys += 1

        else:
            raise ValueError('File is open for read only.')


    def __delitem__(self, key):
        if self._write:
            if key not in self:
                raise KeyError(key)

            delete_key_hash = utils.hash_key(self._pre_key(key))
            with self._thread_lock:
                self._buffer_index[delete_key_hash] = 0
                self._n_keys -= 1
        else:
            raise ValueError('File is open for read only.')

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
                    self._n_keys -= 1
            self.sync()
        else:
            raise ValueError('File is open for read only.')

    def close(self):
        self.sync()
        if self._write:
            self._write_buffer.close()
        # if fcntl_import:
        #     fcntl.flock(self._file, fcntl.LOCK_UN)
        portalocker.lock(self._file, portalocker.LOCK_UN)

        self._mm.close()
        self._file.close()

    # def __del__(self):
    #     self.close()
    #     self._file_path.unlink()


    def sync(self):
        if self._write:
            with self._thread_lock:
                if self._buffer_index:
                    utils.flush_write_buffer(self._mm, self._write_buffer)
                    self._sync_index()
                self._mm.seek(n_keys_pos)
                self._mm.write(utils.int_to_bytes(self._n_keys, 4))
                self._mm.flush()
                self._file.flush()

    def _sync_index(self):
        self._data_pos = utils.update_index(self._mm, self._buffer_index, self._data_pos, self._n_bytes_file, self._n_buckets, self._n_keys)
        self._buffer_index = {}



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

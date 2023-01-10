#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import io
import mmap
import pickle
import json
import pathlib
import inspect
import gzip
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union
# from multiprocessing import shared_memory
# from hashlib import blake2b

import utils
# from . import utils

imports = {}
try:
    import orjson
    imports['orjson'] = True
except:
    imports['orjson'] = False
# try:
#     import zstandard as zstd
#     imports['zstd'] = True
# except:
#     imports['zstd'] = False
# try:
#     import lz4
#     imports['lz4'] = True
# except:
#     imports['lz4'] = False


__all__ = ['Arete']

hidden_keys = (b'01~._value_serializer', b'02~._key_serializer')

uuid_arete = b'O~\x8a?\xe7\\GP\xadC\nr\x8f\xe3\x1c\xfe'
# special_bytes = b'\xff\xff\xff\xff\xff\xff\xff\xff\xff'
version = 1
version_bytes = version.to_bytes(2, 'little', signed=False)

# lock_bytes = (-1).to_bytes(1, 'little', signed=True)
# unlock_bytes = (0).to_bytes(1, 'little', signed=True)
# stale_key_bytes = (0).to_bytes(8, 'little', signed=True)

# page_size = mmap.ALLOCATIONGRANULARITY

#######################################################
### Serializers and compressors

## Serializers
class Pickle:
    def dumps(self, obj):
        return pickle.dumps(obj, 5)
    def loads(self, obj):
        return pickle.loads(obj)


class Json:
    def dumps(obj: Any) -> bytes:
        return json.dumps(obj).encode()
    def loads(obj):
        return json.loads(obj.decode())


class Orjson:
    def dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY)
    def loads(obj):
        return orjson.loads(obj)


class Str:
    def dumps(obj):
        return obj.encode()
    def loads(obj):
        return obj.decode()


# class Numpy:
#     def dumps(obj: np.ndarray) -> bytes:
#         return json.dumps(obj).tobytes()
#     def loads(obj):
#         return np.frombuffer(obj)


## Compressors
# class Gzip:
#     def __init__(self, compress_level):
#         self.compress_level = compress_level
#     def compress(self, obj):
#         return gzip.compress(obj, self.compress_level)
#     def decompress(self, obj):
#         return gzip.decompress(obj)


# class Zstd:
#     def __init__(self, compress_level):
#         self.compress_level = compress_level
#     def compress(self, obj):
#         return zstd.compress(obj, self.compress_level)
#     def decompress(self, obj):
#         return zstd.decompress(obj)


# class Lz4:
#     def __init__(self, compress_level):
#         self.compress_level = compress_level
#     def compress(self, obj):
#         return lz4.frame.compress(obj, self.compress_level)
#     def decompress(self, obj):
#         return lz4.frame.decompress(obj)


#######################################################
### Classes

# class _ClosedDict(MutableMapping):
#     'Marker for a closed dict.  Access attempts raise a ValueError.'

#     def closed(self, *args):
#         raise ValueError('invalid operation on closed shelf')
#     __iter__ = __len__ = __getitem__ = __setitem__ = __delitem__ = keys = closed

#     def __repr__(self):
#         return '<Closed Dictionary>'


# file_path = '/media/nvme1/cache/arete/test.arete'
file_path = '/media/nvme1/git/nzrec/data/node.arete'
n_bytes_file = 4
n_bytes_key=1
n_bytes_value=4
n_buckets = 11
# key = b'$\xd4\xb2o^\x15\xce*\x02\xa3\x1b'
write_buffer_size = 5000000
# flag = 'n'
flag = 'r'
sync: bool = False
# lock: bool = True
# serializer = 'pickle'
protocol: int = 5
# compressor = None
# compress_level: int = 1
# index_serializer = 'str'

key = b'00~._serializer'
# value = pickle.dumps(Pickle(protocol), protocol)


class Arete(MutableMapping):
    """

    """
    def __init__(self, file_path: str, flag: str = "r", value_serializer = None, key_serializer = None, write_buffer_size = 5000000, n_bytes_file=4, n_bytes_key=1, n_bytes_value=4, n_buckets=10000):
        """

        """
        if flag == "r":  # Open existing database for reading only (default)
            write = False
            fp_exists = True
        elif flag == "w":  # Open existing database for reading and writing
            write = True
            fp_exists = True
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            fp = pathlib.Path(file_path)
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

        ## Load or assign encodings and attributes
        if fp_exists:
            if write:
                self._file = io.open(file_path, 'r+b')
                self._mm = mmap.mmap(self._file.fileno(), 0)
                self._write_buffer = mmap.mmap(-1, write_buffer_size)
                self._buffer_index = {}
            else:
                self._file = io.open(file_path, 'rb')
                self._mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)

            ## Pull out base parameters
            base_param_bytes = self._mm.read(utils.sub_index_init_pos)
            sys_uuid = base_param_bytes[:16]
            version = utils.bytes_to_int(base_param_bytes[16:18])
            self._n_bytes_file = utils.bytes_to_int(base_param_bytes[18:19])
            self._n_bytes_key = utils.bytes_to_int(base_param_bytes[19:20])
            self._n_bytes_value = utils.bytes_to_int(base_param_bytes[20:21])
            self._n_buckets = utils.bytes_to_int(base_param_bytes[21:24])

            data_index_pos = utils.get_data_index_pos(self._n_buckets, self._n_bytes_file)
            self._data_pos = utils.get_data_pos(self._mm, data_index_pos, self._n_bytes_file)

            ## Pull out the serializers
            self._value_serializer = pickle.loads(utils.get_value(self._mm, b'01~._value_serializer', self._data_pos, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets))
            self._key_serializer = pickle.loads(utils.get_value(self._mm, b'02~._key_serializer', self._data_pos, self._n_bytes_file, self._n_bytes_key, self._n_bytes_value, self._n_buckets))

        else:
            ## Value Serializer
            if value_serializer is None:
                self._value_serializer = None
            elif value_serializer == 'str':
                self._value_serializer = Str
            elif value_serializer == 'pickle':
                self._value_serializer = Pickle
            elif value_serializer == 'json':
                self._value_serializer = Json
            elif value_serializer == 'orjson':
                if imports['orjson']:
                    self._value_serializer = Orjson
                else:
                    raise ValueError('orjson could not be imported.')
            elif inspect.isclass(value_serializer):
                class_methods = dir(value_serializer)
                if ('dumps' in class_methods) and ('loads' in class_methods):
                    self._value_serializer = value_serializer
                else:
                    raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.')
            else:
                raise ValueError('value serializer must be one of None, str, pickle, json, orjson, or a serializer class with dumps and loads methods.')

            ## Key Serializer
            if key_serializer is None:
                self._key_serializer = None
            elif key_serializer == 'str':
                self._key_serializer = Str
            elif key_serializer == 'pickle':
                self._key_serializer = Pickle
            elif key_serializer == 'json':
                self._key_serializer = Json
            elif key_serializer == 'orjson':
                if imports['orjson']:
                    self._key_serializer = Orjson
                else:
                    raise ValueError('orjson could not be imported.')
            elif inspect.isclass(key_serializer):
                class_methods = dir(key_serializer)
                if ('dumps' in class_methods) and ('loads' in class_methods):
                    self._key_serializer = key_serializer
                else:
                    raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.')
            else:
                raise ValueError('serializer must be one of None, str, pickle, json, orjson, or a serializer class with dumps and loads methods.')

            ## Write uuid, version, and other parameters and save encodings to new file
            self._n_bytes_file = n_bytes_file
            self._n_bytes_key = n_bytes_key
            self._n_bytes_value = n_bytes_value
            self._n_buckets = n_buckets

            n_bytes_file_bytes = utils.int_to_bytes(n_bytes_file, 1)
            n_bytes_key_bytes = utils.int_to_bytes(n_bytes_key, 1)
            n_bytes_value_bytes = utils.int_to_bytes(n_bytes_value, 1)
            n_buckets_bytes = utils.int_to_bytes(n_buckets, 3)

            bucket_bytes = utils.create_initial_bucket_indexes(n_buckets, n_bytes_file)

            self._file = io.open(file_path, 'w+b', buffering=write_buffer_size)

            _ = self._file.write(uuid_arete + version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + n_bytes_value_bytes + n_buckets_bytes + bucket_bytes)
            self._file.flush()

            self._write_buffer = mmap.mmap(-1, write_buffer_size)
            self._buffer_index = {}

            self._mm = mmap.mmap(self._file.fileno(), 0)
            self._data_pos = len(self._mm)

            utils.write_data_blocks(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, b'01~._value_serializer', pickle.dumps(self._value_serializer, 5), self._n_bytes_key, self._n_bytes_value)
            utils.write_data_blocks(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, b'02~._key_serializer', pickle.dumps(self._key_serializer, 5), self._n_bytes_key, self._n_bytes_value)

            self.sync()


    def _pre_key(self, key) -> bytes:

        ## Serialize to bytes
        if self._key_serializer is not None:
            key = self._key_serializer.dumps(key)

        return key

    def _post_key(self, key: bytes):

        ## Serialize from bytes
        if self._key_serializer is not None:
            key = self._key_serializer.loads(key)

        return key

    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        if self._value_serializer is not None:
            value = self._value_serializer.dumps(value)

        return value

    def _post_value(self, value: bytes):

        ## Serialize from bytes
        if self._value_serializer is not None:
            value = self._value_serializer.loads(value)

        return value

    def keys(self):
        for key in utils.iter_keys_values(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, False, self._n_bytes_key, self._n_bytes_value):
            if key not in hidden_keys:
                yield self._post_key(key)

    def items(self):
        for key, value in utils.iter_keys_values(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, True, self._n_bytes_key, self._n_bytes_value):
            if key not in hidden_keys:
                yield self._post_key(key), self._post_value(value)

    def values(self):
        for key, value in utils.iter_keys_values(self._mm, self._n_buckets, self._n_bytes_file, self._data_pos, True, True, self._n_bytes_key, self._n_bytes_value):
            if key not in hidden_keys:
                yield self._post_value(value)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        keys_len = len(self.keys())
        return keys_len - len(hidden_keys)

    def __contains__(self, key):
        return key in self.keys()

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
            for key, value in key_value_dict.items():
                self[key] = value

            self.sync()
        else:
            raise ValueError('File is open for read only.')

    # def _write_many_chunks(self, key_value_dict):
    #     """

    #     """
    #     self._file.seek(0, 2)
    #     file_pos = self._file.tell()

    #     write_bytes = bytearray()
    #     for key, value in key_value_dict.items():
    #         value = self._pre_value(value)
    #         value_len_bytes = len(value)

    #         key_len_bytes = len(key)
    #         key_hash = blake2b(key, digest_size=11).digest()

    #         if key_hash in self._buffer_index:
    #             _ = self._buffer_index.pop(key_hash)

    #         # if key in self.index:
    #         #     pos0 = self.index.pop(key)
    #         #     self.index['00~._stale'].append(pos0)

    #         self._buffer_index[key_hash] = file_pos
    #         file_pos += 1 + key_len_bytes + 4 + value_len_bytes

    #         write_bytes += key_len_bytes.to_bytes(1, 'little', signed=False) + key + value_len_bytes.to_bytes(4, 'little', signed=False) + value

    #     new_n_bytes = self._file.write(write_bytes)

    #     return new_n_bytes


    # def prune(self):
    #     """
    #     Prunes the old keys and associated values. Returns the recovered space in bytes.
    #     """
    #     if self._write and self.index['00~._stale']:
    #         recovered_space = utils.prune_file(self._file, self.index)
    #     else:
    #         raise ValueError('File is open for read only.')

    #     return recovered_space





    # def reorganize(self):
    #     """
    #     Only applies to gdbm.
    #     If you have carried out a lot of deletions and would like to shrink the space used by the gdbm file, this routine will reorganize the database. gdbm objects will not shorten the length of a database file except by using this reorganization; otherwise, deleted file space will be kept and reused as new (key, value) pairs are added.
    #     """
    #     if hasattr(self.env, 'reorganize'):
    #         self.env.reorganize()
    #     else:
    #         raise ValueError('reorganize is unavailable.')
    #     return

    def __getitem__(self, key):

        return self.get(key)

    def __setitem__(self, key, value):
        if self._write:
            utils.write_data_blocks(self._mm, self._write_buffer, self._write_buffer_size, self._buffer_index, self._data_pos, self._pre_key(key), self._pre_value(value), self._n_bytes_key, self._n_bytes_value)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        if self._write:
            if key not in self:
                raise KeyError(key)

            key_hash = utils.hash_key(key)
            self._buffer_index[key_hash] = 0
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        if self._write:
            for key in self.keys():
                key_hash = utils.hash_key(key)
                self._buffer_index[key_hash] = 0
            self.sync()
        else:
            raise ValueError('File is open for read only.')

    def close(self):
        self.sync()
        if self._write:
            self._write_buffer.close()

        self._mm.close()
        self._file.close()

    # def __del__(self):
    #     self.close()

    def sync(self):
        if self._write:
            if self._buffer_index:
                utils.flush_write_buffer(self._mm, self._write_buffer)
                self._sync_index()
            self._mm.flush()
            self._file.flush()

    def _sync_index(self):
        self._data_pos = utils.update_index(self._mm, self._buffer_index, self._data_pos, self._n_bytes_file, self._n_buckets)



# def open(
#     file_path: str, flag: str = "r", sync: bool = False, lock: bool = True, serializer = None, protocol: int = 5, compressor = None, compress_level: int = 1, index_serializer = None):
#     """
#     Open a persistent dictionary for reading and writing. On creation of the file, the encodings (serializer and compressor) will be written to the file. Any reads and new writes do not need to be opened with the encoding parameters. Currently, ShockDB uses pickle to serialize the encodings to the file.

#     Parameters
#     -----------
#     file_path : str or pathlib.Path
#         It must be a path to a local file location.

#     flag : str
#         Flag associated with how the file is opened according to the dbm style. See below for details.

#     serializer : str, class, or None
#         The serializer to use to convert the input object to bytes. Currently, must be one of pickle, json, orjson, or None. If the objects can be serialized to json, then use orjson. It's super fast and you won't have the pickle issues.
#         If None, then the input values must be bytes.
#         A class with dumps and loads methods can also be passed as a custom serializer.

#     protocol : int
#         The pickle protocol to use.

#     compressor : str, class, or None
#         The compressor to use to compress the pickle object before being written. Currently, only zstd is accepted.
#         The amount of compression will vary wildly depending on the input object and the serializer used. It's definitely worth doing some testing before using a compressor. Saying that...if you serialize to json, you'll likely get a lot of benefit from a fast compressor.
#         A class with compress and decompress methods can also be passed as a custom serializer. The class also needs a compress_level parameter in the __init__.

#     compress_level : int
#         The compression level for the compressor.

#     Returns
#     -------
#     Shock

#     The optional *flag* argument can be:

#    +---------+-------------------------------------------+
#    | Value   | Meaning                                   |
#    +=========+===========================================+
#    | ``'r'`` | Open existing database for reading only   |
#    |         | (default)                                 |
#    +---------+-------------------------------------------+
#    | ``'w'`` | Open existing database for reading and    |
#    |         | writing                                   |
#    +---------+-------------------------------------------+
#    | ``'c'`` | Open database for reading and writing,    |
#    |         | creating it if it doesn't exist           |
#    +---------+-------------------------------------------+
#    | ``'n'`` | Always create a new, empty database, open |
#    |         | for reading and writing                   |
#    +---------+-------------------------------------------+

#     """

#     return Arete(file_path, flag, sync, lock, serializer, protocol, compressor, compress_level, index_serializer)

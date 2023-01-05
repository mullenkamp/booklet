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

import utils
# from . import utils

imports = {}
try:
    import orjson
    imports['orjson'] = True
except:
    imports['orjson'] = False

try:
    import zstandard as zstd
    imports['zstd'] = True
except:
    imports['zstd'] = False

try:
    import lz4
    imports['lz4'] = True
except:
    imports['lz4'] = False


__all__ = ['Arete']

hidden_keys = (b'00~._serializer', b'01~._compressor', b'02~._key_serializer')

uuid_arete = b'O~\x8a?\xe7\\GP\xadC\nr\x8f\xe3\x1c\xfe'
special_bytes = b'\xff\xff\xff\xff\xff\xff\xff\xff\xff'
version = 1
version_bytes = version.to_bytes(2, 'little', signed=False)

page_size = mmap.ALLOCATIONGRANULARITY

#######################################################
### Serializers and compressors

## Serializers
class Pickle:
    def __init__(self, protocol):
        self.protocol = protocol
    def dumps(self, obj):
        return pickle.dumps(obj, self.protocol)
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
class Gzip:
    def __init__(self, compress_level):
        self.compress_level = compress_level
    def compress(self, obj):
        return gzip.compress(obj, self.compress_level)
    def decompress(self, obj):
        return gzip.decompress(obj)


class Zstd:
    def __init__(self, compress_level):
        self.compress_level = compress_level
    def compress(self, obj):
        return zstd.compress(obj, self.compress_level)
    def decompress(self, obj):
        return zstd.decompress(obj)


class Lz4:
    def __init__(self, compress_level):
        self.compress_level = compress_level
    def compress(self, obj):
        return lz4.frame.compress(obj, self.compress_level)
    def decompress(self, obj):
        return lz4.frame.decompress(obj)


#######################################################
### Classes

# class _ClosedDict(MutableMapping):
#     'Marker for a closed dict.  Access attempts raise a ValueError.'

#     def closed(self, *args):
#         raise ValueError('invalid operation on closed shelf')
#     __iter__ = __len__ = __getitem__ = __setitem__ = __delitem__ = keys = closed

#     def __repr__(self):
#         return '<Closed Dictionary>'


file_path = '/media/nvme1/cache/arete/test.arete'
flag = 'n'
sync: bool = False
lock: bool = True
serializer = 'pickle'
protocol: int = 5
compressor = None
compress_level: int = 1
key_serializer = 'str'

key = b'00~._serializer'
value = pickle.dumps(Pickle(protocol), protocol)


class Arete(MutableMapping):
    """

    """

    def __init__(self, file_path: str, flag: str = "r", sync: bool = False, lock: bool = True, serializer = None, protocol: int = 5, compressor = None, compress_level: int = 1, key_serializer = None):
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

        # self.db = db
        self._write = write

        ## Load or assign encodings
        if fp_exists:
            if write:
                f = io.FileIO(file_path, 'r+b')
                self._file = io.BufferedWriter(f, 10000000)
                self._mm = mmap.mmap(self._file.fileno(), 0)
                # self._mm = open(file_path, 'r+b')
            else:
                f = io.FileIO(file_path, 'rb')
                self._file = io.BufferedReader(f, 10000000)
                self._mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
                # self._mm = open(file_path, 'rb')

            self._serializer = pickle.loads(utils.get_value(self._mm, b'00~._serializer'))
            self._compressor = pickle.loads(utils.get_value(self._mm, b'01~._compressor'))
            self._key_serializer = pickle.loads(utils.get_value(self._mm, b'02~._key_serial'))
        else:
            ## Value Serializer
            if serializer is None:
                self._serializer = None
            elif serializer == 'str':
                self._serializer = Str
            elif serializer == 'pickle':
                self._serializer = Pickle(protocol)
            elif serializer == 'json':
                self._serializer = Json
            elif serializer == 'orjson':
                if imports['orjson']:
                    self._serializer = Orjson
                else:
                    raise ValueError('orjson could not be imported.')
            elif inspect.isclass(serializer):
                class_methods = dir(serializer)
                if ('dumps' in class_methods) and ('loads' in class_methods):
                    self._serializer = serializer
                else:
                    raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.')
            else:
                raise ValueError('serializer must be one of pickle, json, str, orjson, or a serializer class with dumps and loads methods.')

            ## Key Serializer
            if key_serializer is None:
                self._key_serializer = None
            elif key_serializer == 'str':
                self._key_serializer = Str
            elif key_serializer == 'pickle':
                self._key_serializer = Pickle(protocol)
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
                raise ValueError('serializer must be one of pickle, json, orjson, or a serializer class with dumps and loads methods.')

            ## Compressor
            if compressor is None:
                self._compressor = None
            elif compressor == 'gzip':
                self._compressor = Gzip(compress_level)
            elif compressor == 'zstd':
                if imports['zstd']:
                    self._compressor = Zstd(compress_level)
                else:
                    raise ValueError('zstd could not be imported.')
            elif compressor == 'lz4':
                if imports['lz4']:
                    self._compressor = Lz4(compress_level)
                else:
                    raise ValueError('lz4 could not be imported.')
            elif inspect.isclass(compressor):
                class_methods = dir(compressor)
                if ('compress' in class_methods) and ('decompress' in class_methods):
                    self._compressor = compressor(compress_level)
                else:
                    raise ValueError('If a class is passed for a compressor, then it must have compress and decompress methods as well as a compress_level parameter in the __init__.')
            else:
                raise ValueError('compressor must be one of gzip, zstd, lz4, or a compressor class with compress and decompress methods.')

            ## Write uuid and version and Save encodings to new file
            # with open(file_path, 'w+b') as f:
            #     _ = f.write(uuid_arete + version_bytes)

            with open(file_path, 'w+b') as f:
                _ = f.write(uuid_arete + version_bytes)
                utils.write_chunk(f, b'00~._serializer', pickle.dumps(self._serializer, protocol))
                utils.write_chunk(f, b'01~._compressor', pickle.dumps(self._compressor, protocol))
                utils.write_chunk(f, b'02~._key_serial', pickle.dumps(self._key_serializer, protocol))

            f = io.FileIO(file_path, 'r+b')
            self._file = io.BufferedWriter(f, 10000000)
            self._mm = mmap.mmap(self._file.fileno(), 0)
            # self._mm = open(file_path, 'r+b')


    def _pre_key(self, key) -> bytes:

        ## Serialize to bytes
        if self._key_serializer is not None:
            key = self._key_serializer.dumps(key)

        if len(key) > 255:
            raise ValueError('key length must be less tha 256 bytes.')

        return key

    def _post_key(self, key: bytes):

        ## Serialize from bytes
        if self._key_serializer is not None:
            key = self._key_serializer.loads(key)

        return key

    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        if self._serializer is not None:
            value = self._serializer.dumps(value)

        ## Compress bytes
        if self._compressor is not None:
            value = self._compressor.compress(value)

        return value

    def _post_value(self, value: bytes):

        ## Decompress bytes
        if self._compressor is not None:
            value = self._compressor.decompress(value)

        ## Serialize from bytes
        if self._serializer is not None:
            value = self._serializer.loads(value)

        return value

    def keys(self):
        for key in utils.get_keys_values(self._mm, keys=True):
            if key not in hidden_keys:
                yield self._post_key(key)

    def items(self):
        for key, value in utils.get_keys_values(self._mm, keys=True, values=True):
            if key not in hidden_keys:
                yield self._post_key(key), self._post_value(self.env[key])

    def values(self):
        for key in utils.get_keys_values(self._mm, values=True):
            if key not in hidden_keys:
                yield self._post_value(self.env[key])

    def __iter__(self):
        return self.keys()

    def __len__(self):
        keys_len = 0
        for key in self.keys():
            keys_len += 1
        return keys_len - len(hidden_keys)

    def __contains__(self, key):
        return self._pre_key(key) in self.keys()

    def get(self, key, default=None):
        value = utils.get_value(self._mm, self._pre_key(key))

        if value is None:
            return default
        else:
            return self._post_value(utils.get_value(self._mm, self._pre_key(key)))

    def update(self, key_value_dict):
        """

        """
        if self._write:
            self._write_many_chunks(key_value_dict)

            self.sync()
        else:
            raise ValueError('File is open for read only.')

    def _write_many_chunks(self, key_value_dict):
        """

        """
        write_bytes = bytearray()
        for key, value in key_value_dict.items():
            k = self._pre_key(key)
            v = self._pre_value(value)
            key_len_bytes = len(k).to_bytes(1, 'little', signed=False)
            value_len_bytes = len(v).to_bytes(8, 'little', signed=False)

            write_bytes += memoryview(special_bytes + key_len_bytes + k + value_len_bytes + v)

        self._file.seek(0, 2)

        new_n_bytes = self._file.write(write_bytes)

        # reassign_old_key(mm, key, old_len)

        return new_n_bytes

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
            utils.write_chunk(self._file, self._pre_key(key), self._pre_value(value))
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        utils.reassign_old_key(self._mm, key, len(self._mm))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        if self._write:
            pos = utils.find_chunk_pos(self._mm, hidden_keys[-1])
            key_len = len(hidden_keys[-1])
            self._mm.seek(pos+10+key_len)
            value_len = int.from_bytes(self._mm.read(8), 'little')
            total_len = pos + 10 + key_len + 8 + value_len

            self._mm.resize(total_len)
            self.sync()
        else:
            raise ValueError('File is open for read only.')

    def close(self):
        self.sync()
        try:
            self._mm.close()
        except:
            pass
        try:
            self._file.close()
        except:
            pass

    def __del__(self):
        self.close()

    def sync(self):
        try:
            self._mm.flush()
        except:
            pass
        try:
            self._file.flush()
        except:
            pass


# def open(
#     file_path: str, flag: str = "r", sync: bool = False, lock: bool = True, serializer = None, protocol: int = 5, compressor = None, compress_level: int = 1, key_serializer = None):
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

#     return Arete(file_path, flag, sync, lock, serializer, protocol, compressor, compress_level, key_serializer)

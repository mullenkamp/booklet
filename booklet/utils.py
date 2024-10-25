#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import os
import sys
# import uuid
# import fastuuid as uuid
import uuid6 as uuid
# import math
import io
from hashlib import blake2b, blake2s
import inspect
from threading import Lock
import portalocker
# from fcntl import flock, LOCK_EX, LOCK_SH, LOCK_UN
# import mmap
from datetime import datetime, timezone
import time
from itertools import count
from collections import Counter, defaultdict, deque
import weakref
import pathlib
import orjson
from typing import Union, Optional
import struct
# from time import time

# import serializers
from . import serializers

############################################
### Parameters

sub_index_init_pos = 200

# n_deletes_pos = 33
n_keys_pos = 33
file_timestamp_pos = 42
timestamp_bytes_len = 7

n_keys_crash = 4294967295

# n_bytes_index = 4
n_bytes_file = 6
n_bytes_key = 2
n_bytes_value = 4

key_hash_len = 13

uuid_variable_blt = b'O~\x8a?\xe7\\GP\xadC\nr\x8f\xe3\x1c\xfe'
uuid_fixed_blt = b'\x04\xd3\xb2\x94\xf2\x10Ab\x95\x8d\x04\x00s\x8c\x9e\n'

# metadata_key_bytes0 = b'\xad\xb0\x1e\xbc\x1b\xa3C>\xb0CRw\xd1g\x86\xee'
metadata_key_bytes = b'adb01ebc1ba3433eb043527'
metadata_key_hash = b'B~\xf5\t\xe6\xef,\xbf\x16nn\x82\x01'

current_version = 4
current_version_bytes = current_version.to_bytes(2, 'little', signed=False)

init_n_buckets = 12007
n_buckets_reindex = {
    12007: 144013,
    144013: 1728017,
    1728017: 20736017,
    20736017: None,
    }

## TZ offset
# if time.daylight:
#     tz_offset = time.altzone
# else:
#     tz_offset = time.timezone

############################################
### Exception classes

# class BaseError(Exception):
#     def __init__(self, message, blt=None, *args):
#         self.message = message # without this you may get DeprecationWarning
#         # Special attribute you desire with your Error,
#         blt.close()
#         # allow users initialize misc. arguments as any other builtin Error
#         super(BaseError, self).__init__(message, *args)


# class ValueError(BaseError):
#     pass

# class TypeError(BaseError):
#     pass

# class KeyError(BaseError):
#     pass

# class SerializeError(BaseError):
#     pass


############################################
### Functions


def make_timestamp_int(timestamp=None):
    """
    The timestamp must be either None, an int of the number of microseconds in POSIX UTC time, an ISO 8601 datetime string with timezone, or a datetime object with timezone. None will create a timestamp of now.

    It will return an int of the number of microseconds in POSIX UTC time.
    """
    if timestamp is None:
        int_us = time.time_ns() // 1000
    elif isinstance(timestamp, int):
        int_us = timestamp
    elif isinstance(timestamp, str):
        dt = datetime.datetime.fromisoformat(timestamp)
        if not dt.tzinfo:
            raise ValueError('timestamp needs timezone info.')
        int_us = int(dt.astimezone(datetime.timezone.utc).timestamp() * 1000000)
    elif isinstance(timestamp, datetime.datetime):
        if not timestamp.tzinfo:
            raise ValueError('timestamp needs timezone info.')
        int_us = int(timestamp.astimezone(datetime.timezone.utc).timestamp() * 1000000)
    else:
        raise TypeError('The timestamp must be either None, an int of the number of microseconds in unix time, an ISO 8601 datetime string with timezone, or a datetime object with timezone.')

    return int_us


def encode_metadata(data):
    """

    """
    return orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY)


def close_files(file, n_keys, n_keys_pos, write):
    """
    This is to be run as a finalizer to ensure that the files are closed properly.
    First will be to just close the files, I'll need to modify it to sync the index once I write the sync function.
    """
    if write:
        file.seek(n_keys_pos)
        file.write(int_to_bytes(n_keys, 4))
        # file_mmap.flush()
        # file.flush()

    portalocker.lock(file, portalocker.LOCK_UN)
    file.close()


def bytes_to_int(b, signed=False):
    """
    Remember for a single byte, I only need to do b[0] to get the int. And it's really fast as compared to the function here. This is only needed for bytes > 1.
    """
    return int.from_bytes(b, 'little', signed=signed)


def int_to_bytes(i, byte_len, signed=False):
    """

    """
    return i.to_bytes(byte_len, 'little', signed=signed)


def hash_key(key):
    """

    """
    return blake2s(key, digest_size=key_hash_len).digest()


def write_init_bucket_indexes(file, n_buckets, index_pos, write_buffer_size):
    """

    """
    init_end_pos_bytes = int_to_bytes(0, n_bytes_file)

    file.seek(index_pos)
    temp_bytes = bytearray()
    n_bytes_temp = 0
    for i in range(n_buckets):
        temp_bytes.extend(init_end_pos_bytes)
        n_bytes_temp += n_bytes_file
        if n_bytes_temp > write_buffer_size:
            file.write(temp_bytes)
            temp_bytes.clear()
            n_bytes_temp = 0

    if n_bytes_temp > 0:
        file.write(temp_bytes)


def get_index_bucket(key_hash, n_buckets):
    """
    The modulus of the int representation of the bytes hash puts the keys in evenly filled buckets.
    """
    return bytes_to_int(key_hash) % n_buckets


def get_bucket_index_pos(index_bucket):
    """

    """
    return sub_index_init_pos + (index_bucket * n_bytes_file)


def get_first_data_block_pos(file, bucket_index_pos):
    """

    """
    file.seek(bucket_index_pos)
    data_block_pos = bytes_to_int(file.read(n_bytes_file))

    return data_block_pos


def get_last_data_block_pos(file, key_hash, n_buckets):
    """
    Puts a bunch of the previous functions together.
    """
    index_len = key_hash_len + n_bytes_file

    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket)
    data_block_pos = get_first_data_block_pos(file, bucket_index_pos)

    if data_block_pos:
        while True:
            file.seek(data_block_pos)
            data_index = file.read(index_len)
            next_data_block_pos = bytes_to_int(data_index[key_hash_len:])
            if next_data_block_pos:
                if data_index[:key_hash_len] == key_hash:
                    return data_block_pos
                elif next_data_block_pos == 1:
                    return 0
            else:
                return 0

            data_block_pos = next_data_block_pos
    else:
        return 0


def contains_key(file, key_hash, n_buckets):
    """
    Determine if a key is present in the file.
    """
    data_block_pos = get_last_data_block_pos(file, key_hash, n_buckets)
    if data_block_pos:
        return True
    else:
        return False


def set_timestamp(file, key_hash, n_buckets, timestamp):
    """

    """
    data_block_pos = get_last_data_block_pos(file, key_hash, n_buckets)
    if data_block_pos:
        ts_pos = data_block_pos + key_hash_len + n_bytes_file + n_bytes_key + n_bytes_value
        file.seek(ts_pos)

        ts_bytes = int_to_bytes(timestamp, timestamp_bytes_len)
        file.write(ts_bytes)

        return True
    else:
        return False


def get_value(file, key_hash, n_buckets, ts_bytes_len=0):
    """
    Combines everything necessary to return a value.
    """
    data_block_pos = get_last_data_block_pos(file, key_hash, n_buckets)
    if data_block_pos:
        key_len_pos = data_block_pos + key_hash_len + n_bytes_file
        file.seek(key_len_pos)
        key_len_value_len = file.read(n_bytes_key + n_bytes_value)
        key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
        value_len = bytes_to_int(key_len_value_len[n_bytes_key:])

        file.seek(ts_bytes_len + key_len, 1)
        value = file.read(value_len)
    else:
        value = False

    return value


def get_value_ts(file, key_hash, n_buckets, include_value=True, include_ts=False, ts_bytes_len=0):
    """
    Combines everything necessary to return a value.
    """
    data_block_pos = get_last_data_block_pos(file, key_hash, n_buckets)
    if data_block_pos:
        key_len_pos = data_block_pos + key_hash_len + n_bytes_file
        file.seek(key_len_pos)
        key_len_value_len = file.read(n_bytes_key + n_bytes_value)
        key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
        value_len = bytes_to_int(key_len_value_len[n_bytes_key:])

        if include_value and include_ts:
            ts_key_value = file.read(ts_bytes_len + key_len + value_len)
            ts_int = bytes_to_int(ts_key_value[:ts_bytes_len])
            value = ts_key_value[ts_bytes_len + key_len:]
            output = value, ts_int
        elif include_value:
            file.seek(ts_bytes_len + key_len, 1)
            output = (file.read(value_len), None)
        elif include_ts:
            output = (None, bytes_to_int(file.read(ts_bytes_len)))
        else:
            raise ValueError('include_value and/or include_timestamp must be True.')
    else:
        output = False

    return output


def iter_keys_value_from_start_end_pos(file, start, end, include_key, include_value, include_ts, ts_bytes_len):
    """

    """
    one_extra_index_bytes_len = key_hash_len + n_bytes_file
    init_data_block_len = one_extra_index_bytes_len + n_bytes_key + n_bytes_value

    next_block_pos = start

    while next_block_pos < end:
        # lock.acquire()

        file.seek(next_block_pos)
        init_data_block = file.read(init_data_block_len)

        next_data_block_pos = bytes_to_int(init_data_block[key_hash_len:one_extra_index_bytes_len])
        key_len = bytes_to_int(init_data_block[one_extra_index_bytes_len:one_extra_index_bytes_len + n_bytes_key])
        value_len = bytes_to_int(init_data_block[one_extra_index_bytes_len + n_bytes_key:])
        ts_key_value_len = ts_bytes_len + key_len + value_len
        if next_data_block_pos: # A value of 0 means it was deleted
            ts_key_value = file.read(ts_key_value_len)

            # lock.release()
            next_block_pos += init_data_block_len + ts_key_value_len

            key = ts_key_value[ts_bytes_len:ts_bytes_len + key_len]
            if key != metadata_key_bytes:
                if include_ts:
                    ts_int = bytes_to_int(ts_key_value[:ts_bytes_len])
                    if include_value:
                        value = ts_key_value[ts_bytes_len + key_len:]
                        yield key, ts_int, value
                    else:
                        yield key, ts_int

                elif include_key and include_value:
                    value = ts_key_value[ts_bytes_len + key_len:]
                    yield key, value

                elif include_key:
                    yield key

                elif include_value:
                    value = ts_key_value[ts_bytes_len + key_len:]
                    yield value
                else:
                    raise ValueError('I need to include something for iter_keys_values.')
        else:
            # lock.release()
            next_block_pos += init_data_block_len + ts_key_value_len

            # file.seek(ts_bytes_len + key_len + value_len, 1)


def iter_keys_values(file, n_buckets, include_key, include_value, include_ts, ts_bytes_len):
    """

    """
    end = file.seek(0, 2)
    start = sub_index_init_pos + (n_buckets * n_bytes_file)

    return iter_keys_value_from_start_end_pos(file, start, end, include_key, include_value, include_ts, ts_bytes_len)


def assign_delete_flag(file, key_hash, n_buckets):
    """
    Assigns 0 at the key hash index and the key/value data block.
    """
    index_len = key_hash_len + n_bytes_file

    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket)
    first_data_block_pos = get_first_data_block_pos(file, bucket_index_pos)
    if first_data_block_pos:
        previous_data_index_pos = bucket_index_pos
        data_block_pos = first_data_block_pos
        while True:
            file.seek(data_block_pos)
            data_index = file.read(index_len)
            next_data_block_pos_bytes = data_index[key_hash_len:]
            next_data_block_pos = bytes_to_int(next_data_block_pos_bytes)
            if next_data_block_pos:
                if data_index[:key_hash_len] == key_hash:
                    file.seek(-n_bytes_file, 1)
                    file.write(b'\x00\x00\x00\x00\x00\x00')
                    file.seek(previous_data_index_pos)
                    # file.write(b'\x01\x00\x00\x00\x00\x00')
                    file.write(next_data_block_pos_bytes)
                    return True

                elif next_data_block_pos == 1:
                    return False
            else:
                return False

            previous_data_index_pos = data_block_pos + key_hash_len
            data_block_pos = next_data_block_pos

    else:
        return False


def write_data_blocks(file, key, value, n_buckets, buffer_data, buffer_index, buffer_index_set, write_buffer_size, timestamp=None, ts_bytes_len=0):
    """

    """
    n_keys = 0

    ## Prep data
    file_len = file.seek(0, 2)

    key_hash = hash_key(key)
    key_bytes_len = len(key)
    value_bytes_len = len(value)

    if ts_bytes_len:
        ts_int = make_timestamp_int(timestamp)
        ts_bytes = int_to_bytes(ts_int, ts_bytes_len)
        write_bytes = key_hash + b'\x01\x00\x00\x00\x00\x00' + int_to_bytes(key_bytes_len, n_bytes_key) + int_to_bytes(value_bytes_len, n_bytes_value) + ts_bytes + key + value
    else:
        write_bytes = key_hash + b'\x01\x00\x00\x00\x00\x00' + int_to_bytes(key_bytes_len, n_bytes_key) + int_to_bytes(value_bytes_len, n_bytes_value) + key + value

    ## flush write buffer if the size is getting too large
    bd_pos = len(buffer_data)
    write_len = len(write_bytes)

    bd_space = write_buffer_size - bd_pos
    if write_len > bd_space:
        file_len = flush_data_buffer(file, buffer_data, file_len)
        n_keys += update_index(file, buffer_index, buffer_index_set, n_buckets)
        bd_pos = 0

    ## Append to buffers
    data_pos_bytes = int_to_bytes(file_len + bd_pos, n_bytes_file)

    buffer_index.extend(key_hash + data_pos_bytes)
    buffer_index_set.add(key_hash)
    buffer_data.extend(write_bytes)

    return n_keys


def flush_data_buffer(file, buffer_data, write_pos):
    """

    """
    bd_pos = len(buffer_data)
    file.seek(write_pos)
    if bd_pos > 0:
        _ = file.write(buffer_data)
        buffer_data.clear()
        # file.flush()

        new_file_pos = write_pos + bd_pos
        # file_mmap.resize(new_file_len)
        # file.madvise(mmap.MADV_DONTNEED)

        return new_file_pos
    else:
        return write_pos


def update_index(file, buffer_index, buffer_index_set, n_buckets):
    """

    """
    one_extra_index_bytes_len = key_hash_len + n_bytes_file

    buffer_len = len(buffer_index)

    ## Check for old keys and assign data_block_pos to previous key in chain
    n = int(buffer_len/one_extra_index_bytes_len)

    n_keys = 0
    # for key_hash, new_data_block_pos_bytes in buffer_index.items():
    for i in range(n):
        start = i * one_extra_index_bytes_len
        end = start + one_extra_index_bytes_len
        index_data = buffer_index[start:end]
        key_hash = index_data[:key_hash_len]
        new_data_block_pos_bytes = index_data[key_hash_len:]

        index_bucket = get_index_bucket(key_hash, n_buckets)
        bucket_index_pos = get_bucket_index_pos(index_bucket)
        first_data_block_pos = get_first_data_block_pos(file, bucket_index_pos)
        if first_data_block_pos:
            previous_data_index_pos = bucket_index_pos
            data_block_pos = first_data_block_pos
            while True:
                file.seek(data_block_pos)
                data_index = file.read(one_extra_index_bytes_len)
                next_data_block_pos_bytes = data_index[key_hash_len:]
                next_data_block_pos = bytes_to_int(next_data_block_pos_bytes)
                if next_data_block_pos:
                    if data_index[:key_hash_len] == key_hash:
                        file.seek(-n_bytes_file, 1)
                        file.write(b'\x00\x00\x00\x00\x00\x00')
                        file.seek(previous_data_index_pos)
                        file.write(new_data_block_pos_bytes)
                        if next_data_block_pos > 1:
                            file.seek(bytes_to_int(new_data_block_pos_bytes) + key_hash_len)
                            file.write(next_data_block_pos_bytes)
                        break

                    elif next_data_block_pos == 1:
                        file.seek(-n_bytes_file, 1)
                        file.write(new_data_block_pos_bytes)
                        n_keys += 1
                        break
                else:
                    file.seek(previous_data_index_pos)
                    file.write(new_data_block_pos_bytes)
                    n_keys += 1
                    break

                previous_data_index_pos = data_block_pos + key_hash_len
                data_block_pos = next_data_block_pos
        else:
            file.seek(bucket_index_pos)
            file.write(new_data_block_pos_bytes)
            n_keys += 1

    buffer_index.clear()
    buffer_index_set.clear()

    return n_keys


def clear(file, n_buckets, n_keys_pos, write_buffer_size):
    """

    """
    ## Remove all data in the main file except the init bytes
    os.ftruncate(file.fileno(), sub_index_init_pos)
    os.fsync(file.fileno())

    ## Update the n_keys
    file.seek(n_keys_pos)
    file.write(int_to_bytes(0, 4))

    ## Cut back the file to the bucket index
    write_init_bucket_indexes(file, n_buckets, sub_index_init_pos, write_buffer_size)
    file.flush()


def prune_file(file, timestamp, reindex, n_buckets, n_bytes_file, n_bytes_key, n_bytes_value, write_buffer_size, ts_bytes_len, buffer_data, buffer_index, buffer_index_set):
    """

    """
    metadata_key_added = False

    one_extra_index_bytes_len = key_hash_len + n_bytes_file
    init_data_block_len = one_extra_index_bytes_len + n_bytes_key + n_bytes_value

    file_len = file.seek(0, 2)
    data_block_read_start_pos = sub_index_init_pos + (n_buckets * n_bytes_file)
    total_data_size = file_len - data_block_read_start_pos
    data_block_write_start_pos = data_block_read_start_pos
    n_keys = 0

    ## Reindex if required
    if reindex:
        if isinstance(reindex, bool):
            if n_buckets not in n_buckets_reindex:
                raise ValueError('The existing n_buckets was not the original default value. If a non-default value is originally used, then the reindex value must be an int.')
            new_n_buckets = n_buckets_reindex[n_buckets]
        elif isinstance(reindex, int):
            new_n_buckets = reindex
        else:
            raise TypeError('reindex must be either a bool or an int.')

        if new_n_buckets:
            data_block_write_start_pos = sub_index_init_pos + (new_n_buckets * n_bytes_file)
            extra_bytes = data_block_write_start_pos - data_block_read_start_pos
            file_len = file_len + extra_bytes
            os.ftruncate(file.fileno(), file_len)

            # Move old data blocks to the end of the new file
            copy_file_range(file, file, total_data_size, data_block_read_start_pos, data_block_write_start_pos, write_buffer_size)

            data_block_read_start_pos = data_block_write_start_pos
            n_buckets = new_n_buckets

    ## Clear bucket indexes
    write_init_bucket_indexes(file, n_buckets, sub_index_init_pos, write_buffer_size)

    ## Iter through data blocks and only add the non-deleted ones
    # written_n_bytes = 0
    removed_count = 0
    while data_block_read_start_pos < file_len:
        file.seek(data_block_read_start_pos)
        init_data_block = file.read(init_data_block_len)

        next_data_block_pos = bytes_to_int(init_data_block[key_hash_len:one_extra_index_bytes_len])

        key_len_bytes = init_data_block[one_extra_index_bytes_len:one_extra_index_bytes_len + n_bytes_key]
        key_len = bytes_to_int(key_len_bytes)

        value_len_bytes = init_data_block[one_extra_index_bytes_len + n_bytes_key:]
        value_len = bytes_to_int(value_len_bytes)
        ts_key_value_len = ts_bytes_len + key_len + value_len
        # ts_key_value_bytes = file.read(ts_key_value_len)
        if next_data_block_pos: # A value of 0 means it was deleted
            ts_key_value_bytes = file.read(ts_key_value_len)

            key_hash = init_data_block[:key_hash_len]

            # Check if it's the metadata key - remove from n_keys at the end
            if key_hash == metadata_key_hash:
                metadata_key_added = True

            # timestamp filter - don't remove metadata even if older
            elif timestamp and ts_bytes_len:
                ts_int = bytes_to_int(ts_key_value_bytes[:ts_bytes_len])
                if ts_int < timestamp:
                    data_block_read_start_pos += init_data_block_len + ts_key_value_len
                    removed_count += 1
                    continue

            write_bytes = key_hash + b'\x01\x00\x00\x00\x00\x00' + key_len_bytes + value_len_bytes + ts_key_value_bytes

            ## flush write buffer if the size is getting too large
            write_len = len(write_bytes)
            bd_pos = len(buffer_data)

            bd_space = write_buffer_size - bd_pos
            if write_len > bd_space:
                data_block_write_start_pos = flush_data_buffer(file, buffer_data, data_block_write_start_pos)
                n_keys += update_index(file, buffer_index, buffer_index_set, n_buckets)
                bd_pos = 0

            ## Append to buffers
            data_pos_bytes = int_to_bytes(data_block_write_start_pos + bd_pos, n_bytes_file)

            # buffer_index[key_hash] = data_pos_bytes
            buffer_index.extend(key_hash + data_pos_bytes)
            buffer_data.extend(write_bytes)
        else:
            removed_count += 1
            # print(bytes_to_int(ts_key_value_bytes[ts_bytes_len:ts_bytes_len+key_len]))

        data_block_read_start_pos += init_data_block_len + ts_key_value_len

    ## Finish writing if there's data left in buffer
    if buffer_data:
        data_block_write_start_pos = flush_data_buffer(file, buffer_data, data_block_write_start_pos)
        n_keys += update_index(file, buffer_index, buffer_index_set, n_buckets)

    os.ftruncate(file.fileno(), data_block_write_start_pos)
    os.fsync(file.fileno())

    if metadata_key_added:
        n_keys -= 1

    return n_keys, removed_count, n_buckets


def init_files_variable(self, file_path, flag, key_serializer, value_serializer, n_buckets, write_buffer_size, init_timestamps, init_bytes):
    """

    """
    fp = pathlib.Path(file_path)
    self._file_path = fp

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

    self.writable = write
    self._write_buffer_size = write_buffer_size

    # self._platform = sys.platform

    self._buffer_data = bytearray()
    self._buffer_index = bytearray()
    self._buffer_index_set = set()

    self._thread_lock = Lock()

    if fp_exists:
        if write:
            self._file = io.open(fp, 'r+b', buffering=0)

            ## Locks
            portalocker.lock(self._file, portalocker.LOCK_EX)
            # if self._platform.startswith('linux'):
            #     flock(self._fd, LOCK_EX)
        else:
            self._file = io.open(fp, 'rb', buffering=0)

            ## Lock
            portalocker.lock(self._file, portalocker.LOCK_SH)
            # if self._platform.startswith('linux'):
            #     flock(self._fd, LOCK_SH)

        ## Read in initial bytes
        base_param_bytes = self._file.read(sub_index_init_pos)

        ## system and version check
        sys_uuid = base_param_bytes[:16]
        if sys_uuid != uuid_variable_blt:
            portalocker.lock(self._file, portalocker.LOCK_UN)
            raise TypeError('This is not the correct file type.')

        ## Read the rest of the base parameters
        read_base_params_variable(self, base_param_bytes, key_serializer, value_serializer)
        if self._version < 4:
            if self._version == 3:
               self._init_timestamps = 0
               self._ts_bytes_len = 0
            else:
                raise ValueError('File is an older version.')

        ## Check the n_keys
        if self._n_keys == n_keys_crash:
            if write:
                # print('File must have been closed incorrectly...rebuilding the n_keys...')
                counter = count()
                deque(zip(self.keys(), counter), maxlen=0)

                self._n_keys = next(counter)
            else:
                raise ValueError('File must have been closed incorrectly. Please open with write access to fix it.')

    else:
        if not write:
            raise FileNotFoundError('File was requested to be opened as read-only, but no file exists.')

        # if isinstance(n_buckets, int):
        #     self._n_buckets = n_buckets
        # else:
        #     self._n_buckets = init_n_buckets

        ## If init_bytes are passed, then parse bytes
        if isinstance(init_bytes, (bytes, bytearray)):
            init_bytes = bytearray(init_bytes)
            read_base_params_variable(self, init_bytes, key_serializer, value_serializer)
            # 0 out the n_keys
            init_bytes[n_keys_pos:n_keys_pos+4] = int_to_bytes(0, 4)
        else:
            file_timestamp = make_timestamp_int()

            uuid8 = uuid.uuid8()

            init_bytes = init_base_params_variable(self, key_serializer, value_serializer, n_buckets, init_timestamps, file_timestamp, uuid8)

            self.uuid = uuid8
            self._n_buckets = n_buckets
            self._init_timestamps = init_timestamps
            if self._init_timestamps:
                self._ts_bytes_len = timestamp_bytes_len
            else:
                self._ts_bytes_len = 0

        self._n_bytes_file = n_bytes_file
        self._n_bytes_key = n_bytes_key
        self._n_bytes_value = n_bytes_value

        self._n_keys = 0
        self._n_keys_pos = n_keys_pos

        self._file = io.open(fp, 'w+b', buffering=0)

        ## Locks
        portalocker.lock(self._file, portalocker.LOCK_EX)
        # if self._platform.startswith('linux'):
        #     flock(self._fd, LOCK_EX)

        ## Write new file
        with self._thread_lock:
            self._file.write(init_bytes)

            write_init_bucket_indexes(self._file, self._n_buckets, sub_index_init_pos, write_buffer_size)

    ## Create finalizer
    self._finalizer = weakref.finalize(self, close_files, self._file, n_keys_crash, self._n_keys_pos, self.writable)


def copy_file_range(fsrc, fdst, count, offset_src, offset_dst, write_buffer_size):
    """

    """
    # Need to make sure it's copy rolling the correct direction for the same file
    same_file = fdst.fileno() == fsrc.fileno()
    backwards = offset_dst > offset_src

    write_count = 0
    while write_count < count:
        count_diff = count - write_count - write_buffer_size
        if count_diff > 0:
            read_count = write_buffer_size
        else:
            read_count = count - write_count

        if same_file and backwards:
            new_offset_src = offset_src + count - write_count - read_count
            new_offset_dst = offset_dst + count - write_count - read_count
        else:
            new_offset_src = offset_src + write_count
            new_offset_dst = offset_dst + write_count

        fsrc.seek(new_offset_src)
        data = fsrc.read(read_count)

        fdst.seek(new_offset_dst)
        write_count += fdst.write(data)

    fdst.flush()


def read_base_params_variable(self, base_param_bytes, key_serializer, value_serializer):
    """

    """
    # Read init bytes
    self._version = bytes_to_int(base_param_bytes[16:18])
    self._n_bytes_file = bytes_to_int(base_param_bytes[18:19])
    self._n_bytes_key = bytes_to_int(base_param_bytes[19:20])
    self._n_bytes_value = bytes_to_int(base_param_bytes[20:21])
    self._n_buckets = bytes_to_int(base_param_bytes[21:25])
    # self._n_bytes_index = bytes_to_int(base_param_bytes[25:29])
    saved_value_serializer = bytes_to_int(base_param_bytes[29:31])
    saved_key_serializer = bytes_to_int(base_param_bytes[31:n_keys_pos])
    self._n_keys = bytes_to_int(base_param_bytes[n_keys_pos:n_keys_pos+4])
    # self._value_len = bytes_to_int(base_param_bytes[37:41])
    self._init_timestamps = base_param_bytes[41]
    if self._init_timestamps:
        self._ts_bytes_len = timestamp_bytes_len
    else:
        self._ts_bytes_len = 0

    self._file_timestamp = bytes_to_int(base_param_bytes[file_timestamp_pos:file_timestamp_pos + timestamp_bytes_len])

    self.uuid = uuid.UUID(bytes=bytes(base_param_bytes[49:65]))

    ## Assign attributes
    self._n_keys_pos = n_keys_pos

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
            raise ValueError('If a custom class is passed for a serializer, then it must have dumps and loads methods.', self)
    else:
        raise ValueError('How did you mess up value_serializer so bad?!', self)

    if saved_key_serializer > 0:
        self._key_serializer = serializers.serial_int_dict[saved_key_serializer]
    # elif key_serializer is None:
    #     raise ValueError('key serializer must be a serializer class with dumps and loads methods.')
    elif inspect.isclass(key_serializer):
        class_methods = dir(key_serializer)
        if ('dumps' in class_methods) and ('loads' in class_methods):
            self._key_serializer = key_serializer
        else:
            raise ValueError('If a custom class is passed for a serializer, then it must have dumps and loads methods.', self)
    else:
        raise ValueError('How did you mess up key_serializer so bad?!', self)


def init_base_params_variable(self, key_serializer, value_serializer, n_buckets, init_timestamps, file_timestamp, uuid7):
    """

    """
    ## Value serializer
    if value_serializer in serializers.serial_name_dict:
        value_serializer_code = serializers.serial_name_dict[value_serializer]
        self._value_serializer = serializers.serial_int_dict[value_serializer_code]
    elif inspect.isclass(value_serializer):
        class_methods = dir(value_serializer)
        if ('dumps' in class_methods) and ('loads' in class_methods):
            self._value_serializer = value_serializer
            value_serializer_code = 0
        else:
            raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.', self)
    else:
        raise ValueError('value serializer must be one of None, {}, or a serializer class with dumps and loads methods.'.format(', '.join(serializers.serial_name_dict.keys())), self)

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
            raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.', self)
    else:
        raise ValueError('key serializer must be one of None, {}, or a serializer class with dumps and loads methods.'.format(', '.join(serializers.serial_name_dict.keys())), self)

    ## Write uuid, version, and other parameters and save encodings to new file
    n_bytes_file_bytes = int_to_bytes(n_bytes_file, 1)
    n_bytes_key_bytes = int_to_bytes(n_bytes_key, 1)
    n_bytes_value_bytes = int_to_bytes(n_bytes_value, 1)
    n_buckets_bytes = int_to_bytes(n_buckets, 4)
    n_bytes_index_bytes = int_to_bytes(0, 4) # Need to be removed eventually - depricated
    saved_value_serializer_bytes = int_to_bytes(value_serializer_code, 2)
    saved_key_serializer_bytes = int_to_bytes(key_serializer_code, 2)
    n_keys_bytes = int_to_bytes(0, 4)
    value_len_bytes = int_to_bytes(0, 4)
    if init_timestamps:
        init_timestamps_bytes = b'\x01'
    else:
        init_timestamps_bytes = b'\x00'

    file_ts_bytes = int_to_bytes(file_timestamp, timestamp_bytes_len)

    uuid7_bytes = uuid7.bytes

    init_write_bytes = uuid_variable_blt + current_version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + n_bytes_value_bytes + n_buckets_bytes + n_bytes_index_bytes +  saved_value_serializer_bytes + saved_key_serializer_bytes + n_keys_bytes + value_len_bytes + init_timestamps_bytes + file_ts_bytes + uuid7_bytes

    extra_bytes = b'0' * (sub_index_init_pos - len(init_write_bytes))

    init_write_bytes += extra_bytes

    return init_write_bytes

#######################################
### Fixed value alternative functions


def init_files_fixed(self, file_path, flag, key_serializer, value_len, n_buckets, write_buffer_size, init_bytes):
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

    self.writable = write
    self._write_buffer_size = write_buffer_size
    self._file_path = fp
    # self._platform = sys.platform

    self._buffer_data = bytearray()
    self._buffer_index = bytearray()
    self._buffer_index_set = set()

    self._thread_lock = Lock()

    if fp_exists:
        if write:
            self._file = io.open(fp, 'r+b', buffering=0)

            ## Locks
            portalocker.lock(self._file, portalocker.LOCK_EX)

        else:
            self._file = io.open(fp, 'rb', buffering=0)

            ## Lock
            portalocker.lock(self._file, portalocker.LOCK_SH)

        ## Read in initial bytes
        base_param_bytes = self._file.read(sub_index_init_pos)

        ## system and version check
        sys_uuid = base_param_bytes[:16]
        if sys_uuid != uuid_fixed_blt:
            portalocker.lock(self._file, portalocker.LOCK_UN)
            raise TypeError('This is not the correct file type.')

        version = bytes_to_int(base_param_bytes[16:18])
        if version < 3:
            raise ValueError('File is an older version.')

        ## Read the rest of the base parameters
        read_base_params_fixed(self, base_param_bytes, key_serializer)

        ## Check the n_keys
        if self._n_keys == n_keys_crash:
            if write:
                # print('File must have been closed incorrectly...rebuilding the n_keys...')
                counter = count()
                deque(zip(self.keys(), counter), maxlen=0)

                self._n_keys = next(counter)
            else:
                raise ValueError('File must have been closed incorrectly. Please open with write access to fix it.')


    else:
        if not write:
            raise FileNotFoundError('File was requested to be opened as read-only, but no file exists.')

        # if isinstance(n_buckets, int):
        #     self._n_buckets = n_buckets
        # else:
        #     self._n_buckets = init_n_buckets

        ## If init_bytes are passed, then parse bytes
        if isinstance(init_bytes, (bytes, bytearray)):
            init_bytes = bytearray(init_bytes)
            read_base_params_fixed(self, init_bytes, key_serializer)
            # 0 out the n_keys
            init_bytes[n_keys_pos:n_keys_pos+4] = int_to_bytes(0, 4)
        else:
            if value_len is None:
                raise ValueError('value_len must be an int > 0.')

            file_timestamp = make_timestamp_int()
            uuid8 = uuid.uuid8()

            init_bytes = init_base_params_fixed(self, key_serializer, value_len, n_buckets, file_timestamp, uuid8)

            self.uuid = uuid8
            self._n_buckets = n_buckets
            self._value_len = value_len
            self._init_timestamps = 0
            self._ts_bytes_len = 0

        self._n_bytes_file = n_bytes_file
        self._n_bytes_key = n_bytes_key

        self._n_keys = 0
        self._n_keys_pos = n_keys_pos

        self._file = io.open(fp, 'w+b', buffering=0)

        ## Locks
        portalocker.lock(self._file, portalocker.LOCK_EX)
        # if self._platform.startswith('linux'):
        #     flock(self._fd, LOCK_EX)

        ## Write new file
        with self._thread_lock:
            self._file.write(init_bytes)

            write_init_bucket_indexes(self._file, self._n_buckets, sub_index_init_pos, write_buffer_size)

    ## Create finalizer
    self._finalizer = weakref.finalize(self, close_files, self._file, n_keys_crash, self._n_keys_pos, self.writable)


def read_base_params_fixed(self, base_param_bytes, key_serializer):
    """

    """
    ## Assign attributes from init bytes
    self._n_bytes_file = bytes_to_int(base_param_bytes[18:19])
    self._n_bytes_key = bytes_to_int(base_param_bytes[19:20])
    # self._n_bytes_value = bytes_to_int(base_param_bytes[20:21])
    self._n_buckets = bytes_to_int(base_param_bytes[21:25])
    # self._n_bytes_index = bytes_to_int(base_param_bytes[25:29])
    # saved_value_serializer = bytes_to_int(base_param_bytes[29:31])
    saved_key_serializer = bytes_to_int(base_param_bytes[31:n_keys_pos])
    self._n_keys = bytes_to_int(base_param_bytes[n_keys_pos:n_keys_pos+4])
    self._value_len = bytes_to_int(base_param_bytes[37:41])
    self._init_timestamps = base_param_bytes[41]
    self._ts_bytes_len = 0
    self._file_timestamp = bytes_to_int(base_param_bytes[file_timestamp_pos:file_timestamp_pos + timestamp_bytes_len])

    self.uuid = uuid.UUID(bytes=bytes(base_param_bytes[49:65]))

    ## Other attrs
    self._n_keys_pos = n_keys_pos

    ## Pull out the serializers
    self._value_serializer = serializers.Bytes

    if saved_key_serializer > 0:
        self._key_serializer = serializers.serial_int_dict[saved_key_serializer]
    # elif key_serializer is None:
    #     raise ValueError('key serializer must be a serializer class with dumps and loads methods.')
    elif inspect.isclass(key_serializer):
        class_methods = dir(key_serializer)
        if ('dumps' in class_methods) and ('loads' in class_methods):
            self._key_serializer = key_serializer
        else:
            raise ValueError('If a custom class is passed for a serializer, then it must have dumps and loads methods.', self)
    else:
        raise ValueError('How did you mess up key_serializer so bad?!', self)


def init_base_params_fixed(self, key_serializer, value_len, n_buckets, file_timestamp, uuid7):
    """

    """
    ## Value serializer
    self._value_serializer = serializers.Bytes

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
            raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.', self)
    else:
        raise ValueError('key serializer must be one of None, {}, or a serializer class with dumps and loads methods.'.format(', '.join(serializers.serial_name_dict.keys())), self)

    ## Write uuid, version, and other parameters and save encodings to new file
    n_bytes_file_bytes = int_to_bytes(n_bytes_file, 1)
    n_bytes_key_bytes = int_to_bytes(n_bytes_key, 1)
    value_len_bytes = int_to_bytes(value_len, 4)
    n_buckets_bytes = int_to_bytes(n_buckets, 4)
    n_bytes_index_bytes = int_to_bytes(0, 4) # Need to be removed eventually - depricated
    saved_value_serializer_bytes = int_to_bytes(0, 2)
    saved_key_serializer_bytes = int_to_bytes(key_serializer_code, 2)
    n_keys_bytes = int_to_bytes(0, 4)
    n_bytes_value_bytes = int_to_bytes(0, 1)

    init_timestamps_bytes = b'\x00'

    file_ts_bytes = int_to_bytes(file_timestamp, timestamp_bytes_len)
    uuid7_bytes = uuid7.bytes

    init_write_bytes = uuid_fixed_blt + current_version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + n_bytes_value_bytes + n_buckets_bytes + n_bytes_index_bytes + saved_value_serializer_bytes + saved_key_serializer_bytes + n_keys_bytes + value_len_bytes + init_timestamps_bytes + file_ts_bytes + uuid7_bytes

    extra_bytes = b'0' * (sub_index_init_pos - len(init_write_bytes))
    init_write_bytes += extra_bytes

    return init_write_bytes


def get_value_fixed(file, key_hash, n_buckets, value_len):
    """
    Combines everything necessary to return a value.
    """
    data_block_pos = get_last_data_block_pos(file, key_hash, n_buckets)
    if data_block_pos:
        key_len_pos = data_block_pos + key_hash_len + n_bytes_file
        file.seek(key_len_pos)
        key_len = bytes_to_int(file.read(n_bytes_key))

        file.seek(key_len, 1)
        value = file.read(value_len)
    else:
        value = False

    return value


def iter_keys_values_fixed(file, n_buckets, include_key, include_value, value_len):
    """

    """
    one_extra_index_bytes_len = key_hash_len + n_bytes_file
    init_data_block_len = one_extra_index_bytes_len + n_bytes_key

    file_len = file.seek(0, 2)
    # file_len = len(file)
    file.seek(sub_index_init_pos + (n_buckets * n_bytes_file))

    while file.tell() < file_len:
        init_data_block = file.read(init_data_block_len)
        next_data_block_pos = bytes_to_int(init_data_block[key_hash_len:one_extra_index_bytes_len])
        key_len = bytes_to_int(init_data_block[one_extra_index_bytes_len:])
        if next_data_block_pos: # A value of 0 means it was deleted
            if include_key and include_value:
                key_value = file.read(key_len + value_len)
                key = key_value[:key_len]
                value = key_value[key_len:]
                yield key, value

            elif include_key:
                key = file.read(key_len)
                yield key
                file.seek(value_len, 1)

            else:
                file.seek(key_len, 1)
                value = file.read(value_len)
                yield value

        else:
            file.seek(key_len + value_len, 1)


def write_data_blocks_fixed(file, key, value, n_buckets, buffer_data, buffer_index, buffer_index_set, write_buffer_size):
    """

    """
    n_keys = 0

    ## Prep data
    file_len = file.seek(0, 2)

    key_hash = hash_key(key)
    key_bytes_len = len(key)
    # value_bytes_len = len(value)

    write_bytes = key_hash + b'\x01\x00\x00\x00\x00\x00' + int_to_bytes(key_bytes_len, n_bytes_key) + key + value

    ## flush write buffer if the size is getting too large
    bd_pos = len(buffer_data)
    write_len = len(write_bytes)

    bd_space = write_buffer_size - bd_pos
    if write_len > bd_space:
        file_len = flush_data_buffer(file, buffer_data)
        n_keys += update_index(file, buffer_index, n_buckets)
        bd_pos = 0

    ## Append to buffers
    data_pos_bytes = int_to_bytes(file_len + bd_pos, n_bytes_file)

    buffer_index.extend(key_hash + data_pos_bytes)
    buffer_index_set.add(key_hash)
    buffer_data.extend(write_bytes)

    return n_keys


# def prune_file_fixed(file, index_mmap, n_buckets, n_bytes_index, n_bytes_file, n_bytes_key, value_len, write_buffer_size, index_n_bytes_skip):
#     """

#     """
#     old_file_len = file.seek(0, 2)
#     removed_n_bytes = 0
#     accum_n_bytes = sub_index_init_pos

#     while (accum_n_bytes + removed_n_bytes) < old_file_len:
#         file.seek(accum_n_bytes)
#         del_key_len = file.read(1 + n_bytes_key)
#         key_len = bytes_to_int(del_key_len[1:])
#         data_block_len = 1 + n_bytes_key + key_len + value_len

#         if del_key_len[0]:
#             if removed_n_bytes > 0:
#                 key = file.read(key_len)
#                 key_hash = hash_key(key)
#                 index_bucket = get_index_bucket(key_hash, n_buckets)
#                 bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index, index_n_bytes_skip)
#                 bucket_pos1, bucket_pos2 = get_bucket_pos2(index_mmap, bucket_index_pos, n_bytes_index, index_n_bytes_skip)
#                 key_hash_pos = get_key_hash_pos(index_mmap, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
#                 index_mmap.seek(key_hash_pos + key_hash_len)
#                 data_block_rel_pos = bytes_to_int(index_mmap.read(n_bytes_file))
#                 index_mmap.seek(-n_bytes_file, 1)
#                 index_mmap.write(int_to_bytes(data_block_rel_pos - removed_n_bytes, n_bytes_file))

#             accum_n_bytes += data_block_len

#         else:
#             end_data_block_pos = accum_n_bytes + data_block_len
#             bytes_left_count = old_file_len - end_data_block_pos - removed_n_bytes

#             copy_file_range(file, file, bytes_left_count, end_data_block_pos, accum_n_bytes, write_buffer_size)

#             removed_n_bytes += data_block_len

#     os.ftruncate(file.fileno(), accum_n_bytes)
#     os.fsync(file.fileno())

#     return removed_n_bytes


def prune_file_fixed(file, reindex, n_buckets, n_bytes_file, n_bytes_key, value_len, write_buffer_size, buffer_data, buffer_index, buffer_index_set):
    """

    """
    metadata_key_added = False

    one_extra_index_bytes_len = key_hash_len + n_bytes_file
    init_data_block_len = one_extra_index_bytes_len + n_bytes_key

    file_len = file.seek(0, 2)
    data_block_read_start_pos = sub_index_init_pos + (n_buckets * n_bytes_file)
    total_data_size = file_len - data_block_read_start_pos
    data_block_write_start_pos = data_block_read_start_pos
    n_keys = 0

    ## Reindex if required
    if reindex:
        if isinstance(reindex, bool):
            if n_buckets not in n_buckets_reindex:
                raise ValueError('The existing n_buckets was not the original default value. If a non-default value is originally used, then the reindex value must be an int.')
            new_n_buckets = n_buckets_reindex[n_buckets]
        elif isinstance(reindex, int):
            new_n_buckets = reindex
        else:
            raise TypeError('reindex must be either a bool or an int.')

        if new_n_buckets:
            data_block_write_start_pos = sub_index_init_pos + (new_n_buckets * n_bytes_file)
            extra_bytes = data_block_write_start_pos - data_block_read_start_pos
            file_len = file_len + extra_bytes
            os.ftruncate(file.fileno(), file_len)

            # Move old data blocks to the end of the new file
            copy_file_range(file, file, total_data_size, data_block_read_start_pos, data_block_write_start_pos, write_buffer_size)

            data_block_read_start_pos = data_block_write_start_pos
            n_buckets = new_n_buckets

    ## Clear bucket indexes
    write_init_bucket_indexes(file, n_buckets, sub_index_init_pos, write_buffer_size)

    ## Iter through data blocks and only add the non-deleted ones
    # written_n_bytes = 0
    removed_count = 0
    while data_block_read_start_pos < file_len:
        file.seek(data_block_read_start_pos)
        init_data_block = file.read(init_data_block_len)

        next_data_block_pos = bytes_to_int(init_data_block[key_hash_len:one_extra_index_bytes_len])

        key_len_bytes = init_data_block[one_extra_index_bytes_len:one_extra_index_bytes_len + n_bytes_key]
        key_len = bytes_to_int(key_len_bytes)

        key_value_len = key_len + value_len
        # key_value_bytes = file.read(key_value_len)
        if next_data_block_pos: # A value of 0 means it was deleted
            key_value_bytes = file.read(key_value_len)

            key_hash = init_data_block[:key_hash_len]

            # Check if it's the metadata key - remove from n_keys at the end
            if key_hash == metadata_key_hash:
                metadata_key_added = True

            write_bytes = key_hash + b'\x01\x00\x00\x00\x00\x00' + key_len_bytes + key_value_bytes

            ## flush write buffer if the size is getting too large
            write_len = len(write_bytes)
            bd_pos = len(buffer_data)

            bd_space = write_buffer_size - bd_pos
            if write_len > bd_space:
                data_block_write_start_pos = flush_data_buffer(file, buffer_data, data_block_write_start_pos)
                n_keys += update_index(file, buffer_index, buffer_index_set, n_buckets)
                bd_pos = 0

            ## Append to buffers
            data_pos_bytes = int_to_bytes(data_block_write_start_pos + bd_pos, n_bytes_file)

            buffer_index.extend(key_hash + data_pos_bytes)
            buffer_data.extend(write_bytes)
        else:
            removed_count += 1
            # print(bytes_to_int(ts_key_value_bytes[ts_bytes_len:ts_bytes_len+key_len]))

        data_block_read_start_pos += init_data_block_len + key_value_len

    ## Finish writing if there's data left in buffer
    if buffer_data:
        data_block_write_start_pos = flush_data_buffer(file, buffer_data, data_block_write_start_pos)
        n_keys += update_index(file, buffer_index, buffer_index_set, n_buckets)

    os.ftruncate(file.fileno(), data_block_write_start_pos)
    os.fsync(file.fileno())

    if metadata_key_added:
        n_keys -= 1

    return n_keys, removed_count, n_buckets





















































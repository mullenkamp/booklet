#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import os
import sys
# import math
import io
from hashlib import blake2b, blake2s
import inspect
from threading import Lock
import portalocker
# from fcntl import flock, LOCK_EX, LOCK_SH, LOCK_UN
# import mmap
import weakref
import pathlib
# from time import time

# import serializers
from . import serializers

############################################
### Parameters

sub_index_init_pos = 200

# n_deletes_pos = 33
n_keys_pos = 33

n_bytes_index = 4
n_bytes_file = 6
n_bytes_key = 2
n_bytes_value = 4

key_hash_len = 13

uuid_variable_blt = b'O~\x8a?\xe7\\GP\xadC\nr\x8f\xe3\x1c\xfe'
uuid_fixed_blt = b'\x04\xd3\xb2\x94\xf2\x10Ab\x95\x8d\x04\x00s\x8c\x9e\n'

version = 3
version_bytes = version.to_bytes(2, 'little', signed=False)

init_n_buckets = 12007
n_buckets_reindex = {
    12007: 144013,
    144013: 1728017,
    1728017: 20736017,
    20736017: None,
    }

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


def close_files(file, write):
    """
    This is to be run as a finalizer to ensure that the files are closed properly.
    First will be to just close the files, I'll need to modify it to sync the index once I write the sync function.
    """
    # if write:
    #     # file.seek(n_keys_pos)
    #     # file.write(int_to_bytes(n_keys, 4))
    #     file_mmap.flush()
    #     file.flush()

    # if platform.startswith('linux'):
    #     flock(fd, LOCK_UN)
    # file.madvise(mmap.MADV_DONTNEED)
    # file_mmap.close()
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


def get_first_data_block_pos(file_mmap, bucket_index_pos):
    """

    """
    file_mmap.seek(bucket_index_pos)
    data_block_pos = bytes_to_int(file_mmap.read(n_bytes_file))

    return data_block_pos


def get_last_data_block_pos(file_mmap, key_hash, first_data_block_pos):
    """

    """
    index_len = key_hash_len + n_bytes_file
    data_block_pos = first_data_block_pos
    while True:
        file_mmap.seek(data_block_pos)
        data_index = file_mmap.read(index_len)
        next_data_block_pos = bytes_to_int(data_index[key_hash_len:])
        if next_data_block_pos:
            if data_index[:key_hash_len] == key_hash:
                return data_block_pos
            elif next_data_block_pos == 1:
                return 0
        else:
            return 0

        data_block_pos = next_data_block_pos





# def get_bucket_pos2(index_mmap, bucket_index_pos, n_bytes_index, index_n_bytes_skip):
#     """

#     """
#     index_mmap.seek(bucket_index_pos)
#     bucket_pos2_bytes = index_mmap.read(n_bytes_index*2)
#     bucket_pos1 = bytes_to_int(bucket_pos2_bytes[:n_bytes_index])
#     bucket_pos2 = bytes_to_int(bucket_pos2_bytes[n_bytes_index:])

#     return index_n_bytes_skip + bucket_pos1, index_n_bytes_skip + bucket_pos2


# def get_key_hash_pos(index_mmap, key_hash, bucket_pos1, bucket_pos2, n_bytes_file):
#     """

#     """
#     key_hash_pos = index_mmap.find(key_hash, bucket_pos1, bucket_pos2)

#     if key_hash_pos == -1:
#         return False

#     bucket_block_len = key_hash_len + n_bytes_file
#     while (key_hash_pos - bucket_pos1) % bucket_block_len > 0:
#         key_hash_pos = index_mmap.find(key_hash, key_hash_pos, bucket_pos2)
#         if key_hash_pos == -1:
#             return False

#     return key_hash_pos


# def get_data_block_pos(index_mmap, key_hash_pos, n_bytes_file):
#     """
#     The data block relative position of 0 is a delete/ignore flag, so all data block relative positions have been shifted forward by 1.
#     """
#     index_mmap.seek(key_hash_pos + key_hash_len)
#     data_block_rel_pos = bytes_to_int(index_mmap.read(n_bytes_file))

#     if data_block_rel_pos == 0:
#         return False

#     return data_block_rel_pos


def contains_key(file_mmap, key_hash, n_buckets):
    """
    Determine if a key is present in the file.
    """
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket)
    first_data_block_pos = get_first_data_block_pos(file_mmap, bucket_index_pos)
    if first_data_block_pos:
        data_block_pos = get_last_data_block_pos(file_mmap, key_hash, first_data_block_pos)
        if data_block_pos:
            return True
        else:
            return False
    else:
        return False


def get_value(file, key, n_buckets):
    """
    Combines everything necessary to return a value.
    """
    value = False

    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket)
    first_data_block_pos = get_first_data_block_pos(file, bucket_index_pos)
    if first_data_block_pos:
        data_block_pos = get_last_data_block_pos(file, key_hash, first_data_block_pos)
        if data_block_pos:
            key_len_pos = data_block_pos + key_hash_len + n_bytes_file
            file.seek(key_len_pos)
            key_len_value_len = file.read(n_bytes_key + n_bytes_value)
            key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
            value_len = bytes_to_int(key_len_value_len[n_bytes_key:])

            # file.seek(key_len_pos + key_len + n_bytes_key + n_bytes_value)
            file.seek(key_len, 1)
            value = file.read(value_len)

    return value


def iter_keys_values(file, n_buckets, include_key, include_value):
    """

    """
    one_extra_index_bytes_len = key_hash_len + n_bytes_file
    init_data_block_len = one_extra_index_bytes_len + n_bytes_key + n_bytes_value

    file_len = file.seek(0, 2)
    # file_len = len(file)
    file.seek(sub_index_init_pos + (n_buckets * n_bytes_file))

    while file.tell() < file_len:
        init_data_block = file.read(init_data_block_len)
        next_data_block_pos = bytes_to_int(init_data_block[key_hash_len:one_extra_index_bytes_len])
        key_len = bytes_to_int(init_data_block[one_extra_index_bytes_len:one_extra_index_bytes_len + n_bytes_key])
        value_len = bytes_to_int(init_data_block[one_extra_index_bytes_len + n_bytes_key:])
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


def assign_delete_flags(file_mmap, key, n_buckets):
    """
    Assigns 0 at the key hash index and the key/value data block.
    """
    index_len = key_hash_len + n_bytes_file

    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket)
    first_data_block_pos = get_first_data_block_pos(file_mmap, bucket_index_pos)
    if first_data_block_pos:
        previous_data_index_pos = bucket_index_pos
        data_block_pos = first_data_block_pos
        while True:
            file_mmap.seek(data_block_pos)
            data_index = file_mmap.read(index_len)
            next_data_block_pos = bytes_to_int(data_index[key_hash_len:])
            if next_data_block_pos:
                if data_index[:key_hash_len] == key_hash:
                    file_mmap.seek(-n_bytes_file, 1)
                    file_mmap.write(b'\x00\x00\x00\x00\x00\x00')
                    file_mmap.seek(previous_data_index_pos)
                    file_mmap.write(b'\x01\x00\x00\x00\x00\x00')
                    return True

                elif next_data_block_pos == 1:
                    return False
            else:
                return False

            previous_data_index_pos = data_block_pos + key_hash_len
            data_block_pos = next_data_block_pos

    else:
        return False


def write_data_blocks(file, key, value, n_buckets, buffer_data, buffer_index, write_buffer_size):
    """

    """
    n_keys = 0

    ## Prep data
    file_len = file.seek(0, 2)

    key_hash = hash_key(key)
    key_bytes_len = len(key)
    value_bytes_len = len(value)

    write_bytes = key_hash + b'\x01\x00\x00\x00\x00\x00' + int_to_bytes(key_bytes_len, n_bytes_key) + int_to_bytes(value_bytes_len, n_bytes_value) + key + value

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
    buffer_data.extend(write_bytes)

    return n_keys


def flush_data_buffer(file, buffer_data):
    """

    """
    bd_pos = len(buffer_data)
    old_len = file.seek(0, 2)
    if bd_pos > 0:
        _ = file.write(buffer_data)
        buffer_data.clear()
        # file.flush()

        new_file_len = old_len + bd_pos
        # file_mmap.resize(new_file_len)
        # file.madvise(mmap.MADV_DONTNEED)

        return new_file_len
    else:
        return old_len


def update_index(file_mmap, buffer_index, n_buckets):
    """

    """
    one_extra_index_bytes_len = key_hash_len + n_bytes_file

    buffer_len = len(buffer_index)

    ## Check for old keys and assign data_block_pos to previous key in chain
    n = int(buffer_len/one_extra_index_bytes_len)

    n_keys = 0
    for i in range(n):
        start = i * one_extra_index_bytes_len
        end = start + one_extra_index_bytes_len
        index_data = buffer_index[start:end]
        key_hash = index_data[:key_hash_len]
        new_data_block_pos = index_data[key_hash_len:]

        index_bucket = get_index_bucket(key_hash, n_buckets)
        bucket_index_pos = get_bucket_index_pos(index_bucket)
        first_data_block_pos = get_first_data_block_pos(file_mmap, bucket_index_pos)
        if first_data_block_pos:
            previous_data_index_pos = bucket_index_pos
            data_block_pos = first_data_block_pos
            while True:
                file_mmap.seek(data_block_pos)
                data_index = file_mmap.read(one_extra_index_bytes_len)
                next_data_block_pos = bytes_to_int(data_index[key_hash_len:])
                if next_data_block_pos:
                    if data_index[:key_hash_len] == key_hash:
                        file_mmap.seek(-n_bytes_file, 1)
                        file_mmap.write(b'\x00\x00\x00\x00\x00\x00')
                        file_mmap.seek(previous_data_index_pos)
                        file_mmap.write(new_data_block_pos)
                        break

                    elif next_data_block_pos == 1:
                        file_mmap.seek(-n_bytes_file, 1)
                        file_mmap.write(new_data_block_pos)
                        n_keys += 1
                        break
                else:
                    file_mmap.seek(previous_data_index_pos)
                    file_mmap.write(new_data_block_pos)
                    n_keys += 1
                    break
    
                previous_data_index_pos = data_block_pos + key_hash_len
                data_block_pos = next_data_block_pos
        else:
            file_mmap.seek(bucket_index_pos)
            file_mmap.write(new_data_block_pos)
            n_keys += 1

    buffer_index.clear()

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




# def update_index(file, buffer_index, index_mmap, n_bytes_index, n_bytes_file, n_buckets, index_n_bytes_skip):
#     """

#     """
#     ## Iterate through the write buffer index to determine which indexes already exist and which ones need to be added
#     ## Also update the bucket index along the way for the new indexes

#     buffer_len = len(buffer_index)

#     if buffer_len == 0:
#         return 0

#     # Get old mmap length and resize to new length
#     old_index_mmap_len = len(index_mmap)
#     index_mmap.resize(old_index_mmap_len + buffer_len)

#     # Determine the number of new indexes to process
#     one_extra_index_bytes_len = key_hash_len + n_bytes_file
#     n_new_indexes = int(buffer_len/one_extra_index_bytes_len)

#     # t1 = time()

#     # Get the bucket indexes and convert to numpy for easy math operations
#     # n_bytes_index must be 4 for numpy to work...
#     np_bucket_index = np.frombuffer(index_mmap, offset=0, count=n_buckets, dtype=np.uint32)

#     # Roll through all the new indexes and update the primary index
#     n_deletes = 0
#     for i in range(n_new_indexes):
#         start_pos = i * one_extra_index_bytes_len
#         end_pos = start_pos + one_extra_index_bytes_len
#         bucket_index1 = buffer_index[start_pos:end_pos]
#         key_hash = bucket_index1[:key_hash_len]

#         # Check if key already exists
#         index_bucket = get_index_bucket(key_hash, n_buckets)
#         bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index, index_n_bytes_skip)
#         bucket_pos1, bucket_pos2 = get_bucket_pos2(index_mmap, bucket_index_pos, n_bytes_index, index_n_bytes_skip)
#         key_hash_pos = get_key_hash_pos(index_mmap, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
#         if key_hash_pos:
#             data_block_pos = get_data_block_pos(index_mmap, key_hash_pos, n_bytes_file)
#             if data_block_pos:
#                 index_mmap.seek(-n_bytes_file, 1)
#                 index_mmap.write(int_to_bytes(0, n_bytes_file))

#                 # Now assign the delete flag in the data block to 0
#                 file.seek(data_block_pos)
#                 file.write(b'\x00')

#                 n_deletes += 1

#         # Write the index
#         index_bucket = get_index_bucket(key_hash, n_buckets)
#         old_bucket_pos = np_bucket_index[index_bucket]
#         index_mmap.move(old_bucket_pos + one_extra_index_bytes_len, old_bucket_pos, old_index_mmap_len - old_bucket_pos)
#         index_mmap.seek(old_bucket_pos)
#         index_mmap.write(bucket_index1)
#         if (index_bucket + 1) < n_buckets:
#             np_bucket_index[index_bucket+1:] += one_extra_index_bytes_len
#         old_index_mmap_len += one_extra_index_bytes_len

#     ## Write back the bucket index which includes the data position
#     ## No need when numpy uses the index_mmap directly
#     # index_mmap.seek(0)
#     # index_mmap.write(bucket_index_bytes)
#     # index_mmap.flush()
#     del np_bucket_index

#     buffer_index.clear()
#     # print(time() - t1)

#     return n_deletes


# def reindex(index_mmap, n_bytes_index, n_bytes_file, n_buckets, n_keys):
#     """

#     """
#     new_n_buckets = n_buckets_reindex[n_buckets]
#     if new_n_buckets:

#         ## Assign all of the components for sanity...
#         old_file_len = len(index_mmap)
#         one_extra_index_bytes_len = key_hash_len + n_bytes_file

#         old_bucket_index_len = n_buckets * n_bytes_index
#         new_bucket_index_len = new_n_buckets * n_bytes_index
#         new_data_index_len = one_extra_index_bytes_len * n_keys
#         # new_data_index_pos = new_bucket_index_len
#         # old_data_index_pos = old_bucket_index_len
#         old_data_index_len = old_file_len - old_bucket_index_len
#         old_n_keys = int(old_data_index_len/one_extra_index_bytes_len)

#         new_file_len = new_bucket_index_len + new_data_index_len

#         temp_old_data_index_pos = new_file_len + old_bucket_index_len
#         temp_file_len = new_file_len + old_file_len

#         ## Build the new bucket index and data index
#         index_mmap.resize(temp_file_len)
#         index_mmap.move(new_file_len, 0, old_file_len)

#         ## Run the reindexing
#         new_bucket_index_bytes = bytearray(create_initial_bucket_indexes(new_n_buckets, n_bytes_index))
#         np_bucket_index = np.frombuffer(new_bucket_index_bytes, dtype=np.uint32)

#         np_bucket_index_overflow = np.zeros(new_n_buckets, dtype=np.uint8)

#         ## Determine the positions of all buckets in the bucket_index
#         moving_old_data_index_pos = temp_old_data_index_pos
#         for i in range(old_n_keys):
#             index_mmap.seek(moving_old_data_index_pos)
#             bucket_index1 = index_mmap.read(one_extra_index_bytes_len)
#             data_block_rel_pos = bytes_to_int(bucket_index1[key_hash_len:])
#             if data_block_rel_pos:
#                 key_hash = bucket_index1[:key_hash_len]
#                 index_bucket = get_index_bucket(key_hash, new_n_buckets)
#                 if (index_bucket + 1) < new_n_buckets:
#                     np_bucket_index[index_bucket+1:] += one_extra_index_bytes_len
#             moving_old_data_index_pos += one_extra_index_bytes_len

#         ## Write the indexes in the proper spot
#         moving_old_data_index_pos = temp_old_data_index_pos
#         for i in range(old_n_keys):
#             index_mmap.seek(moving_old_data_index_pos)
#             bucket_index1 = index_mmap.read(one_extra_index_bytes_len)
#             data_block_rel_pos = bytes_to_int(bucket_index1[key_hash_len:])
#             if data_block_rel_pos:
#                 key_hash = bucket_index1[:key_hash_len]
#                 index_bucket = get_index_bucket(key_hash, new_n_buckets)
#                 overflow = np_bucket_index_overflow[index_bucket]
#                 new_bucket_pos = np_bucket_index[index_bucket] + int(overflow * one_extra_index_bytes_len)
#                 # print(new_bucket_pos)
#                 index_mmap.seek(new_bucket_pos)
#                 index_mmap.write(bucket_index1)
#                 np_bucket_index_overflow[index_bucket] += 1
#             moving_old_data_index_pos += one_extra_index_bytes_len

#         # print(np_bucket_index_overflow.max())

#         ## Resize the file
#         # index_mmap.move(new_data_pos, temp_data_pos, new_file_len - temp_data_pos)
#         index_mmap.resize(new_file_len)

#         ## Write back the bucket index which includes the data position
#         index_mmap.seek(0)
#         index_mmap.write(new_bucket_index_bytes)

#         index_mmap.flush()

#         return new_n_buckets
#     else:
#         return n_buckets


# def prune_file(file, index_mmap, n_buckets, n_bytes_index, n_bytes_file, n_bytes_key, n_bytes_value, write_buffer_size, index_n_bytes_skip):
#     """

#     """
#     old_file_len = file.seek(0, 2)
#     removed_n_bytes = 0
#     accum_n_bytes = sub_index_init_pos

#     while (accum_n_bytes + removed_n_bytes) < old_file_len:
#         file.seek(accum_n_bytes)
#         del_key_len_value_len = file.read(1 + n_bytes_key + n_bytes_value)
#         key_len_value_len = del_key_len_value_len[1:]
#         key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
#         value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
#         data_block_len = 1 + n_bytes_key + n_bytes_value + key_len + value_len

#         if del_key_len_value_len[0]:
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


def init_files_variable(self, file_path, flag, key_serializer, value_serializer, n_buckets, write_buffer_size):
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
    self._file_path = fp
    self._platform = sys.platform

    if fp_exists:
        if write:
            self._file = io.open(fp, 'r+b', buffering=0)
            self._fd = self._file.fileno()
            # self._file_mmap = mmap.mmap(self._fd, 0)
            # self._file_mmap = None

            self._buffer_data = bytearray()
            self._buffer_index = bytearray()

            ## Locks
            portalocker.lock(self._file, portalocker.LOCK_EX)
            # if self._platform.startswith('linux'):
            #     flock(self._fd, LOCK_EX)
            self._thread_lock = Lock()
        else:
            self._file = io.open(fp, 'rb', buffering=0)
            self._fd = self._file.fileno()
            # self._file_mmap = mmap.mmap(self._fd, 0, access=mmap.ACCESS_READ)
            # self._file_mmap = None
            self._buffer_data = None
            self._buffer_index = None

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

        version = bytes_to_int(base_param_bytes[16:18])
        if version < version:
            raise ValueError('File is an older version.')

        ## Check the data end pos
        self._n_keys_pos = n_keys_pos
        self._n_keys = bytes_to_int(base_param_bytes[n_keys_pos:n_keys_pos+4])

        # if self._n_keys == 0:
        #     raise ValueError('File must have been closed incorrectly.')

        # TODO : Create a process that will recreate the index if the data end pos is < 200. This can be done by rolling over the data blocks and iteratively writing the indexes.
        # At the moment, I'll just have it fail.
        # if self._data_end_pos < sub_index_init_pos:
        #     portalocker.lock(self._file, portalocker.LOCK_UN)
        #     raise FileExistsError('File has a corrupted index and will need to be rebuilt.')

        ## Read the rest of the base parameters
        read_base_params_variable(self, base_param_bytes, key_serializer, value_serializer)

    else:
        if not write:
            raise FileNotFoundError('File was requested to be opened as read-only, but no file exists.')

        if isinstance(n_buckets, int):
            self._n_buckets = n_buckets
        else:
            self._n_buckets = init_n_buckets

        init_write_bytes = init_base_params_variable(self, key_serializer, value_serializer, self._n_buckets)

        self._file = io.open(fp, 'w+b', buffering=0)
        self._fd = self._file.fileno()

        self._buffer_data = bytearray()
        self._buffer_index = bytearray()

        ## Locks
        portalocker.lock(self._file, portalocker.LOCK_EX)
        # if self._platform.startswith('linux'):
        #     flock(self._fd, LOCK_EX)
        self._thread_lock = Lock()

        ## Write new file
        with self._thread_lock:
            self._file.write(init_write_bytes)

            write_init_bucket_indexes(self._file, self._n_buckets, sub_index_init_pos, write_buffer_size)
            # self._file.flush()

            # self._file_mmap = mmap.mmap(self._fd, 0)
            # self._file_mmap = None
            # self._file_mmap.resize(sub_index_init_pos + (self._n_buckets * n_bytes_file))

    ## Create finalizer
    self._finalizer = weakref.finalize(self, close_files, self._file, self._write)


def copy_file_range(fsrc, fdst, count, offset_src, offset_dst, write_buffer_size):
    """
    Linux has magical copy abilities, but mac and windows do not.
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
            new_offset_src = offset_src + (count - write_count)
            new_offset_dst = offset_dst + (count - write_count)
        else:
            new_offset_src = offset_src + write_count
            new_offset_dst = offset_dst + write_count

        fsrc.seek(new_offset_src)
        data = fsrc.read(read_count)

        fdst.seek(new_offset_dst)
        write_count += fdst.write(data)

    fdst.flush()

    # if hasattr(os, 'copy_file_range'):
    # os.copy_file_range(fsrc.fileno(), fdst.fileno(), count, offset_src, offset_dst)
    # os.fsync(fdst.fileno())


# def init_index_file(file_path, file, file_len, fp_exists, data_end_pos, write_buffer_size, n_buckets, n_bytes_index):
#     """

#     """
#     index_file_path = file_path.parent.joinpath(file_path.name + '.tmp')

#     if fp_exists:
#         index_file = io.open(index_file_path, 'w+b', buffering=write_buffer_size)
#         copy_count = file_len - data_end_pos
#         copy_file_range(file, index_file, copy_count, data_end_pos, 0, write_buffer_size)
#         os.ftruncate(file.fileno(), data_end_pos)
#         os.fsync(file.fileno())
#     else:
#         index_file = io.open(index_file_path, 'w+b', buffering=write_buffer_size)
#         bucket_index_bytes = create_initial_bucket_indexes(n_buckets, n_bytes_index)
#         index_file.write(bucket_index_bytes)

#     index_file.flush()

#     return index_file_path, index_file


def read_base_params_variable(self, base_param_bytes, key_serializer, value_serializer):
    """

    """
    self._n_bytes_file = bytes_to_int(base_param_bytes[18:19])
    self._n_bytes_key = bytes_to_int(base_param_bytes[19:20])
    self._n_bytes_value = bytes_to_int(base_param_bytes[20:21])
    self._n_buckets = bytes_to_int(base_param_bytes[21:25])
    # self._n_bytes_index = bytes_to_int(base_param_bytes[25:29])
    saved_value_serializer = bytes_to_int(base_param_bytes[29:31])
    saved_key_serializer = bytes_to_int(base_param_bytes[31:33])
    # self._n_deletes = bytes_to_int(base_param_bytes[33:37])
    # self._value_len = bytes_to_int(base_param_bytes[37:41])

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


def init_base_params_variable(self, key_serializer, value_serializer, n_buckets):
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
    # self._n_bytes_index = n_bytes_index
    self._n_bytes_file = n_bytes_file
    self._n_bytes_key = n_bytes_key
    self._n_bytes_value = n_bytes_value
    self._n_buckets = n_buckets
    self._n_keys = 0
    self._n_keys_pos = n_keys_pos

    n_bytes_file_bytes = int_to_bytes(n_bytes_file, 1)
    n_bytes_key_bytes = int_to_bytes(n_bytes_key, 1)
    n_bytes_value_bytes = int_to_bytes(n_bytes_value, 1)
    n_buckets_bytes = int_to_bytes(n_buckets, 4)
    n_bytes_index_bytes = int_to_bytes(0, 4)
    saved_value_serializer_bytes = int_to_bytes(value_serializer_code, 2)
    saved_key_serializer_bytes = int_to_bytes(key_serializer_code, 2)
    n_keys_bytes = int_to_bytes(0, 4)
    value_len_bytes = int_to_bytes(0, 4)

    init_write_bytes = uuid_variable_blt + version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + n_bytes_value_bytes + n_buckets_bytes + n_bytes_index_bytes +  saved_value_serializer_bytes + saved_key_serializer_bytes + n_keys_bytes + value_len_bytes

    extra_bytes = b'0' * (sub_index_init_pos - len(init_write_bytes))

    init_write_bytes += extra_bytes

    return init_write_bytes

#######################################
### Fixed value alternative functions


def init_files_fixed(self, file_path, flag, key_serializer, value_len, n_buckets, write_buffer_size):
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
    self._file_path = fp
    self._platform = sys.platform

    if fp_exists:
        if write:
            self._file = io.open(fp, 'r+b', buffering=0)

            self._buffer_data = bytearray()
            self._buffer_index = bytearray()

            ## Locks
            portalocker.lock(self._file, portalocker.LOCK_EX)
            self._thread_lock = Lock()
        else:
            self._file = io.open(fp, 'rb', buffering=0)
            self._buffer_data = None
            self._buffer_index = None

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
        if version < version:
            raise ValueError('File is an older version.')

        ## Check the data end pos
        self._n_keys_pos = n_keys_pos
        self._n_keys = bytes_to_int(base_param_bytes[n_keys_pos:n_keys_pos+4])

        # TODO : Create a process that will recreate the index if the data end pos is < 200. This can be done by rolling over the data blocks and iteratively writing the indexes.
        # At the moment, I'll just have it fail.
        # if self._data_end_pos < sub_index_init_pos:
        #     portalocker.lock(self._file, portalocker.LOCK_UN)
        #     raise FileExistsError('File has a corrupted index and will need to be rebuilt.')

        ## Read the rest of the base parameters
        read_base_params_fixed(self, base_param_bytes, key_serializer)


    else:
        if not write:
            raise FileNotFoundError('File was requested to be opened as read-only, but no file exists.')

        if value_len is None:
            raise ValueError('value_len must be an int > 0.')

        if isinstance(n_buckets, int):
            self._n_buckets = n_buckets
        else:
            self._n_buckets = init_n_buckets

        init_write_bytes = init_base_params_fixed(self, key_serializer, value_len, self._n_buckets)

        self._file = io.open(fp, 'w+b', buffering=0)
        self._fd = self._file.fileno()

        self._buffer_data = bytearray()
        self._buffer_index = bytearray()

        ## Locks
        portalocker.lock(self._file, portalocker.LOCK_EX)
        # if self._platform.startswith('linux'):
        #     flock(self._fd, LOCK_EX)
        self._thread_lock = Lock()

        ## Write new file
        with self._thread_lock:
            self._file.write(init_write_bytes)

            write_init_bucket_indexes(self._file, self._n_buckets, sub_index_init_pos, write_buffer_size)

    ## Create finalizer
    self._finalizer = weakref.finalize(self, close_files, self._file, self._write)


def read_base_params_fixed(self, base_param_bytes, key_serializer):
    """

    """
    self._n_bytes_file = bytes_to_int(base_param_bytes[18:19])
    self._n_bytes_key = bytes_to_int(base_param_bytes[19:20])
    # self._n_bytes_value = bytes_to_int(base_param_bytes[20:21])
    self._n_buckets = bytes_to_int(base_param_bytes[21:25])
    self._n_bytes_index = bytes_to_int(base_param_bytes[25:29])
    # saved_value_serializer = bytes_to_int(base_param_bytes[29:31])
    saved_key_serializer = bytes_to_int(base_param_bytes[31:33])
    self._n_keys = bytes_to_int(base_param_bytes[33:37])
    self._value_len = bytes_to_int(base_param_bytes[37:41])

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


def init_base_params_fixed(self, key_serializer, value_len, n_buckets):
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
    self._n_bytes_index = n_bytes_index
    self._n_bytes_file = n_bytes_file
    self._n_bytes_key = n_bytes_key
    self._value_len = value_len
    self._n_buckets = n_buckets
    self._n_keys = 0
    self._n_keys_pos = n_keys_pos
    # self._data_block_rel_pos_delete_bytes = int_to_bytes(0, n_bytes_file)

    n_bytes_file_bytes = int_to_bytes(n_bytes_file, 1)
    n_bytes_key_bytes = int_to_bytes(n_bytes_key, 1)
    value_len_bytes = int_to_bytes(value_len, 4)
    n_buckets_bytes = int_to_bytes(n_buckets, 4)
    n_bytes_index_bytes = int_to_bytes(n_bytes_index, 4)
    saved_value_serializer_bytes = int_to_bytes(0, 2)
    saved_key_serializer_bytes = int_to_bytes(key_serializer_code, 2)
    n_keys_bytes = int_to_bytes(0, 4)
    n_bytes_value_bytes = int_to_bytes(0, 1)

    init_write_bytes = uuid_fixed_blt + version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + n_bytes_value_bytes + n_buckets_bytes + n_bytes_index_bytes + saved_value_serializer_bytes + saved_key_serializer_bytes + n_keys_bytes + value_len_bytes

    extra_bytes = b'0' * (sub_index_init_pos - len(init_write_bytes))
    init_write_bytes += extra_bytes

    return init_write_bytes


def get_value_fixed(file, key, n_buckets, value_len):
    """
    Combines everything necessary to return a value.
    """
    value = False

    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket)
    first_data_block_pos = get_first_data_block_pos(file, bucket_index_pos)
    if first_data_block_pos:
        data_block_pos = get_last_data_block_pos(file, key_hash, first_data_block_pos)
        if data_block_pos:
            key_len_pos = data_block_pos + key_hash_len + n_bytes_file
            file.seek(key_len_pos)
            key_len = bytes_to_int(file.read(n_bytes_key))

            file.seek(key_len, 1)
            value = file.read(value_len)

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


def write_data_blocks_fixed(file, key, value, n_buckets, buffer_data, buffer_index, write_buffer_size):
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
























































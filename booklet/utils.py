#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
# import os
import math
import io
from hashlib import blake2b, blake2s
import inspect
from threading import Lock
import portalocker
import mmap
import weakref
import numpy as np

# import serializers
from . import serializers

############################################
### Parameters

sub_index_init_pos = 200

n_deletes_pos_dict = {
    'variable': 33,
    'fixed': 36
    }

n_bytes_index = 4
n_bytes_file = 6
n_bytes_key = 2
n_bytes_value = 4

key_hash_len = 13

uuid_variable_blt = b'O~\x8a?\xe7\\GP\xadC\nr\x8f\xe3\x1c\xfe'
uuid_fixed_blt = b'\x04\xd3\xb2\x94\xf2\x10Ab\x95\x8d\x04\x00s\x8c\x9e\n'

version = 2
version_bytes = version.to_bytes(2, 'little', signed=False)

n_buckets_reindex = {
    10007: 100003,
    100003: 1000003,
    1000003: 10000019,
    10000019: 100000007,
    100000007: None
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


def close_file(mm, file):
    """
    This is to be run as a finalizer to ensure that the file is closed properly.
    """
    if not mm.closed:
        mm.flush()
        file.flush()
        portalocker.lock(file, portalocker.LOCK_UN)
        mm.close()
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


def create_initial_bucket_indexes(n_buckets, n_bytes_index):
    """

    """
    end_pos = sub_index_init_pos + ((n_buckets + 1) * n_bytes_index)
    bucket_index_bytes = int_to_bytes(end_pos, n_bytes_index) * (n_buckets + 1)
    return bucket_index_bytes


def get_index_bucket(key_hash, n_buckets):
    """
    The modulus of the int representation of the bytes hash puts the keys in evenly filled buckets.
    """
    return bytes_to_int(key_hash) % n_buckets


def get_bucket_index_pos(index_bucket, n_bytes_index):
    """

    """
    return sub_index_init_pos + (index_bucket * n_bytes_index)


def get_data_index_pos(n_buckets, n_bytes_index):
    """

    """
    return sub_index_init_pos + (n_buckets * n_bytes_index)


def get_bucket_pos(mm, bucket_index_pos, n_bytes_index):
    """

    """
    mm.seek(bucket_index_pos)
    bucket_pos = bytes_to_int(mm.read(n_bytes_index))

    return bucket_pos


def get_bucket_pos2(mm, bucket_index_pos, n_bytes_index):
    """

    """
    mm.seek(bucket_index_pos)
    bucket_pos2_bytes = mm.read(n_bytes_index*2)
    bucket_pos1 = bytes_to_int(bucket_pos2_bytes[:n_bytes_index])
    bucket_pos2 = bytes_to_int(bucket_pos2_bytes[n_bytes_index:])

    return bucket_pos1, bucket_pos2


def get_data_pos(mm, data_index_pos, n_bytes_index):
    """

    """
    mm.seek(data_index_pos)
    data_pos = bytes_to_int(mm.read(n_bytes_index))

    return data_pos


def get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file):
    """

    """
    key_hash_pos = mm.find(key_hash, bucket_pos1, bucket_pos2)

    if key_hash_pos == -1:
        return False

    bucket_block_len = key_hash_len + n_bytes_file
    while (key_hash_pos - bucket_pos1) % bucket_block_len > 0:
        key_hash_pos = mm.find(key_hash, key_hash_pos, bucket_pos2)
        if key_hash_pos == -1:
            return False

    return key_hash_pos


def get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file):
    """
    The data block relative position of 0 is a delete/ignore flag, so all data block relative positions have been shifted forward by 1.
    """
    mm.seek(key_hash_pos + key_hash_len)
    data_block_rel_pos = bytes_to_int(mm.read(n_bytes_file))

    if data_block_rel_pos == 0:
        return False

    data_block_pos = data_pos + data_block_rel_pos - 1

    return data_block_pos


def contains_key(mm, key_hash, n_bytes_index, n_bytes_file, n_buckets):
    """
    Determine if a key is present in the file.
    """
    # key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
    bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)

    bucket_block_len = key_hash_len + n_bytes_file

    key_hash_pos = mm.find(key_hash, bucket_pos1, bucket_pos2)

    if key_hash_pos == -1:
        return False

    while (key_hash_pos - bucket_pos1) % bucket_block_len > 0:
        key_hash_pos = mm.find(key_hash, key_hash_pos, bucket_pos2)
        if key_hash_pos == -1:
            return False

    mm.seek(key_hash_pos + key_hash_len)
    data_block_rel_pos = bytes_to_int(mm.read(n_bytes_file))

    if data_block_rel_pos == 0:
        return False

    return True


def get_value(mm, key, data_pos, n_bytes_index, n_bytes_file, n_bytes_key, n_bytes_value, n_buckets):
    """
    Combines everything necessary to return a value.
    """
    value = False

    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
    bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)
    key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
    if key_hash_pos:
        data_block_pos = get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file)
        if data_block_pos:
            mm.seek(1 + data_block_pos) # First byte is the delete flag
            key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
            key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
            value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
            mm.seek(key_len, 1)
            value = mm.read(value_len)

    return value


def iter_keys_values(mm, n_buckets, data_pos, include_key, include_value, n_bytes_key, n_bytes_value):
    """

    """
    file_len = len(mm)
    mm.seek(data_pos)

    while mm.tell() < file_len:
        del_key_len_value_len = mm.read(1 + n_bytes_key + n_bytes_value)
        key_len_value_len = del_key_len_value_len[1:]
        key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
        value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
        if del_key_len_value_len[0]:
            if include_key and include_value:
                key_value = mm.read(key_len + value_len)
                key = key_value[:key_len]
                value = key_value[key_len:]
                yield key, value
            elif include_key:
                key = mm.read(key_len)
                yield key
                mm.seek(value_len, 1)
            else:
                mm.seek(key_len, 1)
                value = mm.read(value_len)
                yield value
        else:
            mm.seek(key_len + value_len, 1)


def assign_delete_flags(mm, key, n_buckets, n_bytes_index, n_bytes_file, data_pos):
    """
    Assigns 0 at the key hash index and the key/value data block.
    """
    ## Get the data block relative position of the deleted key, then assign it 0.
    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
    bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)
    key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
    if key_hash_pos:
        data_block_pos = get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file)
        # mm.seek(key_hash_pos + key_hash_len)
        # data_block_rel_pos = bytes_to_int(mm.read(n_bytes_file))
        if data_block_pos:
            mm.seek(-n_bytes_file, 1)
            mm.write(int_to_bytes(0, n_bytes_file))
        else:
            return False
    else:
        return False

    ## Now assign the delete flag in the data block to 0
    mm.seek(data_block_pos)
    mm.write(b'\x00')

    return True


def write_data_blocks(mm, write_buffer, write_buffer_size, buffer_index, data_pos, key, value, n_bytes_key, n_bytes_value):
    """

    """
    wb_pos = write_buffer.tell()
    mm.seek(0, 2)
    file_len = mm.tell()

    key_bytes_len = len(key)
    key_hash = hash_key(key)

    value_bytes_len = len(value)

    write_bytes = b'\x01' + int_to_bytes(key_bytes_len, n_bytes_key) + int_to_bytes(value_bytes_len, n_bytes_value) + key + value

    write_len = len(write_bytes)

    wb_space = write_buffer_size - wb_pos
    if write_len > wb_space:
        file_len = flush_write_buffer(mm, write_buffer)
        wb_pos = 0

    if write_len > write_buffer_size:
        mm.resize(file_len + write_len)
        new_n_bytes = mm.write(write_bytes)
        wb_pos = 0
    else:
        new_n_bytes = write_buffer.write(write_bytes)

    # if key_hash in buffer_index:
    #     _ = buffer_index.pop(key_hash)

    # buffer_index[key_hash] = file_len + wb_pos - data_pos + 1
    buffer_index.append((key_hash, file_len + wb_pos - data_pos + 1))


def flush_write_buffer(mm, write_buffer):
    """

    """
    mm.seek(0, 2)
    file_len = mm.tell()
    wb_pos = write_buffer.tell()
    if wb_pos > 0:
        new_size = file_len + wb_pos
        mm.resize(new_size)
        write_buffer.seek(0)
        _ = mm.write(write_buffer.read(wb_pos))
        write_buffer.seek(0)

        return new_size
    else:
        return file_len


def clear(mm, n_buckets, n_bytes_index, n_deletes_pos):
    """

    """
    ## Cut back the file to the initial bytes + bucket index
    bucket_bytes = create_initial_bucket_indexes(n_buckets, n_bytes_index)

    mm.seek(0)
    mm.resize(sub_index_init_pos + len(bucket_bytes))
    mm.seek(sub_index_init_pos)
    mm.write(bucket_bytes)

    ## Update the n deletes
    mm.seek(n_deletes_pos)
    mm.write(int_to_bytes(0, 4))

    mm.flush()


def update_index(mm, buffer_index, data_pos, n_bytes_index, n_bytes_file, n_buckets):
    """

    """
    ## Iterate through the buffer index to determine which indexes already exist and which ones need to be added
    ## Also update the bucket index along the way for the new indexes
    one_extra_index_bytes_len = key_hash_len + n_bytes_file
    extra_bytes = 0
    new_indexes = {}
    key_hashes = set()
    # for key_hash, data_block_rel_pos in buffer_index.items():
    for data in reversed(buffer_index):
        key_hash, data_block_rel_pos = data
        if key_hash in key_hashes:
            mm.seek(data_pos + data_block_rel_pos - 1)
            mm.write(b'\x00')
        else:
            key_hashes.add(key_hash)
            index_bucket = get_index_bucket(key_hash, n_buckets)
            bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
            bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)
            key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
            if key_hash_pos:
                old_data_block_pos = get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file)
                if old_data_block_pos:
                    mm.seek(-n_bytes_file, 1)
                    mm.write(int_to_bytes(data_block_rel_pos, n_bytes_file))

                    ## Now assign the delete flag in the data block to 0
                    mm.seek(old_data_block_pos)
                    mm.write(b'\x00')
                else:
                    new_indexes[key_hash] = index_bucket, data_block_rel_pos
                    extra_bytes += one_extra_index_bytes_len
            else:
                new_indexes[key_hash] = index_bucket, data_block_rel_pos
                extra_bytes += one_extra_index_bytes_len

    ## Resize file for all the new indexes
    old_file_len = len(mm)
    new_file_len = old_file_len + extra_bytes
    mm.resize(new_file_len)

    ## Move all the data blocks forward by the extra bytes
    new_data_pos = data_pos + extra_bytes
    mm.move(new_data_pos, data_pos, old_file_len - data_pos)

    ## Get the bucket indexes and convert to numpy for easy math operations
    ## n_bytes_index must be 4 for numpy to work...
    mm.seek(sub_index_init_pos)
    bucket_index_bytes = bytearray(mm.read((n_buckets + 1) * n_bytes_index))
    np_bucket_index = np.frombuffer(bucket_index_bytes, dtype=np.uint32)

    ## Add in all the new indexes
    moving_data_pos = data_pos
    for key_hash, combo in new_indexes.items():
        index_bucket, data_block_rel_pos = combo
        old_bucket_pos = np_bucket_index[index_bucket]
        mm.move(old_bucket_pos + one_extra_index_bytes_len, old_bucket_pos, moving_data_pos - old_bucket_pos)
        mm.seek(old_bucket_pos)
        mm.write(key_hash + int_to_bytes(data_block_rel_pos, n_bytes_file))
        np_bucket_index[index_bucket+1:] += one_extra_index_bytes_len
        moving_data_pos += one_extra_index_bytes_len

    ## Write back the bucket index which includes the data position
    mm.seek(sub_index_init_pos)
    mm.write(bucket_index_bytes)

    # mm.flush()

    return new_data_pos


def reindex(mm, data_pos, n_bytes_index, n_bytes_file, n_buckets, n_keys):
    """

    """
    new_n_buckets = n_buckets_reindex[n_buckets]
    if new_n_buckets:

        ## Assign all of the components for sanity...
        old_file_len = len(mm)
        # data_len = old_file_len - data_pos
        one_extra_index_bytes_len = key_hash_len + n_bytes_file

        old_bucket_index_pos = sub_index_init_pos
        old_bucket_index_len = (n_buckets + 1) * n_bytes_index
        new_bucket_index_len = (new_n_buckets + 1) * n_bytes_index
        new_data_index_len = one_extra_index_bytes_len * n_keys
        new_data_index_pos = sub_index_init_pos + new_bucket_index_len
        new_data_pos = new_data_index_pos + new_data_index_len
        old_data_index_pos = old_bucket_index_pos + old_bucket_index_len
        old_data_index_len = data_pos - old_data_index_pos
        old_n_keys = int(old_data_index_len/one_extra_index_bytes_len)

        temp_data_pos = data_pos + new_bucket_index_len + new_data_index_len
        temp_old_data_index_pos = old_data_index_pos + new_bucket_index_len + new_data_index_len
        new_file_len = old_file_len + new_bucket_index_len + new_data_index_len

        ## Build the new bucket index and data index
        mm.resize(new_file_len)
        mm.move(temp_old_data_index_pos, old_data_index_pos, old_file_len - old_data_index_pos)

        ## Run the reindexing
        new_bucket_index_bytes = bytearray(create_initial_bucket_indexes(new_n_buckets, n_bytes_index))
        np_bucket_index = np.frombuffer(new_bucket_index_bytes, dtype=np.uint32)

        moving_data_index_pos = temp_old_data_index_pos
        for i in range(old_n_keys):
            mm.seek(moving_data_index_pos)
            bucket_index1 = mm.read(one_extra_index_bytes_len)
            data_block_rel_pos = bytes_to_int(bucket_index1[key_hash_len:])
            if data_block_rel_pos:
                key_hash = bucket_index1[:key_hash_len]
                index_bucket = get_index_bucket(key_hash, new_n_buckets)
                old_bucket_pos = np_bucket_index[index_bucket]
                moving_data_pos = np_bucket_index[-1]
                mm.move(old_bucket_pos + one_extra_index_bytes_len, old_bucket_pos, moving_data_pos - old_bucket_pos)
                mm.seek(old_bucket_pos)
                mm.write(key_hash + int_to_bytes(data_block_rel_pos, n_bytes_file))
                np_bucket_index[index_bucket+1:] += one_extra_index_bytes_len
                moving_data_index_pos += one_extra_index_bytes_len

        ## Move the indexes back to the original position and resize the file
        mm.move(new_data_pos, temp_data_pos, new_file_len - temp_data_pos)
        mm.resize(new_file_len - old_bucket_index_len - old_data_index_len)

        ## Write back the bucket index which includes the data position
        mm.seek(sub_index_init_pos)
        mm.write(new_bucket_index_bytes)

        mm.flush()

        return new_data_pos, new_n_buckets
    else:
        return data_pos, n_buckets














def prune_file(mm, n_buckets, n_bytes_index, n_bytes_file, n_bytes_key, n_bytes_value, data_pos):
    """

    """
    old_file_len = len(mm)
    removed_n_bytes = 0
    accum_n_bytes = data_pos

    while (accum_n_bytes + removed_n_bytes) < old_file_len:
        mm.seek(accum_n_bytes)
        del_key_len_value_len = mm.read(1 + n_bytes_key + n_bytes_value)
        key_len_value_len = del_key_len_value_len[1:]
        key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
        value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
        data_block_len = 1 + n_bytes_key + n_bytes_value + key_len + value_len

        if del_key_len_value_len[0]:
            if removed_n_bytes > 0:
                key = mm.read(key_len)
                key_hash = hash_key(key)
                index_bucket = get_index_bucket(key_hash, n_buckets)
                bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
                bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)
                key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
                mm.seek(key_hash_pos + key_hash_len)
                data_block_rel_pos = bytes_to_int(mm.read(n_bytes_file))
                mm.seek(-n_bytes_file, 1)
                mm.write(int_to_bytes(data_block_rel_pos - removed_n_bytes, n_bytes_file))

            accum_n_bytes += data_block_len

        else:
            end_data_block_pos = accum_n_bytes + data_block_len
            bytes_left_count = old_file_len - end_data_block_pos

            mm.move(accum_n_bytes, end_data_block_pos, bytes_left_count)

            removed_n_bytes += data_block_len

    mm.resize(accum_n_bytes)

    return removed_n_bytes


def init_existing_variable_booklet(self, base_param_bytes, key_serializer, value_serializer):
    """

    """
    self._n_deletes_pos = n_deletes_pos_dict['variable']
    self._n_bytes_file = bytes_to_int(base_param_bytes[18:19])
    self._n_bytes_key = bytes_to_int(base_param_bytes[19:20])
    self._n_bytes_value = bytes_to_int(base_param_bytes[20:21])
    self._n_buckets = bytes_to_int(base_param_bytes[21:25])
    self._n_bytes_index = bytes_to_int(base_param_bytes[25:29])
    saved_value_serializer = bytes_to_int(base_param_bytes[29:31])
    saved_key_serializer = bytes_to_int(base_param_bytes[31:self._n_deletes_pos])
    self._n_deletes = bytes_to_int(base_param_bytes[self._n_deletes_pos:self._n_deletes_pos+4])

    data_index_pos = get_data_index_pos(self._n_buckets, self._n_bytes_index)
    self._data_pos = get_data_pos(self._mm, data_index_pos, self._n_bytes_index)

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


def init_new_variable_booklet(self, key_serializer, value_serializer, n_buckets, file_path, write_buffer_size):
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
    self._n_bytes_index = n_bytes_index
    self._n_bytes_file = n_bytes_file
    self._n_bytes_key = n_bytes_key
    self._n_bytes_value = n_bytes_value
    self._n_buckets = n_buckets
    self._n_deletes = 0
    self._n_deletes_pos = n_deletes_pos_dict['variable']
    # self._data_block_rel_pos_delete_bytes = int_to_bytes(0, n_bytes_file)

    n_bytes_file_bytes = int_to_bytes(n_bytes_file, 1)
    n_bytes_key_bytes = int_to_bytes(n_bytes_key, 1)
    n_bytes_value_bytes = int_to_bytes(n_bytes_value, 1)
    n_buckets_bytes = int_to_bytes(n_buckets, 4)
    n_bytes_index_bytes = int_to_bytes(n_bytes_index, 4)
    saved_value_serializer_bytes = int_to_bytes(value_serializer_code, 2)
    saved_key_serializer_bytes = int_to_bytes(key_serializer_code, 2)
    n_deletes_bytes = int_to_bytes(0, 4)

    bucket_bytes = create_initial_bucket_indexes(n_buckets, n_bytes_index)

    init_write_bytes = uuid_variable_blt + version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + n_bytes_value_bytes + n_buckets_bytes + n_bytes_index_bytes +  saved_value_serializer_bytes + saved_key_serializer_bytes + n_deletes_bytes

    extra_bytes = b'0' * (sub_index_init_pos - len(init_write_bytes))

    init_write_bytes += extra_bytes

    ## Locks
    # if fcntl_import:
    #     fcntl.flock(self._file, fcntl.LOCK_EX)
    # portalocker.lock(self._file, portalocker.LOCK_EX)
    self._thread_lock = Lock()

    with self._thread_lock:
        self._file = io.open(file_path, 'w+b')
        portalocker.lock(self._file, portalocker.LOCK_EX)

        _ = self._file.write(init_write_bytes + bucket_bytes)
        self._file.flush()

        self._write_buffer = mmap.mmap(-1, write_buffer_size)
        self._buffer_index = []

        self._mm = mmap.mmap(self._file.fileno(), 0)
        self._finalizer = weakref.finalize(self, close_file, self._mm, self._file)

        self._data_pos = len(self._mm)



#######################################
### Fixed value alternative functions


def init_existing_fixed_booklet(self, base_param_bytes, key_serializer):
    """

    """
    self._n_deletes_pos = n_deletes_pos_dict['fixed']
    self._n_bytes_file = bytes_to_int(base_param_bytes[18:19])
    self._n_bytes_key = bytes_to_int(base_param_bytes[19:20])
    self._value_len = bytes_to_int(base_param_bytes[20:24])
    self._n_buckets = bytes_to_int(base_param_bytes[24:28])
    self._n_bytes_index = bytes_to_int(base_param_bytes[28:32])
    # saved_value_serializer = bytes_to_int(base_param_bytes[32:34])
    saved_key_serializer = bytes_to_int(base_param_bytes[34:self._n_deletes_pos])
    self._n_deletes = bytes_to_int(base_param_bytes[self._n_deletes_pos:self._n_deletes_pos+4])

    data_index_pos = get_data_index_pos(self._n_buckets, self._n_bytes_index)
    self._data_pos = get_data_pos(self._mm, data_index_pos, self._n_bytes_index)

    ## Pull out the serializers
    self._value_serializer = serializers.Bytes
    # if saved_value_serializer > 0:
    #     self._value_serializer = serializers.serial_int_dict[saved_value_serializer]
    # # elif value_serializer is None:
    # #     raise ValueError('value serializer must be a serializer class with dumps and loads methods.')
    # elif inspect.isclass(value_serializer):
    #     class_methods = dir(value_serializer)
    #     if ('dumps' in class_methods) and ('loads' in class_methods):
    #         self._value_serializer = value_serializer
    #     else:
    #         raise ValueError('If a custom class is passed for a serializer, then it must have dumps and loads methods.')
    # else:
    #     raise ValueError('How did you mess up value_serializer so bad?!')

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


def init_new_fixed_booklet(self, key_serializer, n_bytes_file, n_bytes_key, value_len, n_buckets, file_path, write_buffer_size):
    """

    """
    ## Value serializer
    self._value_serializer = serializers.Bytes
    # if value_serializer in serializers.serial_name_dict:
    #     value_serializer_code = serializers.serial_name_dict[value_serializer]
    #     self._value_serializer = serializers.serial_int_dict[value_serializer_code]
    # elif inspect.isclass(value_serializer):
    #     class_methods = dir(value_serializer)
    #     if ('dumps' in class_methods) and ('loads' in class_methods):
    #         self._value_serializer = value_serializer
    #         value_serializer_code = 0
    #     else:
    #         raise ValueError('If a class is passed for a serializer, then it must have dumps and loads methods.')
    # else:
    #     raise ValueError('value serializer must be one of None, {}, or a serializer class with dumps and loads methods.'.format(', '.join(serializers.serial_name_dict.keys())))

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
    self._n_deletes = 0
    self._n_deletes_pos = n_deletes_pos_dict['fixed']
    # self._data_block_rel_pos_delete_bytes = int_to_bytes(0, n_bytes_file)

    n_bytes_file_bytes = int_to_bytes(n_bytes_file, 1)
    n_bytes_key_bytes = int_to_bytes(n_bytes_key, 1)
    value_len_bytes = int_to_bytes(value_len, 4)
    n_buckets_bytes = int_to_bytes(n_buckets, 4)
    n_bytes_index_bytes = int_to_bytes(n_bytes_index, 4)
    saved_value_serializer_bytes = int_to_bytes(0, 2)
    saved_key_serializer_bytes = int_to_bytes(key_serializer_code, 2)
    n_deletes_bytes = int_to_bytes(0, 4)

    bucket_bytes = create_initial_bucket_indexes(n_buckets, n_bytes_index)

    init_write_bytes = uuid_fixed_blt + version_bytes + n_bytes_file_bytes + n_bytes_key_bytes + value_len_bytes + n_buckets_bytes + n_bytes_index_bytes + saved_value_serializer_bytes + saved_key_serializer_bytes + n_deletes_bytes

    extra_bytes = b'0' * (sub_index_init_pos - len(init_write_bytes))
    init_write_bytes += extra_bytes

    ## Locks
    # if fcntl_import:
    #     fcntl.flock(self._file, fcntl.LOCK_EX)
    # portalocker.lock(self._file, portalocker.LOCK_EX)
    self._thread_lock = Lock()

    with self._thread_lock:
        self._file = io.open(file_path, 'w+b')
        portalocker.lock(self._file, portalocker.LOCK_EX)

        _ = self._file.write(init_write_bytes + bucket_bytes)
        self._file.flush()

        self._write_buffer = mmap.mmap(-1, write_buffer_size)
        self._buffer_index = []

        self._mm = mmap.mmap(self._file.fileno(), 0)
        self._finalizer = weakref.finalize(self, close_file, self._mm, self._file)

        self._data_pos = len(self._mm)


def get_value_fixed(mm, key, data_pos, n_bytes_index, n_bytes_file, n_bytes_key, value_len, n_buckets):
    """
    Combines everything necessary to return a value.
    """
    value = False

    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
    bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)
    key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
    if key_hash_pos:
        data_block_pos = get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file)
        if data_block_pos:
            mm.seek(1 + data_block_pos) # First byte is the delete flag
            key_len = mm.read(n_bytes_key)
            key_len = bytes_to_int(key_len)
            # value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
            mm.seek(key_len, 1)
            value = mm.read(value_len)

    return value


def iter_keys_values_fixed(mm, n_buckets, data_pos, include_key, include_value, n_bytes_key, value_len):
    """

    """
    file_len = len(mm)
    mm.seek(data_pos)

    while mm.tell() < file_len:
        del_key_len = mm.read(1 + n_bytes_key)
        key_len = bytes_to_int(del_key_len[1:])
        if del_key_len[0]:
            if include_key and include_value:
                key_value = mm.read(key_len + value_len)
                key = key_value[:key_len]
                value = key_value[key_len:]
                yield key, value
            elif include_key:
                key = mm.read(key_len)
                yield key
                mm.seek(value_len, 1)
            else:
                mm.seek(key_len, 1)
                value = mm.read(value_len)
                yield value
        else:
            mm.seek(key_len + value_len, 1)


def write_data_blocks_fixed(mm, write_buffer, write_buffer_size, buffer_index, data_pos, key, value, n_bytes_index, n_bytes_key, n_bytes_file, n_buckets):
    """

    """
    key_hash = hash_key(key)

    if contains_key(mm, key_hash, n_bytes_index, n_bytes_file, n_buckets):
        index_bucket = get_index_bucket(key_hash, n_buckets)
        bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
        bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)
        key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
        data_block_pos = get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file)

        mm.seek(data_block_pos + 1)

        key_len = bytes_to_int(mm.read(n_bytes_key))
        mm.seek(key_len, 1)
        _ = mm.write(value)

    else:
        wb_pos = write_buffer.tell()
        mm.seek(0, 2)
        file_len = mm.tell()

        key_bytes_len = len(key)

        write_bytes = b'\x01' + int_to_bytes(key_bytes_len, n_bytes_key) + key + value

        write_len = len(write_bytes)

        wb_space = write_buffer_size - wb_pos
        if write_len > wb_space:
            file_len = flush_write_buffer(mm, write_buffer)
            wb_pos = 0

        if write_len > write_buffer_size:
            mm.resize(file_len + write_len)
            new_n_bytes = mm.write(write_bytes)
            wb_pos = 0
        else:
            new_n_bytes = write_buffer.write(write_bytes)

        # if key_hash in buffer_index:
        #     _ = buffer_index.pop(key_hash)

        buffer_index.append((key_hash, file_len + wb_pos - data_pos + 1))

    # return n_new_keys


def prune_file_fixed(mm, n_buckets, n_bytes_index, n_bytes_file, n_bytes_key, value_len, data_pos):
    """

    """
    old_file_len = len(mm)
    removed_n_bytes = 0
    accum_n_bytes = data_pos

    while (accum_n_bytes + removed_n_bytes) < old_file_len:
        mm.seek(accum_n_bytes)
        del_key_len = mm.read(1 + n_bytes_key)
        key_len = bytes_to_int(del_key_len[1:])
        data_block_len = 1 + n_bytes_key + key_len + value_len

        if del_key_len[0]:
            if removed_n_bytes > 0:
                key = mm.read(key_len)
                key_hash = hash_key(key)
                index_bucket = get_index_bucket(key_hash, n_buckets)
                bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_index)
                bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_index)
                key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
                mm.seek(key_hash_pos + key_hash_len)
                data_block_rel_pos = bytes_to_int(mm.read(n_bytes_file))
                mm.seek(-n_bytes_file, 1)
                mm.write(int_to_bytes(data_block_rel_pos - removed_n_bytes, n_bytes_file))

            accum_n_bytes += data_block_len

        else:
            end_data_block_pos = accum_n_bytes + data_block_len
            bytes_left_count = old_file_len - end_data_block_pos

            mm.move(accum_n_bytes, end_data_block_pos, bytes_left_count)

            removed_n_bytes += data_block_len

    mm.resize(accum_n_bytes)

    return removed_n_bytes



























































#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
# import os
# import io
from hashlib import blake2b, blake2s
# from time import time

############################################
### Parameters

sub_index_init_pos = 33
key_hash_len = 13

############################################
### Functions


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


def create_initial_bucket_indexes(n_buckets, n_bytes_file):
    """

    """
    end_pos = sub_index_init_pos + ((n_buckets + 1) * n_bytes_file)
    bucket_index_bytes = int_to_bytes(end_pos, n_bytes_file) * (n_buckets + 1)
    return bucket_index_bytes


def get_index_bucket(key_hash, n_buckets):
    """
    The modulus of the int representation of the bytes hash puts the keys in evenly filled buckets.
    """
    return bytes_to_int(key_hash) % n_buckets


def get_bucket_index_pos(index_bucket, n_bytes_file):
    """

    """
    return sub_index_init_pos + (index_bucket * n_bytes_file)


def get_data_index_pos(n_buckets, n_bytes_file):
    """

    """
    return sub_index_init_pos + (n_buckets * n_bytes_file)


def get_bucket_pos(mm, bucket_index_pos, n_bytes_file):
    """

    """
    mm.seek(bucket_index_pos)
    bucket_pos = bytes_to_int(mm.read(n_bytes_file))

    return bucket_pos


def get_bucket_pos2(mm, bucket_index_pos, n_bytes_file):
    """

    """
    mm.seek(bucket_index_pos)
    bucket_pos2_bytes = mm.read(n_bytes_file*2)
    bucket_pos1 = bytes_to_int(bucket_pos2_bytes[:n_bytes_file])
    bucket_pos2 = bytes_to_int(bucket_pos2_bytes[n_bytes_file:])

    return bucket_pos1, bucket_pos2


def get_data_pos(mm, data_index_pos, n_bytes_file):
    """

    """
    mm.seek(data_index_pos)
    data_pos = bytes_to_int(mm.read(n_bytes_file))

    return data_pos


def get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file):
    """

    """
    bucket_block_len = key_hash_len + n_bytes_file

    key_hash_pos = mm.find(key_hash, bucket_pos1, bucket_pos2)

    if key_hash_pos == -1:
        raise KeyError('key does not exist')

    while (key_hash_pos - bucket_pos1) % bucket_block_len > 0:
        key_hash_pos = mm.find(key_hash, key_hash_pos, bucket_pos2)
        if key_hash_pos == -1:
            raise KeyError('key does not exist')

    return key_hash_pos


def get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file):
    """
    The data block relative position of 0 is a delete/ignore flag, so all data block relative positions have been shifted forward by 1.
    """
    mm.seek(key_hash_pos + key_hash_len)
    data_block_rel_pos = bytes_to_int(mm.read(n_bytes_file))

    if data_block_rel_pos == 0:
        raise KeyError('key does not exist')

    data_block_pos = data_pos + data_block_rel_pos - 1

    return data_block_pos


def get_data_block(mm, data_block_pos, key=False, value=False, n_bytes_key=1, n_bytes_value=4):
    """
    Function to get either the key or the value or both from a data block.
    """
    mm.seek(data_block_pos)

    if key and value:
        key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
        key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
        value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
        key_value = mm.read(key_len + value_len)
        key = key_value[:key_len]
        value = key_value[key_len:]
        return key, value

    elif key:
        key_len = bytes_to_int(mm.read(n_bytes_key))
        mm.seek(n_bytes_value, 1)
        key = mm.read(key_len)
        return key

    elif value:
        key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
        key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
        value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
        mm.seek(key_len, 1)
        value = mm.read(value_len)
        return value
    else:
        raise ValueError('One or both key and value must be True.')


def contains_key(mm, key, n_bytes_file, n_buckets):
    """
    Determine if a key is present in the file.
    """
    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_file)
    bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_file)

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


def get_value(mm, key, data_pos, n_bytes_file, n_bytes_key, n_bytes_value, n_buckets):
    """
    Combines everything necessary to return a value.
    """
    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_file)
    bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_file)
    key_hash_pos = get_key_hash_pos(mm, key_hash, bucket_pos1, bucket_pos2, n_bytes_file)
    data_block_pos = get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file)
    value = get_data_block(mm, data_block_pos, key=False, value=True, n_bytes_key=n_bytes_key, n_bytes_value=n_bytes_value)

    return value


def iter_keys_values(mm, n_buckets, n_bytes_file, data_pos, key=False, value=False, n_bytes_key=1, n_bytes_value=4):
    """

    """
    bucket_init_pos = sub_index_init_pos + ((n_buckets + 1) * n_bytes_file)
    bucket_len = data_pos - bucket_init_pos
    hash_block_len = n_bytes_file + key_hash_len
    n_hash_blocks = bucket_len // hash_block_len

    key_hash_set = set()

    read_bytes = 0
    for b in range(n_hash_blocks):
        mm.seek(bucket_init_pos + read_bytes)
        hash_block = mm.read(hash_block_len)
        read_bytes += hash_block_len

        key_hash = hash_block[:key_hash_len]

        if key_hash in key_hash_set:
            continue

        key_hash_set.add(key_hash)

        data_block_rel_pos = bytes_to_int(hash_block[key_hash_len:])
        if data_block_rel_pos == 0:
            continue

        data_block_pos = data_pos + data_block_rel_pos - 1

        yield get_data_block(mm, data_block_pos, key, value, n_bytes_key, n_bytes_value)


def write_data_blocks(mm, write_buffer, write_buffer_size, buffer_index, data_pos, key, value, n_bytes_key, n_bytes_value):
    """

    """
    wb_pos = write_buffer.tell()
    mm.seek(0, 2)
    file_len = mm.tell()

    key_bytes_len = len(key)
    key_hash = hash_key(key)

    value_bytes_len = len(value)

    write_bytes = int_to_bytes(key_bytes_len, n_bytes_key) + int_to_bytes(value_bytes_len, n_bytes_value) + key + value

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

    if key_hash in buffer_index:
        _ = buffer_index.pop(key_hash)

    buffer_index[key_hash] = file_len + wb_pos - data_pos + 1


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


# def assign_delete_flag(mm, delete_key_hash, data_pos, n_bytes_file, n_buckets, data_block_rel_pos_delete_bytes, n_bytes_key, n_bytes_value):
#     """

#     """
#     index_bucket = get_index_bucket(delete_key_hash, n_buckets)
#     bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_file)
#     bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_file)
#     key_hash_pos = get_key_hash_pos(mm, delete_key_hash, bucket_pos1, bucket_pos2, n_bytes_file)

#     mm.seek(key_hash_pos + key_hash_len)
#     mm.write(data_block_rel_pos_delete_bytes)



# def delete_key_value(mm, delete_key_hash, data_pos, n_bytes_file, n_buckets, data_block_rel_pos_delete_bytes, n_bytes_key, n_bytes_value):
#     """

#     """
#     index_bucket = get_index_bucket(delete_key_hash, n_buckets)
#     bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_file)
#     bucket_pos1, bucket_pos2 = get_bucket_pos2(mm, bucket_index_pos, n_bytes_file)
#     key_hash_pos = get_key_hash_pos(mm, delete_key_hash, bucket_pos1, bucket_pos2, n_bytes_file)

#     data_block_pos = get_data_block_pos(mm, key_hash_pos, data_pos, n_bytes_file)

#     mm.seek(data_block_pos)
#     key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
#     key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
#     value_len = bytes_to_int(key_len_value_len[n_bytes_key:])

#     old_end_file_len = len(mm)
#     data_block_len = n_bytes_key + n_bytes_value + key_len + value_len
#     end_data_block_pos = data_block_pos + data_block_len
#     bytes_left_count = old_end_file_len - end_data_block_pos

#     mm.move(data_block_pos, end_data_block_pos, bytes_left_count)

#     new_end_file_len = old_end_file_len - data_block_len
#     mm.resize(new_end_file_len)

#     mm.seek(key_hash_pos + key_hash_len)
#     mm.write(data_block_rel_pos_delete_bytes)


def update_index(mm, buffer_index, data_pos, n_bytes_file, n_buckets, n_keys):
    """

    """
    ## Resize file and move data to end
    file_len = len(mm)
    n_new_indexes = len(buffer_index)
    extra_bytes = n_new_indexes * (n_bytes_file + key_hash_len)
    new_file_len = file_len + extra_bytes
    mm.resize(new_file_len)
    new_data_pos = data_pos + extra_bytes
    mm.move(new_data_pos, data_pos, file_len - data_pos)

    ## Organize the new indexes into the buckets
    index1 = {}
    for key_hash, data_block_rel_pos in buffer_index.items():
        buffer_bytes = key_hash + int_to_bytes(data_block_rel_pos, n_bytes_file)

        bucket = get_index_bucket(key_hash, n_buckets)
        if bucket in index1:
            index1[bucket] += buffer_bytes
        else:
            index1[bucket] = bytearray(buffer_bytes)

    ## Write new indexes
    buckets_end_pos = data_pos
    new_indexes_len = 0
    new_bucket_indexes = {}
    for bucket in range(n_buckets):
        bucket_index_pos = get_bucket_index_pos(bucket, n_bytes_file)
        old_bucket_pos = get_bucket_pos(mm, bucket_index_pos, n_bytes_file)
        new_bucket_pos = old_bucket_pos + new_indexes_len
        new_bucket_indexes[bucket] = new_bucket_pos

        if bucket in index1:
            bucket_data = index1[bucket]
            bucket_data_len = len(bucket_data)

            n_bytes_to_move = buckets_end_pos - new_bucket_pos
            if n_bytes_to_move > 0:
                mm.move(new_bucket_pos + bucket_data_len, new_bucket_pos, n_bytes_to_move)
            mm.seek(new_bucket_pos)
            mm.write(bucket_data)

            new_indexes_len += bucket_data_len
            buckets_end_pos += bucket_data_len

    ## Update the bucket indexes
    new_bucket_index_bytes = bytearray()
    for bucket, bucket_index in new_bucket_indexes.items():
        new_bucket_index_bytes += int_to_bytes(bucket_index, n_bytes_file)

    new_bucket_index_bytes += int_to_bytes(buckets_end_pos, n_bytes_file)

    # print(n_new_indexes)

    mm.seek(sub_index_init_pos)
    mm.write(new_bucket_index_bytes)
    # mm.flush()

    return new_data_pos


def prune_file(mm, n_buckets, n_bytes_file, n_bytes_key, n_bytes_value):
    """

    """
    data_index_pos = get_data_index_pos(n_buckets, n_bytes_file)
    data_pos = get_data_pos(mm, data_index_pos, n_bytes_file)
    bucket_block_len = key_hash_len + n_bytes_file
    hash_buckets_pos = data_index_pos + n_bytes_file
    hash_bucket_len = data_pos - hash_buckets_pos

    ## Iterate over the bucket indexes
    n_bucket_blocks = int(hash_bucket_len/bucket_block_len)

    mm.seek(hash_buckets_pos)
    hash_bucket_bytes = mm.read(hash_bucket_len)

    data_rel_pos_list = []
    hash_key_list = []
    delete_set = set()
    start_pos = 0
    for i in range(n_bucket_blocks):
        # print(i)
        data_rel_pos_index = start_pos+key_hash_len
        key_hash = hash_bucket_bytes[start_pos:data_rel_pos_index]
        start_pos = data_rel_pos_index+n_bytes_file
        data_rel_pos = bytes_to_int(hash_bucket_bytes[data_rel_pos_index:start_pos])
        hash_key_list.append(key_hash)
        data_rel_pos_list.append(data_rel_pos)

        if data_rel_pos == 0:
            delete_set.add(i)

    ## Remove data blocks associated with the deleted keys
    recovered_space = 0
    for pos in delete_set:
        key_hash = hash_key_list[pos]

        hash_key_positions = []
        append = hash_key_positions.append
        while True:
            try:
               pos = hash_key_list[pos+1:].index(key_hash) + pos+1
               append(pos)
            except ValueError:
                break

        for data_index_pos in hash_key_positions:
            data_rel_pos = data_rel_pos_list[data_index_pos]

            if data_rel_pos > 0:
                data_block_pos = data_pos + data_rel_pos - 1

                mm.seek(data_block_pos)
                key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
                key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
                value_len = bytes_to_int(key_len_value_len[n_bytes_key:])

                old_end_file_len = len(mm)
                data_block_len = n_bytes_key + n_bytes_value + key_len + value_len
                end_data_block_pos = data_block_pos + data_block_len
                bytes_left_count = old_end_file_len - end_data_block_pos

                mm.move(data_block_pos, end_data_block_pos, bytes_left_count)

                new_end_file_len = old_end_file_len - data_block_len
                mm.resize(new_end_file_len)

                for i, pos in enumerate(data_rel_pos_list):
                    if pos > data_rel_pos:
                        data_rel_pos_list[i] = pos - data_block_len

                data_rel_pos_list[data_index_pos] = 0
                recovered_space += data_block_len

    ## Save relative data positions back to file at hash buckets
    key_hash_data_rel_bytes = bytearray()
    for key_hash, data_rel_pos in zip(hash_key_list, data_rel_pos_list):
        if data_rel_pos < 0:
            data_rel_pos = 0
        data_rel_pos_bytes = int_to_bytes(data_rel_pos, n_bytes_file)
        key_hash_data_rel_bytes += key_hash + data_rel_pos_bytes

    mm.seek(hash_buckets_pos)
    mm.write(key_hash_data_rel_bytes)
    mm.flush()

    ## Update the hash bucket index by removing delete flags
    # TODO: Need to do this at some point...

    return data_pos, recovered_space
















































































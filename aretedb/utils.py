#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import os
import io
import mmap
from hashlib import blake2b
from time import time

############################################
### Parameters

# special_bytes = b'\xff\xff\xff\xff\xff\xff\xff\xff\xff'
# old_special_bytes = b'\xfe\xff\xff\xff\xff\xff\xff\xff\xff'

sub_index_init_pos = 18
key_hash_len = 11

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


def hash_key(key, key_hash_len=11):
    """

    """
    return blake2b(key, digest_size=key_hash_len, usedforsecurity=False).digest()


def create_initial_bucket_indexes(n_buckets, n_bytes_file):
    """

    """
    end_pos = sub_index_init_pos + ((n_buckets + 1) * n_bytes_file)
    bucket_index_bytes = int_to_bytes(end_pos, n_bytes_file) * (n_buckets + 1)
    return bucket_index_bytes


def get_index_bucket(key_hash, n_buckets=11):
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


def get_data_pos(mm, data_index_pos, n_bytes_file):
    """

    """
    mm.seek(data_index_pos)
    data_pos = bytes_to_int(mm.read(n_bytes_file))

    return data_pos


def get_data_block_pos(mm, key_hash, bucket_pos, data_pos, n_bytes_file):
    """
    The data block relative position of 0 is a delete/ignore flag, so all data block relative positions have been shifted forward by 1.
    """
    # mm.seek(bucket_pos)
    key_hash_pos = mm.find(key_hash, bucket_pos)

    if key_hash_pos == -1:
        raise KeyError(key_hash)

    while (key_hash_pos - bucket_pos) % key_hash_len > 0:
        key_hash_pos = mm.find(key_hash, key_hash_pos)

    mm.seek(key_hash_pos + key_hash_len)
    data_block_rel_pos = bytes_to_int(mm.read(n_bytes_file))

    if data_block_rel_pos == 0:
        raise KeyError(key_hash)

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
        key_len = mm.read(n_bytes_key)
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


def iter_keys_values(mm, n_buckets, n_bytes_file, data_pos, key=False, value=False, n_bytes_key=1, n_bytes_value=4):
    """

    """
    bucket_pos = sub_index_init_pos + ((n_buckets + 1) * n_bytes_file)
    bucket_len = data_pos - bucket_pos
    hash_block_len = n_bytes_file + key_hash_len
    n_hash_blocks = int(bucket_len / hash_block_len)

    read_bytes = 0
    for b in range(n_hash_blocks):
        mm.seek(bucket_pos + read_bytes)
        hash_block = mm.read(hash_block_len)
        read_bytes += hash_block_len
        data_block_rel_pos = bytes_to_int(hash_block[key_hash_len:])
        if data_block_rel_pos == 0:
            continue

        data_block_pos = data_pos + data_block_rel_pos - 1

        return get_data_block(mm, data_block_pos, key, value, n_bytes_key, n_bytes_value)


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

    if write_len > write_buffer_size:
        file_len += write_len
        mm.resize(file_len)
        new_n_bytes = mm.write(write_bytes)
        wb_pos = 0
    else:
        wb_space = write_buffer_size - wb_pos
        if write_len > wb_space:
            file_len = flush_write_buffer(mm, write_buffer)
            wb_pos = 0

        new_n_bytes = write_buffer.write(write_bytes)

    if key_hash in buffer_index:
        _ = buffer_index.pop(key_hash)

    buffer_index[key_hash] = file_len + wb_pos - data_pos + 1


def flush_write_buffer(mm, write_buffer):
    """

    """
    file_len = len(mm)
    wb_pos = write_buffer.tell()
    if wb_pos > 0:
        wb_pos = write_buffer.tell()
        write_buffer.seek(0)
        new_size = file_len + wb_pos
        mm.resize(new_size)
        _ = mm.write(write_buffer.read(wb_pos))
        write_buffer.seek(0)

        return new_size
    else:
        return file_len


def update_index(mm, buffer_index, data_pos, n_bytes_file, n_buckets):
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
    # The problem is here!!!!!!
    index1 = {}
    for key_hash, data_block_rel_pos in buffer_index.items():
        buffer_bytes = key_hash + int_to_bytes(data_block_rel_pos, n_bytes_file)

        bucket = get_index_bucket(key_hash, n_buckets)
        if bucket in index1:
            index1[bucket] += index1[bucket] + buffer_bytes
        else:
            index1[bucket] = bytearray(buffer_bytes)

    ## Write new indexes
    buckets_end_pos = data_pos
    n_new_indexes = 0
    new_bucket_indexes = {}
    for bucket in range(n_buckets):
        bucket_index_pos = get_bucket_index_pos(bucket, n_bytes_file)
        old_bucket_pos = get_bucket_pos(mm, bucket_index_pos, n_bytes_file)
        new_bucket_pos = old_bucket_pos + n_new_indexes
        new_bucket_indexes[bucket] = new_bucket_pos

        if bucket in index1:
            bucket_data = index1[bucket]
            bucket_data_len = len(bucket_data)

            n_bytes_to_move = buckets_end_pos - new_bucket_pos
            if n_bytes_to_move > 0:
                mm.move(new_bucket_pos + bucket_data_len, new_bucket_pos, n_bytes_to_move)
            mm.seek(new_bucket_pos)
            mm.write(bucket_data)

            n_new_indexes += bucket_data_len
            buckets_end_pos += bucket_data_len

    ## Update the bucket indexes
    new_bucket_index_bytes = bytearray()
    for bucket, bucket_index in new_bucket_indexes.items():
        new_bucket_index_bytes += int_to_bytes(bucket_index, n_bytes_file)

    new_bucket_index_bytes += int_to_bytes(buckets_end_pos, n_bytes_file)

    mm.seek(sub_index_init_pos)
    mm.write(new_bucket_index_bytes)

    buffer_index = {}

    return new_data_pos













# def write_chunk(file, index, key, value):
#     """

#     """
#     # key_len_bytes = len(key).to_bytes(1, 'little', signed=False)
#     # value_len_bytes = len(value).to_bytes(8, 'little', signed=False)

#     # write_bytes = memoryview(special_bytes + key_len_bytes + key + value_len_bytes + value)

#     # new_n_bytes = len(write_bytes)
#     # old_len = len(mm)

#     # mm.resize(old_len + new_n_bytes)

#     file.seek(0, 2)
#     pos = file.tell()

#     new_n_bytes = file.write(value)

#     # reassign_old_key(mm, key, old_len)

#     if key in index:
#         # old_index = list(index.pop(key))
#         # old_index.insert(0, key)
#         pos, len1 = index.pop(key)

#         index['00~._stale'].update({pos: len1})

#     index[key] = (pos, new_n_bytes)



# def write_many_chunks(file, index, key_value_dict):
#     """

#     """
#     file.seek(0, 2)
#     pos = file.tell()

#     write_bytes = bytearray()
#     for key, value in key_value_dict.items():
#         value_len_bytes = len(value)

#         if key in index:
#             pos0, len0 = index.pop(key)
#             index['00~._stale'].update({pos0: len0})

#         index[key] = (pos, value_len_bytes)
#         pos += value_len_bytes

#         write_bytes += value

#     new_n_bytes = file.write(write_bytes)

#     return new_n_bytes


def prune_file(file, index):
    """

    """
    file.flush()
    file_len = file.seek(0, 2)

    stale_pos = index['00~._stale'].copy()
    stale_pos.sort()

    stale_list = []
    for pos in stale_pos:
        file.seek(pos)
        value_len_bytes = int.from_bytes(file.read(4), 'little', signed=False)
        if value_len_bytes > file_len:
            raise ValueError('something went wrong...')
        stale_list.append((pos, value_len_bytes))

    stale_list.append((file_len + 1, 0))

    mm = mmap.mmap(file.fileno(), 0)

    extra_space = 0
    for i, pos in enumerate(stale_list[1:]):
        left_stale_pos, lost_space = stale_list[i]
        left_stale_pos = left_stale_pos - extra_space
        extra_space += lost_space
        left_chunk_pos = sum(stale_list[i])
        right_chunk_pos = stale_list[i+1][0] - 1
        count = right_chunk_pos - left_chunk_pos

        mm.move(left_stale_pos, left_chunk_pos, count)

    mm.flush()
    mm.close()
    file.truncate(file_len - extra_space)
    file.flush()

    index['00~._stale'] = []

    return extra_space


# def serialize_index(index):
#     """

#     """
#     index_bytes = bytearray()
#     for h, pos in index.items():
#         index_bytes += h + pos.to_bytes(8, 'little', signed=False)

#     return index_bytes


# def deserialize_index(index_path, read_buffer_size):
#     """

#     """
#     # start = time()
#     base_index = {}
#     file_len = os.stat(index_path).st_size
#     with io.open(index_path, 'rb') as file:
#         with io.BufferedReader(file, buffer_size=read_buffer_size) as mm:
#             n_chunks = (file_len//read_buffer_size)
#             read_len_list = [read_buffer_size] * (file_len//read_buffer_size)
#             read_len_list.append(file_len - (n_chunks * read_buffer_size))
#             for i in read_len_list:
#                 # print(i)
#                 key_chunk = mm.read(i)
#                 base_index.update({key_chunk[i:i+11]: int.from_bytes(key_chunk[i+11:i+19], 'little', signed=False) for i in range(0, len(key_chunk), 19)})
#     # end = time()
#     # print(end - start)

#     return base_index





# def find_key_pos(mm, key, start_pos=19, end_pos=None):
#     """

#     """
#     # key_len = len(key)
#     # key_len_bytes = key_len.to_bytes(1, 'little', signed=False)
#     # key_chunk = memoryview(special_bytes + key_len_bytes + key)

#     with io.open(index_path, 'rb') as file:
#         with io.BufferedReader(file, buffer_size=read_buffer_size) as buf:
#             with mmap(buf.fileno(), 0, access=ACCESS_READ) as mm:
#                 if end_pos is None:
#                     end_pos = len(mm)

#                 mm.seek(19)
#                 print(mm.read(11))

#                 key_pos = mm.find(key, start_pos, end_pos)
#                 if key_pos == -1:
#                     raise KeyError(key)
#                 while key_pos % 19 > 0:
#                     key_pos = mm.find(key, key_pos, end_pos)

#     return key_pos



# def reassign_old_key(mm, key, last_pos):
#     """

#     """
#     old_pos = find_chunk_pos(mm, key, last_pos)

#     if old_pos > -1:
#         mm.seek(old_pos)
#         _ = mm.write(old_special_bytes)


# def get_keys_values(mm, keys=False, values=False):
#     """

#     """
#     mm_len = len(mm)
#     mm.seek(18)

#     while mm.tell() < mm_len:
#         sp = mm.read(9)
#         key_len = int.from_bytes(mm.read(1), 'little')

#         if sp == special_bytes:
#             key = mm.read(key_len)
#             value_len = int.from_bytes(mm.read(8), 'little')
#             if keys and values:
#                 value = mm.read(value_len)
#                 yield key, value
#             elif keys:
#                 mm.seek(value_len, 1)
#                 yield key
#             elif values:
#                 value = mm.read(value_len)
#                 yield value
#             else:
#                 raise ValueError('keys and/or values must be True.')
#         else:
#             mm.seek(key_len, 1)
#             value_len = int.from_bytes(mm.read(8), 'little')
#             mm.seek(value_len, 1)


# def get_value(mm, key):
#     """

#     """
#     pos = find_chunk_pos(mm, key)

#     if pos > -1:
#         key_len = len(key)
#         mm.seek(pos+10+key_len)
#         value_len = int.from_bytes(mm.read(8), 'little')

#         value = mm.read(value_len)

#         return value
#     else:
#         return None




# def test_scan():
#     with io.open(index_path, 'rb') as file:
#         with io.BufferedReader(file, buffer_size=read_buffer_size) as buf:
#             with mmap(buf.fileno(), 0, access=ACCESS_READ) as mm:
#                 end_pos = len(mm)

#                 key_pos = mm.find(key, start_pos, end_pos)
#                 while key_pos % 19 > 0:
#                     key_pos = mm.find(key, key_pos, end_pos)
#     return key_pos






















































































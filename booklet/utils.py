#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import os
import io
from hashlib import blake2b, blake2s
from time import time

############################################
### Parameters

# special_bytes = b'\xff\xff\xff\xff\xff\xff\xff\xff\xff'
# old_special_bytes = b'\xfe\xff\xff\xff\xff\xff\xff\xff\xff'

sub_index_init_pos = 26
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
    key_hash_pos = mm.find(key_hash, bucket_pos, data_pos)
    bucket_block_len = key_hash_len + n_bytes_file

    if key_hash_pos == -1:
        raise KeyError(key_hash)

    while (key_hash_pos - bucket_pos) % bucket_block_len > 0:
        key_hash_pos = mm.find(key_hash, key_hash_pos, data_pos)
        if key_hash_pos == -1:
            raise KeyError(key_hash)

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


def get_value(mm, key, data_pos, n_bytes_file, n_bytes_key, n_bytes_value, n_buckets):
    """
    Combines everything necessary to return a value.
    """
    key_hash = hash_key(key)
    index_bucket = get_index_bucket(key_hash, n_buckets)
    bucket_index_pos = get_bucket_index_pos(index_bucket, n_bytes_file)
    bucket_pos = get_bucket_pos(mm, bucket_index_pos, n_bytes_file)
    data_block_pos = get_data_block_pos(mm, key_hash, bucket_pos, data_pos, n_bytes_file)
    value = get_data_block(mm, data_block_pos, key=False, value=True, n_bytes_key=n_bytes_key, n_bytes_value=n_bytes_value)

    return value


def iter_keys_values(mm, n_buckets, n_bytes_file, data_pos, key=False, value=False, n_bytes_key=1, n_bytes_value=4):
    """

    """
    bucket_init_pos = sub_index_init_pos + ((n_buckets + 1) * n_bytes_file)
    bucket_len = data_pos - bucket_init_pos
    hash_block_len = n_bytes_file + key_hash_len
    n_hash_blocks = int(bucket_len / hash_block_len)

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
        file_len += write_len
        mm.resize(file_len)
        new_n_bytes = mm.write(write_bytes)
        # mm.flush()
        wb_pos = 0
    else:
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
        # mm.flush()

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

    # print(n_new_indexes)

    mm.seek(sub_index_init_pos)
    mm.write(new_bucket_index_bytes)
    # mm.flush()

    return new_data_pos, {}


def prune_file(mm, n_buckets, n_bytes_file, n_bytes_key, n_bytes_value):
    """
    Doesn't seem to work. I'll need to come back to this...
    """
    data_index_pos = get_data_index_pos(n_buckets, n_bytes_file)
    data_pos = get_data_pos(mm, data_index_pos, n_bytes_file)

    ## Get bucket positions
    bucket_poss = {}
    for b in range(n_buckets):
        bucket_index_pos = get_bucket_index_pos(b, n_bytes_file)
        bucket_pos = get_bucket_pos(mm, bucket_index_pos, n_bytes_file)
        bucket_poss[b] = bucket_pos

    bucket_poss[n_buckets] = data_pos

    old_file_len = len(mm)

    del_dict = {b: [] for b in range(n_buckets)}

    # file_len_reduce = 0
    new_file_len = old_file_len

    hash_block_len = n_bytes_file + key_hash_len

    ## Iterate through the bucket indexes and move data blocks
    for b in range(n_buckets):
        bucket_pos = bucket_poss[b]
        next_bucket_pos = bucket_poss[b+1]
        n_hash_blocks = int((next_bucket_pos - bucket_pos)/hash_block_len)

        key_hash_set = set()

        read_bytes = 0
        for hb in range(n_hash_blocks):
            read_pos = bucket_pos + read_bytes
            mm.seek(read_pos)
            hash_block = mm.read(hash_block_len)

            key_hash = hash_block[:key_hash_len]
            data_block_rel_pos = bytes_to_int(hash_block[key_hash_len:])

            if data_block_rel_pos == 0:
                key_hash_set.add(key_hash)
                # print('trigger 0')
                continue

            if key_hash in key_hash_set:
                # print('trigger delete')
                del_dict[b].append(read_pos)

                ## Move data block
                data_block_pos = data_pos + data_block_rel_pos - 1
                mm.seek(data_block_pos)
                key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
                key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
                value_len = bytes_to_int(key_len_value_len[n_bytes_key:])

                data_block_len = n_bytes_key + n_bytes_value + key_len + value_len
                end_data_block_pos = data_block_pos + data_block_len
                end_file_len = new_file_len - end_data_block_pos

                mm.move(data_block_pos, end_data_block_pos, end_file_len)
                new_file_len = new_file_len - data_block_len
            else:
                key_hash_set.add(key_hash)

            read_bytes += hash_block_len

    ## Prune the indexes
    for b, del_list in del_dict.items():
        if del_list:
            for bucket_index_pos in del_list:
                # print('trigger del index')
                end_bucket_index_pos = bucket_index_pos + hash_block_len
                end_file_len = new_file_len - end_bucket_index_pos

                mm.move(bucket_index_pos, end_bucket_index_pos, end_file_len)
                new_file_len = new_file_len - hash_block_len

                for i in range(b+1, n_buckets+1):
                    # print(i)
                    bucket_poss[i] -= hash_block_len

    ## Save the new bucket indexes
    bucket_indexes_bytes = bytearray()
    for b, index in bucket_poss.items():
        bucket_indexes_bytes += int_to_bytes(index, n_bytes_file)

    mm.seek(sub_index_init_pos)
    mm.write(bucket_indexes_bytes)
    # print(len(bucket_indexes_bytes))
    # print(bucket_poss[n_buckets])

    ## Resize the entire file
    mm.resize(new_file_len)

    ## Flush
    mm.flush()

    ## new data position
    data_pos = get_data_pos(mm, data_index_pos, n_bytes_file)

    return data_pos, old_file_len - new_file_len

















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






















































































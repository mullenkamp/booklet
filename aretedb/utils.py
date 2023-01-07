#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import os
import io
from mmap import mmap, ACCESS_READ
from hashlib import blake2b
from time import time

############################################
### Parameters

# special_bytes = b'\xff\xff\xff\xff\xff\xff\xff\xff\xff'
# old_special_bytes = b'\xfe\xff\xff\xff\xff\xff\xff\xff\xff'

############################################
### Functions


def write_chunk(file, write_buffer, write_buffer_size, buffer_index, key, value):
    """

    """
    wb_pos = write_buffer.tell()
    file_pos = file.seek(0, 2)

    key_len_bytes = len(key)
    key_hash = blake2b(key, digest_size=11).digest()

    value_len_bytes = len(value)
    write_bytes = key_len_bytes.to_bytes(1, 'little', signed=False) + key + value_len_bytes.to_bytes(4, 'little', signed=False) + value

    if (value_len_bytes + 4) > write_buffer_size:
        new_n_bytes = file.write(write_bytes)
        wb_pos = 0
        file_pos = file.tell()
    else:
        wb_space = write_buffer_size - wb_pos
        if (value_len_bytes + 4) > wb_space:
            write_buffer.seek(0)
            _ = file.write(write_buffer.read(wb_pos))
            write_buffer.seek(0)
            wb_pos = 0
            file_pos = file.tell()

        new_n_bytes = write_buffer.write(write_bytes)

    if key_hash in buffer_index:
        _ = buffer_index.pop(key_hash)

    # if key in index:
    #     pos0 = index.pop(key)
    #     index['00~._stale'].append(pos0)

    buffer_index[key_hash] = file_pos + wb_pos



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


def get_value(file, index, key):
    """

    """
    key_len_bytes = len(key)
    key_hash = blake2b(key, digest_size=11).digest()
    pos = index[key_hash]

    file.seek(pos + 1 + key_len_bytes)
    value_len_bytes = int.from_bytes(file.read(4), 'little', signed=False)

    value = file.read(value_len_bytes)

    return value


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

    mm = mmap(file.fileno(), 0)

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


def serialize_index(index):
    """

    """
    index_bytes = bytearray()
    for h, pos in index.items():
        index_bytes += h + pos.to_bytes(8, 'little', signed=False)

    return index_bytes


def deserialize_index(index_path, read_buffer_size):
    """

    """
    # start = time()
    base_index = {}
    file_len = os.stat(index_path).st_size
    with io.open(index_path, 'rb') as file:
        with io.BufferedReader(file, buffer_size=read_buffer_size) as mm:
            n_chunks = (file_len//read_buffer_size)
            read_len_list = [read_buffer_size] * (file_len//read_buffer_size)
            read_len_list.append(file_len - (n_chunks * read_buffer_size))
            for i in read_len_list:
                # print(i)
                key_chunk = mm.read(i)
                base_index.update({key_chunk[i:i+11]: int.from_bytes(key_chunk[i+11:i+19], 'little', signed=False) for i in range(0, len(key_chunk), 19)})
    # end = time()
    # print(end - start)

    return base_index





def find_key_pos(mm, key, start_pos=19, end_pos=None):
    """

    """
    # key_len = len(key)
    # key_len_bytes = key_len.to_bytes(1, 'little', signed=False)
    # key_chunk = memoryview(special_bytes + key_len_bytes + key)

    with io.open(index_path, 'rb') as file:
        with io.BufferedReader(file, buffer_size=read_buffer_size) as buf:
            with mmap(buf.fileno(), 0, access=ACCESS_READ) as mm:
                if end_pos is None:
                    end_pos = len(mm)

                mm.seek(19)
                print(mm.read(11))

                key_pos = mm.find(key, start_pos, end_pos)
                if key_pos == -1:
                    raise KeyError(key)
                while key_pos % 19 > 0:
                    key_pos = mm.find(key, key_pos, end_pos)

    return key_pos



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




def test_scan():
    with io.open(index_path, 'rb') as file:
        with io.BufferedReader(file, buffer_size=read_buffer_size) as buf:
            with mmap(buf.fileno(), 0, access=ACCESS_READ) as mm:
                end_pos = len(mm)

                key_pos = mm.find(key, start_pos, end_pos)
                while key_pos % 19 > 0:
                    key_pos = mm.find(key, key_pos, end_pos)
    return key_pos






















































































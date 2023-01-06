#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
from mmap import mmap

############################################
### Parameters

# special_bytes = b'\xff\xff\xff\xff\xff\xff\xff\xff\xff'
# old_special_bytes = b'\xfe\xff\xff\xff\xff\xff\xff\xff\xff'

############################################
### Functions


def write_chunk(file, write_buffer, write_buffer_size, wb_pos, index, key, value):
    """

    """
    # key_len_bytes = len(key).to_bytes(1, 'little', signed=False)
    # value_len_bytes = len(value).to_bytes(8, 'little', signed=False)

    # write_bytes = memoryview(special_bytes + key_len_bytes + key + value_len_bytes + value)

    # new_n_bytes = len(write_bytes)
    # old_len = len(mm)

    # mm.resize(old_len + new_n_bytes)

    file_pos = file.seek(0, 2)

    value_len_bytes = len(value)
    write_bytes = value_len_bytes.to_bytes(4, 'little', signed=False) + value

    if (value_len_bytes + 4) > write_buffer_size:
        new_n_bytes = file.write(write_bytes)
        # file.flush()
        new_wb_pos = wb_pos
        wb_pos = 0
        file_pos = file.tell()
    else:
        # wb_pos = write_buffer.tell()
        wb_space = write_buffer_size - wb_pos
        if wb_space < (value_len_bytes + 4):
            write_buffer.seek(0)
            _ = file.write(write_buffer.read(wb_pos))
            # file.flush()
            write_buffer.seek(0)
            wb_pos = 0
            file_pos = file.tell()
        # else:
        #     write_buffer.seek(wb_pos)

        new_n_bytes = write_buffer.write(write_bytes)
        new_wb_pos = wb_pos + new_n_bytes

    if key in index:
        # old_index = list(index.pop(key))
        # old_index.insert(0, key)
        pos = index.pop(key)

        index['00~._stale'].append(pos)

    index[key] = file_pos + wb_pos

    return new_wb_pos


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


def read_index(file_path):
    """

    """



def get_value(file, index, key):
    """

    """
    pos = index[key]

    file.seek(pos)
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








# def find_chunk_pos(mm, key, end_pos=None):
#     """

#     """
#     key_len = len(key)
#     key_len_bytes = key_len.to_bytes(1, 'little', signed=False)
#     key_chunk = memoryview(special_bytes + key_len_bytes + key)

#     if end_pos is None:
#         end_pos = len(mm)

#     # Is this that fastest way to find the last position when the file is large?
#     last_pos = mm.find(key_chunk, 18, end_pos)

#     # if last_pos > -1:
#     #     while True:
#     #          mm.seek(last_pos+10+key_len)

#     #          value_len = int.from_bytes(mm.read(8), 'little')
#     #          total_len = 10 + key_len + 8 + value_len

#     #          end_pos0 = last_pos + total_len

#     #          if end_pos0 == end_pos:
#     #              break
#     #          else:
#     #              mm.seek(value_len, 1)
#     #              next_special = mm.read(9)

#     #              if next_special == special_bytes:
#     #                  break

#     #          last_pos = mm.find(key_chunk, len(last_pos), end_pos)

#     return last_pos


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



























































































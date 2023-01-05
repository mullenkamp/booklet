#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""


############################################
### Parameters

special_bytes = b'\xff\xff\xff\xff\xff\xff\xff\xff\xff'
old_special_bytes = b'\xfe\xff\xff\xff\xff\xff\xff\xff\xff'

############################################
### Functions


def write_chunk(file, key, value):
    """

    """
    key_len_bytes = len(key).to_bytes(1, 'little', signed=False)
    value_len_bytes = len(value).to_bytes(8, 'little', signed=False)

    write_bytes = memoryview(special_bytes + key_len_bytes + key + value_len_bytes + value)

    # new_n_bytes = len(write_bytes)
    # old_len = len(mm)

    # mm.resize(old_len + new_n_bytes)

    file.seek(0, 2)

    new_n_bytes = file.write(write_bytes)

    # reassign_old_key(mm, key, old_len)

    return new_n_bytes


def write_many_chunks(file, key_value_dict):
    """

    """
    write_bytes = bytearray()
    for key, value in key_value_dict.items():
        key_len_bytes = len(key).to_bytes(1, 'little', signed=False)
        value_len_bytes = len(value).to_bytes(8, 'little', signed=False)

        write_bytes += memoryview(special_bytes + key_len_bytes + key + value_len_bytes + value)

    file.seek(0, 2)

    new_n_bytes = file.write(write_bytes)

    # reassign_old_key(mm, key, old_len)

    return new_n_bytes


def find_chunk_pos(mm, key, end_pos=None):
    """

    """
    key_len = len(key)
    key_len_bytes = key_len.to_bytes(1, 'little', signed=False)
    key_chunk = memoryview(special_bytes + key_len_bytes + key)

    if end_pos is None:
        end_pos = len(mm)

    # Is this that fastest way to find the last position when the file is large?
    last_pos = mm.find(key_chunk, 18, end_pos)

    # if last_pos > -1:
    #     while True:
    #          mm.seek(last_pos+10+key_len)

    #          value_len = int.from_bytes(mm.read(8), 'little')
    #          total_len = 10 + key_len + 8 + value_len

    #          end_pos0 = last_pos + total_len

    #          if end_pos0 == end_pos:
    #              break
    #          else:
    #              mm.seek(value_len, 1)
    #              next_special = mm.read(9)

    #              if next_special == special_bytes:
    #                  break

    #          last_pos = mm.find(key_chunk, len(last_pos), end_pos)

    return last_pos


def reassign_old_key(mm, key, last_pos):
    """

    """
    old_pos = find_chunk_pos(mm, key, last_pos)

    if old_pos > -1:
        mm.seek(old_pos)
        _ = mm.write(old_special_bytes)


def get_keys_values(mm, keys=False, values=False):
    """

    """
    mm_len = len(mm)
    mm.seek(18)

    while mm.tell() < mm_len:
        sp = mm.read(9)
        key_len = int.from_bytes(mm.read(1), 'little')

        if sp == special_bytes:
            key = mm.read(key_len)
            value_len = int.from_bytes(mm.read(8), 'little')
            if keys and values:
                value = mm.read(value_len)
                yield key, value
            elif keys:
                mm.seek(value_len, 1)
                yield key
            elif values:
                value = mm.read(value_len)
                yield value
            else:
                raise ValueError('keys and/or values must be True.')
        else:
            mm.seek(key_len, 1)
            value_len = int.from_bytes(mm.read(8), 'little')
            mm.seek(value_len, 1)


def get_value(mm, key):
    """

    """
    pos = find_chunk_pos(mm, key)

    if pos > -1:
        key_len = len(key)
        mm.seek(pos+10+key_len)
        value_len = int.from_bytes(mm.read(8), 'little')

        value = mm.read(value_len)

        return value
    else:
        return None



























































































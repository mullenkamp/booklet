#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 13:55:17 2024

@author: mike
"""
import pytest
import io
import os
from booklet import __version__, FixedValue, VariableValue, utils
from tempfile import NamedTemporaryFile
import concurrent.futures
from hashlib import blake2s
import mmap
from time import time

##############################################
### Parameters



##############################################
### Functions


def set_item(f, key, value):
    f[key] = value

    return key


##############################################
### Tests

print(__version__)

tf = NamedTemporaryFile()
file_path = tf.name

data_dict = {key: key*2 for key in range(2, 30)}


def test_set_items():
    with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with VariableValue(file_path) as f:
        value = f[10]

    assert value == data_dict[10]


def test_update():
    with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        f.update(data_dict)

    with VariableValue(file_path) as f:
        value = f[10]

    assert value == data_dict[10]


def test_threading_writes():
    with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for key, value in data_dict.items():
                future = executor.submit(set_item, f, key, value)
                futures.append(future)

        _ = concurrent.futures.wait(futures)

    with VariableValue(file_path) as f:
        value = f[10]

    assert value == data_dict[10]


def test_keys():
    with VariableValue(file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict.keys()))

    assert source_keys == keys


def test_items():
    with VariableValue(file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


def test_contains():
    with VariableValue(file_path) as f:
        for key in data_dict:
            if key not in f:
                raise KeyError(key)

    assert True


def test_len():
    with VariableValue(file_path) as f:
        new_len = len(f)

    assert len(data_dict) == new_len


# @pytest.mark.parametrize('index', [10, 12])
def test_delete_len():
    indexes = [10, 12]

    for index in indexes:
        _ = data_dict.pop(index)

        with VariableValue(file_path, 'w') as f:
            f[index] = 0
            f[index] = 0
            del f[index]

            # f.sync()

            new_len = len(f)

            try:
                _ = f[index]
                raise ValueError()
            except KeyError:
                pass

        assert new_len == len(data_dict)

def test_items2():
    with VariableValue(file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value

def test_values():
    with VariableValue(file_path) as f:
        for key, source_value in data_dict.items():
            value = f[key]
            assert source_value == value


# def test_prune():
#     with VariableValue(file_path, 'w') as f:
#         recovered_space = f.prune()

#     assert recovered_space > 0

#     with VariableValue(file_path) as f:
#         for key, source_value in data_dict.items():
#             value = f[key]
#             assert source_value == value


def test_set_items_get_items():
    with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with VariableValue(file_path, 'w') as f:
        f[50] = [0, 0]
        value = f[11]

    with VariableValue(file_path) as f:
        value = f[50]
        assert value == [0, 0]

        value = f[11]
        assert value == data_dict[11]


# def test_reindex():
#     """

#     """
#     with VariableValue(file_path, 'w') as f:
#         old_n_buckets = f._n_buckets
#         for i in range(old_n_buckets*11):
#             f[21+i] = i

#         f.sync()
#         value = f[21]

#     assert value == 0

#     with VariableValue(file_path) as f:
#         new_n_buckets = f._n_buckets
#         value = f[21]

#     assert (new_n_buckets > 20000) and (value == 0)


## Always make this last!!!
def test_clear():
    with VariableValue(file_path, 'w') as f:
        f.clear()

        assert (len(f) == 0) and (len(list(f.keys())) == 0)



# f = Booklet(file_path)
# f = Booklet(file_path, 'w')


data_dict2 = {key: blake2s(key.to_bytes(4, 'little', signed=True), digest_size=13).digest() for key in range(2, 100)}



def test_set_items_fixed():
    with FixedValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        for key, value in data_dict2.items():
            f[key] = value

    with FixedValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_update_fixed():
    with FixedValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        f.update(data_dict2)

    with FixedValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_threading_writes_fixed():
    with FixedValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for key, value in data_dict2.items():
                future = executor.submit(set_item, f, key, value)
                futures.append(future)

        _ = concurrent.futures.wait(futures)

    with FixedValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_keys_fixed():
    with FixedValue(file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict2.keys()))

    assert source_keys == keys

    with FixedValue(file_path) as f:
        for key in keys:
            val = f[key]


def test_items_fixed():
    with FixedValue(file_path) as f:
        for key, value in f.items():
            source_value = data_dict2[key]
            assert source_value == value


def test_contains_fixed():
    with FixedValue(file_path) as f:
        for key in data_dict2:
            if key not in f:
                raise KeyError(key)

    assert True


def test_len_fixed():
    with FixedValue(file_path) as f:
        new_len = len(f)

    assert len(data_dict2) == new_len


# @pytest.mark.parametrize('index', [10, 12])
def test_delete_len_fixed():
    indexes = [10, 12]
    b1 = blake2s(b'0', digest_size=13).digest()

    for index in indexes:
        _ = data_dict2.pop(index)

        with FixedValue(file_path, 'w') as f:
            f[index] = b1
            f[index] = b1
            del f[index]

            new_len = len(f)

            f.sync()

            try:
                _ = f[index]
                raise ValueError()
            except KeyError:
                pass

        assert new_len == len(data_dict2)


def test_values_fixed():
    with FixedValue(file_path) as f:
        for key, source_value in data_dict2.items():
            value = f[key]
            assert source_value == value


# def test_prune_fixed():
#     with FixedValue(file_path, 'w') as f:
#         f.prune()

#     with FixedValue(file_path) as f:
#         for key, source_value in data_dict2.items():
#             value = f[key]
#             assert source_value == value


def test_set_items_get_items_fixed():
    b1 = blake2s(b'0', digest_size=13).digest()
    with FixedValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        for key, value in data_dict2.items():
            f[key] = value

    with FixedValue(file_path, 'w') as f:
        f[50] = b1
        value = f[11]

    with FixedValue(file_path) as f:
        value = f[50]
        assert value == b1

        value = f[11]
        assert value == data_dict2[11]


# def test_reindex_fixed():
#     """

#     """
#     b1 = blake2s(b'0', digest_size=13).digest()
#     with FixedValue(file_path, 'w') as f:
#         old_n_buckets = f._n_buckets
#         for i in range(old_n_buckets*11):
#             f[21+i] = b1

#         f.sync()
#         value = f[21]

#     assert value == b1

#     with FixedValue(file_path) as f:
#         new_n_buckets = f._n_buckets
#         value = f[21]

#     assert (new_n_buckets > 20000) and (value == b1)


## Always make this last!!!
def test_clear_fixed():
    with FixedValue(file_path, 'w') as f:
        f.clear()

        assert (len(f) == 0) and (len(list(f.keys())) == 0)





# data_dict2 = {key: blake2s(key.to_bytes(4, 'little', signed=True), digest_size=13).digest() for key in range(2, 100)}
# b1 = blake2s(b'0', digest_size=13).digest()

# def set_test2():
#     with FixedValue(file_path, 'n', key_serializer='uint2', value_len=13) as f:
#         for key in range(2, 10000):
#             f[key] = b1


# def set_test1():
#     with Booklet(file_path, 'n', key_serializer='uint2') as f:
#         for key in range(2, 10000):
#             f[key] = b1



# data_dict = {str(key): list(range(key)) for key in range(2, 1000)}

# def blt_write_test():
#     with Booklet(file_path, 'n', key_serializer='str', value_serializer='pickle') as f:
#         for key, value in data_dict.items():
#             f[key] = value

# def blt_read_test():
#     with Booklet(file_path) as f:
#         for key in f:
#             value = f[key]

# def shelve_write_test():
#     with shelve.open(file_path, 'n') as f:
#         for key, value in data_dict.items():
#             f[key] = value

# def shelve_read_test():
#     with shelve.open(file_path) as f:
#         for key in f:
#             value = f[key]


# if not f._mm.closed:
#     print('oops')


# file_path = '/home/mike/data/cache/test1.blt'

# n_buckets = 12007
# n_buckets = 1728017
# chunk_size = 1000
# b2 = b'0' * chunk_size
# n = 100000

# def make_test_file(n):
#     with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle', n_buckets=n_buckets) as f:
#         for i in range(n):
#             f[i] = b2


# def test_index_speed1(n):
#     with VariableValue(file_path, 'r') as f:
#         for i in range(n):
#             val = f[i]

# def test_index_speed2(n):
#     with VariableValue(file_path, 'r') as f:
#         for k, v in f.items():
#             pass


# t1 = time()
# make_test_file(n)
# print(time() - t1)

# t1 = time()
# test_index_speed1(n)
# print(time() - t1)

# t1 = time()
# test_index_speed2(n)
# print(time() - t1)

# def test_resize1():
#     f = io.open(file_path, 'w+b')
#     f.write(b'0')
#     f.flush()

#     fm = mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED)
#     f.close()

#     fm.resize(256**5)

#     for i in range(n):
#         start = i * chunk_size
#         end = start + chunk_size
#         # fm.resize(end)
#         fm[start:end] = b2

#     fm.resize(chunk_size*n)

#     fm.close()


# def test_resize2():
#     f = io.open(file_path, 'w+b')
#     f.write(b'0')
#     f.flush()

#     fm = mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED)
#     f.close()

#     # fm.resize(256**5)

#     for i in range(n):
#         start = i * chunk_size
#         end = start + chunk_size
#         fm.resize(end)
#         fm[start:end] = b2
#         # fm.flush()

#     # fm.resize(chunk_size*n)

#     fm.close()


# def test_write1():
#     f = io.open(file_path, 'w+b')
#     for i in range(n):
#         # start = i * chunk_size
#         # end = start + chunk_size
#         f.write(b2)

#     f.close()


# def test_write2():
#     f = io.open(file_path, 'w+b')
#     f.write(b'0')
#     f.flush()

#     fm = mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED)
#     f.close()

#     fm.madvise(mmap.MADV_SEQUENTIAL)

#     max_mem = 2**22
#     mem = 0
#     for i in range(n):
#         fm.resize((i+1) * chunk_size)
#         mem += fm.write(b2)
#         if mem > max_mem:
#             fm.madvise(mmap.MADV_DONTNEED)
#             mem = 0

#     fm.flush()
#     fm.close()


# t1 = time()
# test_write3()
# print(time() - t1)


# def test_write3():
#     f = io.open(file_path, 'w+b')
#     f.write(b'0')
#     f.flush()

#     fm = mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED)
#     # f.close()

#     max_mem = 2**22
#     mem = 0
#     for i in range(n):
#         mem += f.write(b2)
#         if mem > max_mem:
#             f.flush()
#             old_len = len(fm)
#             fm.resize(old_len + mem)
#             mem = 0

#     f.close()
#     fm.close()


# def test_read1():
#     f = io.open(file_path, 'rb')
#     fm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
#     fm.madvise(mmap.MADV_SEQUENTIAL)

#     max_mem = 2**22
#     mem = 0
#     for i in range(n):
#         data = fm.read(chunk_size)
#         mem += len(data)
#         if mem > max_mem:
#             fm.madvise(mmap.MADV_DONTNEED)
#             mem = 0

#     fm.close()
#     f.close()


# def test_read2():
#     f = io.open(file_path, 'rb')
#     fm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
#     fm.madvise(mmap.MADV_SEQUENTIAL)

#     max_mem = 2**22
#     mem = 0
#     for i in range(n):
#         data = fm.read(chunk_size)
#         # mem += len(data)

#     fm.madvise(mmap.MADV_DONTNEED)
#     fm.close()
#     f.close()


# def test_read3():
#     f = io.open(file_path, 'rb')
#     for i in range(n):
#         data = f.read(chunk_size)

#     f.close()



# f = io.open(file_path, 'w+b')
# f.write(b'0123456789')
# f.flush()
# fm = mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED)
# f.seek(1000000001)
# f.write(b'1234')

# fd = f.fileno()
# os.copy_file_range(fd, fd, 1000000000, 1000000000, 0)

# f.seek(0)

# f.read(10)

















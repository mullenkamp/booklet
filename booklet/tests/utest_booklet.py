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
from sqlitedict import SqliteDict

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

data_dict = {key: key*2 for key in range(2, 30)}



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


file_path = '/home/mike/data/cache/test1.blt'

n_buckets = 12007
n_buckets = 1728017
chunk_size = 1000
b2 = b'0' * chunk_size
n = 1000000

def make_test_file(n):
    with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle', n_buckets=n_buckets) as f:
        for i in range(n):
            f[i] = b2


def test_index_speed1(n):
    with VariableValue(file_path, 'r') as f:
        for i in range(n):
            val = f[i]

def test_index_speed2(n):
    with VariableValue(file_path, 'r') as f:
        for k, v in f.items():
            pass


t1 = time()
make_test_file(n)
print(time() - t1)

t1 = time()
test_index_speed1(n)
print(time() - t1)

t1 = time()
test_index_speed2(n)
print(time() - t1)


file_path = '/home/mike/data/cache/test1.sqlite'

n_buckets = 12007
n_buckets = 1728017
chunk_size = 1000
b2 = b'0' * chunk_size
n = 1000000

def make_test_file(n):
    with SqliteDict(file_path, outer_stack=False, flag='n') as f:
        for i in range(n):
            f[i] = b2
        f.commit()

def test_index_speed1(n):
    with SqliteDict(file_path, outer_stack=False, flag='r') as f:
        for i in range(n):
            val = f[i]

def test_index_speed2(n):
    with SqliteDict(file_path, outer_stack=False, flag='r') as f:
        for k, v in f.items():
            pass


t1 = time()
make_test_file(n)
print(time() - t1)

t1 = time()
test_index_speed1(n)
print(time() - t1)

t1 = time()
test_index_speed2(n)
print(time() - t1)


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
















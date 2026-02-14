#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 13:55:17 2024

@author: mike
"""
import sys
import pytest
import io
import os
from tempfile import NamedTemporaryFile
import concurrent.futures
from hashlib import blake2s
import mmap
from time import time
# from sqlitedict import SqliteDict
from threading import Lock

module_path = '/home/mike/git/booklet'
if module_path not in sys.path:
    sys.path.append(module_path)

import booklet


##############################################
### Parameters

tf1 = NamedTemporaryFile()
file_path1 = tf1.name

data_dict = {key: key*2 for key in range(2, 30)}
data_dict[97] = 97*2 # key hash conflict test - 97 conflicts with 11

max_size = 73095
timestamp = None
reindex = False
n_buckets = 12007
write_buffer_size = 4194304
ts_bytes_len = 7

##############################################
### Functions


def set_item(f, key, value):
    f[key] = value

    return key


##############################################
### Tests

print(__version__)

with VariableValue(file_path1, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=True) as f:
    for key, value in data_dict.items():
        f[key] = value


indexes = [11, 12]

for index in indexes:
    # _ = data_dict.pop(index)

    with VariableValue(file_path1, 'w') as f:
        f[index] = 0
        f[index] = 0
        del f[index]

        # f.sync()

        # new_len = len(f)

        try:
            _ = f[index]
            raise ValueError()
        except KeyError:
            pass

    # assert new_len == len(data_dict)


self = VariableValue(file_path1, 'w')
# self[97] = 97*2
# self.sync()
file = self._file
buffer_data = self._buffer_data
buffer_index = self._buffer_index

self.prune()
self.prune(reindex=True)
timestamp = utils.make_timestamp_int()
self.prune(timestamp=timestamp)

self.close()


lock = Lock()

def thread_lock_test1():
    """

    """
    with lock:
        pass

def thread_lock_test2():
    """

    """
    lock.acquire()
    lock.release()


def pass_test():
    pass

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

os.remove(file_path)

f = VariableValue(file_path, 'r')

iter1 = f.items()

for k, v in iter1:
    if k == 6:
        break

val = f[1]


def test_open_read():
    with VariableValue(file_path, 'r') as f:
        pass

def test_open_write():
    with VariableValue(file_path, 'w') as f:
        pass

def test_create_file():
    with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle', n_buckets=n_buckets) as f:
        pass


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



with booklet.open(file_path1, 'n', key_serializer='uint4', value_serializer=None) as f:
    for i in range(1000):
        f[i] = b'0' * 1000000



with booklet.open(file_path1) as f:
    for i, val in f.items():
        _ = len(val)








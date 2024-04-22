#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 13:55:17 2024

@author: mike
"""
import pytest
import io
from booklet import Booklet, __version__, FixedValue, utils
from tempfile import NamedTemporaryFile
import concurrent.futures
from hashlib import blake2s

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

data_dict = {key: list(range(key)) for key in range(2, 30)}


def test_set_items():
    with Booklet(file_path, 'n', key_serializer='uint1', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with Booklet(file_path) as f:
        value = f[10]

    assert value == list(range(10))


def test_update():
    with Booklet(file_path, 'n', key_serializer='uint1', value_serializer='pickle') as f:
        f.update(data_dict)

    with Booklet(file_path) as f:
        value = f[10]

    assert value == list(range(10))


def test_threading_writes():
    with Booklet(file_path, 'n', key_serializer='uint1', value_serializer='pickle') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for key, value in data_dict.items():
                future = executor.submit(set_item, f, key, value)
                futures.append(future)
    
        _ = concurrent.futures.wait(futures)

    with Booklet(file_path) as f:
        value = f[10]

    assert value == list(range(10))


def test_keys():
    with Booklet(file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict.keys()))

    assert source_keys == keys


def test_items():
    with Booklet(file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


def test_contains():
    with Booklet(file_path) as f:
        for key in data_dict:
            if key not in f:
                raise KeyError(key)

    assert True


def test_len():
    with Booklet(file_path) as f:
        new_len = len(f)

    assert len(data_dict) == new_len


# @pytest.mark.parametrize('index', [10, 12])
def test_delete_len():
    indexes = [10, 12]

    for index in indexes:
        _ = data_dict.pop(index)
    
        with Booklet(file_path, 'w') as f:
            f[index] = 0
            f[index] = 0
            del f[index]

            f.sync()

            new_len = len(f)

            try:
                _ = f[index]
                raise ValueError()
            except utils.KeyError:
                pass
    
        assert new_len == len(data_dict)


def test_values():
    with Booklet(file_path) as f:
        for key, source_value in data_dict.items():
            value = f[key]
            assert source_value == value


def test_prune():
    with Booklet(file_path, 'w') as f:
        f.prune()

    with Booklet(file_path) as f:
        for key, source_value in data_dict.items():
            value = f[key]
            assert source_value == value


def test_set_items_get_items():
    with Booklet(file_path, 'n', key_serializer='uint1', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with Booklet(file_path, 'w') as f:
        f[50] = [0, 0]
        value = f[11]

    with Booklet(file_path) as f:
        value = f[50]
        assert value == [0, 0]

        value = f[11]
        assert value == list(range(11))



## Always make this last!!!
def test_clear():
    with Booklet(file_path, 'w') as f:
        f.clear()

        assert (len(f) == 0) and (len(list(f.keys())) == 0)



# f = Booklet(file_path)
# f = Booklet(file_path, 'w')


data_dict2 = {key: blake2s(key.to_bytes(4, 'little', signed=True), digest_size=13).digest() for key in range(2, 100)}



def test_set_items_fixed():
    with FixedValue(file_path, 'n', key_serializer='uint1', value_len=13) as f:
        for key, value in data_dict2.items():
            f[key] = value

    with FixedValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_update_fixed():
    with FixedValue(file_path, 'n', key_serializer='uint1', value_len=13) as f:
        f.update(data_dict2)

    with FixedValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_threading_writes_fixed():
    with FixedValue(file_path, 'n', key_serializer='uint1', value_len=13) as f:
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
            except utils.KeyError:
                pass
    
        assert new_len == len(data_dict2)


def test_values_fixed():
    with FixedValue(file_path) as f:
        for key, source_value in data_dict2.items():
            value = f[key]
            assert source_value == value


def test_prune_fixed():
    with FixedValue(file_path, 'w') as f:
        f.prune()

    with FixedValue(file_path) as f:
        for key, source_value in data_dict2.items():
            value = f[key]
            assert source_value == value


def test_set_items_get_items_fixed():
    b1 = blake2s(b'0', digest_size=13).digest()
    with FixedValue(file_path, 'n', key_serializer='uint1', value_len=13) as f:
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


















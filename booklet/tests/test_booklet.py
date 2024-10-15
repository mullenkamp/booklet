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
from copy import deepcopy
# import mmap
import time

##############################################
### Parameters

tf1 = NamedTemporaryFile()
file_path1 = tf1.name
tf2 = NamedTemporaryFile()
file_path2 = tf2.name

data_dict = {key: key*2 for key in range(2, 30)}
data_dict[97] = 97*2 # key hash conflict test - 97 conflicts with 11

data_dict2 = deepcopy(data_dict)

meta = {'test1': 'data'}

file_path = file_path2
data = deepcopy(data_dict)

##############################################
### Functions


def set_item(f, key, value):
    f[key] = value

    return key


##############################################
### Tests

print(__version__)


def test_set_items():
    with VariableValue(file_path1, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=True) as f:
        for key, value in data_dict.items():
            f[key] = value

    with VariableValue(file_path1) as f:
        value = f[10]

    assert value == data_dict[10]


def test_update():
    with VariableValue(file_path1, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        f.update(data_dict)

    with VariableValue(file_path1) as f:
        value = f[10]

    assert value == data_dict[10]


def test_threading_writes():
    with VariableValue(file_path1, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for key, value in data_dict.items():
                future = executor.submit(set_item, f, key, value)
                futures.append(future)

        _ = concurrent.futures.wait(futures)

    with VariableValue(file_path1) as f:
        value = f[10]

    assert value == data_dict[10]


#######################
### Set up files for following tests


with VariableValue(file_path1, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=False) as f:
    for key, value in data_dict.items():
        f[key] = value

with VariableValue(file_path2, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=True) as f:
    for key, value in data_dict.items():
        f[key] = value


def test_init_bytes_input():
    """

    """
    with io.open(file_path2, 'rb') as f:
        init_bytes = f.read(200)

    with VariableValue(file_path2, 'n', init_bytes=init_bytes) as f:
        for key, value in data_dict.items():
            f[key] = value


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_set_get_metadata(file_path):
    """

    """
    with VariableValue(file_path, 'w') as f:
        old_meta = f.get_metadata()
        f.set_metadata(meta)

    assert old_meta is None

    with VariableValue(file_path) as f:
        new_meta = f.get_metadata()

    assert new_meta == meta


@pytest.mark.parametrize("file_path", [file_path2])
def test_set_get_timestamp(file_path):
    with VariableValue(file_path, 'w') as f:
        ts_old, value = f.get_timestamp(10, True)
        ts_new = utils.make_timestamp_int()
        f.set_timestamp(10, ts_new)

    with VariableValue(file_path) as f:
        ts_new = f.get_timestamp(10)

    assert ts_new > ts_old and value == data_dict[10]


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_keys(file_path):
    with VariableValue(file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict.keys()))

    assert source_keys == keys


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_items(file_path):
    with VariableValue(file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


@pytest.mark.parametrize("file_path", [file_path2])
def test_timestamps(file_path):
    with VariableValue(file_path) as f:
        for key, ts, value in f.timestamps(True):
            source_value = data_dict[key]
            assert source_value == value

        ts_new = utils.make_timestamp_int()
        for key, ts in f.timestamps():
            assert ts_new > ts


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_contains(file_path):
    with VariableValue(file_path) as f:
        for key in data_dict:
            if key not in f:
                raise KeyError(key)

    assert True


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_len(file_path):
    with VariableValue(file_path) as f:
        new_len = len(f)

    assert len(data_dict) == new_len


@pytest.mark.parametrize("file_path,data", [(file_path1, data_dict), (file_path2, data_dict2)])
def test_delete_len(file_path, data):
    indexes = [11, 12]

    for index in indexes:
        _ = data.pop(index)

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

        assert new_len == len(data)


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_items2(file_path):
    with VariableValue(file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_values(file_path):
    with VariableValue(file_path) as f:
        for value in f.values():
            pass

    with VariableValue(file_path) as f:
        for key, source_value in data_dict.items():
            value = f[key]
            assert source_value == value


@pytest.mark.parametrize("file_path", [file_path2])
def test_prune(file_path):
    with VariableValue(file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f[2]

    assert (removed_items > 0)  and (old_len > removed_items) and (new_len == old_len) and isinstance(test_value, int)

    # Reindex
    with VariableValue(file_path, 'w') as f:
        old_len = len(f)
        old_n_buckets = f._n_buckets
        removed_items = f.prune(reindex=True)
        new_n_buckets = f._n_buckets
        new_len = len(f)
        test_value = f[2]

    assert (removed_items == 0) and (new_n_buckets > old_n_buckets) and (new_len == old_len) and isinstance(test_value, int)

    # Remove the rest via timestamp filter
    timestamp = utils.make_timestamp_int()

    with VariableValue(file_path, 'w') as f:
        removed_items = f.prune(timestamp=timestamp)
        new_len = len(f)
        meta = f.get_metadata()

    assert (old_len == removed_items) and (new_len == 0) and isinstance(meta, dict)


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_set_items_get_items(file_path):
    with VariableValue(file_path, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with VariableValue(file_path, 'w') as f:
        f[50] = [0, 0]
        value1 = f[10]
        value2 = f[50]
        assert (value1 == data_dict[10]) and (value2 == [0, 0])

    # with VariableValue(file_path) as f:
    #     value = f[50]
    #     assert value == [0, 0]

    #     value = f[10]
    #     assert value == data_dict[10]


## Always make this last!!!
@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_clear(file_path):
    with VariableValue(file_path, 'w') as f:
        f.clear()
        f_meta = f.get_metadata()

        assert (len(f) == 0) and (len(list(f.keys())) == 0) and (f_meta is None)



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


def test_init_bytes_input_fixed():
    """

    """
    with io.open(file_path, 'rb') as f:
        init_bytes = f.read(200)

    with FixedValue(file_path, 'n', init_bytes=init_bytes) as f:
        for key, value in data_dict2.items():
            f[key] = value


def test_keys_fixed():
    with FixedValue(file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict2.keys()))

    assert source_keys == keys

    with FixedValue(file_path) as f:
        for key in keys:
            _ = f[key]


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


def test_prune_fixed():
    with FixedValue(file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f[2]

    assert (removed_items > 0)  and (old_len > removed_items) and (new_len == old_len) and isinstance(test_value, bytes)

    # Reindex
    with FixedValue(file_path, 'w') as f:
        old_len = len(f)
        old_n_buckets = f._n_buckets
        removed_items = f.prune(reindex=True)
        new_n_buckets = f._n_buckets
        new_len = len(f)
        test_value = f[2]

    assert (removed_items == 0) and (new_n_buckets > old_n_buckets) and (new_len == old_len) and isinstance(test_value, bytes)


def test_set_items_get_items_fixed():
    b1 = blake2s(b'0', digest_size=13).digest()
    with FixedValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        for key, value in data_dict2.items():
            f[key] = value

    with FixedValue(file_path, 'w') as f:
        f[50] = b1
        value1 = f[11]
        value2 = f[50]

    assert (value1 == data_dict2[11]) and (value2 == b1)

    # with FixedValue(file_path) as f:
    #     value = f[50]
    #     assert value == b1

    #     value = f[11]
    #     assert value == data_dict2[11]


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


















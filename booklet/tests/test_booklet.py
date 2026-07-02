#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 13:55:17 2024

@author: mike
"""
import pytest
import io
import os
import tracemalloc
from datetime import datetime, timezone
import booklet
from booklet import __version__, FixedLengthValue, VariableLengthValue, utils
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
tf3 = NamedTemporaryFile()
file_path3 = tf3.name

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
    with booklet.open(file_path1, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=True) as f:
        for key, value in data_dict.items():
            f[key] = value

    with booklet.open(file_path1) as f:
        value = f[10]

    assert value == data_dict[10]


def test_update():
    with booklet.open(file_path1, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        f.update(data_dict)

    with booklet.open(file_path1) as f:
        value = f[10]

    assert value == data_dict[10]


def test_threading_writes():
    with booklet.open(file_path1, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for key, value in data_dict.items():
                future = executor.submit(set_item, f, key, value)
                futures.append(future)

        _ = concurrent.futures.wait(futures)

    with booklet.open(file_path1) as f:
        value = f[10]

    assert value == data_dict[10]


###################################################
### Variable len value


with booklet.open(file_path1, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=False) as f:
    for key, value in data_dict.items():
        f[key] = value

with booklet.open(file_path2, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=True) as f:
    for key, value in data_dict.items():
        f[key] = value


def test_init_bytes_input():
    """

    """
    with io.open(file_path2, 'rb') as f:
        init_bytes = f.read(200)

    with booklet.open(file_path2, 'n', init_bytes=init_bytes) as f:
        for key, value in data_dict.items():
            f[key] = value


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_set_get_metadata(file_path):
    """

    """
    with booklet.open(file_path, 'w') as f:
        old_meta = f.get_metadata()
        f.set_metadata(meta)

    assert old_meta is None

    with booklet.open(file_path) as f:
        new_meta = f.get_metadata()

    assert new_meta == meta


@pytest.mark.parametrize("file_path", [file_path2])
def test_set_get_timestamp(file_path):
    with booklet.open(file_path, 'w') as f:
        ts_old, value = f.get_timestamp(10, True)
        ts_new = utils.make_timestamp_int()
        f.set_timestamp(10, ts_new)

    with booklet.open(file_path) as f:
        ts_new = f.get_timestamp(10)

    assert ts_new > ts_old and value == data_dict[10]


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_keys(file_path):
    with booklet.open(file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict.keys()))

    assert source_keys == keys


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_items(file_path):
    with booklet.open(file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


@pytest.mark.parametrize("file_path", [file_path2])
def test_timestamps(file_path):
    with booklet.open(file_path) as f:
        for key, ts, value in f.timestamps(True):
            source_value = data_dict[key]
            assert source_value == value

        ts_new = utils.make_timestamp_int()
        for key, ts in f.timestamps():
            assert ts_new > ts


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_contains(file_path):
    with booklet.open(file_path) as f:
        for key in data_dict:
            if key not in f:
                raise KeyError(key)

    assert True


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_len(file_path):
    with booklet.open(file_path) as f:
        new_len = len(f)

    assert len(data_dict) == new_len


@pytest.mark.parametrize("file_path,data", [(file_path1, data_dict), (file_path2, data_dict2)])
def test_delete_len(file_path, data):
    indexes = [11, 12]

    for index in indexes:
        _ = data.pop(index)

        with booklet.open(file_path, 'w') as f:
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
    with booklet.open(file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_values(file_path):
    with booklet.open(file_path) as f:
        for value in f.values():
            pass

    with booklet.open(file_path) as f:
        for key, source_value in data_dict.items():
            value = f[key]
            assert source_value == value


@pytest.mark.parametrize("file_path", [file_path2])
def test_prune(file_path):
    with booklet.open(file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f[2]

    assert (removed_items > 0)  and (old_len > removed_items) and (new_len == old_len) and isinstance(test_value, int)

    # Prune again (no deleted items left)
    with booklet.open(file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f[2]

    assert (removed_items == 0) and (new_len == old_len) and isinstance(test_value, int)

    # Remove the rest via timestamp filter
    timestamp = utils.make_timestamp_int()

    with booklet.open(file_path, 'w') as f:
        removed_items = f.prune(timestamp=timestamp)
        new_len = len(f)
        meta = f.get_metadata()

    assert (old_len == removed_items) and (new_len == 0) and isinstance(meta, dict)


def test_prune_timestamp_datetime_and_str():
    """
    prune's timestamp parameter accepts the documented datetime and ISO-string
    forms, not only int microseconds (regression: prune_file compared the raw
    value against int timestamps and raised TypeError for datetime/str).
    """
    tf = NamedTemporaryFile()

    with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=True) as f:
        for key, value in data_dict.items():
            f[key] = value

    with booklet.open(tf.name, 'w') as f:
        old_len = len(f)
        removed_items = f.prune(timestamp=datetime.now(timezone.utc))
        assert (removed_items == old_len) and (len(f) == 0)

        for key, value in data_dict.items():
            f[key] = value
        removed_items = f.prune(timestamp=datetime.now(timezone.utc).isoformat())
        assert (removed_items == len(data_dict)) and (len(f) == 0)


@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_set_items_get_items(file_path):
    with booklet.open(file_path, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with booklet.open(file_path, 'w') as f:
        f[50] = [0, 0]
        value1 = f[10]
        value2 = f[50]
        assert (value1 == data_dict[10]) and (value2 == [0, 0])

    # with booklet.open(file_path) as f:
    #     value = f[50]
    #     assert value == [0, 0]

    #     value = f[10]
    #     assert value == data_dict[10]


## Always make this last!!!
@pytest.mark.parametrize("file_path", [file_path1, file_path2])
def test_clear(file_path):
    with booklet.open(file_path, 'w') as f:
        f.clear()
        f_meta = f.get_metadata()

        assert (len(f) == 0) and (len(list(f.keys())) == 0) and (f_meta is None)



# f = Booklet(file_path)
# f = Booklet(file_path, 'w')


###########################################################
### Variable len value using BytesIO object

data_dict2 = deepcopy(data_dict)


def make_bytesio_booklet(data=None):
    bytes_io = io.BytesIO()
    f = booklet.open(bytes_io, 'n', key_serializer='uint4', value_serializer='pickle')

    if data:
        for key, value in data.items():
            f[key] = value
    
        f.sync()

    return f


def test_set_items_bytesio():
    f = make_bytesio_booklet(data_dict2)

    value = f[10]

    f.close()

    assert value == data_dict2[10]


def test_threading_writes_bytesio():
    f = make_bytesio_booklet()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for key, value in data_dict2.items():
            future = executor.submit(set_item, f, key, value)
            futures.append(future)

    _ = concurrent.futures.wait(futures)

    value = f[10]

    f.close()

    assert value == data_dict2[10]


def test_init_bytes_input_bytesio():
    """

    """
    bytes_io = io.BytesIO()
    f = booklet.open(bytes_io, 'n', key_serializer='uint4', value_serializer='pickle')

    for key, value in data_dict2.items():
        f[key] = value

    f.sync()

    bytes_io.seek(0)
    init_bytes = bytes_io.read(200)

    f.close()

    bytes_io = io.BytesIO()

    f = booklet.open(bytes_io, 'n', init_bytes=init_bytes)
    for key, value in data_dict2.items():
        f[key] = value

    f.sync()

    ## Test existing filled BytesIO object
    bytes_io.seek(0)
    new_bytes_io = io.BytesIO(bytes_io.read())

    f.close()

    f = booklet.open(new_bytes_io)

    for key, source_value in data_dict2.items():
        value = f[key]
        assert source_value == value


def test_keys_bytesio():
    f = make_bytesio_booklet(data_dict2)
    keys = set(list(f.keys()))

    source_keys = set(list(data_dict2.keys()))

    assert source_keys == keys

    for key in keys:
        _ = f[key]


def test_items_bytesio():
    f = make_bytesio_booklet(data_dict2)
    for key, value in f.items():
        source_value = data_dict2[key]
        assert source_value == value


def test_contains_bytesio():
    f = make_bytesio_booklet(data_dict2)
    for key in data_dict2:
        if key not in f:
            raise KeyError(key)

    assert True


def test_len_bytesio():
    f = make_bytesio_booklet(data_dict2)
    new_len = len(f)

    assert len(data_dict2) == new_len


# @pytest.mark.parametrize('index', [10, 12])
def test_delete_len_bytesio():
    f = make_bytesio_booklet(data_dict2)

    data_dict3 = deepcopy(data_dict2)

    indexes = [10, 12]
    b1 = blake2s(b'0', digest_size=13).digest()

    for index in indexes:
        _ = data_dict3.pop(index)

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

        assert new_len == len(data_dict3)


def test_values_bytesio():
    f = make_bytesio_booklet(data_dict2)

    for key, source_value in data_dict2.items():
        value = f[key]
        assert source_value == value


# def test_prune_bytesio():
#     f = make_bytesio_booklet(data_dict2)
#     del f[10]
#     del f[12]
#     f.sync()

#     old_len = len(f)
#     removed_items = f.prune()
#     new_len = len(f)
#     test_value = f[2]

#     assert (removed_items > 0)  and (old_len > removed_items) and (new_len == old_len) and isinstance(test_value, int)

#     # Reindex
#     old_len = len(f)
#     old_n_buckets = f._n_buckets
#     removed_items = f.prune(reindex=True)
#     new_n_buckets = f._n_buckets
#     new_len = len(f)
#     test_value = f[2]

#     assert (removed_items == 0) and (new_n_buckets > old_n_buckets) and (new_len == old_len) and isinstance(test_value, int)


# def test_set_items_get_items_bytesio():
#     b1 = blake2s(b'0', digest_size=13).digest()
#     with FixedLengthValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
#         for key, value in data_dict2.items():
#             f[key] = value

#     with FixedLengthValue(file_path, 'w') as f:
#         f[50] = b1
#         value1 = f[11]
#         value2 = f[50]

#     assert (value1 == data_dict2[11]) and (value2 == b1)

    # with FixedLengthValue(file_path) as f:
    #     value = f[50]
    #     assert value == b1

    #     value = f[11]
    #     assert value == data_dict2[11]


# def test_reindex_bytesio():
#     """

#     """
#     b1 = blake2s(b'0', digest_size=13).digest()
#     with FixedLengthValue(file_path, 'w') as f:
#         old_n_buckets = f._n_buckets
#         for i in range(old_n_buckets*11):
#             f[21+i] = b1

#         f.sync()
#         value = f[21]

#     assert value == b1

#     with FixedLengthValue(file_path) as f:
#         new_n_buckets = f._n_buckets
#         value = f[21]

#     assert (new_n_buckets > 20000) and (value == b1)


## Always make this last!!!
# def test_clear_bytesio():
#     f = make_bytesio_booklet()

#     f.clear()

#     assert (len(f) == 0) and (len(list(f.keys())) == 0)



###########################################################
### Fixed len value

data_dict2 = {key: blake2s(key.to_bytes(4, 'little', signed=True), digest_size=13).digest() for key in range(2, 100)}


def test_set_items_fixed():
    with FixedLengthValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        for key, value in data_dict2.items():
            f[key] = value

    with FixedLengthValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_update_fixed():
    with FixedLengthValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        f.update(data_dict2)

    with FixedLengthValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_threading_writes_fixed():
    with FixedLengthValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for key, value in data_dict2.items():
                future = executor.submit(set_item, f, key, value)
                futures.append(future)

        _ = concurrent.futures.wait(futures)

    with FixedLengthValue(file_path) as f:
        value = f[10]

    assert value == data_dict2[10]


def test_init_bytes_input_fixed():
    """

    """
    with io.open(file_path, 'rb') as f:
        init_bytes = f.read(200)

    with FixedLengthValue(file_path, 'n', init_bytes=init_bytes) as f:
        for key, value in data_dict2.items():
            f[key] = value


def test_keys_fixed():
    with FixedLengthValue(file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict2.keys()))

    assert source_keys == keys

    with FixedLengthValue(file_path) as f:
        for key in keys:
            _ = f[key]


def test_items_fixed():
    with FixedLengthValue(file_path) as f:
        for key, value in f.items():
            source_value = data_dict2[key]
            assert source_value == value


def test_contains_fixed():
    with FixedLengthValue(file_path) as f:
        for key in data_dict2:
            if key not in f:
                raise KeyError(key)

    assert True


def test_len_fixed():
    with FixedLengthValue(file_path) as f:
        new_len = len(f)

    assert len(data_dict2) == new_len


# @pytest.mark.parametrize('index', [10, 12])
def test_delete_len_fixed():
    indexes = [10, 12]
    b1 = blake2s(b'0', digest_size=13).digest()

    for index in indexes:
        _ = data_dict2.pop(index)

        with FixedLengthValue(file_path, 'w') as f:
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
    with FixedLengthValue(file_path) as f:
        for key, source_value in data_dict2.items():
            value = f[key]
            assert source_value == value


def test_prune_fixed():
    with FixedLengthValue(file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f[2]

    assert (removed_items > 0)  and (old_len > removed_items) and (new_len == old_len) and isinstance(test_value, bytes)

    # Prune again (no deleted items left)
    with FixedLengthValue(file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f[2]

    assert (removed_items == 0) and (new_len == old_len) and isinstance(test_value, bytes)


def test_set_items_get_items_fixed():
    b1 = blake2s(b'0', digest_size=13).digest()
    with FixedLengthValue(file_path, 'n', key_serializer='uint4', value_len=13) as f:
        for key, value in data_dict2.items():
            f[key] = value

    with FixedLengthValue(file_path, 'w') as f:
        f[50] = b1
        value1 = f[11]
        value2 = f[50]

    assert (value1 == data_dict2[11]) and (value2 == b1)

    # with FixedLengthValue(file_path) as f:
    #     value = f[50]
    #     assert value == b1

    #     value = f[11]
    #     assert value == data_dict2[11]


# def test_reindex_fixed():
#     """

#     """
#     b1 = blake2s(b'0', digest_size=13).digest()
#     with FixedLengthValue(file_path, 'w') as f:
#         old_n_buckets = f._n_buckets
#         for i in range(old_n_buckets*11):
#             f[21+i] = b1

#         f.sync()
#         value = f[21]

#     assert value == b1

#     with FixedLengthValue(file_path) as f:
#         new_n_buckets = f._n_buckets
#         value = f[21]

#     assert (new_n_buckets > 20000) and (value == b1)


## Always make this last!!!
def test_clear_fixed():
    with FixedLengthValue(file_path, 'w') as f:
        f.clear()

        assert (len(f) == 0) and (len(list(f.keys())) == 0)


###########################################################
### Fixed len value BytesIO

data_dict3 = {key: blake2s(key.to_bytes(4, 'little', signed=True), digest_size=13).digest() for key in range(2, 100)}


def make_bytesio_FixedLengthValue(data=None):
    bytes_io = io.BytesIO()
    f = FixedLengthValue(bytes_io, 'n', key_serializer='uint4', value_len=13)

    if data:
        for key, value in data.items():
            f[key] = value
    
        f.sync()

    return f


def test_set_items_fixed_bytesio():
    f = make_bytesio_FixedLengthValue(data_dict3)

    value = f[10]

    assert value == data_dict3[10]


def test_update_fixed_bytesio():
    f = make_bytesio_FixedLengthValue()
    f.update(data_dict3)

    value = f[10]

    assert value == data_dict3[10]


def test_threading_writes_fixed_bytesio():
    f = make_bytesio_FixedLengthValue()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for key, value in data_dict3.items():
            future = executor.submit(set_item, f, key, value)
            futures.append(future)

    _ = concurrent.futures.wait(futures)

    value = f[10]

    assert value == data_dict3[10]


def test_init_bytes_input_fixed_bytesio():
    """

    """
    bytes_io = io.BytesIO()
    f = FixedLengthValue(bytes_io, 'n', key_serializer='uint4', value_len=13)

    bytes_io.seek(0)
    init_bytes = bytes_io.read(200)

    bytes_io = io.BytesIO()
    f = FixedLengthValue(bytes_io, 'n', init_bytes=init_bytes)

    for key, value in data_dict3.items():
        f[key] = value


def test_keys_fixed_bytesio():
    f = make_bytesio_FixedLengthValue(data_dict3)

    keys = set(list(f.keys()))

    source_keys = set(list(data_dict3.keys()))

    assert source_keys == keys

    for key in keys:
        _ = f[key]


def test_items_fixed_bytesio():
    f = make_bytesio_FixedLengthValue(data_dict3)

    for key, value in f.items():
        source_value = data_dict3[key]
        assert source_value == value


def test_contains_fixed_bytesio():
    f = make_bytesio_FixedLengthValue(data_dict3)

    for key in data_dict3:
        if key not in f:
            raise KeyError(key)

    assert True


def test_len_fixed_bytesio():
    f = make_bytesio_FixedLengthValue(data_dict3)

    new_len = len(f)

    assert len(data_dict3) == new_len


# @pytest.mark.parametrize('index', [10, 12])
def test_delete_len_fixed_bytesio():
    f = make_bytesio_FixedLengthValue(data_dict3)

    indexes = [10, 12]
    b1 = blake2s(b'0', digest_size=13).digest()

    for index in indexes:
        _ = data_dict3.pop(index)

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

    assert new_len == len(data_dict3)


###########################################################
### Regression tests for bug fixes


def test_fixed_buffer_overflow():
    """
    Test 1: FixedLengthValue with a small buffer_size to trigger the
    flush path in write_data_blocks_fixed (Fix A).
    """
    tf = NamedTemporaryFile()
    value_len = 1024
    val = b'\xab' * value_len

    with FixedLengthValue(tf.name, 'n', key_serializer='uint4', value_len=value_len, buffer_size=4096) as f:
        for i in range(20):
            f[i] = val

    with FixedLengthValue(tf.name) as f:
        assert len(f) == 20
        for i in range(20):
            assert f[i] == val


def test_make_timestamp_string_and_datetime():
    """
    Test 2: make_timestamp_int with string and datetime inputs (Fix B).
    These branches previously crashed due to wrong datetime references.
    """
    from datetime import datetime, timezone

    # String timestamp
    ts_str = utils.make_timestamp_int('2024-01-01T00:00:00+00:00')
    assert isinstance(ts_str, int)
    assert ts_str == 1704067200000000

    # datetime object
    dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    ts_dt = utils.make_timestamp_int(dt)
    assert isinstance(ts_dt, int)
    assert ts_dt == int(dt.timestamp() * 1000000)

    # None (existing behavior, sanity check)
    ts_none = utils.make_timestamp_int()
    assert isinstance(ts_none, int)
    assert ts_none > 0

    # int passthrough
    ts_int = utils.make_timestamp_int(42)
    assert ts_int == 42


def test_fixed_falsy_values():
    """
    Test 3: FixedLengthValue storing and retrieving falsy byte values (Fix E/F).
    b'\\x00' is falsy in Python; get() and __getitem__() must still return it.
    """
    tf = NamedTemporaryFile()
    value_len = 4

    with FixedLengthValue(tf.name, 'n', key_serializer='uint4', value_len=value_len) as f:
        f[1] = b'\x00' * value_len
        f[2] = b'\x00\x01\x00\x00'
        f[3] = b'\xff' * value_len

    with FixedLengthValue(tf.name) as f:
        # __getitem__ should not raise KeyError for zero-bytes value
        assert f[1] == b'\x00' * value_len
        assert f[2] == b'\x00\x01\x00\x00'
        assert f[3] == b'\xff' * value_len

        # get() should return the value, not the default
        assert f.get(1) == b'\x00' * value_len
        assert f.get(999) is None
        assert f.get(999, 'missing') == 'missing'


def test_fixed_iteration_sees_buffered_writes():
    """
    Test 4: FixedLengthValue iteration methods should see buffered (unsynced)
    writes without requiring an explicit sync() call (Fix G).
    """
    tf = NamedTemporaryFile()
    value_len = 13
    expected = {}

    with FixedLengthValue(tf.name, 'n', key_serializer='uint4', value_len=value_len) as f:
        for i in range(5):
            val = blake2s(i.to_bytes(4, 'little'), digest_size=value_len).digest()
            f[i] = val
            expected[i] = val

        # Do NOT call f.sync() — iteration should trigger it internally

        keys = set(f.keys())
        assert keys == set(expected.keys())

        items = dict(f.items())
        assert items == expected

        values = list(f.values())
        assert len(values) == len(expected)
        for v in values:
            assert v in expected.values()


def test_fixed_prune_persists_n_keys():
    """
    Test 5: FixedLengthValue prune should persist n_keys to disk so that
    reopening the file shows the correct count (Fix H).
    """
    tf = NamedTemporaryFile()
    value_len = 13
    b1 = blake2s(b'0', digest_size=value_len).digest()

    # Create file with some entries, then delete a few
    with FixedLengthValue(tf.name, 'n', key_serializer='uint4', value_len=value_len) as f:
        for i in range(10):
            f[i] = b1

    with FixedLengthValue(tf.name, 'w') as f:
        del f[0]
        del f[1]
        del f[2]
        assert len(f) == 7
        removed = f.prune()
        assert removed == 3
        assert len(f) == 7

    # Reopen and verify n_keys was persisted correctly
    with FixedLengthValue(tf.name) as f:
        assert len(f) == 7
        # Verify the pruned keys are actually gone
        for i in range(3, 10):
            assert f[i] == b1


def test_metadata_with_timestamp():
    """
    Test 6: get_metadata with include_timestamp=True (Fix I).
    """
    tf = NamedTemporaryFile()

    with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle', init_timestamps=True) as f:
        f[1] = 'test'
        f.set_metadata({'key': 'value'})

    with booklet.open(tf.name) as f:
        # Without timestamp
        meta = f.get_metadata()
        assert meta == {'key': 'value'}

        # With timestamp
        result = f.get_metadata(include_timestamp=True)
        assert isinstance(result, tuple)
        meta, ts = result
        assert meta == {'key': 'value'}
        assert isinstance(ts, int)
        assert ts > 0


def test_reopen():
    """
    Test 7: The reopen() method should work for both 'r' and 'w' flags
    and should not carry stale buffer state (Fix J).
    """
    tf = NamedTemporaryFile()

    # Create and populate file
    with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as f:
        for i in range(5):
            f[i] = i * 10

    # Open for reading, then reopen for writing
    f = booklet.open(tf.name, 'r')
    assert f[0] == 0
    assert f[3] == 30

    f.reopen('w')
    f[10] = 100
    f.sync()
    assert f[10] == 100
    # Old data still accessible
    assert f[0] == 0

    # Reopen back to read
    f.reopen('r')
    assert f[10] == 100
    assert f[0] == 0
    assert len(f) == 6
    f.close()


def test_fixed_bytesio_reopen_existing():
    """
    Test 8: FixedLengthValue with BytesIO should detect existing content
    when reopening a BytesIO object that already has data (Fix C).
    """
    value_len = 13
    b1 = blake2s(b'test', digest_size=value_len).digest()

    # Create a FixedLengthValue in a BytesIO
    bytes_io = io.BytesIO()
    f = FixedLengthValue(bytes_io, 'n', key_serializer='uint4', value_len=value_len)
    for i in range(5):
        f[i] = b1
    f.sync()

    # Copy the BytesIO content to a new BytesIO to simulate reopening
    bytes_io.seek(0)
    new_bytes_io = io.BytesIO(bytes_io.read())
    f.close()

    # Open the new BytesIO for reading — should detect existing content
    f2 = FixedLengthValue(new_bytes_io, 'r')
    assert len(f2) == 5
    for i in range(5):
        assert f2[i] == b1
    f2.close()


def test_init_bytes_resets_index_offset(tmp_path):
    """init_bytes from a reindexed file should not create an oversized sparse file."""
    fp1 = tmp_path / 'original.blt'
    fp2 = tmp_path / 'from_init.blt'

    # n_buckets=3 forces reindex after just a few keys
    with booklet.open(fp1, 'n', key_serializer='uint4', value_serializer='pickle', n_buckets=3) as f:
        for i in range(100):
            f[i] = f'value_{i}'.encode()

    # The original file's index_offset should be large (reindexed to end of file)
    original_size = fp1.stat().st_size

    # Read init_bytes (first 200 bytes of the reindexed file)
    with open(fp1, 'rb') as f:
        init_bytes = f.read(200)

    # Create a new file from init_bytes
    with booklet.open(fp2, 'n', init_bytes=init_bytes) as f:
        f[999] = b'hello'

    # The new file should NOT be as large as the original
    new_size = fp2.stat().st_size
    assert new_size < original_size, (
        f'New file from init_bytes is {new_size} bytes, '
        f'should be much smaller than original {original_size} bytes'
    )


def test_init_bytes_resets_index_offset_fixed(tmp_path):
    """Same test for FixedLengthValue files."""
    from booklet import FixedLengthValue

    fp1 = tmp_path / 'original_fixed.blt'
    fp2 = tmp_path / 'from_init_fixed.blt'

    with FixedLengthValue(fp1, 'n', key_serializer='uint4', value_len=10, n_buckets=3) as f:
        for i in range(100):
            f[i] = b'0123456789'

    original_size = fp1.stat().st_size

    with open(fp1, 'rb') as f:
        init_bytes = f.read(200)

    with FixedLengthValue(fp2, 'n', init_bytes=init_bytes) as f:
        f[999] = b'0123456789'

    new_size = fp2.stat().st_size
    assert new_size < original_size, (
        f'New file from init_bytes is {new_size} bytes, '
        f'should be much smaller than original {original_size} bytes'
    )


###########################################################
### In-place streaming prune (bounded-memory rewrite)
#
# These tests are self-contained (own temp files, own 'n' opens) so they don't depend on the
# ordering-sensitive shared-state tests above. They exercise the two-pass in-place prune: round-trip +
# on-disk shrink, dataset-independent (bounded) memory, the relocated-index input layout, the
# empty-after-prune layout, and the crash-recovery _mmap ordering fix.


def test_prune_inplace_roundtrip_variable(tmp_path):
    fp = tmp_path / 'prune_rt_var.blt'
    n = 300
    live = {i: (f'{i:08d}'.encode() * 128) for i in range(n)}  # ~1 KB values

    with booklet.open(fp, 'n', key_serializer='uint4', value_serializer=None) as f:
        for k, v in live.items():
            f[k] = v
        f.sync()
        # Overwrite every key -> the original blocks become dead space to reclaim.
        for k in list(live):
            live[k] = (f'{k:08d}zz'.encode() * 128)
            f[k] = live[k]

    old_size = fp.stat().st_size

    with booklet.open(fp, 'w') as f:
        old_len = len(f)
        removed = f.prune()
        new_len = len(f)
        # Every key must round-trip immediately after prune (same open handle, relocated layout).
        for k, v in live.items():
            assert f[k] == v
        assert f._index_offset != utils.sub_index_init_pos  # relocated: index sits after the data

    new_size = fp.stat().st_size
    assert removed > 0
    assert old_len == n and new_len == n
    assert new_size < old_size

    # Reopen (re-parses the header) and re-verify; a second prune has nothing to reclaim.
    with booklet.open(fp, 'w') as f:
        assert len(f) == n
        for k, v in live.items():
            assert f[k] == v
        assert f.prune() == 0
        assert len(f) == n


def test_prune_inplace_roundtrip_fixed(tmp_path):
    fp = tmp_path / 'prune_rt_fixed.blt'
    n = 300
    vlen = 16
    live = {i: f'{i:016d}'.encode() for i in range(n)}

    with FixedLengthValue(fp, 'n', key_serializer='uint4', value_len=vlen) as f:
        for k, v in live.items():
            f[k] = v
        f.sync()
        for k in list(live):
            live[k] = f'{k + 500000:016d}'.encode()
            f[k] = live[k]

    old_size = fp.stat().st_size

    with FixedLengthValue(fp, 'w') as f:
        old_len = len(f)
        removed = f.prune()
        new_len = len(f)
        for k, v in live.items():
            assert f[k] == v
        assert f._index_offset != utils.sub_index_init_pos

    new_size = fp.stat().st_size
    assert removed > 0
    assert old_len == n and new_len == n
    assert new_size < old_size

    with FixedLengthValue(fp, 'w') as f:
        assert len(f) == n
        for k, v in live.items():
            assert f[k] == v
        assert f.prune() == 0
        assert len(f) == n


def _build_overwrite_and_prune(fp, n, val_size, buffer_size):
    """Build a booklet of n keys, overwrite them all (creating n dead blocks), then prune while
    tracing memory. write_buffer_size must be passed on the prune-time open too -- it's a runtime
    parameter, not persisted in the file. Returns (peak_bytes, live_dict)."""
    live = {}
    with booklet.open(fp, 'n', key_serializer='uint4', value_serializer=None, buffer_size=buffer_size) as f:
        for i in range(n):
            f[i] = bytes([i % 251]) * val_size
        f.sync()
        for i in range(n):
            v = bytes([(i + 7) % 251]) * val_size
            live[i] = v
            f[i] = v

    with booklet.open(fp, 'w', buffer_size=buffer_size) as f:
        tracemalloc.start()
        tracemalloc.reset_peak()
        removed = f.prune()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert removed == n
        for k, v in live.items():  # data intact after the bounded prune
            assert f[k] == v

    return peak, live


def test_prune_bounded_memory(tmp_path):
    """Peak Python memory during prune must track write_buffer_size, NOT the dataset size.

    Regresses the old 'read all live blocks into a list' behaviour, which peaked at ~file size. Prove
    dataset-independence by pruning a small and a 4x-larger dataset with the SAME small write buffer:
    both peaks stay bounded by the buffer (a few blocks), rather than scaling with the data.
    """
    val_size = 8192  # 8 KB per value
    buffer_size = 64 * 1024  # 64 KB write buffer
    ceiling = buffer_size + 8 * val_size  # buffer + a handful of transient blocks

    peak_small, live_small = _build_overwrite_and_prune(tmp_path / 'mem_small.blt', 200, val_size, buffer_size)
    peak_big, live_big = _build_overwrite_and_prune(tmp_path / 'mem_big.blt', 800, val_size, buffer_size)

    small_bytes = 200 * val_size
    big_bytes = 800 * val_size  # ~6.5 MB live; old behaviour would peak here

    # Bounded by the write buffer, and essentially flat as the dataset grows 4x.
    assert peak_small < ceiling, f'small peak {peak_small} exceeds buffer ceiling {ceiling}'
    assert peak_big < ceiling, f'big peak {peak_big} exceeds buffer ceiling {ceiling}'
    assert peak_big < big_bytes // 4, f'big peak {peak_big} scales with dataset {big_bytes}'
    assert peak_big < peak_small * 2, f'peak grew with data: {peak_small} -> {peak_big}'


@pytest.mark.parametrize('fixed', [False, True])
def test_prune_relocated_index(tmp_path, fixed):
    """Prune must handle a relocated-index INPUT layout (produced by auto-reindex)."""
    fp = tmp_path / 'prune_reloc.blt'
    n = 200
    if fixed:
        live = {i: f'{i:012d}'.encode() for i in range(n)}
        opener_w = lambda: FixedLengthValue(fp, 'w')

        def opener_n():
            return FixedLengthValue(fp, 'n', key_serializer='uint4', value_len=12, n_buckets=12)
    else:
        live = {i: (f'{i:08d}'.encode() * 32) for i in range(n)}
        opener_w = lambda: booklet.open(fp, 'w')

        def opener_n():
            return booklet.open(fp, 'n', key_serializer='uint4', value_serializer=None, n_buckets=12)

    # n_buckets=12 with 200 keys forces at least one auto-reindex -> relocated index on close.
    with opener_n() as f:
        for k, v in live.items():
            f[k] = v

    with opener_w() as f:
        assert f._index_offset != utils.sub_index_init_pos  # confirm relocated input
        for k in range(0, n, 3):  # delete a third to create dead space
            del f[k]
            live.pop(k, None)

    old_size = fp.stat().st_size

    with opener_w() as f:
        assert f._index_offset != utils.sub_index_init_pos
        removed = f.prune()
        assert removed > 0
        assert len(f) == len(live)
        for k, v in live.items():
            assert f[k] == v

    assert fp.stat().st_size < old_size

    # Reopen and re-verify integrity through the post-prune (relocated) header, and confirm new writes
    # append correctly into the relocated layout.
    with opener_w() as f:
        assert len(f) == len(live)
        for k, v in live.items():
            assert f[k] == v
        assert set(f.keys()) == set(live)
        assert f.prune() == 0
        new = {900001: live[next(iter(live))], 900002: live[max(live)]}
        for k, v in new.items():
            f[k] = v
        for k, v in {**live, **new}.items():
            assert f[k] == v


@pytest.mark.parametrize('fixed', [False, True])
def test_prune_empty_after_delete(tmp_path, fixed):
    """Deleting every key then pruning yields the standard cleared-empty layout, reusable afterwards."""
    fp = tmp_path / 'prune_empty.blt'
    keys = list(range(50))
    if fixed:
        opener_w = lambda: FixedLengthValue(fp, 'w')
        mkval = lambda k: f'{k:08d}'.encode()

        def opener_n():
            return FixedLengthValue(fp, 'n', key_serializer='uint4', value_len=8)
    else:
        opener_w = lambda: booklet.open(fp, 'w')
        mkval = lambda k: f'val{k}'.encode()

        def opener_n():
            return booklet.open(fp, 'n', key_serializer='uint4', value_serializer=None)

    with opener_n() as f:
        for k in keys:
            f[k] = mkval(k)

    with opener_w() as f:
        for k in keys:
            del f[k]
        removed = f.prune()
        assert removed > 0
        assert len(f) == 0
        assert list(f.keys()) == []
        # Standard empty layout: index back at byte 200.
        assert f._index_offset == utils.sub_index_init_pos

    # Reopen: iterates clean and accepts new writes.
    with opener_w() as f:
        assert len(f) == 0
        assert list(f.keys()) == []
        f[999] = mkval(999)
        assert f[999] == mkval(999)

    with opener_w() as f:
        assert len(f) == 1
        assert f[999] == mkval(999)


@pytest.mark.parametrize('fixed', [False, True])
def test_reopen_uncleanly_closed_write(tmp_path, fixed):
    """A file left with n_keys == n_keys_crash (unclean close) reopens in write mode without an
    AttributeError on _mmap, rebuilds n_keys, and can still be pruned."""
    fp = tmp_path / 'prune_crash.blt'
    n = 40
    if fixed:
        opener_w = lambda: FixedLengthValue(fp, 'w')
        mkval = lambda k: f'{k:08d}'.encode()

        def opener_n():
            return FixedLengthValue(fp, 'n', key_serializer='uint4', value_len=8)
    else:
        opener_w = lambda: booklet.open(fp, 'w')
        mkval = lambda k: f'val{k}'.encode()

        def opener_n():
            return booklet.open(fp, 'n', key_serializer='uint4', value_serializer=None)

    with opener_n() as f:
        for k in range(n):
            f[k] = mkval(k)

    # Simulate an unclean close: stamp the crash sentinel into the n_keys header field.
    with open(fp, 'r+b') as raw:
        raw.seek(utils.n_keys_pos)
        raw.write(utils.int_to_bytes(utils.n_keys_crash, 4))

    # Reopen in write mode -> must NOT raise AttributeError('_mmap'); n_keys is rebuilt via keys().
    with opener_w() as f:
        assert len(f) == n
        for k in range(n):
            assert f[k] == mkval(k)
        # And prune still works on the recovered file.
        assert f.prune() == 0
        assert len(f) == n












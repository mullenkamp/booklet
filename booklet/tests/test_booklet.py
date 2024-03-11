#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 13:55:17 2024

@author: mike
"""
import pytest
import io
from booklet import Booklet, __version__
from tempfile import NamedTemporaryFile
import concurrent.futures

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
def test_delete():
    indexes = [10, 12]

    for index in indexes:
        _ = data_dict.pop(index)
    
        with Booklet(file_path, 'w') as f:
            del f[index]
    
            new_len = len(f)
    
            f.sync()
    
            try:
                _ = f[index]
                raise ValueError()
            except KeyError:
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



















































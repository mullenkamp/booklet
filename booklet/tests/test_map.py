#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for Booklet.map() multiprocessing method.
"""
import booklet
from booklet import FixedLengthValue
from tempfile import NamedTemporaryFile


##############################################
### Top-level functions (must be picklable)

def double_value(key, value):
    return (key, value * 2)


def make_new_key(key, value):
    return (f"processed_{key}", value + 100)


def skip_even(key, value):
    if key % 2 == 0:
        return None
    return (key, value)


def identity(key, value):
    return (key, value)


def fixed_transform(key, value):
    """Transform for fixed-length: reverse the bytes."""
    return (key, value[::-1])


##############################################
### Tests


def test_map_all_keys():
    data = {i: i * 10 for i in range(1, 21)}

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf.name, 'w') as db:
            results = dict(db.map(double_value, n_workers=2))

        assert len(results) == len(data)
        for k, v in data.items():
            assert results[k] == v * 2


def test_map_specific_keys():
    data = {i: i * 10 for i in range(1, 21)}
    keys_to_process = [1, 5, 10]

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf.name, 'w') as db:
            results = dict(db.map(double_value, keys=keys_to_process, n_workers=2))

        assert len(results) == len(keys_to_process)
        for k in keys_to_process:
            assert results[k] == data[k] * 2


def test_map_different_output_keys():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='str', value_serializer='pickle') as db:
            for k, v in data.items():
                db[str(k)] = v

        with booklet.open(tf.name, 'w') as db:
            results = dict(db.map(make_new_key, n_workers=2))

        for k, v in data.items():
            assert results[f"processed_{k}"] == v + 100


def test_map_skip_none():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf.name, 'w') as db:
            results = list(db.map(skip_even, n_workers=2))

        # Only odd keys should be yielded
        assert len(results) == 5
        result_keys = {k for k, v in results}
        assert all(k % 2 != 0 for k in result_keys)


def test_map_read_only_input():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf.name, 'r') as db:
            results = dict(db.map(double_value, n_workers=2))

        assert len(results) == len(data)
        for k, v in data.items():
            assert results[k] == v * 2


def test_map_fixed_length():
    data = {str(i): b'\x00' * 8 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with FixedLengthValue(tf.name, 'n', key_serializer='str', value_len=8) as db:
            for k, v in data.items():
                db[k] = v

        with FixedLengthValue(tf.name, 'w') as db:
            results = dict(db.map(fixed_transform, n_workers=2))

        for k in data:
            assert results[k] == b'\x00' * 8  # reversed null bytes are still null bytes

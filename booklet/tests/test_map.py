#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for Booklet.map() multiprocessing method.
"""
import pytest
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
            stats = db.map(double_value, n_workers=2)

        with booklet.open(tf.name) as db:
            for k, v in data.items():
                assert db[k] == v * 2


def test_map_specific_keys():
    data = {i: i * 10 for i in range(1, 21)}
    keys_to_process = [1, 5, 10]

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf.name, 'w') as db:
            stats = db.map(double_value, keys=keys_to_process, n_workers=2)

        with booklet.open(tf.name) as db:
            for k in keys_to_process:
                assert db[k] == data[k] * 2
            # Unprocessed keys should be unchanged
            for k in data:
                if k not in keys_to_process:
                    assert db[k] == data[k]


def test_map_different_output_keys():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='str', value_serializer='pickle') as db:
            for k, v in data.items():
                db[str(k)] = v

        with booklet.open(tf.name, 'w') as db:
            stats = db.map(make_new_key, n_workers=2)

        with booklet.open(tf.name) as db:
            for k, v in data.items():
                assert db[f"processed_{k}"] == v + 100


def test_map_skip_none():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf.name, 'w') as db:
            stats = db.map(skip_even, n_workers=2)

        assert stats['written'] == 5  # only odd keys
        assert stats['processed'] == len(data)


def test_map_separate_write_db():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf_in, NamedTemporaryFile() as tf_out:
        with booklet.open(tf_in.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf_in.name, 'w') as input_db:
            with booklet.open(tf_out.name, 'n', key_serializer='uint4', value_serializer='pickle') as output_db:
                stats = input_db.map(double_value, write_db=output_db, n_workers=2)

        # Input should be unchanged
        with booklet.open(tf_in.name) as db:
            for k, v in data.items():
                assert db[k] == v

        # Output should have doubled values
        with booklet.open(tf_out.name) as db:
            for k, v in data.items():
                assert db[k] == v * 2


def test_map_read_only_input():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf_in, NamedTemporaryFile() as tf_out:
        with booklet.open(tf_in.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf_in.name, 'r') as input_db:
            with booklet.open(tf_out.name, 'n', key_serializer='uint4', value_serializer='pickle') as output_db:
                stats = input_db.map(double_value, write_db=output_db, n_workers=2)

        with booklet.open(tf_out.name) as db:
            for k, v in data.items():
                assert db[k] == v * 2


def test_map_read_only_no_write_db_raises():
    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            db[1] = 10

        with booklet.open(tf.name, 'r') as db:
            with pytest.raises(ValueError, match='read only'):
                db.map(double_value, n_workers=2)


def test_map_fixed_length():
    data = {str(i): b'\x00' * 8 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with FixedLengthValue(tf.name, 'n', key_serializer='str', value_len=8) as db:
            for k, v in data.items():
                db[k] = v

        with FixedLengthValue(tf.name, 'w') as db:
            stats = db.map(fixed_transform, n_workers=2)

        with FixedLengthValue(tf.name) as db:
            for k in data:
                assert db[k] == b'\x00' * 8  # reversed null bytes are still null bytes


def test_map_stats():
    data = {i: i * 10 for i in range(1, 11)}

    with NamedTemporaryFile() as tf:
        with booklet.open(tf.name, 'n', key_serializer='uint4', value_serializer='pickle') as db:
            for k, v in data.items():
                db[k] = v

        with booklet.open(tf.name, 'w') as db:
            stats = db.map(identity, n_workers=2)

        assert stats['processed'] == 10
        assert stats['written'] == 10
        assert stats['errors'] == 0

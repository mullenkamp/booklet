import booklet
from booklet import utils
import pytest
import os

@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.blt"

def test_auto_reindex_variable(db_path):
    # Start with a very small number of buckets
    n_buckets = 10
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        assert db._n_buckets == n_buckets

        # Insert more keys than buckets to trigger reindexing
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        # Check if reindexing was triggered
        assert db._n_buckets > n_buckets

        # Verify all data is still there
        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == f'value_{i}'

    # Reopen and verify
    with booklet.open(db_path, 'r') as db:
        assert db._n_buckets > n_buckets
        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == f'value_{i}'

def test_auto_reindex_fixed(db_path):
    # Start with a very small number of buckets
    n_buckets = 10
    value_len = 10
    with booklet.FixedLengthValue(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_len=value_len, buffer_size=512) as db:
        assert db._n_buckets == n_buckets

        # Insert more keys than buckets to trigger reindexing
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = b'0' * value_len

        db.sync()

        # Check if reindexing was triggered
        assert db._n_buckets > n_buckets

        # Verify all data is still there
        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == b'0' * value_len

    # Reopen and verify
    with booklet.FixedLengthValue(db_path, 'r') as db:
        assert db._n_buckets > n_buckets
        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == b'0' * value_len


def test_prune_resets_index_layout(db_path):
    """After auto-reindex, prune should reset the index back to byte 200."""
    n_buckets = 10
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        # After reindex, index should be relocated
        assert db._index_offset != utils.sub_index_init_pos

        db.prune()

        # After prune, index should be back at byte 200
        assert db._index_offset == utils.sub_index_init_pos
        assert db._first_data_block_pos == utils.sub_index_init_pos + (db._n_buckets * utils.n_bytes_file)

        # Data should still be accessible
        for i in range(n_buckets + 1):
            assert db[f'key_{i}'] == f'value_{i}'


def test_reopen_after_reindex(db_path):
    """Reopened file after reindex should read the relocated index correctly."""
    n_buckets = 10
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        saved_n_buckets = db._n_buckets
        saved_index_offset = db._index_offset

    # Reopen for reading
    with booklet.open(db_path, 'r') as db:
        assert db._n_buckets == saved_n_buckets
        assert db._index_offset == saved_index_offset
        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == f'value_{i}'

    # Reopen for writing and add more data
    with booklet.open(db_path, 'w') as db:
        db['extra_key'] = 'extra_value'

    with booklet.open(db_path, 'r') as db:
        assert db['extra_key'] == 'extra_value'
        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == f'value_{i}'


def test_non_mapped_n_buckets_jumps(db_path):
    """Non-mapped n_buckets should jump to the smallest chain value larger than current."""
    n_buckets = 50  # Not in n_buckets_reindex mapping
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        # Should jump to 12007 (smallest chain value > 50)
        assert db._n_buckets == 12007

        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == f'value_{i}'


def test_writes_after_reindex(db_path):
    """Writes after reindex should be appended after the new index and be readable."""
    n_buckets = 10
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        # Reindex should have happened
        assert db._n_buckets > n_buckets

        # Write more data after reindex
        for i in range(100, 120):
            db[f'key_{i}'] = f'value_{i}'

    with booklet.open(db_path, 'r') as db:
        for i in range(n_buckets + 2):
            assert db[f'key_{i}'] == f'value_{i}'
        for i in range(100, 120):
            assert db[f'key_{i}'] == f'value_{i}'


def test_delete_after_reindex(db_path):
    """Deletes after reindex should work correctly with the new index."""
    n_buckets = 10
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        assert db._n_buckets > n_buckets

        del db['key_0']
        assert len(db) == n_buckets + 1
        assert 'key_0' not in db
        assert db['key_1'] == 'value_1'


def test_iteration_after_reindex(db_path):
    """Iteration should correctly scan both data regions after reindex."""
    n_buckets = 10
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        # Write more after reindex
        for i in range(100, 110):
            db[f'key_{i}'] = f'value_{i}'

        keys = set(db.keys())
        expected_keys = set(f'key_{i}' for i in list(range(n_buckets + 2)) + list(range(100, 110)))
        assert keys == expected_keys

        db.sync()

        items = dict(db.items())
        assert len(items) == n_buckets + 2 + 10
        for i in range(n_buckets + 2):
            assert items[f'key_{i}'] == f'value_{i}'
        for i in range(100, 110):
            assert items[f'key_{i}'] == f'value_{i}'


def test_clear_after_reindex(db_path):
    """Clear after reindex should reset the index layout."""
    n_buckets = 10
    with booklet.open(db_path, 'n', n_buckets=n_buckets, key_serializer='str', value_serializer='str', buffer_size=512) as db:
        for i in range(n_buckets + 2):
            db[f'key_{i}'] = f'value_{i}'

        db.sync()

        assert db._index_offset != utils.sub_index_init_pos

        db.clear()
        assert len(db) == 0
        assert db._index_offset == utils.sub_index_init_pos
        assert list(db.keys()) == []

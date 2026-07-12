"""
Tests for the 0.12.7 application-reserved internal slots: hidden bookkeeping
entries (like the metadata key) that must stay invisible to every enumeration
path, survive prune, die with clear, and never touch the key count. Plus the
prune(keep_keys=...) eviction exemption and the reserved-delete count-skew fix.
"""
import pytest

import booklet
from booklet import utils


def _new_file(path, **kwargs):
    kwargs.setdefault('key_serializer', 'str')
    kwargs.setdefault('value_serializer', 'bytes')
    kwargs.setdefault('n_buckets', 101)
    return booklet.open(path, 'n', **kwargs)


def _keyfunc(key, value):
    return key, value


def test_round_trip_and_overwrite(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        assert f.get_reserved(1) is None
        f.set_reserved(1, b'journal-v1')
        f.set_reserved(2, b'cache-v1')
        assert f.get_reserved(1) == b'journal-v1'
        assert f.get_reserved(2) == b'cache-v1'
        f.set_reserved(1, b'journal-v2', timestamp=1_600_000_000_000_000)
        out = f.get_reserved(1, include_timestamp=True)
        assert out == (b'journal-v2', 1_600_000_000_000_000)

    with booklet.open(p) as f:   # read-mode reopen (mmap path)
        assert f.get_reserved(1) == b'journal-v2'
        assert f.get_reserved(2) == b'cache-v1'


def test_slot_validation(tmp_path):
    with _new_file(tmp_path / 'f.blt') as f:
        with pytest.raises(ValueError):
            f.set_reserved(3, b'x')
        with pytest.raises(ValueError):
            f.get_reserved(0)
        with pytest.raises(TypeError):
            f.set_reserved(1, 'not-bytes')


def test_invisible_to_enumeration_write_mode(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p, init_timestamps=True) as f:
        f['a'] = b'1'
        f['b'] = b'2'
        f.set_reserved(1, b'hidden')
        f.set_reserved(2, b'hidden2')
        f.set_metadata({'m': 1})

        assert sorted(f.keys()) == ['a', 'b']
        assert sorted(k for k, v in f.items()) == ['a', 'b']
        assert len(list(f.values())) == 2
        assert len(list(f.timestamps())) == 2
        assert len(f) == 2


def test_invisible_to_enumeration_read_mode(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        f['a'] = b'1'
        f.set_reserved(1, b'hidden')

    with booklet.open(p) as f:   # 'r' -> mmap iterators
        assert sorted(f.keys()) == ['a']
        assert len(f) == 1
        assert f.get_reserved(1) == b'hidden'


def test_invisible_to_map(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        f['a'] = b'1'
        f['b'] = b'2'
        f.set_reserved(1, b'hidden')

    with booklet.open(p) as f:
        seen = sorted(k for k, v in f.map(_keyfunc, n_workers=2))
    assert seen == ['a', 'b']


def test_survives_prune_and_counts_stay_right(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p, init_timestamps=True) as f:
        f['a'] = b'1'
        f['a'] = b'1b'           # dead block for prune to compact
        f['b'] = b'2'
        f.set_reserved(1, b'hidden')
        f.set_reserved(1, b'hidden-v2')   # overwritten slot block too
        f.set_metadata({'m': 1})
        removed = f.prune()
        assert removed >= 2
        assert len(f) == 2
        assert f.get_reserved(1) == b'hidden-v2'
        assert f.get_metadata() == {'m': 1}

    with booklet.open(p) as f:
        assert len(f) == 2
        assert sorted(f.keys()) == ['a', 'b']
        assert f.get_reserved(1) == b'hidden-v2'


def test_prune_keep_keys(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p, init_timestamps=True) as f:
        f.set('old-keep', b'k', timestamp=1_000_000_000_000_000)
        f.set('old-evict', b'e', timestamp=1_000_000_000_000_000)
        f.set('new', b'n', timestamp=2_000_000_000_000_000)
        f.set_reserved(1, b'hidden')
        f.prune(timestamp=1_500_000_000_000_000, keep_keys=['old-keep'])

        assert 'old-keep' in f, 'keep_keys entry was evicted by the timestamp prune'
        assert 'old-evict' not in f
        assert 'new' in f
        assert len(f) == 2
        assert f.get_reserved(1) == b'hidden'


def test_destroyed_by_clear(tmp_path):
    with _new_file(tmp_path / 'f.blt') as f:
        f['a'] = b'1'
        f.set_reserved(1, b'hidden')
        f.clear()
        assert f.get_reserved(1) is None
        assert len(f) == 0


def test_set_reserved_during_iteration_raises(tmp_path):
    with _new_file(tmp_path / 'f.blt') as f:
        for i in range(10):
            f[f'k{i}'] = b'v'
        it = f.keys()
        next(it)
        f.set_reserved(1, b'hidden')
        with pytest.raises(RuntimeError, match='mutated during iteration'):
            next(it)


def test_fixed_length_set_reserved_raises(tmp_path):
    p = tmp_path / 'fx.blt'
    with booklet.FixedLengthValue(p, 'n', key_serializer='str', value_len=5, n_buckets=101) as f:
        f['k1'] = b'AAAAA'
        with pytest.raises(NotImplementedError):
            f.set_reserved(1, b'x')
        assert f.get_reserved(1) is None
        assert sorted(f.keys()) == ['k1']


def test_reserved_delete_count_skew_regression(tmp_path):
    """Deleting the metadata key or a reserved slot via the raw internal key
    must not decrement the user-key count (it was never incremented)."""
    with _new_file(tmp_path / 'f.blt') as f:
        f['a'] = b'1'
        f['b'] = b'2'
        f.set_metadata({'m': 1})
        f.set_reserved(1, b'hidden')
        assert len(f) == 2

        del f[utils.metadata_key_bytes.decode()]
        assert len(f) == 2, 'deleting the metadata key skewed the key count'
        del f[utils.reserved_slot_key_bytes[1].decode()]
        assert len(f) == 2, 'deleting a reserved slot skewed the key count'

        ## Normal deletes still count down.
        del f['a']
        assert len(f) == 1

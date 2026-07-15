"""
Tests for the 0.12.8 locations() header-only iterator and the compaction_count
property: physical (offset, length) resolution of value bytes for external
readers, the append-only validity contract (offsets survive overwrite/delete/
auto-reindex, die on prune/clear), and the fixed-length refusal.
"""
import io

import pytest

import booklet
from booklet import utils


def _new_file(path, **kwargs):
    kwargs.setdefault('key_serializer', 'str')
    kwargs.setdefault('value_serializer', 'bytes')
    kwargs.setdefault('n_buckets', 101)
    return booklet.open(path, 'n', **kwargs)


def _read_at(path, offset, length):
    with open(path, 'rb') as f:
        f.seek(offset)
        return f.read(length)


def _locs(f):
    return {key: (ts, off, ln) for key, ts, off, ln in f.locations()}


def test_triples_match_independent_reads_and_timestamps(tmp_path):
    p = tmp_path / 'f.blt'
    data = {f'k{i}': bytes([i]) * (i + 1) for i in range(20)}
    with _new_file(p) as f:
        for k, v in data.items():
            f[k] = v
        locs = _locs(f)
        assert set(locs) == set(data)
        for k, (ts, off, ln) in locs.items():
            assert ln == len(data[k])
            assert _read_at(p, off, ln) == data[k], f'{k}: independent read differs'
            assert ts == f.get_timestamp(k)


def test_excludes_metadata_and_reserved_keys(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        f['a'] = b'1'
        f['b'] = b'2'
        f.set_metadata({'m': 1})
        f.set_reserved(1, b'hidden')
        locs = _locs(f)
        assert sorted(locs) == ['a', 'b']
        assert len(locs) == len(f)


def test_overwrite_appends_and_delete_omits(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        f['a'] = b'old-bytes'
        f['b'] = b'stays'
        f.sync()
        old = _locs(f)['a']

        f['a'] = b'new-bytes'
        f['gone'] = b'x'
        del f['gone']
        locs = _locs(f)

        assert locs['a'][1] != old[1], 'overwrite must append a new block'
        assert _read_at(p, locs['a'][1], locs['a'][2]) == b'new-bytes'
        ## Append-only: the captured old offset still addresses the OLD bytes.
        assert _read_at(p, old[1], old[2]) == b'old-bytes'
        assert 'gone' not in locs
        assert sorted(locs) == ['a', 'b']


def test_empty_value(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        f['empty'] = b''
        f['full'] = b'x'
        locs = _locs(f)
        assert locs['empty'][2] == 0
        assert locs['empty'][1] > 0
        assert _read_at(p, locs['empty'][1], locs['empty'][2]) == b''


def test_buffered_unsynced_writes_are_flushed_first(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        f['buffered'] = b'not-yet-on-disk'
        ## No explicit sync: locations() must flush first so the returned
        ## offset is a real on-disk position.
        locs = _locs(f)
        assert 'buffered' in locs
        _, off, ln = locs['buffered']
        assert _read_at(p, off, ln) == b'not-yet-on-disk'


def test_offsets_survive_auto_reindex(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p, n_buckets=13) as f:
        seed = {f'k{i}': f'v{i}'.encode() for i in range(30)}
        for k, v in seed.items():
            f[k] = v
        f.sync()
        before = _locs(f)
        comp0 = f.compaction_count

        ## Blow past the load factor to force an auto-reindex on sync.
        for i in range(3000):
            f[f'fill{i}'] = b'z'
        f.sync()
        assert f._index_offset != utils.sub_index_init_pos, 'reindex did not relocate the index'

        ## Old captured offsets still read the correct bytes...
        for k, (_ts, off, ln) in before.items():
            assert _read_at(p, off, ln) == seed[k], f'{k}: offset invalidated by reindex'
        ## ...a fresh scan (two-region layout) still resolves everything...
        after = _locs(f)
        assert set(after) == set(seed) | {f'fill{i}' for i in range(3000)}
        for k, v in seed.items():
            assert _read_at(p, after[k][1], after[k][2]) == v
        ## ...and a reindex is NOT a compaction.
        assert f.compaction_count == comp0


def test_compaction_count_bumps_on_prune_and_clear(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        f['a'] = b'1'
        f['a'] = b'1b'   # dead block for prune to compact
        f['b'] = b'2'
        assert f.compaction_count == 0

        removed = f.prune()
        assert removed >= 1
        assert f.compaction_count == 1
        ## Post-prune (relocated, two-region) layout: fresh scan verifies.
        locs = _locs(f)
        assert sorted(locs) == ['a', 'b']
        for k, v in (('a', b'1b'), ('b', b'2')):
            assert _read_at(p, locs[k][1], locs[k][2]) == v

        f.clear()
        assert f.compaction_count == 2
        assert _locs(f) == {}


def test_read_mode_mmap_path(tmp_path):
    p = tmp_path / 'f.blt'
    data = {f'k{i}': bytes([65 + i]) * 3 for i in range(10)}
    with _new_file(p) as f:
        for k, v in data.items():
            f[k] = v
        f.set_reserved(1, b'hidden')
        write_locs = _locs(f)

    with booklet.open(p) as f:   # 'r' -> mmap iterator
        assert f._mmap is not None
        read_locs = _locs(f)
        assert read_locs == write_locs
        for k, (ts, off, ln) in read_locs.items():
            assert _read_at(p, off, ln) == data[k]
            assert ts == f.get_timestamp(k)


def test_fixed_length_raises(tmp_path):
    p = tmp_path / 'fx.blt'
    with booklet.FixedLengthValue(p, 'n', key_serializer='str', value_len=5, n_buckets=101) as f:
        f['k1'] = b'AAAAA'
        f['k1'] = b'BBBBB'   # dead block so prune has work
        with pytest.raises(NotImplementedError):
            list(f.locations())
        ## compaction_count stays inherited and meaningful on fixed files.
        assert f.compaction_count == 0
        f.prune()
        assert f.compaction_count == 1


def test_no_timestamp_file_yields_none(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p, init_timestamps=False) as f:
        f['a'] = b'payload'
        locs = _locs(f)
        ts, off, ln = locs['a']
        assert ts is None
        assert _read_at(p, off, ln) == b'payload'
        ## timestamps() raises on such files - locations() deliberately does not.
        with pytest.raises(ValueError):
            list(f.timestamps())


def test_mutation_during_iteration_raises_set_timestamp_allowed(tmp_path):
    p = tmp_path / 'f.blt'
    with _new_file(p) as f:
        for i in range(10):
            f[f'k{i}'] = b'v'
        f.sync()

        it = f.locations()
        next(it)
        f.set_timestamp('k5', 1_600_000_000_000_000)   # allowed during iteration
        next(it)
        f['k99'] = b'mutation'
        with pytest.raises(RuntimeError, match='mutated during iteration'):
            next(it)


def test_bytesio_backed_offsets_slice_the_buffer(tmp_path):
    buf = io.BytesIO()
    f = booklet.VariableLengthValue(buf, 'n', key_serializer='str', value_serializer='bytes', n_buckets=101)
    try:
        f['a'] = b'alpha'
        f['b'] = b'beta'
        locs = _locs(f)
        raw = buf.getvalue()
        assert raw[locs['a'][1]:locs['a'][1] + locs['a'][2]] == b'alpha'
        assert raw[locs['b'][1]:locs['b'][1] + locs['b'][2]] == b'beta'
    finally:
        f.close()

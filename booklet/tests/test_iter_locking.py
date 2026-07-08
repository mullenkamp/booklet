#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regression tests for the 0.12.6 iteration contract: per-step locking (reads
during iteration must not deadlock) + mutation detection (writes during
iteration must raise RuntimeError instead of deadlocking/corrupting), plus the
fixed-length write-path integrity guards shipped in the same release.

Every deadlock-class test runs its body in a daemon thread with a join
timeout, so on pre-0.12.6 code these tests FAIL cleanly (timeout) instead of
hanging the suite. Each test uses its own file: a pre-fix deadlocked body
leaves a daemon thread holding that instance's lock, which must not poison
other tests.
"""
import io
import os
from datetime import datetime, timezone
import threading

import pytest

import booklet
from booklet import FixedLengthValue, utils

##############################################
### Parameters

N_KEYS = 50          # > n_buckets so a sync triggers auto-reindex (two-region layout)
N_BUCKETS = 13
FIXED_LEN = 12
# Generous; the real bodies run in milliseconds. Overridable so a
# demonstrate-the-old-deadlocks run against pre-fix code finishes quickly.
TIMEOUT = float(os.environ.get('BOOKLET_TEST_TIMEOUT', 15))

##############################################
### Helpers


def run_timed(fn, timeout=TIMEOUT):
    """Run fn in a daemon thread; fail the test on timeout (= deadlock pre-fix)."""
    out = {}

    def runner():
        try:
            out['result'] = fn()
        except BaseException as e:  # noqa: BLE001 - re-raised in the test thread
            out['exc'] = e

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), 'deadlock: body did not complete within timeout'
    if 'exc' in out:
        raise out['exc']
    return out.get('result')


def make_var(path, n=N_KEYS):
    """Variable-length booklet; relocated (two-region) layout when n > N_BUCKETS."""
    b = booklet.open(path, 'n', key_serializer='str', value_serializer='orjson', n_buckets=N_BUCKETS)
    for i in range(n):
        b[f'key{i}'] = {'v': i}
    b.sync()   # n > n_buckets -> auto-reindex -> relocated index
    if n > N_BUCKETS:
        assert b._index_offset != utils.sub_index_init_pos
    return b


def fixed_value(i):
    return f'{i:0{FIXED_LEN}d}'.encode()


def make_fixed(path_or_buf, n=N_KEYS):
    """Fixed-length booklet; relocated (two-region) layout when n > N_BUCKETS."""
    b = FixedLengthValue(path_or_buf, 'n', key_serializer='str', value_len=FIXED_LEN, n_buckets=N_BUCKETS)
    for i in range(n):
        b[f'key{i}'] = fixed_value(i)
    b.sync()
    if n > N_BUCKETS:
        assert b._index_offset != utils.sub_index_init_pos
    return b


##############################################
### Reads during iteration (pre-fix: deadlock)


def test_get_during_keys_variable_write_mode(tmp_path):
    b = make_var(tmp_path / 'v.blt')

    def body():
        return {k: b[k]['v'] for k in b.keys()}

    result = run_timed(body)
    assert result == {f'key{i}': i for i in range(N_KEYS)}
    b.close()


def test_get_during_keys_variable_read_mode_mmap(tmp_path):
    make_var(tmp_path / 'v.blt').close()
    b = booklet.open(tmp_path / 'v.blt')   # 'r' -> mmap path
    assert b._mmap is not None

    def body():
        return {k: b[k]['v'] for k in b.keys()}

    result = run_timed(body)
    assert result == {f'key{i}': i for i in range(N_KEYS)}
    b.close()


def test_get_during_keys_fixed_write_mode(tmp_path):
    b = make_fixed(tmp_path / 'f.blt')

    def body():
        return {k: b[k] for k in b.keys()}

    result = run_timed(body)
    assert result == {f'key{i}': fixed_value(i) for i in range(N_KEYS)}
    b.close()


def test_get_during_keys_fixed_read_mode_mmap(tmp_path):
    make_fixed(tmp_path / 'f.blt').close()
    b = FixedLengthValue(tmp_path / 'f.blt')   # 'r' -> mmap path
    assert b._mmap is not None

    def body():
        return {k: b[k] for k in b.keys()}

    result = run_timed(body)
    assert result == {f'key{i}': fixed_value(i) for i in range(N_KEYS)}
    b.close()


def test_get_during_keys_fixed_bytesio(tmp_path):
    # BytesIO never gets an mmap, even read-only: this exercises the rewritten
    # fixed FILE iterator, whose pre-fix version carried its cursor in the
    # shared file position (interleaved reads corrupted/livelocked the scan).
    b = make_fixed(io.BytesIO())

    def body():
        return {k: b[k] for k in b.keys()}

    result = run_timed(body)
    assert result == {f'key{i}': fixed_value(i) for i in range(N_KEYS)}
    b.close()


def test_contains_during_keys_fixed(tmp_path):
    # Pre-fix this was the livelock case on the fixed file iterator: the
    # interleaved __contains__ reset the shared position every step.
    b = make_fixed(tmp_path / 'f.blt')

    def body():
        count = 0
        for _key in b.keys():
            assert 'key0' in b
            count += 1
        return count

    assert run_timed(body) == N_KEYS
    b.close()


def test_get_during_items_values_and_timestamps_variable(tmp_path):
    b = make_var(tmp_path / 'v.blt')

    def body():
        n_items = sum(1 for k, v in b.items() if b[k] == v)
        n_values = sum(1 for _v in b.values() if b['key0'] is not None)
        n_ts = sum(1 for _k, _ts in b.timestamps() if b['key1'] is not None)
        return n_items, n_values, n_ts

    assert run_timed(body) == (N_KEYS, N_KEYS, N_KEYS)
    b.close()


def test_nested_iterators(tmp_path):
    b = make_var(tmp_path / 'v.blt', n=8)

    def body():
        return sum(1 for _k1 in b.keys() for _k2 in b.keys())

    assert run_timed(body) == 64
    b.close()


def test_get_timestamp_during_keys(tmp_path):
    b = make_var(tmp_path / 'v.blt')

    def body():
        return sum(1 for k in b.keys() if isinstance(b.get_timestamp(k), int))

    assert run_timed(body) == N_KEYS
    b.close()


def test_abandoned_generator_releases_lock(tmp_path):
    # Pre-fix, a half-consumed generator kept holding the lock, so the
    # subsequent set() deadlocked even though the loop had exited.
    b = make_var(tmp_path / 'v.blt')

    def body():
        it = b.keys()
        next(it)
        # keep `it` referenced and suspended; the lock must NOT be held here
        b['after_abandon'] = {'v': -1}
        return b['after_abandon']['v']

    assert run_timed(body) == -1
    b.close()


##############################################
### Mutation during iteration -> RuntimeError (pre-fix: deadlock)


@pytest.mark.parametrize('mutate', [
    lambda b: b.set('brand_new_key', {'v': 999}),
    lambda b: b.update({'brand_new_key': {'v': 999}}),
    lambda b: b.__delitem__('key1'),
    lambda b: b.set_metadata({'m': 1}),
    lambda b: b.prune(),
    lambda b: b.clear(),
    lambda b: b.set('key2', {'v': 999}),   # overwriting an EXISTING key also raises
])
def test_mutation_during_keys_raises_variable(tmp_path, mutate):
    b = make_var(tmp_path / 'v.blt')

    def body():
        it = b.keys()
        next(it)
        mutate(b)
        with pytest.raises(RuntimeError, match='mutated during iteration'):
            next(it)
        return True

    assert run_timed(body)
    b.close()


@pytest.mark.parametrize('mutate', [
    lambda b: b.set('brand_new_key', b'x' * FIXED_LEN),
    lambda b: b.__setitem__('brand_new_key', b'x' * FIXED_LEN),
    lambda b: b.update({'brand_new_key': b'x' * FIXED_LEN}),
    lambda b: b.prune(),
])
def test_mutation_during_keys_raises_fixed(tmp_path, mutate):
    b = make_fixed(tmp_path / 'f.blt')

    def body():
        it = b.keys()
        next(it)
        mutate(b)
        with pytest.raises(RuntimeError, match='mutated during iteration'):
            next(it)
        return True

    assert run_timed(body)
    b.close()


def test_fresh_iteration_after_mutation_error(tmp_path):
    b = make_var(tmp_path / 'v.blt')

    def body():
        it = b.keys()
        next(it)
        b['late_key'] = {'v': 1}
        with pytest.raises(RuntimeError):
            next(it)
        b.sync()
        return sum(1 for _k in b.keys())

    assert run_timed(body) == N_KEYS + 1
    b.close()


def test_concurrent_writer_thread_raises_not_corrupts(tmp_path):
    b = make_var(tmp_path / 'v.blt')
    started = threading.Event()
    written = threading.Event()

    def writer():
        started.wait(TIMEOUT)
        b['from_other_thread'] = {'v': 1}
        written.set()

    w = threading.Thread(target=writer, daemon=True)
    w.start()

    def body():
        it = b.keys()
        keys = [next(it)]
        started.set()
        assert written.wait(TIMEOUT)
        with pytest.raises(RuntimeError, match='mutated during iteration'):
            keys.extend(iter(lambda: next(it), None))
        return keys

    assert len(run_timed(body)) >= 1
    w.join(TIMEOUT)
    b.close()


##############################################
### Allowed pattern: set_timestamp during timestamps()


def test_set_timestamp_during_timestamps_iteration(tmp_path):
    b = make_var(tmp_path / 'v.blt')

    def body():
        count = 0
        for key, ts in b.timestamps():
            b.set_timestamp(key, ts + 1)
            count += 1
        return count

    assert run_timed(body) == N_KEYS
    b.close()


def test_set_timestamp_accepts_datetime_and_str(tmp_path):
    # Pre-0.12.6, non-int timestamps crashed with AttributeError inside
    # utils.set_timestamp despite the documented signature.
    b = make_var(tmp_path / 'v.blt', n=3)

    dt = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    b.set_timestamp('key0', dt)
    assert b.get_timestamp('key0') == utils.make_timestamp_int(dt)

    iso = '2026-01-02T03:04:05+00:00'
    b.set_timestamp('key1', iso)
    assert b.get_timestamp('key1') == utils.make_timestamp_int(iso)
    b.close()


##############################################
### Fixed-length write-path integrity guards


def test_fixed_set_writes_fixed_framing(tmp_path):
    # Pre-0.12.6 the inherited variable-framing set() corrupted iteration AND
    # point reads on fixed files.
    b = make_fixed(tmp_path / 'f.blt', n=5)
    b.set('via_set', b'x' * FIXED_LEN)
    b.sync()

    assert b['via_set'] == b'x' * FIXED_LEN

    def body():
        return dict(b.items())

    result = run_timed(body)
    assert result['via_set'] == b'x' * FIXED_LEN
    assert len(result) == 6
    b.close()


def test_fixed_set_rejects_timestamp(tmp_path):
    b = make_fixed(tmp_path / 'f.blt', n=2)
    with pytest.raises(ValueError, match='timestamp'):
        b.set('k', b'x' * FIXED_LEN, timestamp=123456)
    b.close()


def test_fixed_set_encode_value_false(tmp_path):
    b = make_fixed(tmp_path / 'f.blt', n=2)
    b.set('raw', b'y' * FIXED_LEN, encode_value=False)
    assert b['raw'] == b'y' * FIXED_LEN
    with pytest.raises(TypeError):
        b.set('notbytes', 'y' * FIXED_LEN, encode_value=False)
    b.close()


@pytest.mark.parametrize('write', [
    lambda b, v: b.__setitem__('k', v),
    lambda b, v: b.set('k', v),
    lambda b, v: b.set('k', v, encode_value=False),
    lambda b, v: b.update({'k': v}),
])
def test_fixed_wrong_value_length_raises(tmp_path, write):
    # Pre-0.12.6 a wrong-length value was accepted and silently corrupted the
    # fixed stride for every later scan.
    b = make_fixed(tmp_path / 'f.blt', n=2)
    with pytest.raises(ValueError, match='bytes'):
        write(b, b'short')
    with pytest.raises(ValueError, match='bytes'):
        write(b, b'z' * (FIXED_LEN + 1))
    # file still healthy
    b.sync()
    assert sum(1 for _k in b.keys()) == 2
    b.close()


##############################################
### map() scan: compaction guard


@pytest.mark.parametrize('make', [make_var, make_fixed], ids=['variable', 'fixed'])
def test_prune_during_map_scan_raises(tmp_path, make):
    # map()'s underlying scan tolerates plain writes (its contract) but a
    # compaction relocating data blocks must fail loudly, not return garbage.
    b = make(tmp_path / 'b.blt')
    # leave something for prune to reclaim so it takes the relocating path
    del b[f'key{N_KEYS - 1}']
    b.sync()

    def body():
        it = b._iter_items_unlocked()
        next(it)
        b.prune()
        with pytest.raises(RuntimeError, match='compacted'):
            next(it)
        return True

    assert run_timed(body)
    b.close()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for the 0.12.9 lock-acquire change: opening a booklet whose OS lock is
held no longer hangs silently.

- default (timeout=None): keep the fair, zero-CPU blocking wait, but log ONE
  warning naming the file if the wait exceeds a grace window (never silent).
- finite timeout: raise booklet.LockTimeoutError instead of blocking forever.
- create path: lock BEFORE truncating, so a flag='n' open no longer destroys a
  file another writer holds.
- reopen()/close(): a failed acquire leaves the object safely closeable.

Same-process flock conflict is real: portalocker uses fcntl.flock on POSIX, so a
second open() of the same path (a distinct open-file-description) genuinely
conflicts. Blocking-class bodies run in a daemon thread with a join timeout, so
pre-fix code FAILS on timeout instead of hanging the suite.
"""
import io
import os
import time
import logging
import threading
import multiprocessing as mp

import pytest
import portalocker

import booklet
from booklet import FixedLengthValue, utils

TIMEOUT = float(os.environ.get('BOOKLET_TEST_TIMEOUT', 15))


def run_timed(fn, timeout=TIMEOUT):
    """Run fn in a daemon thread; fail on timeout (= a pre-fix silent block)."""
    out = {}

    def runner():
        try:
            out['result'] = fn()
        except BaseException as e:  # noqa: BLE001 - re-raised in the test thread
            out['exc'] = e

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), 'blocked: body did not complete within timeout'
    if 'exc' in out:
        raise out['exc']
    return out.get('result')


def _make(path, n=20):
    """Create a small valid variable-length booklet with n keys."""
    with booklet.open(path, 'n', key_serializer='str', value_serializer='orjson') as b:
        for i in range(n):
            b[f'k{i}'] = {'v': i}
    return path


def _booklet_warnings(caplog):
    return [r for r in caplog.records if r.name == 'booklet']


# ---- module-level multiprocessing workers (picklable under 'spawn') ----

def _hold_lock_worker(path, ready_evt, release_evt):
    import booklet
    b = booklet.open(path, 'w')          # acquires LOCK_EX
    ready_evt.set()
    release_evt.wait(15)
    b.close()


def _writer_worker(path, key, result_q):
    import booklet
    try:
        with booklet.open(path, 'w') as b:   # default timeout=None -> blocking wait
            b[key] = {'k': key}
        result_q.put((key, None))
    except Exception as e:                    # noqa: BLE001
        result_q.put((key, repr(e)))


# ==========================================================================
# Finite timeout -> LockTimeoutError (same-process)
# ==========================================================================

def test_finite_timeout_raises_naming_file(tmp_path):
    p = tmp_path / 'v.blt'
    _make(p)
    holder = booklet.open(p, 'w')            # holds LOCK_EX
    try:
        with pytest.raises(booklet.LockTimeoutError) as ei:
            run_timed(lambda: booklet.open(p, 'w', timeout=0.3))
        assert 'v.blt' in str(ei.value)
    finally:
        holder.close()
    # lock released -> a normal open works again
    with booklet.open(p, 'w') as b:
        b['z'] = {'v': -1}


def test_timeout_zero_raises_immediately_without_warning(tmp_path, caplog):
    p = tmp_path / 'v.blt'
    _make(p)
    holder = booklet.open(p, 'w')
    try:
        with caplog.at_level(logging.WARNING, logger='booklet'):
            with pytest.raises(booklet.LockTimeoutError):
                run_timed(lambda: booklet.open(p, 'w', timeout=0))
        assert not _booklet_warnings(caplog), 'no warning expected for timeout<=0'
    finally:
        holder.close()


# ==========================================================================
# Default (timeout=None): warn once, then block, then acquire
# ==========================================================================

def test_default_warns_once_then_blocks_then_acquires(tmp_path, caplog, monkeypatch):
    p = tmp_path / 'v.blt'
    _make(p)
    monkeypatch.setattr(utils, '_LOCK_WARN_AFTER', 0.05)
    holder = booklet.open(p, 'w')

    def release():
        time.sleep(0.3)
        holder.close()

    rel = threading.Thread(target=release, daemon=True)
    rel.start()
    with caplog.at_level(logging.WARNING, logger='booklet'):
        b = run_timed(lambda: booklet.open(p, 'w'))   # default: blocks until release
    try:
        warnings = _booklet_warnings(caplog)
        assert len(warnings) == 1, f'expected exactly one warning, got {len(warnings)}'
        assert 'v.blt' in warnings[0].getMessage()
    finally:
        b.close()
        rel.join(5)


# ==========================================================================
# Shared read locks coexist; fast path is silent
# ==========================================================================

def test_shared_read_locks_coexist(tmp_path, caplog):
    p = tmp_path / 'v.blt'
    _make(p)
    r1 = booklet.open(p, 'r')
    try:
        with caplog.at_level(logging.WARNING, logger='booklet'):
            r2 = run_timed(lambda: booklet.open(p, 'r'))   # LOCK_SH -> no block
        r2.close()
        assert not _booklet_warnings(caplog)
    finally:
        r1.close()


def test_uncontended_open_fast_path_no_warning(tmp_path, caplog):
    p = tmp_path / 'v.blt'
    _make(p)
    with caplog.at_level(logging.WARNING, logger='booklet'):
        b = booklet.open(p, 'w')
        b.close()
    assert not _booklet_warnings(caplog)


# ==========================================================================
# FixedLengthValue mirror
# ==========================================================================

def test_fixed_length_finite_timeout_and_fast_path(tmp_path):
    p = tmp_path / 'f.blt'
    b = FixedLengthValue(p, 'n', key_serializer='str', value_len=8)
    b['a'] = b'12345678'
    b.close()

    holder = FixedLengthValue(p, 'w')        # holds LOCK_EX
    try:
        with pytest.raises(booklet.LockTimeoutError):
            run_timed(lambda: FixedLengthValue(p, 'w', timeout=0.3))
    finally:
        holder.close()

    # uncontended fast path
    f2 = FixedLengthValue(p, 'w')
    f2.close()


# ==========================================================================
# Helper contract (portable; monkeypatched portalocker.lock)
# ==========================================================================

def test_helper_retries_on_alreadylocked(monkeypatch):
    monkeypatch.setattr(utils, '_LOCK_POLL_INTERVAL', 0.001)
    calls = {'n': 0}

    def fake_lock(file, flags):
        calls['n'] += 1
        if calls['n'] < 3:
            raise portalocker.exceptions.AlreadyLocked('busy')

    monkeypatch.setattr(portalocker, 'lock', fake_lock)
    utils._acquire_lock(object(), portalocker.LOCK_EX, 5.0, 'x')
    assert calls['n'] == 3


def test_helper_propagates_other_lockexception_without_closing(monkeypatch):
    def fake_lock(file, flags):
        raise portalocker.exceptions.LockException('bad fd')

    monkeypatch.setattr(portalocker, 'lock', fake_lock)

    class Spy:
        closed = False

        def close(self):
            self.closed = True

    spy = Spy()
    with pytest.raises(portalocker.exceptions.LockException):
        utils._acquire_lock(spy, portalocker.LOCK_EX, 5.0, 'x')
    assert spy.closed is False, 'the helper must not close the file; the caller owns cleanup'


@pytest.mark.skipif(not os.path.isdir('/proc/self/fd'), reason='needs /proc for the fd count')
def test_caller_closes_fd_on_propagated_lock_error(tmp_path, monkeypatch):
    p = tmp_path / 'v.blt'
    _make(p)

    def fake_lock(file, flags):
        raise portalocker.exceptions.LockException('boom')

    monkeypatch.setattr(portalocker, 'lock', fake_lock)
    before = len(os.listdir('/proc/self/fd'))
    with pytest.raises(portalocker.exceptions.LockException):
        booklet.open(p, 'w')
    after = len(os.listdir('/proc/self/fd'))
    assert after <= before, f'fd leaked on failed acquire: {before} -> {after}'


# ==========================================================================
# reopen() recovery: a failed acquire must not brick the object
# ==========================================================================

def test_reopen_timeout_leaves_object_closeable(tmp_path, monkeypatch):
    p = tmp_path / 'v.blt'
    _make(p)
    b = booklet.open(p, 'w', timeout=0.2)

    def boom(file, flags, timeout, path_repr):
        raise booklet.LockTimeoutError('simulated reopen timeout')

    monkeypatch.setattr(utils, '_acquire_lock', boom)
    with pytest.raises(booklet.LockTimeoutError):
        b.reopen('w')
    monkeypatch.undo()
    # Pre-fix: close() -> unlock/sync on a closed file raises ValueError.
    b.close()


# ==========================================================================
# Create-path lock-before-truncate: a flag='n' open must not truncate a
# file another writer holds. (Pre-fix: 'w+b' truncates before the lock.)
# ==========================================================================

def test_create_race_same_process_preserves_held_file(tmp_path):
    p = tmp_path / 'race.blt'
    _make(p, n=20)
    # Hold an exclusive lock via a raw handle (a distinct fd, same process).
    raw = io.open(str(p), 'r+b', buffering=0)
    portalocker.lock(raw, portalocker.LOCK_EX)
    try:
        with pytest.raises(booklet.LockTimeoutError):
            run_timed(lambda: booklet.open(p, 'n', key_serializer='str',
                                           value_serializer='orjson', timeout=0.3))
    finally:
        portalocker.lock(raw, portalocker.LOCK_UN)
        raw.close()
    # Data intact (0.12.8 truncates before the lock -> data destroyed).
    with booklet.open(p, 'r') as b:
        assert len(b) == 20
        assert b['k7'] == {'v': 7}


def test_create_race_multiprocess_preserves_held_file(tmp_path):
    p = tmp_path / 'racemp.blt'
    _make(p, n=20)
    ctx = mp.get_context('spawn')
    ready = ctx.Event()
    release = ctx.Event()
    proc = ctx.Process(target=_hold_lock_worker, args=(str(p), ready, release))
    proc.start()
    try:
        assert ready.wait(15), 'holder process failed to acquire the lock'
        with pytest.raises(booklet.LockTimeoutError):
            booklet.open(p, 'n', key_serializer='str', value_serializer='orjson', timeout=0.5)
    finally:
        release.set()
        proc.join(15)
    with booklet.open(p, 'r') as b:
        assert len(b) == 20
        assert b['k3'] == {'v': 3}


def test_concurrent_writers_default_all_succeed(tmp_path):
    p = tmp_path / 'conc.blt'
    _make(p, n=5)
    ctx = mp.get_context('spawn')
    q = ctx.Queue()
    keys = [f'w{i}' for i in range(4)]
    procs = [ctx.Process(target=_writer_worker, args=(str(p), k, q)) for k in keys]
    for pr in procs:
        pr.start()
    results = [q.get(timeout=30) for _ in keys]
    for pr in procs:
        pr.join(15)
    errs = [(k, e) for k, e in results if e is not None]
    assert not errs, f'concurrent writers errored under the default lock: {errs}'
    with booklet.open(p, 'r') as b:
        for k in keys:
            assert k in b, f'{k} missing after concurrent writes'

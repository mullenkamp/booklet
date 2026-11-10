"""The second reindex of a booklet, and what a reindex that fails part-way leaves behind (0.12.11).

booklet <= 0.12.10: at the second reindex of a FixedLengthValue (its index already relocated), the old index was
covered with ONE deleted block whose 2-byte key length overflowed (OverflowError above 65,575 B, i.e. a relocated
index of >= 10,930 buckets). The reindex had already rewired the chains in place, and close() -> sync() retried it
on the half-rewired file, which looped forever or completed over the damage. Seen in production on an ebooklet
remote-index sidecar crossing 144,013 keys (reviews booklet-reindex-plan-1, -code-1, -code-2; 2026-11-10).

The property every failure scenario checks: whatever goes wrong inside a reindex, the file afterwards either opens
CORRECTLY in both modes, or REFUSES both modes. It never opens and reads wrong.

Each scenario runs in a subprocess with a timeout, so a regression to the old loop FAILS a test instead of hanging
the suite. The shared preludes use only names booklet <= 0.12.10 also has (new ones are looked up with defaults),
so BOOKLET_IMPL_PATH can point the scenarios at another tree, for example a pristine 0.12.10, where they must fail on
behaviour, not on a missing attribute. BOOKLET_PRISTINE_PATH (a 0.12.10 tree) enables the cross-version read test.

Runtime matters (mutation runs and review arms re-run this file many times): each scenario builds its state the
cheap way, and the timeout is set from the measured healthy runtime (every scenario finishes in a few seconds).
"""
import os
import subprocess
import sys
import textwrap

import pytest

from booklet import utils

TIMEOUT = 30

PRELUDE = '''
import os, sys, io, json, pathlib, shutil
impl = os.environ.get('BOOKLET_IMPL_PATH')
if impl:
    sys.path.insert(0, impl)
import booklet
from booklet import utils
from booklet import main as bmain
import portalocker
D = pathlib.Path(sys.argv[1])
FLAG_POS = getattr(utils, 'reindex_flag_pos', 199)
INTERRUPTED = 'reindex of this file was interrupted'

def val(i, n=19):
    return f'{i:0{n}d}'.encode()[-n:]

def check_fixed(path, keys, n=19):
    """Every key readable with its value, len() and keys() exact; read-only and write mode."""
    for flag in ('r', 'w'):
        with booklet.FixedLengthValue(str(path), flag) as db:
            assert len(db) == len(keys), (flag, len(db), len(keys))
            got = list(db.keys())
            assert len(got) == len(keys) and set(got) == set(keys), (flag, len(got), len(set(got)), len(keys))
            for k in keys:
                assert db[k] == val(int(k[1:]), n), (flag, k)

def lock_is_free(path):
    with open(path, 'rb+') as f:
        try:
            portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.exceptions.LockException:
            return False
        portalocker.unlock(f)
        return True

def make(kind, p, n_buckets=12007):
    if kind == 'fixed':
        return booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=n_buckets)
    return booklet.open(str(p), 'n', key_serializer='str', value_serializer='bytes', n_buckets=n_buckets)

def reopen(kind, p, flag):
    return booklet.FixedLengthValue(str(p), flag) if kind == 'fixed' else booklet.open(str(p), flag)

def header(p):
    with open(p, 'rb') as f:
        b = f.read(200)
    return int.from_bytes(b[utils.n_keys_pos:utils.n_keys_pos + 4], 'little'), b[FLAG_POS]

def assert_refused_both_modes(kind, p):
    for flag in ('r', 'w'):
        try:
            reopen(kind, p, flag)
            raise AssertionError(f'a {flag!r} open of the interrupted file succeeded')
        except ValueError as e:
            assert INTERRUPTED in str(e), e
            ## checked while the exception (and its traceback, which keeps the half-built object alive) still
            ## exists: CPython would otherwise close the file at garbage collection and hide a refusal that kept it
            assert lock_is_free(p), f'a refused {flag!r} open left the file locked'

def outcome(kind, p, truth):
    """'correct' (both modes read exactly truth), 'refused' (both modes refuse as interrupted), or what went wrong."""
    seen = []
    for flag in ('r', 'w'):
        try:
            with reopen(kind, p, flag) as r:
                if len(r) != len(truth):
                    return f'WRONG {flag}: len {len(r)} != {len(truth)}'
                bad = [k for k, v in truth.items() if r.get(k) != v]
                if bad:
                    return f'WRONG {flag}: {len(bad)} keys missing or wrong'
                ks = list(r.keys())
                if len(ks) != len(truth) or set(ks) != set(truth):
                    return f'WRONG {flag}: keys() yields {len(ks)}'
                seen.append('correct')
        except ValueError as e:
            if INTERRUPTED not in str(e):
                return f'WRONG {flag}: refused otherwise: {e}'
            seen.append('refused')
        except Exception as e:
            return f'WRONG {flag}: {type(e).__name__}: {e}'
    return seen[0] if seen[0] == seen[1] else f'WRONG: {seen}'

class OpFault:
    """Wraps a booklet's file. Once armed it counts file operations, and at operation number ``at`` it raises
    (``error``: OSError, ``interrupt``: KeyboardInterrupt) or kills the process (``kill``)."""
    def __init__(self, f, at, fault):
        self._f, self.at, self.fault, self.n, self.armed, self.fired = f, at, fault, 0, False, False
    def _op(self, name):
        if self.armed and not self.fired:
            self.n += 1
            if self.n == self.at:
                self.fired = True
                if self.fault == 'kill':
                    os._exit(9)
                if self.fault == 'interrupt':
                    raise KeyboardInterrupt(f'injected before op {self.n} ({name})')
                raise OSError(5, f'injected before op {self.n} ({name})')
    def seek(self, *a):
        self._op('seek'); return self._f.seek(*a)
    def read(self, *a):
        self._op('read'); return self._f.read(*a)
    def write(self, b):
        self._op('write'); return self._f.write(b)
    def flush(self):
        self._op('flush'); return self._f.flush()
    def truncate(self, *a):
        self._op('truncate'); return self._f.truncate(*a)
    def __getattr__(self, name):
        return getattr(self._f, name)

def faulted_sync(db, at, fault):
    """Sync with an OpFault armed for the whole reindex, including the caller's work after reindex() returns."""
    proxy = db._file = OpFault(db._file, at, fault)
    real = utils.reindex
    def armed(*a, **k):
        proxy.armed = True
        return real(*a, **k)
    utils.reindex = armed
    try:
        try:
            db.sync()
        except (OSError, KeyboardInterrupt):
            pass
    finally:
        utils.reindex = real
        proxy.armed = False
    return proxy

def relocated_base(kind, p):
    """40 keys in a 60-bucket file whose index prune() relocated; 40 more then trigger one small reindex."""
    with make(kind, p, n_buckets=60) as db:
        for i in range(40):
            db[f'k{i}'] = val(i)
    with reopen(kind, p, 'w') as db:
        db.prune()
    return {f'k{i}': val(i) for i in range(80)}
'''


def run(tmp_path, body, timeout=TIMEOUT):
    """Run ``body`` (after PRELUDE) in a fresh interpreter; it must print OK. Returns the output."""
    code = PRELUDE + textwrap.dedent(body) + '\nprint("OK")\n'
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
    try:
        r = subprocess.run([sys.executable, '-c', code, str(tmp_path)], capture_output=True, text=True,
                           timeout=timeout, env=env)
    except subprocess.TimeoutExpired:
        pytest.fail(f'scenario did not finish within {timeout} s (a hang)')
    out = r.stdout + r.stderr
    assert r.returncode == 0 and out.rstrip().endswith('OK'), out[-3000:]
    return out


## 1. fails on 0.12.10 (OverflowError, then a hang in close): two reindexes, the second over an index the first
## relocated (10 -> 12,007, then 12,007 -> 144,013 with a 72,042 B old index to tile). Syncing after 11 keys keeps
## the first reindex's chains short.
def test_second_reindex_fixed(tmp_path):
    run(tmp_path, '''
    p = D / 'a.blt'
    keys = [f'k{i}' for i in range(12008)]
    with booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=10) as db:
        for k in keys[:11]:
            db[k] = val(int(k[1:]))
        db.sync()
        assert db._n_buckets == 12007 and db._index_offset != utils.sub_index_init_pos
        for k in keys[11:]:
            db[k] = val(int(k[1:]))
    with booklet.FixedLengthValue(str(p), 'r') as db:
        assert db._n_buckets == 144013, db._n_buckets
    check_fixed(p, keys)
    with booklet.FixedLengthValue(str(p), 'w') as db:
        db.prune()
        for i in range(12008, 12108):
            db[f'k{i}'] = val(i)
    check_fixed(p, keys + [f'k{i}' for i in range(12008, 12108)])
    ''')


## 2. fails on 0.12.10: the ebooklet staged-copy pattern (prune relocates the index, then the next push grows it)
def test_prune_then_grow_fixed(tmp_path):
    run(tmp_path, '''
    p = D / 'a.blt'
    keys = [f'k{i}' for i in range(5000)]
    with booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=12007) as db:
        for k in keys:
            db[k] = val(int(k[1:]))
    with booklet.FixedLengthValue(str(p), 'w') as db:
        db.prune()
        assert db._index_offset != utils.sub_index_init_pos
        more = [f'k{i}' for i in range(5000, 13000)]
        for k in more:
            db[k] = val(int(k[1:]))
        db.prune()
    check_fixed(p, keys + more)
    with booklet.FixedLengthValue(str(p), 'r') as db:
        assert db._n_buckets == 144013
    ''')


## 3. off-chain bucket counts whose old index leaves a short tail (5 B) or exactly one minimum block (41 B)
@pytest.mark.parametrize('n_buckets', [10930, 10936])
def test_off_chain_relocated_grow(tmp_path, n_buckets):
    run(tmp_path, f'''
    p = D / 'a.blt'
    n = {n_buckets}
    keys = [f'k{{i}}' for i in range(n // 2)]
    with booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=n) as db:
        for k in keys:
            db[k] = val(int(k[1:]))
    with booklet.FixedLengthValue(str(p), 'w') as db:
        db.prune()
        more = [f'k{{i}}' for i in range(n // 2, n + 50)]
        for k in more:
            db[k] = val(int(k[1:]))
    with booklet.FixedLengthValue(str(p), 'r') as db:
        assert db._n_buckets == 12007, db._n_buckets
    check_fixed(p, keys + more)
    ''')


## 4. the tiling, read back by booklet's own block readers (file and mmap). The region is prefilled with
## bucket-pointer bytes, because zeros parse as a valid empty block and would hide a gap; a live sentinel block
## follows it, and the readers must yield exactly that block.
def _live_fixed_block(key, value):
    return (b'\x07' * utils.key_hash_len + utils.int_to_bytes(1, utils.n_bytes_file)
            + utils.int_to_bytes(len(key), utils.n_bytes_key) + key + value)


@pytest.mark.parametrize('value_len', [15, 19])
@pytest.mark.parametrize('extra', ['min', 'min+1', 65575, 65576, 65615, 65575 + 40, 2 * 65575 - 1, 72042, 864078])
def test_tiling_read_back(value_len, extra):
    import io
    import mmap
    min_block = utils.key_hash_len + utils.n_bytes_file + utils.n_bytes_key + 1 + value_len
    dead = {'min': min_block, 'min+1': min_block + 1}.get(extra, extra)
    sizes = utils.skip_block_sizes_fixed(dead, value_len)
    assert sizes is not None and sum(sizes) == dead
    sentinel = _live_fixed_block(b'sentinel', b'v' * value_len)
    buf = io.BytesIO((b'\x01\x00\x00\x00\x00\x00' * (dead // 6 + 1))[:dead] + sentinel)
    utils.write_skip_block_fixed(buf, 0, dead, value_len)
    end = dead + len(sentinel)
    got = list(utils._iter_keys_values_fixed_region(buf, 0, end, True, True, value_len))
    assert got == [(b'sentinel', b'v' * value_len)]
    mm = mmap.mmap(-1, end)
    mm[:] = buf.getvalue()
    got_mm = [tuple(bytes(x) for x in kv) for kv in
              utils._mmap_iter_keys_values_fixed_region(mm, 0, end, True, True, value_len)]
    assert got_mm == [(b'sentinel', b'v' * value_len)]


## 5. the tiling plan at every boundary that matters: exact cover, every block a valid size, None only below a
## minimum block. Sizes: everything up to four minimum blocks, a window of +-2 minimum blocks around each of the
## first 200 multiples of the maximum block, and the old-index sizes of real bucket counts (n * 6).
@pytest.mark.parametrize('value_len', [15, 19])
def test_skip_block_sizes_property(value_len):
    overhead = utils.key_hash_len + utils.n_bytes_file + utils.n_bytes_key
    min_block, max_block = overhead + 1 + value_len, overhead + 65535 + value_len
    sizes = set(range(0, 4 * min_block))
    for k in range(1, 201):
        sizes.update(range(k * max_block - 2 * min_block, k * max_block + 2 * min_block + 1))
    for n in (list(range(1, 2001)) + list(range(10900, 10960)) + [12007, 144013, 1728017, 20736017]):
        sizes.add(n * utils.n_bytes_file)
    for dead in sorted(sizes):
        got = utils.skip_block_sizes_fixed(dead, value_len)
        if dead < min_block:
            assert got is None, dead
            continue
        assert got is not None, dead
        assert sum(got) == dead, dead
        assert all(min_block <= b <= max_block for b in got), (dead, got)


## 6. fails on 0.12.10 (silently corrupt): a relocated index too small to cover with deleted blocks. The reindex is
## skipped (it is only an optimisation) and the file stays correct.
@pytest.mark.parametrize('n_buckets', [1, 2, 3, 4, 5, 6])
def test_untileable_fixed_skips_reindex(tmp_path, n_buckets):
    run(tmp_path, f'''
    p = D / 'a.blt'
    n = {n_buckets}
    keys = [f'k{{i}}' for i in range(n)]
    with booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=n) as db:
        for k in keys:
            db[k] = val(int(k[1:]))
    with booklet.FixedLengthValue(str(p), 'w') as db:
        db.prune()
        more = [f'k{{i}}' for i in range(n, n + 40)]
        for k in more:
            db[k] = val(int(k[1:]))
        db.sync()
        assert getattr(db, '_reindex_failed', None) is None  # getattr: also runs against booklet <= 0.12.10
    check_fixed(p, keys + more)
    ''')


@pytest.mark.parametrize('n_buckets', [1, 2, 3, 4, 5])
def test_untileable_variable_skips_reindex(tmp_path, n_buckets):
    run(tmp_path, f'''
    p = D / 'a.blt'
    n = {n_buckets}
    keys = [f'k{{i}}' for i in range(n)]
    with booklet.open(str(p), 'n', key_serializer='str', value_serializer='str', n_buckets=n) as db:
        for k in keys:
            db[k] = 'v' + k
    with booklet.open(str(p), 'w') as db:
        db.prune()
        more = [f'k{{i}}' for i in range(n, n + 40)]
        for k in more:
            db[k] = 'v' + k
    for flag in ('r', 'w'):
        with booklet.open(str(p), flag) as db:
            assert len(db) == len(keys + more)
            assert sorted(db.keys()) == sorted(keys + more)
            assert all(db[k] == 'v' + k for k in keys + more)
    ''')


## 7. fails on 0.12.10: a failure after the chains were rewired (step 3, and a KeyboardInterrupt mid-walk), for both
## classes. The error propagates, the reindex is not retried, EVERY public method refuses, a method bound earlier
## fails on the closed file, the instance is already finalized (marker + flag on disk, lock free), and the file then
## refuses to open in BOTH modes (a write-mode open used to recount over the damage and adopt it).
INJECT = """
calls = {'reindex': 0}
real_reindex = utils.reindex
def counting_reindex(*a, **k):
    calls['reindex'] += 1
    return real_reindex(*a, **k)
utils.reindex = counting_reindex
CALLS = {
    'set_metadata': lambda db: db.set_metadata({'a': 1}), 'get_metadata': lambda db: db.get_metadata(),
    'set_reserved': lambda db: db.set_reserved(1, b'x'), 'get_reserved': lambda db: db.get_reserved(1),
    'keys': lambda db: db.keys(), 'items': lambda db: db.items(), 'values': lambda db: db.values(),
    'timestamps': lambda db: db.timestamps(), 'locations': lambda db: db.locations(),
    '__iter__': lambda db: db.__iter__(), '__len__': lambda db: db.__len__(),
    '__contains__': lambda db: db.__contains__('k1'), 'get': lambda db: db.get('k1'),
    'get_items': lambda db: db.get_items(['k1']), 'get_timestamp': lambda db: db.get_timestamp('k1'),
    'set_timestamp': lambda db: db.set_timestamp('k1', 1), 'set': lambda db: db.set('k1', val(1)),
    'update': lambda db: db.update({'k1': val(1)}), 'prune': lambda db: db.prune(),
    '__getitem__': lambda db: db.__getitem__('k1'), '__setitem__': lambda db: db.__setitem__('k1', val(1)),
    '__delitem__': lambda db: db.__delitem__('k1'), 'clear': lambda db: db.clear(),
    'reopen': lambda db: db.reopen('r'), 'sync': lambda db: db.sync(), 'map': lambda db: db.map(len),
}
"""

FAIL_BODY = """
## every public method is guarded: the list covers every public method the classes define (a new one must be added)
G = getattr(bmain, '_GUARDED_METHODS', None)
if G is not None:
    public = set()
    for cls in (bmain.Booklet, bmain.VariableLengthValue, bmain.FixedLengthValue):
        for n, v in vars(cls).items():
            if callable(v) and (not n.startswith('_') or n in ('__iter__', '__len__', '__contains__', '__getitem__',
                                                                '__setitem__', '__delitem__')):
                public.add(n)
    missing = public - set(G) - {'close'}
    assert not missing, f'public methods not refused after a failed reindex: {sorted(missing)}'
    assert set(CALLS) == set(G), set(CALLS) ^ set(G)
p = D / 'a.blt'
with make(kind, p) as db:
    for i in range(5000):
        db[f'k{i}'] = val(i)
if where == 'step3':
    def boom(*a, **k):
        raise OSError(5, 'injected after the rewire')
    if kind == 'fixed':
        utils.write_skip_block_fixed = boom
    else:
        utils.write_skip_block_variable = boom
    expected = OSError
else:
    real_int_to_bytes = utils.int_to_bytes
    state = {'n': 0}
    def int_to_bytes(i, n, signed=False):
        if calls['reindex']:
            state['n'] += 1
            if state['n'] > 2000:
                raise KeyboardInterrupt('injected mid-walk')
        return real_int_to_bytes(i, n, signed)
    utils.int_to_bytes = int_to_bytes
    expected = KeyboardInterrupt
db = reopen(kind, p, 'w')
db.prune()
for i in range(5000, 13000):
    db[f'k{i}'] = val(i)
bound_get = db.get  # taken before the failure
try:
    db.sync()
    raise AssertionError('sync did not raise')
except expected:
    pass
if where == 'midwalk':
    utils.int_to_bytes = real_int_to_bytes
assert calls['reindex'] == 1, calls
assert db._file.closed, 'the failed instance must be finalized at once'
assert lock_is_free(p)
assert header(p) == (utils.n_keys_crash, 1), header(p)
for name, call in CALLS.items():
    try:
        call(db)
        raise AssertionError(f'{name} did not refuse')
    except RuntimeError as e:
        assert 'reindex failed' in str(e), (name, e)
try:
    bound_get('k1')
    raise AssertionError('a method bound before the failure still answered')
except ValueError:
    pass  # I/O on the closed file
db.close()
db.close()
assert calls['reindex'] == 1, calls
assert header(p) == (utils.n_keys_crash, 1), header(p)
assert_refused_both_modes(kind, p)
"""


@pytest.mark.parametrize('kind', ['fixed', 'variable'])
@pytest.mark.parametrize('where', ['step3', 'midwalk'])
def test_failed_reindex_is_loud_and_final(tmp_path, where, kind):
    run(tmp_path, INJECT + f'where, kind = {where!r}, {kind!r}\n' + FAIL_BODY)


## 8. fails on 0.12.10 (opens and reads wrong): a process killed mid-walk leaves a file refused in both modes
KILL_CHILD = """
real_int_to_bytes = utils.int_to_bytes
state = {'n': 0}
def int_to_bytes(i, n, signed=False):
    if calls['reindex']:
        state['n'] += 1
        if state['n'] > 2000:
            os._exit(9)
    return real_int_to_bytes(i, n, signed)
utils.int_to_bytes = int_to_bytes
db = reopen(kind, D / 'a.blt', 'w')
db.prune()
for i in range(5000, 13000):
    db[f'k{i}'] = val(i)
db.sync()
"""


@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_killed_mid_reindex_is_refused(tmp_path, kind):
    child = PRELUDE + INJECT + f'kind = {kind!r}\n' + KILL_CHILD
    run(tmp_path, INJECT + f'kind = {kind!r}\nCHILD = {child!r}\n' + """
import subprocess
p = D / 'a.blt'
with make(kind, p) as db:
    for i in range(5000):
        db[f'k{i}'] = val(i)
r = subprocess.run([sys.executable, '-c', CHILD, str(D)], timeout=20)
assert r.returncode == 9, r.returncode
assert header(p)[1] == 1, header(p)
assert_refused_both_modes(kind, p)
""")


## 9. a half-rewired file with NO marker and NO flag (as booklet <= 0.12.10 leaves it), reopened and given one more
## key: the next reindex walks the rewired chains. It must end (raise or finish) instead of looping forever.
def test_half_rewired_file_without_marker_does_not_hang(tmp_path):
    run(tmp_path, """
p = D / 'a.blt'
with booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=12007) as db:
    for i in range(5000):
        db[f'k{i}'] = val(i)
real = utils.write_skip_block_fixed
def boom(*a, **k):
    raise OSError(5, 'injected after the rewire')
utils.write_skip_block_fixed = boom
db = booklet.FixedLengthValue(str(p), 'w')
db.prune()
for i in range(5000, 13000):
    db[f'k{i}'] = val(i)
try:
    db.sync()
except OSError:
    pass
db.close()
utils.write_skip_block_fixed = real
with open(p, 'r+b') as f:  # remove the marker and the flag, as 0.12.10 would have left the header
    f.seek(utils.n_keys_pos)
    f.write(utils.int_to_bytes(13000, 4))
    f.seek(FLAG_POS)
    f.write(bytes(1))
db = booklet.FixedLengthValue(str(p), 'w')
db['k13000'] = val(13000)
try:
    db.sync()
except RuntimeError as e:
    print('bounded:', e)
try:
    db.close()
except Exception:
    pass
""")


## 10. a transient failure of close()'s sync that writes nothing keeps the instance open with its buffered writes,
## so a retried close() persists them (as 0.12.10 did). Test 17 covers the harder case, a SHORT write.
def test_close_retry_after_transient_sync_failure_keeps_writes(tmp_path):
    run(tmp_path, """
p = D / 'a.blt'
db = booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=12007)
for i in range(100):
    db[f'k{i}'] = val(i)
real = utils.flush_data_buffer
def boom(*a, **k):
    raise OSError(28, 'injected ENOSPC')
utils.flush_data_buffer = boom
try:
    db.close()
    raise AssertionError('close did not raise')
except OSError:
    pass
assert not db._file.closed
utils.flush_data_buffer = real
db.close()
check_fixed(p, [f'k{i}' for i in range(100)])
""")


## 10b. the finalizer releases the lock and closes the file even when its marker write raises
def test_finalizer_releases_when_marker_write_raises(tmp_path):
    run(tmp_path, """
p = D / 'a.blt'
db = booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=12007)
db['k1'] = val(1)
real = utils.int_to_bytes
def boom(*a, **k):
    raise OSError(5, 'injected EIO on the marker write')
utils.int_to_bytes = boom
try:
    db._finalizer()
    raise AssertionError('the finalizer did not raise')
except OSError:
    pass
utils.int_to_bytes = real
assert db._file.closed
assert lock_is_free(p)
""")


## 11. the step bound must not fire on a healthy file whose chains hold blocks n_keys does not count
def test_bound_does_not_fire_on_metadata_and_reserved_blocks(tmp_path):
    run(tmp_path, '''
    p = D / 'a.blt'
    with booklet.open(str(p), 'n', key_serializer='str', value_serializer='str', n_buckets=1) as db:
        db.set_metadata({'a': 1})
        db.set_reserved(1, b'r1')
        db.set_reserved(2, b'r2')
        for i in range(3):
            db[f'k{i}'] = 'v'
    with booklet.open(str(p), 'w') as db:
        for i in range(3, 13000):
            db[f'k{i}'] = 'v'
    with booklet.open(str(p), 'r') as db:
        assert len(db) == 13000 and db.get_metadata() == {'a': 1} and db.get_reserved(2) == b'r2'
        assert all(db[f'k{i}'] == 'v' for i in range(13000))
    ''')


## 12. files written by this version (a live file holding tiles, and its pruned published layout) read correctly
## with a pristine 0.12.10, which consumers of published indexes keep using for a while
@pytest.mark.skipif(not os.environ.get('BOOKLET_PRISTINE_PATH'), reason='set BOOKLET_PRISTINE_PATH to a 0.12.10 tree')
def test_written_files_read_by_0_12_10(tmp_path):
    run(tmp_path, '''
    live = D / 'live.blt'
    keys = [f'k{i}' for i in range(12008)]
    with booklet.FixedLengthValue(str(live), 'n', key_serializer='str', value_len=19, n_buckets=10) as db:
        for k in keys[:11]:
            db[k] = val(int(k[1:]))
        db.sync()
        for k in keys[11:]:
            db[k] = val(int(k[1:]))
    published = D / 'published.blt'
    published.write_bytes(live.read_bytes())
    with booklet.FixedLengthValue(str(published), 'w') as db:
        db.prune()
    json.dump(keys, open(D / 'keys.json', 'w'))
    ''')
    env = dict(os.environ, BOOKLET_IMPL_PATH=os.environ['BOOKLET_PRISTINE_PATH'])
    reader = PRELUDE + textwrap.dedent('''
    assert booklet.__version__ == '0.12.10', booklet.__version__
    keys = json.load(open(D / 'keys.json'))
    for name in ('live.blt', 'published.blt'):
        check_fixed(D / name, keys)
    print('OK')
    ''')
    r = subprocess.run([sys.executable, '-c', reader, str(tmp_path)], capture_output=True, text=True,
                       timeout=TIMEOUT, env=env)
    assert r.returncode == 0 and r.stdout.rstrip().endswith('OK'), (r.stdout + r.stderr)[-3000:]


## 13. a failure while allocating the new index (a real EFBIG from RLIMIT_FSIZE, which arrives as a short write)
## damages nothing: the file is truncated back, the instance stays usable, and a retried close() completes
@pytest.mark.skipif(sys.platform == 'win32', reason='RLIMIT_FSIZE is POSIX')
def test_allocation_failure_leaves_file_intact_and_instance_usable(tmp_path):
    run(tmp_path, """
import resource, signal
signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
p = D / 'a.blt'
truth = {f'k{i}': f'value-{i}' for i in range(12010)}
with booklet.open(str(p), 'n', key_serializer='str', value_serializer='str') as db:
    for i in range(12000):
        db[f'k{i}'] = truth[f'k{i}']
size0 = p.stat().st_size
db = booklet.open(str(p), 'w')
for i in range(12000, 12010):
    db[f'k{i}'] = truth[f'k{i}']
soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
resource.setrlimit(resource.RLIMIT_FSIZE, (size0 + 100_000, hard))  # room for the blocks, not the new index
try:
    db.close()
    raise AssertionError('close did not raise')
except OSError as e:
    assert e.errno in (27, 28), e  # EFBIG from the write, or ENOSPC from booklet's short-write check
finally:
    resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
assert getattr(db, '_reindex_failed', None) is None and not db._file.closed
assert header(p)[1] == 0, header(p)
db.close()  # the retry: the reindex now completes
for flag in ('r', 'w'):
    with booklet.open(str(p), flag) as db:
        assert len(db) == 12010 and dict(db.items()) == truth
        assert db._n_buckets == 144013
""")


## 13b. a KeyboardInterrupt during the new index's allocation is rolled back like any other failure there (the
## fault sweep accepts "refused" as safe, so it cannot tell a rollback from a needless refusal): the file stays
## intact and the instance usable
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_interrupt_during_allocation_is_rolled_back(tmp_path, kind):
    run(tmp_path, f'kind = {kind!r}\n' + """
p = D / 'a.blt'
truth = relocated_base(kind, p)
db = reopen(kind, p, 'w')
for i in range(40, 80):
    db[f'k{i}'] = truth[f'k{i}']
real = utils.write_init_bucket_indexes
def interrupted(*a, **k):
    raise KeyboardInterrupt('injected during the allocation')
utils.write_init_bucket_indexes = interrupted
try:
    db.sync()
    raise AssertionError('sync did not raise')
except KeyboardInterrupt:
    pass
utils.write_init_bucket_indexes = real
assert not db._file.closed and getattr(db, '_reindex_failed', None) is None
assert header(p)[1] == 0, header(p)
db.close()  # the retry completes the reindex
assert outcome(kind, p, truth) == 'correct'
""")


## 14. a fault before EVERY file operation of a reindex, as an exception and as a KeyboardInterrupt, for both
## classes: the file then opens correctly in both modes (the instance was undamaged, so a retried close completed)
## or refuses both. Never opens and reads wrong. Keyed on file operations, so no window between them is skipped.
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_fault_before_every_file_operation(tmp_path, kind):
    out = run(tmp_path, f'kind = {kind!r}\n' + """
base = D / 'base.blt'
truth = relocated_base(kind, base)
def scenario(at, fault):
    p = D / f'{fault}-{at}.blt'
    shutil.copy(base, p)
    db = reopen(kind, p, 'w')
    for i in range(40, 80):
        db[f'k{i}'] = truth[f'k{i}']
    proxy = faulted_sync(db, at, fault)
    db.close()  # an undamaged instance completes the reindex; a failed one is already closed
    result = outcome(kind, p, truth)
    p.unlink()
    return proxy, result
dry, result = scenario(10 ** 9, 'error')
assert result == 'correct', result
n_ops = dry.n
assert n_ops > 20, n_ops
bad, seen = [], {}
for fault in ('error', 'interrupt'):
    for at in range(1, n_ops + 1):
        proxy, result = scenario(at, fault)
        assert proxy.fired, (fault, at)
        seen[result] = seen.get(result, 0) + 1
        if result not in ('correct', 'refused'):
            bad.append((fault, at, result))
assert not bad, bad[:10]
assert seen.get('refused') and seen.get('correct'), seen  # both outcomes occur: the sweep spans the flag
print('ops', n_ops, seen)
""", timeout=60)
    print(out)


## 14b. the same with a real process kill, at sampled operations (each needs its own process)
KILL_SWEEP_CHILD = """
at = int(sys.argv[2])
p = D / 'killed.blt'
db = reopen(kind, p, 'w')
for i in range(40, 80):
    db[f'k{i}'] = val(i)
faulted_sync(db, at, 'kill')
db.close()
"""


@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_kill_at_sampled_file_operations(tmp_path, kind):
    child = PRELUDE + f'kind = {kind!r}\n' + KILL_SWEEP_CHILD
    run(tmp_path, f'kind = {kind!r}\nCHILD = {child!r}\n' + """
import subprocess
base = D / 'base.blt'
truth = relocated_base(kind, base)
p = D / 'killed.blt'
shutil.copy(base, p)
db = reopen(kind, p, 'w')
for i in range(40, 80):
    db[f'k{i}'] = truth[f'k{i}']
n_ops = faulted_sync(db, 10 ** 9, 'error').n
db.close()
bad = []
for at in sorted({1, 2, 3, 4, 5, 6, n_ops // 4, n_ops // 2, 3 * n_ops // 4, n_ops - 3, n_ops - 2, n_ops - 1, n_ops}):
    shutil.copy(base, p)
    r = subprocess.run([sys.executable, '-c', CHILD, str(D), str(at)], timeout=20)
    result = outcome(kind, p, truth) if r.returncode == 9 else f'child exit {r.returncode}'
    if result not in ('correct', 'refused'):
        bad.append((at, result))
assert not bad, bad
""")


## 15. a successful reindex writes the live key count, so a file that carried the crash marker (an earlier unclean
## exit) is clean afterwards
def test_marker_cleared_by_recount_and_by_successful_reindex(tmp_path):
    child = PRELUDE + """
db = booklet.FixedLengthValue(str(D / 'a.blt'), 'n', key_serializer='str', value_len=19, n_buckets=12007, buffer_size=4096)
for i in range(13000):
    db[f'k{i}'] = val(i)
# no close(): the finalizer writes the crash marker at interpreter exit, as for any unclean exit
"""
    run(tmp_path, f'CHILD = {child!r}\n' + """
import subprocess
p = D / 'a.blt'
r = subprocess.run([sys.executable, '-c', CHILD, str(D)], timeout=20)
assert r.returncode == 0
assert header(p)[0] == utils.n_keys_crash
db = booklet.FixedLengthValue(str(p), 'w')
n = len(db)
db.close()
with booklet.FixedLengthValue(str(p), 'r') as db:
    assert len(db) == n
    assert all(db[k] == val(int(k[1:])) for k in db.keys())
""")


## 15b. the recount itself is persisted, for both classes: an unclean exit with too few keys for any reindex, then a
## write-mode open and close with no writes, must leave a file whose read-only open succeeds
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_write_mode_recount_is_persisted_without_writes(tmp_path, kind):
    child = PRELUDE + f'kind = {kind!r}\n' + """
db = make(kind, D / 'a.blt')
for i in range(100):
    db[f'k{i}'] = val(i)
db.sync()  # the blocks on disk, so a recount can find them
# no close(): the finalizer writes the crash marker at interpreter exit
"""
    run(tmp_path, f'kind = {kind!r}\nCHILD = {child!r}\n' + """
import subprocess
p = D / 'a.blt'
assert subprocess.run([sys.executable, '-c', CHILD, str(D)], timeout=20).returncode == 0
assert header(p)[0] == utils.n_keys_crash
db = reopen(kind, p, 'w')
n = len(db)
db.close()
with reopen(kind, p, 'r') as db:
    assert len(db) == n and n > 0, n
""")


## 16. a skipped (untileable) reindex is a no-op: a sync must not kill a live iterator
def test_skipped_reindex_does_not_kill_iterators(tmp_path):
    run(tmp_path, """
p = D / 'a.blt'
with booklet.FixedLengthValue(str(p), 'n', key_serializer='str', value_len=19, n_buckets=2) as db:
    db['k0'] = val(0)
with booklet.FixedLengthValue(str(p), 'w') as db:
    db.prune()
    for i in range(1, 60):
        db[f'k{i}'] = val(i)
    db.sync()
    n = 0
    for k in db.keys():
        db.sync()
        n += 1
    assert n == 60, n
""")


## 17. fails on 0.12.10: a SHORT write while flushing buffered data (a disk filling up: RLIMIT_FSIZE gives short
## writes, then EFBIG). close() raises, the instance stays open, and once space is freed the retried close() persists
## every key. 0.12.10 dropped the unwritten bytes and cleared the buffer, then read wrong or hung.
@pytest.mark.skipif(sys.platform == 'win32', reason='RLIMIT_FSIZE is POSIX')
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_short_write_in_data_flush_then_retry_keeps_every_key(tmp_path, kind):
    run(tmp_path, f'kind = {kind!r}\n' + """
import resource, signal
signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
base = D / 'base.blt'
truth = {f'k{i}': val(i) for i in range(400)}
with make(kind, base) as db:
    for i in range(200):
        db[f'k{i}'] = truth[f'k{i}']
size0 = base.stat().st_size
probe = D / 'probe.blt'
shutil.copy(base, probe)
with reopen(kind, probe, 'w') as db:
    for i in range(200, 400):
        db[f'k{i}'] = truth[f'k{i}']
data = probe.stat().st_size - size0
for limit in (1, 500, data // 2, data - 1, data):
    p = D / f'limit-{limit}.blt'
    shutil.copy(base, p)
    db = reopen(kind, p, 'w')
    for i in range(200, 400):
        db[f'k{i}'] = truth[f'k{i}']
    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    resource.setrlimit(resource.RLIMIT_FSIZE, (size0 + limit, hard))
    try:
        db.close()
    except OSError:
        pass
    finally:
        resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
    if not db._file.closed:
        db.close()  # the retry, after space was freed
    result = outcome(kind, p, truth)
    assert result == 'correct', (limit, data, result)
""")


## 18. an interrupt right after reindex() returned, before the instance adopted the new layout: the file is still
## flagged, so the instance is finalized and the file refuses both modes (it must never keep running on the stale
## layout over a reindexed file, where the next close would reindex again from the old index and write a clean
## header over damage)
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_interrupt_after_reindex_returns_is_refused(tmp_path, kind):
    run(tmp_path, f'kind = {kind!r}\n' + """
p = D / 'a.blt'
truth = relocated_base(kind, p)
db = reopen(kind, p, 'w')
for i in range(40, 80):
    db[f'k{i}'] = truth[f'k{i}']
real = utils.reindex
def interrupted(*a, **k):
    real(*a, **k)
    raise KeyboardInterrupt('right after reindex() returned')
utils.reindex = interrupted
try:
    db.sync()
    raise AssertionError('sync did not raise')
except KeyboardInterrupt:
    pass
utils.reindex = real
assert db._file.closed, 'the instance must not keep running on the old layout'
db.close()
assert_refused_both_modes(kind, p)
""")


## 19. the new index's allocation fails AND the rollback truncate fails: the flag stays up, the instance is
## finalized, and the file refuses both modes (the partial index left at EOF would otherwise be read as blocks)
@pytest.mark.skipif(sys.platform == 'win32', reason='RLIMIT_FSIZE is POSIX')
def test_allocation_and_rollback_both_fail_is_refused(tmp_path):
    run(tmp_path, """
import resource, signal
signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
p = D / 'a.blt'
with booklet.open(str(p), 'n', key_serializer='str', value_serializer='str') as db:
    for i in range(12000):
        db[f'k{i}'] = f'value-{i}'
size0 = p.stat().st_size
db = booklet.open(str(p), 'w')
for i in range(12000, 12010):
    db[f'k{i}'] = f'value-{i}'
class NoTruncate:
    def __init__(self, f):
        self._f = f
    def truncate(self, *a):
        raise OSError(5, 'injected: truncate failed')
    def __getattr__(self, n):
        return getattr(self._f, n)
db._file = NoTruncate(db._file)
soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
resource.setrlimit(resource.RLIMIT_FSIZE, (size0 + 100_000, hard))
try:
    db.close()
    raise AssertionError('close did not raise')
except OSError:
    pass
finally:
    resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
assert db._file.closed
assert_refused_both_modes('variable', p)
""")


## 20. a file created from init_bytes never inherits an interrupted-reindex flag (ebooklet builds local files and
## sidecars from a remote's header bytes)
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_init_bytes_never_inherit_the_flag(tmp_path, kind):
    run(tmp_path, f'kind = {kind!r}\n' + """
src = D / 'src.blt'
with make(kind, src) as db:
    db['k1'] = val(1)
hdr = bytearray(src.read_bytes()[:200])
hdr[FLAG_POS] = 1
new = D / 'new.blt'
if kind == 'fixed':
    db = booklet.FixedLengthValue(str(new), 'n', init_bytes=bytes(hdr))
else:
    db = booklet.open(str(new), 'n', init_bytes=bytes(hdr))
db['k2'] = val(2)
db.close()
assert header(new)[1] == 0, header(new)
with reopen(kind, new, 'r') as r:
    assert r['k2'] == val(2)
""")


## 21. a legacy header (files written before 2026-02 pad byte 199 with ASCII '0' = 0x30): it opens, and a reindex
## puts the byte back as it was rather than writing 0x00 over it
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_legacy_padding_byte_is_kept(tmp_path, kind):
    run(tmp_path, f'kind = {kind!r}\n' + """
p = D / 'a.blt'
with make(kind, p, n_buckets=10) as db:
    for i in range(5):
        db[f'k{i}'] = val(i)
with open(p, 'r+b') as f:
    f.seek(FLAG_POS)
    f.write(b'0')
with reopen(kind, p, 'w') as db:
    for i in range(5, 40):
        db[f'k{i}'] = val(i)
assert header(p)[1] == 0x30, header(p)
truth = {f'k{i}': val(i) for i in range(40)}
assert outcome(kind, p, truth) == 'correct'
""")


## 22. the ordinary "closed incorrectly" refusal of a read-only open releases the file at once, for both classes
@pytest.mark.parametrize('kind', ['fixed', 'variable'])
def test_marker_refusal_releases_the_lock(tmp_path, kind):
    run(tmp_path, f'kind = {kind!r}\n' + """
p = D / 'a.blt'
with make(kind, p) as db:
    db['k1'] = val(1)
with open(p, 'r+b') as f:
    f.seek(utils.n_keys_pos)
    f.write(utils.int_to_bytes(utils.n_keys_crash, 4))
try:
    reopen(kind, p, 'r')
    raise AssertionError('a marked file opened read-only')
except ValueError as e:
    assert 'closed incorrectly' in str(e), e
    assert lock_is_free(p), 'the refused open left the file locked'
""")


## 23. if the flag cannot even be read after a failure, the instance is treated as damaged (finalized), not as fine
def test_unreadable_flag_after_a_failure_means_damaged(tmp_path):
    run(tmp_path, """
p = D / 'a.blt'
truth = relocated_base('fixed', p)
db = reopen('fixed', p, 'w')
for i in range(40, 80):
    db[f'k{i}'] = truth[f'k{i}']
real_reindex, real_check = utils.reindex, getattr(utils, 'reindex_in_progress', None)
def failing(*a, **k):
    raise OSError(5, 'injected: the reindex failed')
def unreadable(f):
    raise OSError(5, 'injected: the flag cannot be read')
utils.reindex, utils.reindex_in_progress = failing, unreadable
try:
    db.sync()
    raise AssertionError('sync did not raise')
except OSError:
    pass
utils.reindex = real_reindex
if real_check is not None:
    utils.reindex_in_progress = real_check
assert db._file.closed, 'an unreadable flag must be treated as damage'
""")

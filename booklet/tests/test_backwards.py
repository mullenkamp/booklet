import booklet
import pathlib
import pytest
import os


script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))
file_v4_path = script_path.joinpath('test_v4.blt')

def test_backwards_compat_v4():
    
    with booklet.open(file_v4_path) as f:
        assert len(f) == 100
        keys = list(f.keys())
        assert len(keys) == 100

        i = 0
        for key, val in f.items():
            assert key == str(i)
            assert val == f'This is value {i}'
            i += 1

        for i, key in enumerate(keys):
            val = f[key]
            assert key == str(i)
            assert val == f'This is value {i}'


def make_file():
    """
    Only make this file with booklet version 0.9.2.
    """
    with booklet.open(file_v4_path, 'n', 'str', 'pickle') as f:
        for i in range(100):
            f[str(i)] = f'This is value {i}'


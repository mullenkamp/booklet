#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multiprocessing helpers for Booklet.map().
"""


def _map_worker(args):
    """Worker function for multiprocessing.Pool."""
    func, key, value = args
    result = func(key, value)
    if result is not None:
        return result  # (new_key, new_value) tuple
    return None

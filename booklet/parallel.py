#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multiprocessing helpers for Booklet.map().
"""
import queue


_SENTINEL = object()


def _map_worker(args):
    """Worker function for multiprocessing.Pool."""
    func, key, value = args
    result = func(key, value)
    if result is not None:
        return result  # (new_key, new_value) tuple
    return None


def _writer_thread_func(db, result_queue, stats, done_event):
    """Writer thread: drains result_queue and writes to the output booklet."""
    while True:
        try:
            item = result_queue.get(timeout=0.1)
        except queue.Empty:
            if done_event.is_set():
                # Drain any remaining items after pool is done
                while not result_queue.empty():
                    try:
                        item = result_queue.get_nowait()
                        if item is _SENTINEL:
                            return
                        key, value = item
                        try:
                            db[key] = value
                            stats['written'] += 1
                        except Exception:
                            stats['errors'] += 1
                    except queue.Empty:
                        break
                return
            continue

        if item is _SENTINEL:
            return

        key, value = item
        try:
            db[key] = value
            stats['written'] += 1
        except Exception:
            stats['errors'] += 1

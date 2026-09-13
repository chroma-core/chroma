"""Tests for asyncio event loop handling in test_client.py.

Regression tests for https://github.com/chroma-core/chroma/issues/6659
"""
import asyncio
import pytest


def test_run_async_creates_event_loop() -> None:
    """_run_async should create an event loop if none exists."""
    from chromadb.test.test_client import _run_async

    async def sample_coro():
        return 42

    result = _run_async(sample_coro())
    assert result == 42


def test_run_async_works_in_thread() -> None:
    """_run_async should work in a thread without an existing event loop."""
    import threading

    from chromadb.test.test_client import _run_async

    results = []

    def run_in_thread():
        async def sample_coro():
            return 123

        results.append(_run_async(sample_coro()))

    thread = threading.Thread(target=run_in_thread)
    thread.start()
    thread.join()

    assert len(results) == 1
    assert results[0] == 123


def test_run_async_with_closed_loop() -> None:
    """_run_async should handle a closed event loop."""
    from chromadb.test.test_client import _run_async

    # Create and close a loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.close()

    # _run_async should create a new loop
    async def sample_coro():
        return 99

    result = _run_async(sample_coro())
    assert result == 99

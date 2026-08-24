import threading
import time

from chromadb.utils.read_write_lock import ReadWriteLock


def _wait_until(predicate, timeout=5.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_pending_writer_blocks_new_readers():
    """Regression test for writer starvation.

    A steady stream of new readers must not be able to keep a waiting
    writer starved forever: once a writer starts waiting for readers to
    drain, any *new* reader must itself wait until that writer has run
    and released the lock.
    """
    lock = ReadWriteLock()

    # An existing reader holds the lock, so the writer below will have to
    # wait.
    lock.acquire_read()

    writer_acquired = threading.Event()

    def writer():
        lock.acquire_write()
        writer_acquired.set()
        lock.release_write()

    writer_thread = threading.Thread(target=writer)
    writer_thread.start()

    # Wait for the writer to register that it's waiting.
    assert _wait_until(lambda: lock._writers_waiting > 0), (
        "writer never started waiting"
    )

    # A brand new reader arrives while the writer is waiting. It must be
    # blocked by the fix, not slip in ahead of the writer.
    new_reader_acquired = threading.Event()

    def new_reader():
        lock.acquire_read()
        new_reader_acquired.set()
        lock.release_read()

    new_reader_thread = threading.Thread(target=new_reader)
    new_reader_thread.start()

    # The new reader should NOT be able to acquire while the writer is
    # still waiting for the original reader to release.
    time.sleep(0.2)
    assert not new_reader_acquired.is_set(), (
        "a new reader acquired the lock while a writer was waiting -- "
        "this starves the writer"
    )
    assert not writer_acquired.is_set()

    # Release the original reader: the writer should now be able to run.
    lock.release_read()

    assert _wait_until(writer_acquired.is_set), "writer was never able to acquire"
    writer_thread.join(timeout=5.0)

    # And now that the writer has finished, the new reader should proceed.
    assert _wait_until(new_reader_acquired.is_set), (
        "new reader never acquired after the writer finished"
    )
    new_reader_thread.join(timeout=5.0)


def test_multiple_readers_can_acquire_concurrently():
    lock = ReadWriteLock()
    lock.acquire_read()
    lock.acquire_read()
    assert lock._readers == 2
    lock.release_read()
    lock.release_read()
    assert lock._readers == 0


def test_write_lock_is_exclusive():
    lock = ReadWriteLock()
    lock.acquire_write()
    try:
        second_writer_acquired = threading.Event()

        def second_writer():
            lock.acquire_write()
            second_writer_acquired.set()
            lock.release_write()

        t = threading.Thread(target=second_writer)
        t.start()
        time.sleep(0.2)
        assert not second_writer_acquired.is_set()
    finally:
        lock.release_write()

    assert _wait_until(second_writer_acquired.is_set)
    t.join(timeout=5.0)

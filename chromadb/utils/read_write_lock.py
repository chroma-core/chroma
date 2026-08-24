import threading
from types import TracebackType
from typing import Optional, Type


class ReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0
        # Number of writers currently waiting for readers to drain (0 or 1,
        # since acquire_write() holds the underlying lock for its whole
        # critical section, so at most one thread can be in that phase at a
        # time). Used to stop new readers from continually renewing
        # `_readers > 0` and starving a waiting writer.
        self._writers_waiting = 0

    def acquire_read(self) -> None:
        """Acquire a read lock. Blocks only if a thread has
        acquired the write lock, or a thread is waiting to acquire
        the write lock."""
        self._read_ready.acquire()
        try:
            while self._writers_waiting > 0:
                self._read_ready.wait()
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self) -> None:
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notify_all()
        finally:
            self._read_ready.release()

    def acquire_write(self) -> None:
        """Acquire a write lock. Blocks until there are no
        acquired read or write locks. Once a thread starts waiting
        here, new readers are blocked so they cannot starve it."""
        self._read_ready.acquire()
        self._writers_waiting += 1
        try:
            while self._readers > 0:
                self._read_ready.wait()
        finally:
            self._writers_waiting -= 1

    def release_write(self) -> None:
        """Release a write lock."""
        # Wake any readers parked in acquire_read() because a writer was
        # waiting; without this they'd never be notified and would hang.
        self._read_ready.notify_all()
        self._read_ready.release()


class ReadRWLock:
    def __init__(self, rwLock: ReadWriteLock):
        self.rwLock = rwLock

    def __enter__(self) -> None:
        self.rwLock.acquire_read()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.rwLock.release_read()


class WriteRWLock:
    def __init__(self, rwLock: ReadWriteLock):
        self.rwLock = rwLock

    def __enter__(self) -> None:
        self.rwLock.acquire_write()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.rwLock.release_write()

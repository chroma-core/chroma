import threading
from types import TracebackType
from typing import Optional, Type


class ReadWriteLock:
    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0
        self._writers = 0
        self._writer_active = False

    def acquire_read(self) -> None:
        """Acquire a read lock. Blocks only if a thread has
        acquired the write lock."""
        self._read_ready.acquire()
        try:
            while self._writers > 0 or self._writer_active:
                self._read_ready.wait()
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self) -> None:
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self) -> None:
        self._read_ready.acquire()
        try:
            self._writers += 1
            while self._readers > 0 or self._writer_active:
                self._read_ready.wait()
            self._writers -= 1
            self._writer_active = True
        finally:
            self._read_ready.release()

    def release_write(self) -> None:
        self._read_ready.acquire()
        try:
            self._writer_active = False
            self._read_ready.notifyAll()
        finally:
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
        # TODO: handle exceptions


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
        # TODO: handle exceptions

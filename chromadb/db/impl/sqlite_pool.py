import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Set, Optional

from overrides import override


class Connection:
    """A threadpool connection that returns itself to the pool on close()"""

    _pool: "Pool"
    _db_file: str
    _conn: sqlite3.Connection
    _lock: threading.Lock
    _is_closed: bool
    _last_used: Optional[float]

    def __init__(
        self, pool: "Pool", db_file: str, is_uri: bool, *args: Any, **kwargs: Any
    ):
        self._pool = pool
        self._db_file = db_file
        self._conn = sqlite3.connect(
            db_file, timeout=1000, check_same_thread=False, uri=is_uri, *args, **kwargs
        )  # type: ignore
        self._conn.isolation_level = None  # Handle commits explicitly
        self._lock = threading.Lock()
        self._is_closed = False

    def execute(self, sql: str, parameters=...) -> sqlite3.Cursor:  # type: ignore
        if parameters is ...:
            return self._conn.execute(sql)
        return self._conn.execute(sql, parameters)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def cursor(self) -> sqlite3.Cursor:
        return self._conn.cursor()

    def close_actual(self) -> None:
        """Actually closes the connection to the db"""
        with self._lock:
            if self._is_closed:
                raise RuntimeError("Connection is already closed")
            self._is_closed = True
        self._conn.close()

    @property
    def last_used(self) -> Optional[float]:
        return self._last_used

    @property
    def is_closed(self) -> bool:
        with self._lock:
            return self._is_closed

    def checkout(self) -> None:
        """Checkout the connection from the pool."""
        with self._lock:
            self._last_used = None

    def checkin(self) -> None:
        with self._lock:
            self._last_used = time.time()


class Pool(ABC):
    """Abstract base class for a pool of connections to a sqlite database."""

    @abstractmethod
    def __init__(self, db_file: str, is_uri: bool) -> None:
        pass

    @abstractmethod
    def connect(self, *args: Any, **kwargs: Any) -> Connection:
        """Return a connection from the pool."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close all connections in the pool."""
        pass

    @abstractmethod
    def return_to_pool(self, conn: Connection) -> None:
        """Return a connection to the pool."""
        pass


class LockPool(Pool):
    """A pool that has a single connection per thread but uses a lock to ensure that only one thread can use it at a time.
    This is used because sqlite does not support multithreaded access with connection timeouts when using the
    shared cache mode. We use the shared cache mode to allow multiple threads to share a database.
    """

    _connections: Set[Connection]
    _lock: threading.RLock
    _connection: threading.local
    _db_file: str
    _is_uri: bool

    def __init__(self, db_file: str, is_uri: bool = False):
        self._connections = set()
        self._connection = threading.local()
        self._lock = threading.RLock()
        self._db_file = db_file
        self._is_uri = is_uri

    @override
    def connect(self, *args: Any, **kwargs: Any) -> Connection:
        self._lock.acquire()
        if hasattr(self._connection, "conn") and self._connection.conn is not None:
            return self._connection.conn  # type: ignore # cast doesn't work here for some reason
        else:
            new_connection = Connection(
                self, self._db_file, self._is_uri, *args, **kwargs
            )
            self._connection.conn = new_connection
            self._connections.add(new_connection)
            return new_connection

    @override
    def return_to_pool(self, conn: Connection) -> None:
        try:
            self._lock.release()
        except RuntimeError:
            pass

    @override
    def close(self) -> None:
        for conn in self._connections:
            conn.close_actual()
        self._connections.clear()
        self._connection = threading.local()
        try:
            self._lock.release()
        except RuntimeError:
            pass


class PerThreadPool(Pool):
    """Maintains a connection per thread. For now this does not maintain a cap on the number of connections, but it could be
    extended to do so and block on connect() if the cap is reached.
    """

    _connections: Set[Connection]
    _lock: threading.Lock
    _connection: threading.local
    _db_file: str
    _is_uri_: bool

    def __init__(
        self,
        db_file: str,
        is_uri: bool = False,
        min_size: int = 5,
        max_size: int = 20,
        connection_ttl: int = 60,
        lru_check_interval: int = 120,
    ):
        """
        Creates a new thread pool.
        Max size is best-effort, but the pool will try to maintain at least min_size connections.
        """
        if connection_ttl <= 0:
            raise ValueError("Connection TTL must be greater than 0")
        if min_size <= 0:
            raise ValueError("Min size must be greater than 0")
        if max_size <= 0 or max_size < min_size:
            raise ValueError(
                "Max size must be greater than min size and greater than 0"
            )
        if lru_check_interval <= 0:
            raise ValueError("LRU check interval must be greater than 0")
        self._connections = set()
        self._connection = threading.local()
        self._lock = threading.Lock()
        self._db_file = db_file
        self._is_uri = is_uri
        self._min_size = min_size
        self._max_size = max_size
        self._lru_check_interval = (
            lru_check_interval  # How often to check for connections to remove
        )
        self._connection_ttl = (
            connection_ttl  # Time to live for a connection in seconds
        )
        self._lru_last_check = time.time()

    @override
    def connect(self, *args: Any, **kwargs: Any) -> Connection:
        if (
            hasattr(self._connection, "conn")
            and self._connection.conn is not None
            and not self._connection.conn.is_closed
        ):
            self._connection.conn.checkout()
            return self._connection.conn  # type: ignore # cast doesn't work here for some reason
        else:
            new_connection = Connection(
                self, self._db_file, self._is_uri, *args, **kwargs
            )
            self._connection.conn = new_connection
            new_connection.checkout()
            with self._lock:
                self._connections.add(new_connection)
                if (
                    len(self._connections) >= self._max_size
                ):  # if we've reached the maximum we attempt to remove LRU connection
                    self._lru_remove_from_pool()
            return new_connection

    @override
    def close(self) -> None:
        with self._lock:
            for conn in self._connections:
                conn.close_actual()
            self._connections.clear()
            self._connection = threading.local()

    def _lru_remove_from_pool(self) -> None:
        now = time.time()
        if self._lru_last_check > now - self._lru_check_interval:
            return
        with self._lock:
            connections_to_remove = []
            if len(self._connections) < self._min_size:
                return
            for conn in self._connections:
                if (
                    conn.last_used is not None
                    and conn.last_used < now - self._connection_ttl
                ):
                    connections_to_remove.append(conn)
                    conn.close_actual()
                if (
                    len(self._connections) - len(connections_to_remove)
                    <= self._min_size
                ):
                    break
            for conn in connections_to_remove:
                self._connections.remove(conn)
            self._lru_last_check = time.time()

    @override
    def return_to_pool(self, conn: Connection) -> None:
        conn.checkin()
        self._lru_remove_from_pool()

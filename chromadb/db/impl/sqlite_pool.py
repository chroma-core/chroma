import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Set
import threading
from overrides import override


class Connection(sqlite3.Connection):
    """A threadpool connection that returns itself to the pool on close()"""

    pool: "Pool"
    db_file: str

    def __init__(self, pool: "Pool", db_file: str, *args: Any, **kwargs: Any):
        self._pool = pool
        self._db_file = db_file
        # TODO: abstract out the uri :memory: check
        super().__init__(
            db_file, check_same_thread=False, uri=":memory:" in db_file, *args, **kwargs
        )

    def close(self) -> None:
        self._pool.return_to_pool(self)

    def close_actual(self) -> None:
        super().close()


class Pool(ABC):
    """Abstract base class for a pool of connections to a sqlite database."""

    @abstractmethod
    def __init__(self, db_file: str) -> None:
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


class EmptyPool(Pool):
    """A pool that creates a new connection each time connect() is called. It never holds
    connections and is therefore empty.
    """

    _connections: Set[Connection]
    _lock: threading.Lock
    _db_file: str

    def __init__(self, db_file: str):
        self._connections = set()
        self._lock = threading.Lock()
        self._db_file = db_file

    @override
    def connect(self, *args: Any, **kwargs: Any) -> Connection:
        new_connection = Connection(self, self._db_file, *args, **kwargs)
        with self._lock:
            self._connections.add(new_connection)
        return new_connection

    @override
    def return_to_pool(self, conn: Connection) -> None:
        conn.close_actual()
        with self._lock:
            self._connections.remove(conn)

    @override
    def close(self) -> None:
        with self._lock:
            for conn in self._connections:
                conn.close_actual()
            self._connections.clear()


class PerThreadPool(Pool):
    """Maintains a connection per thread. This should be used with in-memory sqlite dbs.
    For now this does not maintain a cap on the number of connections, but it could be
    extended to do so and block on connect() if the cap is reached.
    """

    _connections: Set[Connection]
    _lock: threading.Lock
    _connection: threading.local
    _db_file: str

    def __init__(self, db_file: str):
        self._connections = set()
        self._connection = threading.local()
        self._lock = threading.Lock()
        self._db_file = db_file

    @override
    def connect(self, *args: Any, **kwargs: Any) -> Connection:
        if hasattr(self._connection, "conn") and self._connection.conn is not None:
            return self._connection.conn  # type: ignore # cast doesn't work here for some reason
        else:
            new_connection = Connection(self, self._db_file, *args, **kwargs)
            self._connection.conn = new_connection
            with self._lock:
                self._connections.add(new_connection)
            return new_connection

    @override
    def close(self) -> None:
        with self._lock:
            for conn in self._connections:
                conn.close_actual()
            self._connections.clear()
            self._connection = threading.local()

    @override
    def return_to_pool(self, conn: Connection) -> None:
        pass  # Each thread gets its own connection, so we don't need to return it to the pool

import json
import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Set, Union, List
import threading
from overrides import override


class Connection:
    """A threadpool connection that returns itself to the pool on close()"""

    _pool: "Pool"
    _db_file: str
    _conn: sqlite3.Connection
    _tid: Any

    def __init__(
        self, pool: "Pool", db_file: str, is_uri: bool, *args: Any, **kwargs: Any
    ):
        self._pool = pool
        self._db_file = db_file
        if "t" in kwargs:
            self._tid = kwargs.pop("t")
        else:
            self._tid = threading.get_ident()
        self._conn = sqlite3.connect(
            db_file, timeout=1000, check_same_thread=False, uri=is_uri, *args, **kwargs
        )  # type: ignore
        self._conn.isolation_level = None  # Handle commits explicitly

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
        self._conn.close()


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
    def return_to_pool(self, conn: Union[Connection,List[Connection]]) -> None:
        """Return a connection to the pool."""
        pass


class LockPool(Pool):
    """A pool that has a single connection per thread but uses a lock to ensure that only one thread can use it at a time.
    This is used because sqlite does not support multithreaded access with connection timeouts when using the
    shared cache mode. We use the shared cache mode to allow multiple threads to share a database.
    """

    _connections: Set[Connection]
    _lock: threading.RLock
    # _connection: threading.local
    _db_file: str
    _is_uri: bool

    def __init__(self, db_file: str, is_uri: bool = False):
        self._connections = set()
        # self._connection = threading.local()
        self._lock = threading.RLock()
        self._db_file = db_file
        self._is_uri = is_uri

    @override
    def connect(self, *args: Any, **kwargs: Any) -> Connection:
        self._lock.acquire()
        if len(self._connections)>0:
            return self._connections.pop()
        else:
            if kwargs:
                kwargs["t"] = threading.get_ident()
            new_connection = Connection(
                self, self._db_file, self._is_uri, *args, **kwargs
            )
            # self._connection.conn = new_connection
            # with self._lock:
            #     self._connections.add(new_connection)
            return new_connection

    @override
    def return_to_pool(self, conn: Union[Connection,List[Connection]]) -> None:
        try:
            if isinstance(conn,list):
                for c in conn:
                    self._connections.add(c)
            else:
                self._connections.add(conn)
            self._lock.release()
        except RuntimeError:
            pass

    @override
    def close(self) -> None:
        for conn in self._connections:
            conn.close_actual()
        self._connections.clear()
        # self._connection = threading.local()
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
    # _connection: threading.local
    _db_file: str
    _is_uri_: bool

    def __init__(self, db_file: str, is_uri: bool = False):
        self._connections = set()
        # self._connection = threading.local()
        self._lock = threading.Lock()
        self._db_file = db_file
        self._is_uri = is_uri

    @override
    def connect(self, *args: Any, **kwargs: Any) -> Connection:
        print("Connections",len(self._connections))
        with open("td.txt","w") as f:
            tid_connections = {}
            for c in self._connections:
                if c._tid not in tid_connections:
                    tid_connections[c._tid] = []
                tid_connections[c._tid].append(id(c))
            f.write(json.dumps(tid_connections))

        # if hasattr(self._connection, "conn") and self._connection.conn is not None:
        with self._lock:
            if len(self._connections)>0:
                return self._connections.pop()
            else:
                if kwargs:
                    kwargs["t"] = threading.get_ident()
                new_connection = Connection(
                    self, self._db_file, self._is_uri, *args, **kwargs
                )
                # self._connection.conn = new_connection
                # with self._lock:
                #     self._connections.add(new_connection)
                return new_connection

    @override
    def close(self) -> None:
        with self._lock:
            for conn in self._connections:
                conn.close_actual()
            self._connections.clear()
            # self._connection = threading.local()

    @override
    def return_to_pool(self, conn: Union[Connection,List[Connection]]) -> None:
        with self._lock:
            if isinstance(conn,list):
                for c in conn:
                    c.commit()
                    self._connections.add(c)
            else:
                conn.commit()
                self._connections.add(conn)

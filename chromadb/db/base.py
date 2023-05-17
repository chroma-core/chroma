from typing import Any, Optional, Sequence, Tuple, Type
from types import TracebackType
from typing_extensions import Protocol, Self, Literal
from abc import ABC, abstractmethod
from threading import local
from overrides import override, EnforceOverrides
import pypika
import pypika.queries
import itertools


class Cursor(Protocol):
    """Reifies methods we use from a DBAPI2 Cursor since DBAPI2 is not typed."""

    def execute(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> Self:
        ...

    def executemany(
        self, sql: str, params: Optional[Sequence[Tuple[Any, ...]]] = None
    ) -> Self:
        ...

    def fetchone(self) -> Tuple[Any, ...]:
        ...

    def fetchall(self) -> Sequence[Tuple[Any, ...]]:
        ...


class TxWrapper(ABC, EnforceOverrides):
    """Wrapper class for DBAPI 2.0 Connection objects, with which clients can implement transactions.
    Makes two guarantees that basic DBAPI 2.0 connections do not:

    - __enter__ returns a Cursor object consistently (instead of a Connection like some do)
    - Always re-raises an exception if one was thrown from the body
    """

    @abstractmethod
    def __enter__(self) -> Cursor:
        pass

    @abstractmethod
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        pass


class SqlDB(ABC, EnforceOverrides):
    """DBAPI 2.0 interface wrapper to ensure consistent behavior between implementations"""

    @abstractmethod
    def tx(self) -> TxWrapper:
        """Return a transaction wrapper"""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset the database to a clean state. Implementations may throw an exception
        if they do not support reset. In all cases, implementations should respect the
        `allow_reset` config setting and throw an exception if it is set to False."""
        pass

    @staticmethod
    @abstractmethod
    def querybuilder() -> Type[pypika.Query]:
        """Return a PyPika Query builder of an appropriate subtype for this database
        implementation (see
        https://pypika.readthedocs.io/en/latest/3_advanced.html#handling-different-database-platforms)
        """
        pass

    @staticmethod
    @abstractmethod
    def parameter_format() -> str:
        """Return the appropriate parameter format for this database implementation.
        Will be called with str.format(i) where i is the numeric index of the parameter.
        """
        pass


_context = local()


class ParameterValue(pypika.Parameter):  # type: ignore
    """
    Wrapper class for PyPika paramters that allows the values for Parameters
    to be expressed inline while building a query. See get_sql() for
    detailed usage information.
    """

    def __init__(self, value: Any):
        self.value = value

    @override
    def get_sql(self, **kwargs: Any) -> str:
        _context.values.append(self.value)
        val = _context.formatstr.format(next(_context.generator))
        return str(val)


def get_sql(
    query: pypika.queries.QueryBuilder, formatstr: str = "?"
) -> Tuple[str, Tuple[Any, ...]]:
    """
    Wrapper for pypika's get_sql method that allows the values for Parameters
    to be expressed inline while building a query, and that returns a tuple of the
    SQL string and parameters. This makes it easier to construct complex queries
    programmatically and automatically matches up the generated SQL with the required
    parameter vector.

    Doing so requires using the ParameterValue class defined in this module instead
    of the base pypika.Parameter class.

    Usage Example:

        q = (
            pypika.Query().from_("table")
            .select("col1")
            .where("col2"==ParameterValue("foo"))
            .where("col3"==ParameterValue("bar"))
        )

        sql, params = get_sql(q)

        cursor.execute(sql, params)

    Note how it is not necessary to construct the parameter vector manually... it
    will always be generated with the parameter values in the same order as emitted
    SQL string.

    The format string should match the parameter format for the database being used.
    It will be called with str.format(i) where i is the numeric index of the parameter.
    For example, Postgres requires parameters like `:1`, `:2`, etc. so the format string
    should be `":{}"`.

    See https://pypika.readthedocs.io/en/latest/2_tutorial.html#parametrized-queries for more
    information on parameterized queries in PyPika.
    """

    _context.values = []
    _context.generator = itertools.count(1)
    _context.formatstr = formatstr
    sql = query.get_sql()
    params = tuple(_context.values)
    return sql, params

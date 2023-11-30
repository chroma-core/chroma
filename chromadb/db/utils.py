from typing import Optional, Union, Tuple as TypedTuple, Any

from pypika import Dialects, Column, Index, Table, EmptyCriterion, Database, Query
from pypika.terms import Term
from pypika.utils import builder, format_quotes


class CreateIndexBuilder:

    def __init__(self, dialect: Optional[Dialects] = None) -> None:
        self._dialect = dialect
        self._index = None
        self._columns = []
        self._table = None
        self._wheres = None
        self._is_unique = False
        self._if_not_exists = False

    @builder
    def create_index(self, index: Union[str, Index]) -> "CreateIndexBuilder":
        self._index = index

    @builder
    def columns(self, *columns: Union[str, TypedTuple[str, str], Column]) -> "CreateIndexBuilder":
        for column in columns:
            if isinstance(column, str):
                column = Column(column)
            elif isinstance(column, tuple):
                column = Column(column_name=column[0], column_type=column[1])
            self._columns.append(column)

    @builder
    def on(self, table: Union[Table, str]) -> "CreateIndexBuilder":
        self._table = table

    @builder
    def where(self, criterion: Union[Term, EmptyCriterion]) -> "CreateIndexBuilder":
        """
        Partial index where clause.
        """
        if isinstance(criterion, EmptyCriterion):
            return self

        if self._wheres:
            self._wheres &= criterion
        else:
            self._wheres = criterion
        return self

    @builder
    def unique(self) -> "CreateIndexBuilder":
        self._is_unique = True
        return self

    @builder
    def if_not_exists(self) -> "CreateIndexBuilder":
        self._if_not_exists = True
        return self

    def get_sql(self) -> str:
        if not self._columns or len(self._columns) == 0:
            raise AttributeError("Cannot create index without columns")
        if not self._table:
            raise AttributeError("Cannot create index without table")
        columns_str = ", ".join([c.name for c in self._columns])
        unique_str = "UNIQUE" if self._is_unique else ""
        if_not_exists_str = "IF NOT EXISTS" if self._if_not_exists else ""
        base_sql = f"CREATE {unique_str} INDEX {if_not_exists_str} {self._index} ON {self._table}({columns_str})"
        if self._wheres:
            base_sql += f" WHERE {self._wheres}"
        return base_sql.replace("  ", " ")

    def __str__(self) -> str:
        return self.get_sql()

    def __repr__(self) -> str:
        return self.__str__()


class DropQueryBuilder:
    """
    Query builder used to build DROP queries.
    """

    QUOTE_CHAR = '"'
    SECONDARY_QUOTE_CHAR = "'"
    ALIAS_QUOTE_CHAR = None
    QUERY_CLS = Query

    def __init__(self, dialect: Optional[Dialects] = None) -> None:
        self._drop_target_kind = None
        self._drop_target: Union[Database, Table, str] = ""
        self._if_exists = None
        self.dialect = dialect

    def _set_kwargs_defaults(self, kwargs: dict) -> None:
        kwargs.setdefault("quote_char", self.QUOTE_CHAR)
        kwargs.setdefault("secondary_quote_char", self.SECONDARY_QUOTE_CHAR)
        kwargs.setdefault("dialect", self.dialect)

    @builder
    def drop_database(self, database: Union[Database, str]) -> "DropQueryBuilder":
        target = database if isinstance(database, Database) else Database(database)
        self._set_target('DATABASE', target)

    @builder
    def drop_table(self, table: Union[Table, str]) -> "DropQueryBuilder":
        target = table if isinstance(table, Table) else Table(table)
        self._set_target('TABLE', target)

    @builder
    def drop_user(self, user: str) -> "DropQueryBuilder":
        self._set_target('USER', user)

    @builder
    def drop_view(self, view: str) -> "DropQueryBuilder":
        self._set_target('VIEW', view)

    @builder
    def drop_index(self, index: str) -> "DropQueryBuilder":
        self._set_target('INDEX', index)

    @builder
    def if_exists(self) -> "DropQueryBuilder":
        self._if_exists = True

    def _set_target(self, kind: str, target: Union[Database, Table, str]) -> None:
        if self._drop_target:
            raise AttributeError("'DropQuery' object already has attribute drop_target")
        self._drop_target_kind = kind
        self._drop_target = target

    def get_sql(self, **kwargs: Any) -> str:
        self._set_kwargs_defaults(kwargs)

        if_exists = 'IF EXISTS ' if self._if_exists else ''
        target_name: str = ""

        if isinstance(self._drop_target, Database):
            target_name = self._drop_target.get_sql(**kwargs)
        elif isinstance(self._drop_target, Table):
            target_name = self._drop_target.get_sql(**kwargs)
        else:
            target_name = format_quotes(self._drop_target, self.QUOTE_CHAR)

        return "DROP {kind} {if_exists}{name}".format(
            kind=self._drop_target_kind, if_exists=if_exists, name=target_name
        )

    def __str__(self) -> str:
        return self.get_sql()

    def __repr__(self) -> str:
        return self.__str__()


class IndexQuery:
    """
    Temporary Index query for creating and dropping indices
    """

    @classmethod
    def create_index(cls, index: Union[str, Index]) -> "CreateIndexBuilder":
        """
        Query builder entry point. Initializes query building and sets the index name to be created. When using this
        function, the query becomes a CREATE statement.
        """
        return CreateIndexBuilder().create_index(index)

    @classmethod
    def drop_index(cls, index: Union[str, Index]) -> "DropQueryBuilder":
        """
        Query builder entry point. Initializes query building and sets the index name to be dropped. When using this
        function, the query becomes a DROP statement.
        """
        return DropQueryBuilder().drop_index(index)

from typing import Any, List, Optional, Tuple
from chromadb.db.index import Index
import numpy.typing as npt

from overrides import override
from chromadb.config import Settings
from uuid import UUID

from psycopg2.extensions import connection
from pypika import Query, Table

from chromadb.api.types import (
    Embeddings,
    Metadata,
)

import re


def delete_all_indexes(settings: Settings) -> None:
    raise NotImplementedError


PGVECTOR_OPERATIONS = {
    "vector_l2_ops": "<#>",
    "vector_ip_ops": "<->",
    "vector_cosine_ops": "<=>",
}


# TODO: Refactor this implementation. It's a bit weird
# because our state is stored in the database, not in
# an external hnswlib index. This means that we need to
# execute queries through this vector index class.
class Pgvector(Index):
    @override(check_signature=False)
    def __init__(
        self,
        id: UUID,
        settings: Settings,
        metadata: Metadata,
        conn: connection,
        size: int,
    ):
        # TODO: Refactor how connections are maintained to adhere to best practices
        metadata = metadata or {}

        # Convert all values to strings for future compatibility.
        metadata = {k: str(v) for k, v in metadata.items()}

        # TODO: Add support for other spaces, figure out if we need to
        # create a custom space for Postgres instead of the technically
        # incorrect "hnsw".
        space = metadata.get("hnsw:space", "l2")
        if space == "l2":
            self._space = "vector_l2_ops"
        elif space == "ip":
            self._space = "vector_ip_ops"
        elif space == "cosine":
            self._space = "vector_cosine_ops"
        else:
            raise ValueError(f"Invalid space {space}")
        self._id = id
        self._settings = settings
        self._conn = conn
        self._size = size
        # TODO: Include self._space in indexname to have multiple indexes on
        # collection w/ different distance funcs
        self._index_name = f"index{self._id}"
        self._embeddings_table_name = f"embeddings{str(self._size)}"

        self._create_index()

    def _create_index(
        self,
        lists: int = 5,
    ) -> None:
        # Pypika has no index creation support - we need to use raw SQL
        query = (
            f'CREATE INDEX IF NOT EXISTS "{self._index_name}" ON'
            f" {self._embeddings_table_name} USING ivfflat (embedding {self._space})"
            f" WITH (lists = {lists}) WHERE (collection_uuid = '{self._id}');"
        )
        # TODO: Use {self._index_name} to create name of index in future
        self._execute_query(query)

    @override(check_signature=False)
    def delete(self) -> None:
        # Concurrent drop is necessary to avoid locking the table
        query = f"DROP INDEX CONCURRENTLY {self._index_name};"
        self._execute_query(query)

    @override(check_signature=False)
    def delete_from_index(self, ids: List[UUID]) -> None:
        raise NotImplementedError(
            "Pgvector will automatically delete embeddings from index"
        )

    @override(check_signature=False)
    def add(
        self, ids: List[UUID], embeddings: Embeddings, update: bool = False
    ) -> None:
        raise NotImplementedError("Pgvector will automatically add embeddings to index")

    @override(check_signature=False)
    def get_nearest_neighbors(
        self,
        embeddings: Optional[Embeddings],
        n_results: int,
        ids: Optional[List[UUID]] = None,
    ) -> Tuple[List[List[UUID]], npt.NDArray[Any]]:
        pg_embeddings_table = Table(self._embeddings_table_name)
        query = Query.from_(pg_embeddings_table).select("*").limit(n_results)
        if ids is not None:
            query = query.where(pg_embeddings_table.uuid.isin(ids))
        if embeddings is not None:
            for embedding in embeddings:
                query = query.orderby(
                    f"embedding {PGVECTOR_OPERATIONS[self._space]} '{embedding}'"
                )
            corrected_query = self._correct_order_by_pgvector_query(str(query))
        else:
            corrected_query = str(query)

        res = self._execute_query_with_response(corrected_query)
        print(res)
        # return [[*x] for x in res]  # type: ignore
        raise NotImplementedError

    # UTILITY FUNCTIONS
    # TODO: Separate these out to a postgres-specific utility class
    # that we can share with the postgres backend.
    def _execute_query(self, query: str) -> None:
        with self._conn.cursor() as curs:
            curs.execute(query)
        self._conn.commit()

    # def _execute_query_with_response(self, query: str) -> list[tuple[Any, ...]]:
    def _execute_query_with_response(self, query: str):  # type: ignore
        with self._conn.cursor() as curs:
            curs.execute(query)
            res = curs.fetchall()
        self._conn.commit()
        return res

    def _correct_order_by_pgvector_query(self, query: str) -> str:
        """
        TODO: This is a hack to get the query to work. Embedding orderbys
        don't work with pgvector syntax because of extra quotes, so we remove them
        and reinsert them around embedding column names with this function.
        """
        split_query = re.split("ORDER BY|LIMIT", query)
        split_query[1] = (
            split_query[1].replace('"', "").replace("embedding", '"embedding"')
        )
        corrected_query = (
            split_query[0] + "ORDER BY" + split_query[1] + "LIMIT" + split_query[2]
        )
        return corrected_query

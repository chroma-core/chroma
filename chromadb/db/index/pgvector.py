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
    @override
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
        self._index_name = f"{self._id}index"
        self._embeddings_table_name = f"embeddings{str(self._size)}"

        self._create_index()

    def _create_index(
        self,
        lists: int = 100,
    ) -> None:
        # Pypika has no index creation support - we need to use raw SQL
        query = (
            f"CREATE INDEX {self._index_name} ON {self._embeddings_table_name} USING"
            f" ivfflat (embedding {self._space}) WITH (lists = {lists}) WHERE"
            f" (category_id = {self._id});"
        )
        self._execute_query(query)

    @override
    def delete(self) -> None:
        # Concurrent drop is necessary to avoid locking the table
        query = f"DROP INDEX CONCURRENTLY {self._index_name};"
        self._execute_query(query)

    @override
    def delete_from_index(self, ids: List[UUID]) -> None:
        raise NotImplementedError(
            "Pgvector will automatically delete embeddings from index"
        )

    @override
    def add(
        self, ids: List[UUID], embeddings: Embeddings, update: bool = False
    ) -> None:
        raise NotImplementedError("Pgvector will automatically add embeddings to index")

    @override
    def get_nearest_neighbors(
        self,
        embeddings: Optional[Embeddings],
        n_results: int,
        ids: Optional[List[UUID]] = None,
    ) -> Tuple[List[List[UUID]], npt.NDArray[Any]]:
        # query = (
        #     f"SELECT * FROM {self._embeddings_table_name} ORDER BY embedding"
        #     f" {PGVECTOR_OPERATIONS[self._space]} '{embeddings}' LIMIT"
        #     f" {str(n_results)};"
        # )
        pg_embeddings_table = Table(self._embeddings_table_name)
        query = (
            Query.from_(pg_embeddings_table)
            .select("*")
            .limit(n_results)
            .orderby("INJECT_ORDERBY_HERE")
            .where(pg_embeddings_table.uuid.isin(ids))
        )
        # orderby = ""
        # if embeddings is not None:
        #     orderby = "ORDER BY"
        #     for embedding in embeddings:
        #         orderbyorderby.join()
        #             f"embedding {PGVECTOR_OPERATIONS[self._space]} '{embedding}'"
        #         )

        _ = self._execute_query_with_response(str(query))
        # return [[x[0], x[1], x[2]] for x in resp]
        raise NotImplementedError

    # UTILITY FUNCTIONS
    # TODO: Separate these out to a postgres-specific utility class
    # that we can share with the postgres backend.
    def _execute_query(self, query: str) -> None:
        with self._conn.cursor() as curs:
            curs.execute(query)
        self._conn.commit()

    def _execute_query_with_response(self, query: str) -> List[Tuple[Any, ...]]:
        with self._conn.cursor() as curs:
            curs.execute(query)
            res = curs.fetchall()
        self._conn.commit()
        return res

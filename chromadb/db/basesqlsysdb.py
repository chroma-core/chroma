from chromadb.types import EmbeddingFunction, Collection, ScalarEncoding, Segment
from chromadb.db import SysDB, SqlDB
import chromadb.db.querytools as qt
from pypika import Table, CustomFunction
from collections import defaultdict
import json
from overrides import override
from typing import Sequence
from uuid import UUID


class BaseSqlSysDB(SysDB, SqlDB):
    """Base class for SQL-based SysDB instances, allowing common code to be shared between implementations."""

    @override
    def create_embedding_function(self, embedding_function: EmbeddingFunction) -> None:
        with self.tx() as cur:
            cur.execute(
                "INSERT INTO embedding_functions (name, dimension, scalar_encoding) VALUES (?, ?, ?)",
                (
                    embedding_function["name"],
                    embedding_function["dimension"],
                    embedding_function["scalar_encoding"].value,
                ),
            )

    @override
    def get_embedding_functions(self, name=None) -> Sequence[EmbeddingFunction]:
        with self.tx() as cur:
            table = Table("embedding_functions")
            query = (
                self.querybuilder()
                .from_(table)
                .select(table.name, table.dimension, table.scalar_encoding)
            )
            if name is not None:
                query = query.where(table.name == name)
            cur.execute(str(query))
            return [
                EmbeddingFunction(
                    name=row[0], dimension=row[1], scalar_encoding=ScalarEncoding(row[2])
                )
                for row in cur.fetchall()
            ]

    @override
    def create_collection(self, collection: Collection) -> None:
        with self.tx() as cur:

            if collection["metadata"] and len(collection["metadata"]) > 0:
                metadata = json.dumps(collection["metadata"])
            else:
                metadata = None

            cur.execute(
                "INSERT INTO collections (id, name, topic, metadata) VALUES (?, ?, ?, ?)",
                (
                    collection["id"],
                    collection["name"],
                    collection["topic"],
                    metadata,
                ),
            )

    @override
    def delete_collection(self, id: UUID) -> None:
        with self.tx() as cur:
            cur.execute("DELETE FROM collections WHERE id = ?", (id,))

    @override
    def get_collections(
        self, id=None, topic=None, name=None, embedding_function=None, metadata=None
    ) -> Sequence[Collection]:
        with self.tx() as cur:
            table = Table("collections")
            query = self.querybuilder().from_(table)
            query = query.select(table.id, table.name, table.topic, table.metadata)

            if id is not None:
                query = query.where(table.id == qt.Value(id))

            if name is not None:
                query = query.where(table.name == qt.Value(name))

            if topic is not None:
                query = query.where(table.topic == qt.Value(topic))

            if embedding_function is not None:
                query = query.where(table.embedding_function == qt.Value(embedding_function))

            if metadata is not None and len(metadata) > 0:
                for key, value in metadata.items():
                    query = query.where(
                        _SQL_json_extract(table.metadata, f"$.{key}") == qt.Value(value)
                    )

            sql, params = qt.build(query, self.parameter_format())
            cur.execute(sql, params)
            results = cur.fetchall()

            return [
                Collection(
                    id=row[0],
                    name=row[1],
                    topic=row[2],
                    metadata=_parse_json(row[3]),
                )
                for row in results
            ]

    @override
    def create_segment(self, segment) -> Segment:

        if segment["metadata"] and len(segment["metadata"]) > 0:
            metadata = json.dumps(segment["metadata"])
        else:
            metadata = None

        with self.tx() as cur:
            cur.execute(
                "INSERT INTO segments (id, type, scope, topic, collection, metadata) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    segment["id"],
                    segment["type"],
                    segment["scope"],
                    segment["topic"],
                    segment["collection"],
                    metadata,
                ),
            )

        return segment

    @override
    def get_segments(
        self, id=None, scope=None, topic=None, collection=None, metadata=None
    ) -> Sequence[Segment]:
        with self.tx() as cur:
            segments_t = Table("segments")

            query = self.querybuilder().from_(segments_t)
            query = query.select(
                segments_t.id,
                segments_t.type,
                segments_t.scope,
                segments_t.topic,
                segments_t.collection,
                segments_t.metadata,
            )
            if id is not None:
                query = query.where(segments_t.id == qt.Value(id))

            if topic is not None:
                query = query.where(segments_t.topic == qt.Value(topic))

            if scope is not None:
                query = query.where(segments_t.scope == qt.Value(scope))

            if collection is not None:
                query = query.where(segments_t.collection == qt.Value(collection))

            if metadata is not None and len(metadata) > 0:
                for key, value in metadata.items():
                    query = query.where(
                        _SQL_json_extract(segments_t.metadata, f"$.{key}") == qt.Value(value)
                    )

            sql, params = qt.build(query, self.parameter_format())

            cur.execute(sql, params)
            results = cur.fetchall()

            return [
                Segment(
                    id=row[0],
                    type=row[1],
                    scope=row[2],
                    topic=row[3],
                    collection=row[4],
                    metadata=_parse_json(row[5]),
                )
                for row in results
            ]


_SQL_json_extract = CustomFunction("json_extract_string", ["value", "expression"])


def _parse_json(value):
    if value is None:
        return None
    return json.loads(value)

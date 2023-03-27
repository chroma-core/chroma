from chromadb.types import EmbeddingFunction, Topic, ScalarEncoding
from chromadb.db import SysDB, SqlDB
import chromadb.db.querytools as qt
from pypika import Table, Parameter
from collections import defaultdict


class BaseSqlSysDB(SysDB, SqlDB):
    """Base class for SQL-based SysDB instances, allowing common code to be shared between implementations."""

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

    def get_embedding_functions(self, name=None):
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

    def create_topic(self, topic: Topic) -> None:
        with self.tx() as cur:

            cur.execute(
                "INSERT INTO topics (name, embedding_function) VALUES (?, ?)",
                (topic["name"], topic["embedding_function"]),
            )
            if topic["metadata"] and len(topic["metadata"]) > 0:
                cur.executemany(
                    "INSERT INTO topic_metadata (topic, key, value) VALUES (?, ?, ?)",
                    [(topic["name"], key, value) for key, value in topic["metadata"].items()],
                )

    def delete_topic(self, topic_name: str) -> None:
        raise NotImplementedError()

    def get_topics(self, name=None, embedding_function=None, metadata=None):
        with self.tx() as cur:
            table = Table("topics")
            metadata_table = Table("topic_metadata")
            query = (
                self.querybuilder()
                .from_(table)
                .left_join(metadata_table)
                .on(table.name == metadata_table.topic)
            )
            query = query.select(
                table.name,
                table.embedding_function,
                metadata_table.key,
                metadata_table.value,
            )
            if name is not None:
                query = query.where(table.name == name)

            if embedding_function is not None:
                query = query.where(table.embedding_function == embedding_function)

            if metadata is not None and len(metadata) > 0:
                subquery = self.querybuilder().from_(metadata_table).select(metadata_table.topic)

                for key, value in metadata.items():
                    subquery = subquery.where(metadata_table.key == key).where(
                        metadata_table.value == value
                    )

                query = query.join(subquery).on(table.name == subquery.topic)

            cur.execute(str(query))

            results = cur.fetchall()

            return _rows_to_entities(
                results, {"name": 0, "embedding_function": 1, "key": 2, "value": 3}
            )

    def create_segment(self, segment):

        with self.tx() as cur:
            cur.execute(
                "INSERT INTO segments (id, type, scope, topic) VALUES (?, ?, ?, ?)",
                (
                    segment["id"],
                    segment["type"],
                    segment["scope"],
                    segment["topic"],
                ),
            )

            if segment["metadata"]:
                cur.executemany(
                    "INSERT INTO segment_metadata (segment, key, value) VALUES (?, ?, ?)",
                    [(segment["id"], key, value) for key, value in segment["metadata"].items()],
                )

        return segment

    def get_segments(self, id=None, scope=None, topic=None, metadata=None):
        with self.tx() as cur:
            segments_t = Table("segments")
            metadata_t = Table("segment_metadata")

            query = (
                self.querybuilder()
                .from_(segments_t)
                .left_join(metadata_t)
                .on(segments_t.id == metadata_t.segment)
            )
            query = query.select(
                segments_t.id,
                segments_t.type,
                segments_t.scope,
                segments_t.topic,
                metadata_t.key,
                metadata_t.value,
            )
            if id is not None:
                query = query.where(segments_t.id == qt.Value(id))

            if topic is not None:
                query = query.where(segments_t.topic == qt.Value(topic))

            if scope is not None:
                query = query.where(segments_t.scope == qt.Value(scope))

            if metadata is not None and len(metadata) > 0:
                subquery = self.querybuilder().from_(metadata_t).select(metadata_t.segment)

                for key, value in metadata.items():
                    subquery = subquery.where(metadata_t.key == qt.Value(key)).where(
                        metadata_t.value == qt.Value(value)
                    )

                query = query.join(subquery).on(segments_t.id == subquery.segment)

            sql, params = qt.build(query, self.parameter_format())

            cur.execute(sql, params)
            results = cur.fetchall()

            return _rows_to_entities(
                results, {"id": 0, "type": 1, "scope": 2, "topic": 3, "key": 4, "value": 5}
            )


def _group_by(rows, idx):
    """Group rows by the value at the given index."""
    groups = defaultdict(list)
    for row in rows:
        groups[row[idx]].append(row)
    return groups


def _rows_to_entities(results, colmap):
    """Given a list of rows, convert them to a list of entities."""

    groups = _group_by(results, 0)

    entities = []
    for id, rows in groups.items():
        metadata = {row[colmap["key"]]: row[colmap["value"]] for row in rows}
        if None in metadata:
            entity = {"metadata": None}
        else:
            entity = {"metadata": metadata}

        for key, idx in colmap.items():
            if key not in ("key", "value"):
                entity[key] = rows[0][idx]
        entities.append(entity)

    return entities

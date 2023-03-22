from chromadb.db import Segment, SysDB, SqlDB
import chromadb.db.querytools as qt
from pypika import Table, Parameter
from collections import defaultdict


class BaseSqlSysDB(SysDB, SqlDB):
    """Base class for SQL-based SysDB instances, allowing common code to be shared between implementations."""

    def create_segment(self, segment):

        with self.tx() as cur:
            cur.execute(
                "INSERT INTO segments (id, type, scope, embedding_function) VALUES (?, ?, ?, ?)",
                (segment["id"], segment["type"], segment["scope"], segment["embedding_function"]),
            )

            if segment["metadata"]:
                cur.executemany(
                    "INSERT INTO segment_metadata (segment, key, value) VALUES (?, ?, ?)",
                    [(segment["id"], key, value) for key, value in segment["metadata"].items()],
                )

        return segment

    def get_segments(self, id=None, embedding_function=None, metadata=None):
        with self.tx() as cur:
            segments_t = Table("segments")
            metadata_t = Table("segment_metadata")

            query = (
                self.querybuilder()
                .from_(segments_t)
                .join(metadata_t)
                .on(segments_t.id == metadata_t.segment)
            )
            query = query.select(
                segments_t.id,
                segments_t.type,
                segments_t.scope,
                segments_t.embedding_function,
                metadata_t.key,
                metadata_t.value,
            )
            if id is not None:
                query = query.where(segments_t.id == qt.Value(id))

            if embedding_function is not None:
                query = query.where(segments_t.embedding_function == qt.Value(embedding_function))

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

            segment_rows = defaultdict(list)
            for row in results:
                segment_rows[row[0]].append(row)

            segments = []
            for segment_id, rows in segment_rows.items():
                metadata = {row[4]: row[5] for row in rows}
                segments.append(
                    Segment(
                        id=segment_id,
                        metadata=metadata,
                        type=rows[0][1],
                        scope=rows[0][2],
                        embedding_function=rows[0][3],
                    )
                )

            return segments

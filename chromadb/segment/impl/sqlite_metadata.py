from typing import (
    Optional,
    Sequence,
    Any,
    Tuple,
    cast,
    Generator,
    List,
)
from chromadb.segment import MetadataReader
from chromadb.ingest import Consumer
from chromadb.config import System
from chromadb.types import Segment
from chromadb.db.impl.sqlite import SqliteDB
from overrides import override
from chromadb.db.base import (
    Cursor,
    ParameterValue,
    get_sql,
)
from chromadb.types import (
    Where,
    WhereDocument,
    MetadataEmbeddingRecord,
    EmbeddingRecord,
    SeqId,
    Operation,
    UpdateMetadata,
)
from uuid import UUID
from pypika import Table
from pypika.queries import QueryBuilder
import pypika.functions as fn
from itertools import islice


class SqliteMetadataSegment(MetadataReader):
    _consumer: Consumer
    _db: SqliteDB
    _id: UUID
    _topic: Optional[str]
    _subscription: Optional[UUID]

    def __init__(self, system: System, segment: Segment):
        self._db = system.instance(SqliteDB)
        self._consumer = system.instance(Consumer)
        self._id = segment["id"]
        self._topic = segment["topic"]

    @override
    def start(self) -> None:
        if self._topic:
            seq_id = self._max_seq_id()
            self._subscription = self._consumer.subscribe(
                self._topic, self._write_metadata, start=seq_id
            )

    @override
    def stop(self) -> None:
        if self._subscription:
            self._consumer.unsubscribe(self._subscription)

    @override
    def count_metadata(self) -> int:
        embeddings_t = Table("embeddings")
        q = (
            self._db.querybuilder()
            .select("COUNT(*)")
            .from_(embeddings_t)
            .where(
                embeddings_t.segment_id == ParameterValue(self._db.uuid_to_db(self._id))
            )
        )
        sql, params = get_sql(q)
        with self._db.tx() as cur:
            result = cur.execute(sql, params).fetchone()[0]
            return cast(int, result)

    @override
    def get_metadata(
        self,
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        ids: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        """Query for embedding metadata."""

        embeddings_t = Table("embeddings")
        metadata_t = Table("embedding_metadata")
        fulltext_t = Table("embedding_fulltext")

        q = (
            (
                self._db.querybuilder()
                .from_(embeddings_t)
                .left_join(metadata_t)
                .on(embeddings_t.id == metadata_t.id)
            )
            .select(
                embeddings_t.id,
                embeddings_t.embedding_id,
                embeddings_t.seq_id,
                metadata_t.key,
                metadata_t.string_value,
                metadata_t.int_value,
                metadata_t.float_value,
            )
            .where(
                embeddings_t.segment_id == ParameterValue(self._db.uuid_to_db(self._id))
            )
            .orderby(embeddings_t.id)
        )

        if where:
            q = self._where_query(q, where, metadata_t)

        if where_document:
            q = self._where_document_query(q, where_document, embeddings_t, fulltext_t)

        if ids:
            q = q.where(embeddings_t.embedding_id.isin(ids))

        limit = limit or 2**63 - 1
        offset = offset or 0

        with self._db.tx() as cur:
            return list(islice(self._records(cur, q), offset, offset + limit))

    def _records(
        self, cur: Cursor, q: QueryBuilder
    ) -> Generator[MetadataEmbeddingRecord, None, None]:
        """Given a cursor and a QueryBuilder, yield a generator of records. Assumes
        cursor returns rows in ID order."""

        sql, params = get_sql(q)
        cur.execute(sql, params)

        row = cur.fetchone()
        record_id: int = row[0] if row else -1
        current_rows: List[Tuple[Any, ...]] = []
        while row:
            current_rows.append(row)
            if record_id != row[0]:
                yield self._record(current_rows)
                current_rows = []
                record_id = row[0]
            row = cur.fetchone()

    def _record(self, rows: List[Tuple[Any, ...]]) -> MetadataEmbeddingRecord:
        """Given a list of DB rows with the same ID, construct a
        MetadataEmbeddingRecord"""
        _, embedding_id, seq_id = rows[0][:3]
        metadata = {}
        for row in rows:
            key, string_value, int_value, float_value = row[3:]
            if string_value is not None:
                metadata[key] = string_value
            elif int_value is not None:
                metadata[key] = int_value
            elif float_value is not None:
                metadata[key] = float_value

        return MetadataEmbeddingRecord(
            id=embedding_id,
            seq_id=_decode_seq_id(seq_id),
            metadata=metadata or None,
        )

    def _insert_record(
        self, cur: Cursor, record: EmbeddingRecord, upsert: bool
    ) -> None:
        """Add or update a single EmbeddingRecord into the DB"""

        t = Table("embeddings")
        q = (
            self._db.querybuilder()
            .into(t)
            .columns(t.segment_id, t.embedding_id, t.seq_id)
            .where(t.segment_id == ParameterValue(self._db.uuid_to_db(self._id)))
            .where(t.embedding_id == ParameterValue(record["id"]))
        ).insert(
            self._db.uuid_to_db(self._id),
            record["id"],
            _encode_seq_id(record["seq_id"]),
        )
        sql, params = get_sql(q)
        if upsert:
            sql = sql.replace("INSERT", "INSERT OR REPLACE")
        sql = sql + "RETURNING id"
        id = cur.execute(sql, params).fetchone()[0]

        if record["metadata"]:
            if upsert:
                self._update_metadata(cur, id, record["metadata"])
            else:
                self._insert_metadata(cur, id, record["metadata"], False)

    def _update_metadata(self, cur: Cursor, id: int, metadata: UpdateMetadata) -> None:
        """Update the metadata for a single EmbeddingRecord"""
        t = Table("embedding_metadata")
        to_delete = [k for k, v in metadata.items() if v is None]
        q = (
            self._db.querybuilder()
            .from_(t)
            .where(t.id == ParameterValue(id))
            .where(t.key.isin(to_delete))
            .delete()
        )
        sql, params = get_sql(q)
        cur.execute(sql, params)
        self._insert_metadata(cur, id, metadata, True)

    def _insert_metadata(
        self, cur: Cursor, id: int, metadata: UpdateMetadata, upsert: bool
    ) -> None:
        """Insert or update each metadata row for a single embedding record"""
        t = Table("embedding_metadata")
        q = (
            self._db.querybuilder()
            .into(t)
            .columns(t.id, t.key, t.string_value, t.int_value, t.float_value)
        )
        for key, value in metadata.items():
            if isinstance(value, str):
                q = q.insert(id, key, value, None, None)
            elif isinstance(value, int):
                q = q.insert(id, key, None, value, None)
            elif isinstance(value, float):
                q = q.insert(id, key, None, None, value)

        sql, params = get_sql(q)
        if upsert:
            sql.replace("INSERT", "INSERT OR REPLACE")
        if sql:
            cur.execute(sql, params)

    def _delete_record(self, cur: Cursor, record: EmbeddingRecord) -> None:
        """Delete a single EmbeddingRecord from the DB"""
        t = Table("embeddings")
        q = (
            self._db.querybuilder()
            .from_(t)
            .where(t.segment_id == ParameterValue(self._db.uuid_to_db(self._id)))
            .where(t.embedding_id == ParameterValue(record["id"]))
            .delete()
        )
        sql, params = get_sql(q)
        sql = sql + " RETURNING id"
        id = cur.execute(sql, params).fetchone()[0]

        # Manually delete metadata; cannot use cascade because
        # that triggers on replace
        metadata_t = Table("embedding_metadata")
        q = (
            self._db.querybuilder()
            .from_(metadata_t)
            .where(metadata_t.id == ParameterValue(id))
            .delete()
        )
        sql, params = get_sql(q)
        cur.execute(sql, params)

    def _update_record(self, cur: Cursor, record: EmbeddingRecord) -> None:
        """Update a single EmbeddingRecord in the DB"""
        t = Table("embeddings")
        q = (
            self._db.querybuilder()
            .from_(t)
            .where(t.segment_id == ParameterValue(self._db.uuid_to_db(self._id)))
            .where(t.embedding_id == ParameterValue(record["id"]))
            .update(t.seq_id, _encode_seq_id(record["seq_id"]))
        )
        sql, params = get_sql(q)
        sql = sql + " RETURNING id"
        id = cur.execute(sql, params).fetchone()[0]
        if record["metadata"]:
            self._update_metadata(cur, id, record["metadata"])

    def _where_query(self, q: QueryBuilder, where: Where, table: Table) -> QueryBuilder:
        "Add where clauses to the given Pypika query"
        return q

    def _where_document_query(
        self,
        q: QueryBuilder,
        where_document: WhereDocument,
        embeddings_table: Table,
        fulltext_table: Table,
    ) -> QueryBuilder:
        "Add where-document clauses to the given Pypika query"
        return q

    def _max_seq_id(self) -> SeqId:
        """Returns the maximum SeqID that's already been consumed."""
        t = Table("embeddings")
        q = (
            self._db.querybuilder()
            .from_(t)
            .select(fn.Max(t.seq_id))
            .where(t.segment_id == ParameterValue(self._db.uuid_to_db(self._id)))
        )
        sql, params = get_sql(q)
        with self._db.tx() as cur:
            result = cur.execute(sql, params).fetchone()

            if result is None:
                return self._consumer.min_seqid()
            else:
                return _decode_seq_id(result[0])

    def _write_metadata(self, records: Sequence[EmbeddingRecord]) -> None:
        """Write embedding metadata to the database. Care should be taken to ensure
        records are append-only (that is, that seq-ids should increase monotonically)"""
        with self._db.tx() as cur:
            for record in records:
                if record["operation"] == Operation.ADD:
                    self._insert_record(cur, record, False)
                elif record["operation"] == Operation.UPSERT:
                    self._insert_record(cur, record, True)
                elif record["operation"] == Operation.DELETE:
                    self._delete_record(cur, record)
                elif record["operation"] == Operation.UPDATE:
                    self._update_record(cur, record)

    def _begin_consumption(self, topic: str) -> None:
        """Begin consuming from the consumer, starting with the most recently seen
        SeqID"""

        seq_id = self._max_seq_id()
        self._subscription = self._consumer.subscribe(
            topic, self._write_metadata, start=seq_id
        )


def _encode_seq_id(seq_id: SeqId) -> bytes:
    """Encode a SeqID into a byte array"""
    if seq_id.bit_length() < 64:
        return int.to_bytes(seq_id, 8, "big")
    elif seq_id.bit_length() < 192:
        return int.to_bytes(seq_id, 24, "big")
    else:
        raise ValueError(f"Unsupported SeqID: {seq_id}")


def _decode_seq_id(seq_id_bytes: bytes) -> SeqId:
    """Decode a byte array into a SeqID"""
    if len(seq_id_bytes) == 8:
        return int.from_bytes(seq_id_bytes, "big")
    elif len(seq_id_bytes) == 24:
        return int.from_bytes(seq_id_bytes, "big")
    else:
        raise ValueError(f"Unknown SeqID type with length {len(seq_id_bytes)}")

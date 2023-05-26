from typing import (
    Optional,
    Sequence,
    Any,
    Tuple,
    cast,
    Generator,
    List,
)
from uuid import UUID
from overrides import override
from pypika import Table
from pypika.queries import QueryBuilder
import pypika.functions as fn
from itertools import islice

from chromadb.config import System
from chromadb.db.metadata import MetadataDB, OutdatedOperationError
from chromadb.db.base import (
    Cursor,
    SqlDB,
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
)


class SqlMetaDB(SqlDB, MetadataDB):
    """A SQL database for storing and retrieving embedding metadata."""

    def __init__(self, system: System):
        super().__init__(system)

    def count_metadata(self, segment_id: UUID) -> int:
        embeddings_t = Table("embeddings")
        q = (
            self.querybuilder()
            .select("COUNT(*)")
            .from_(embeddings_t)
            .where(
                embeddings_t.segment_id == ParameterValue(self.uuid_to_db(segment_id))
            )
        )
        sql, params = get_sql(q)
        with self.tx() as cur:
            result = cur.execute(sql, params).fetchone()[0]
            return cast(int, result)

    @override
    def get_metadata(
        self,
        segment_id: UUID,
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
                self.querybuilder()
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
                embeddings_t.segment_id == ParameterValue(self.uuid_to_db(segment_id))
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

        with self.tx() as cur:
            return list(islice(self._records(cur, q), offset, offset + limit))

    @override
    def max_seq_id(self, segment_id: UUID) -> SeqId:
        t = Table("embeddings")
        q = (
            self.querybuilder()
            .from_(t)
            .select(fn.Max(t.seq_id))
            .where(t.segment_id == ParameterValue(self.uuid_to_db(segment_id)))
        )
        sql, params = get_sql(q)
        with self.tx() as cur:
            result = cur.execute(sql, params).fetchone()[0]
            return _decode_seq_id(result)

    @override
    def write_metadata(
        self, segment_id: UUID, records: Sequence[EmbeddingRecord]
    ) -> None:
        """Write embedding metadata to the database."""
        with self.tx() as cur:
            max_seq_id = self.max_seq_id(segment_id)

            for record in records:
                if record["seq_id"] <= max_seq_id:
                    raise OutdatedOperationError(record["seq_id"], max_seq_id)
                if record["operation"] == Operation.ADD:
                    self._insert_record(cur, segment_id, record, False)
                elif record["operation"] == Operation.UPSERT:
                    self._insert_record(cur, segment_id, record, True)
                elif record["operation"] == Operation.DELETE:
                    self._delete_record(cur, segment_id, record)
                elif record["operation"] == Operation.UPDATE:
                    self._update_record(cur, segment_id, record)

    def _insert_record(
        self, cur: Cursor, segment_id: UUID, record: EmbeddingRecord, upsert: bool
    ) -> None:
        # t = Table("embeddings")
        # q = (
        #     self.querybuilder()
        #     .into(t)
        #     .columns(t.segment_id, t.embedding_id, t.seq_id)
        #     .where(t.segment_id == ParameterValue(self.uuid_to_db(segment_id)))
        #     .where(t.embedding_id == ParameterValue(record["id"]))
        # )
        pass

    def _delete_record(
        self, cur: Cursor, segment_id: UUID, record: EmbeddingRecord
    ) -> None:
        raise NotImplementedError()

    def _update_record(
        self, cur: Cursor, segment_id: UUID, record: EmbeddingRecord
    ) -> None:
        raise NotImplementedError()

    def _record(self, rows: List[Tuple[Any, ...]]) -> MetadataEmbeddingRecord:
        """Given a list of DB rows with the same ID, construct a
        MetadataEmbeddingRecord"""
        id, embedding_id, seq_id = rows[0][:3]
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

    def _records(
        self, cur: Cursor, q: QueryBuilder
    ) -> Generator[MetadataEmbeddingRecord, None, None]:
        """Given a cursor and a QueryBuilder, yield a generator of records"""

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

    def _where_query(self, q: QueryBuilder, where: Where, table: Table) -> QueryBuilder:
        return q

    def _where_document_query(
        self,
        q: QueryBuilder,
        where_document: WhereDocument,
        embeddings_table: Table,
        fulltext_table: Table,
    ) -> QueryBuilder:
        return q


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

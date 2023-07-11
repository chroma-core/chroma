from typing import Optional, Sequence, Any, Tuple, cast, Generator, Union, Dict
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
    LiteralValue,
    WhereOperator,
)
from uuid import UUID
from pypika import Table, Tables
from pypika.queries import QueryBuilder
import pypika.functions as fn
from pypika.terms import Criterion, Function
from itertools import islice, groupby
from functools import reduce
import sqlite3

import logging

logger = logging.getLogger(__name__)


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
            seq_id = self.max_seqid()
            self._subscription = self._consumer.subscribe(
                self._topic, self._write_metadata, start=seq_id
            )

    @override
    def stop(self) -> None:
        if self._subscription:
            self._consumer.unsubscribe(self._subscription)

    @override
    def max_seqid(self) -> SeqId:
        t = Table("max_seq_id")
        q = (
            self._db.querybuilder()
            .from_(t)
            .select(t.seq_id)
            .where(t.segment_id == ParameterValue(self._db.uuid_to_db(self._id)))
        )
        sql, params = get_sql(q)
        with self._db.tx() as cur:
            result = cur.execute(sql, params).fetchone()

            if result is None:
                return self._consumer.min_seqid()
            else:
                return _decode_seq_id(result[0])

    @override
    def count(self) -> int:
        embeddings_t = Table("embeddings")
        q = (
            self._db.querybuilder()
            .from_(embeddings_t)
            .where(
                embeddings_t.segment_id == ParameterValue(self._db.uuid_to_db(self._id))
            )
            .select(fn.Count(embeddings_t.id))
        )
        sql, params = get_sql(q)
        with self._db.tx() as cur:
            result = cur.execute(sql, params).fetchone()[0]
            return cast(int, result)

    @override
    def get_metadata(
        self,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        ids: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        """Query for embedding metadata."""

        embeddings_t, metadata_t, fulltext_t = Tables(
            "embeddings", "embedding_metadata", "embedding_fulltext"
        )

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
            q = q.where(self._where_map_criterion(q, where, embeddings_t, metadata_t))

        if where_document:
            q = q.where(
                self._where_doc_criterion(q, where_document, embeddings_t, fulltext_t)
            )
            pass
            # q = self._where_document_query(q, where_document, embeddings_t, fulltext_t)

        if ids:
            q = q.where(embeddings_t.embedding_id.isin(ParameterValue(ids)))

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

        cur_iterator = iter(cur.fetchone, None)
        group_iterator = groupby(cur_iterator, lambda r: int(r[0]))

        for _, group in group_iterator:
            yield self._record(list(group))

    def _record(self, rows: Sequence[Tuple[Any, ...]]) -> MetadataEmbeddingRecord:
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
            ParameterValue(self._db.uuid_to_db(self._id)),
            ParameterValue(record["id"]),
            ParameterValue(_encode_seq_id(record["seq_id"])),
        )
        sql, params = get_sql(q)
        sql = sql + "RETURNING id"
        try:
            id = cur.execute(sql, params).fetchone()[0]
        except sqlite3.IntegrityError:
            # Can't use INSERT OR REPLACE here because it changes the primary key.
            if upsert:
                return self._update_record(cur, record)
            else:
                logger.warning(f"Insert of existing embedding ID: {record['id']}")
                # We are trying to add for a record that already exists. Fail the call.
                # We don't throw an exception since this is in principal an async path
                return

        if record["metadata"]:
            self._update_metadata(cur, id, record["metadata"])

    def _update_metadata(self, cur: Cursor, id: int, metadata: UpdateMetadata) -> None:
        """Update the metadata for a single EmbeddingRecord"""
        t = Table("embedding_metadata")
        to_delete = [k for k, v in metadata.items() if v is None]
        if to_delete:
            q = (
                self._db.querybuilder()
                .from_(t)
                .where(t.id == ParameterValue(id))
                .where(t.key.isin(ParameterValue(to_delete)))
                .delete()
            )
            sql, params = get_sql(q)
            cur.execute(sql, params)

        if "chroma:document" in metadata:
            t = Table("embedding_fulltext")
            q = (
                self._db.querybuilder()
                .from_(t)
                .where(t.id == ParameterValue(id))
                .delete()
            )
            sql, params = get_sql(q)
            cur.execute(sql, params)

        self._insert_metadata(cur, id, metadata)

    def _insert_metadata(self, cur: Cursor, id: int, metadata: UpdateMetadata) -> None:
        """Insert or update each metadata row for a single embedding record"""
        t = Table("embedding_metadata")
        q = (
            self._db.querybuilder()
            .into(t)
            .columns(t.id, t.key, t.string_value, t.int_value, t.float_value)
        )
        for key, value in metadata.items():
            if isinstance(value, str):
                q = q.insert(
                    ParameterValue(id),
                    ParameterValue(key),
                    ParameterValue(value),
                    None,
                    None,
                )
            elif isinstance(value, int):
                q = q.insert(
                    ParameterValue(id),
                    ParameterValue(key),
                    None,
                    ParameterValue(value),
                    None,
                )
            elif isinstance(value, float):
                q = q.insert(
                    ParameterValue(id),
                    ParameterValue(key),
                    None,
                    None,
                    ParameterValue(value),
                )

        sql, params = get_sql(q)
        sql = sql.replace("INSERT", "INSERT OR REPLACE")
        if sql:
            cur.execute(sql, params)

        if "chroma:document" in metadata:
            t = Table("embedding_fulltext")
            q = (
                self._db.querybuilder()
                .into(t)
                .columns(t.id, t.string_value)
                .insert(ParameterValue(id), ParameterValue(metadata["chroma:document"]))
            )
            sql, params = get_sql(q)
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
        result = cur.execute(sql, params).fetchone()
        if result is None:
            logger.warning(f"Delete of nonexisting embedding ID: {record['id']}")
        else:
            id = result[0]

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
            .update(t)
            .set(t.seq_id, ParameterValue(_encode_seq_id(record["seq_id"])))
            .where(t.segment_id == ParameterValue(self._db.uuid_to_db(self._id)))
            .where(t.embedding_id == ParameterValue(record["id"]))
        )
        sql, params = get_sql(q)
        sql = sql + " RETURNING id"
        result = cur.execute(sql, params).fetchone()
        if result is None:
            logger.warning(f"Update of nonexisting embedding ID: {record['id']}")
        else:
            id = result[0]
            if record["metadata"]:
                self._update_metadata(cur, id, record["metadata"])

    def _write_metadata(self, records: Sequence[EmbeddingRecord]) -> None:
        """Write embedding metadata to the database. Care should be taken to ensure
        records are append-only (that is, that seq-ids should increase monotonically)"""
        with self._db.tx() as cur:
            for record in records:
                q = (
                    self._db.querybuilder()
                    .into(Table("max_seq_id"))
                    .columns("segment_id", "seq_id")
                    .insert(
                        ParameterValue(self._db.uuid_to_db(self._id)),
                        ParameterValue(_encode_seq_id(record["seq_id"])),
                    )
                )
                sql, params = get_sql(q)
                sql = sql.replace("INSERT", "INSERT OR REPLACE")
                cur.execute(sql, params)

                if record["operation"] == Operation.ADD:
                    self._insert_record(cur, record, False)
                elif record["operation"] == Operation.UPSERT:
                    self._insert_record(cur, record, True)
                elif record["operation"] == Operation.DELETE:
                    self._delete_record(cur, record)
                elif record["operation"] == Operation.UPDATE:
                    self._update_record(cur, record)

    def _where_map_criterion(
        self, q: QueryBuilder, where: Where, embeddings_t: Table, metadata_t: Table
    ) -> Criterion:
        clause: list[Criterion] = []

        for k, v in where.items():
            if k == "$and":
                criteria = [
                    self._where_map_criterion(q, w, embeddings_t, metadata_t)
                    for w in cast(Sequence[Where], v)
                ]
                clause.append(reduce(lambda x, y: x & y, criteria))
            elif k == "$or":
                criteria = [
                    self._where_map_criterion(q, w, embeddings_t, metadata_t)
                    for w in cast(Sequence[Where], v)
                ]
                clause.append(reduce(lambda x, y: x | y, criteria))
            else:
                expr = cast(Union[LiteralValue, Dict[WhereOperator, LiteralValue]], v)
                sq = (
                    self._db.querybuilder()
                    .from_(metadata_t)
                    .select(metadata_t.id)
                    .where(metadata_t.key == ParameterValue(k))
                    .where(_where_clause(expr, metadata_t))
                )
                clause.append(embeddings_t.id.isin(sq))
        return reduce(lambda x, y: x & y, clause)

    class EscapedLike(Function):  # type: ignore
        def __init__(self, column_name: str, search_term: str, escape_char: str = "\\"):
            self.column_name = column_name
            self.search_term = search_term
            self.escape_char = escape_char
            super().__init__("LIKE", column_name, search_term)

        def get_function_sql(self, **kwargs: Any) -> str:
            return f"{self.column_name} LIKE {self.search_term} ESCAPE '{self.escape_char}'"

    def _where_doc_criterion(
        self,
        q: QueryBuilder,
        where: WhereDocument,
        embeddings_t: Table,
        fulltext_t: Table,
    ) -> Criterion:
        for k, v in where.items():
            if k == "$and":
                criteria = [
                    self._where_doc_criterion(q, w, embeddings_t, fulltext_t)
                    for w in cast(Sequence[WhereDocument], v)
                ]
                return reduce(lambda x, y: x & y, criteria)
            elif k == "$or":
                criteria = [
                    self._where_doc_criterion(q, w, embeddings_t, fulltext_t)
                    for w in cast(Sequence[WhereDocument], v)
                ]
                return reduce(lambda x, y: x | y, criteria)
            elif k == "$contains":
                v = cast(str, v)
                search_term = f"%{_escape_characters(v)}%"

                sq = (
                    self._db.querybuilder()
                    .from_(fulltext_t)
                    .select(fulltext_t.id)
                    .where(
                        self.EscapedLike(
                            fulltext_t.string_value,
                            ParameterValue(search_term),
                        )
                    )
                )
                return embeddings_t.id.isin(sq)
            else:
                raise ValueError(f"Unknown where_doc operator {k}")
        raise ValueError("Empty where_doc")


def _escape_characters(string: str) -> str:
    """Escape % and _ characters in a string with a backslash as they are reserved in
    LIKE clauses"""
    escaped_string = ""
    for char in string:
        if char == "%" or char == "_":
            escaped_string += "\\"
        escaped_string += char
    return escaped_string


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


def _where_clause(
    expr: Union[LiteralValue, Dict[WhereOperator, LiteralValue]],
    table: Table,
) -> Criterion:
    """Given a field name, an expression, and a table, construct a Pypika Criterion"""

    # Literal value case
    if isinstance(expr, (str, int, float)):
        return _where_clause({"$eq": expr}, table)

    # Operator dict case
    operator, value = next(iter(expr.items()))
    return _value_criterion(value, operator, table)


def _value_criterion(value: LiteralValue, op: WhereOperator, table: Table) -> Criterion:
    """Return a criterion to compare a value with the appropriate columns given its type
    and the operation type."""

    if isinstance(value, str):
        cols = [table.string_value]
    elif isinstance(value, int) and op in ("$eq", "$ne"):
        cols = [table.int_value]
    elif isinstance(value, float) and op in ("$eq", "$ne"):
        cols = [table.float_value]
    else:
        cols = [table.int_value, table.float_value]

    if op == "$eq":
        col_exprs = [col == ParameterValue(value) for col in cols]
    elif op == "$ne":
        col_exprs = [col != ParameterValue(value) for col in cols]
    elif op == "$gt":
        col_exprs = [col > ParameterValue(value) for col in cols]
    elif op == "$gte":
        col_exprs = [col >= ParameterValue(value) for col in cols]
    elif op == "$lt":
        col_exprs = [col < ParameterValue(value) for col in cols]
    elif op == "$lte":
        col_exprs = [col <= ParameterValue(value) for col in cols]

    if op == "$ne":
        return reduce(lambda x, y: x & y, col_exprs)
    else:
        return reduce(lambda x, y: x | y, col_exprs)

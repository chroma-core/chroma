from chromadb.db.base import SqlDB, ParameterValue, get_sql
from chromadb.ingest import (
    Producer,
    Consumer,
    encode_vector,
    decode_vector,
    ConsumerCallbackFn,
)
from chromadb.types import (
    SubmitEmbeddingRecord,
    EmbeddingRecord,
    SeqId,
    ScalarEncoding,
    Operation,
)
from chromadb.config import System
from overrides import override
from collections import defaultdict
from typing import Sequence, Tuple, Optional, Dict, Set, cast
from uuid import UUID
from pypika import Table, functions
import uuid
import json
import logging

logger = logging.getLogger(__name__)

_operation_codes = {
    Operation.ADD: 0,
    Operation.UPDATE: 1,
    Operation.UPSERT: 2,
    Operation.DELETE: 3,
}
_operation_codes_inv = {v: k for k, v in _operation_codes.items()}

# Set in conftest.py to rethrow errors in the "async" path during testing
# https://doc.pytest.org/en/latest/example/simple.html#detect-if-running-from-within-a-pytest-run
_called_from_test = False


class SqlEmbeddingsQueue(SqlDB, Producer, Consumer):
    """A SQL database that stores embeddings, allowing a traditional RDBMS to be used as
    the primary ingest queue and satisfying the top level Producer/Consumer interfaces.

    Note that this class is only suitable for use cases where the producer and consumer
    are in the same process.

    This is because notifiaction of new embeddings happens solely in-process: this
    implementation does not actively listen to the the database for new records added by
    other processes.
    """

    class Subscription:
        id: UUID
        topic_name: str
        start: int
        end: int
        callback: ConsumerCallbackFn

        def __init__(
            self,
            id: UUID,
            topic_name: str,
            start: int,
            end: int,
            callback: ConsumerCallbackFn,
        ):
            self.id = id
            self.topic_name = topic_name
            self.start = start
            self.end = end
            self.callback = callback

    _subscriptions: Dict[str, Set[Subscription]]
    _max_batch_size: Optional[int]
    # How many variables are in the insert statement for a single record
    VARIABLES_PER_RECORD = 6

    def __init__(self, system: System):
        self._subscriptions = defaultdict(set)
        self._max_batch_size = None
        super().__init__(system)

    @override
    def reset_state(self) -> None:
        super().reset_state()
        self._subscriptions = defaultdict(set)

    @override
    def create_topic(self, topic_name: str) -> None:
        # Topic creation is implicit for this impl
        pass

    @override
    def delete_topic(self, topic_name: str) -> None:
        t = Table("embeddings_queue")
        q = (
            self.querybuilder()
            .from_(t)
            .where(t.topic == ParameterValue(topic_name))
            .delete()
        )
        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            cur.execute(sql, params)

    @override
    def submit_embedding(
        self, topic_name: str, embedding: SubmitEmbeddingRecord
    ) -> SeqId:
        if not self._running:
            raise RuntimeError("Component not running")

        return self.submit_embeddings(topic_name, [embedding])[0]

    @override
    def submit_embeddings(
        self, topic_name: str, embeddings: Sequence[SubmitEmbeddingRecord]
    ) -> Sequence[SeqId]:
        if not self._running:
            raise RuntimeError("Component not running")

        if len(embeddings) == 0:
            return []

        if len(embeddings) > self.max_batch_size:
            raise ValueError(
                f"""
                Cannot submit more than {self.max_batch_size:,} embeddings at once.
                Please submit your embeddings in batches of size
                {self.max_batch_size:,} or less.
                """
            )

        t = Table("embeddings_queue")
        insert = (
            self.querybuilder()
            .into(t)
            .columns(t.operation, t.topic, t.id, t.vector, t.encoding, t.metadata)
        )
        id_to_idx: Dict[str, int] = {}
        for embedding in embeddings:
            (
                embedding_bytes,
                encoding,
                metadata,
            ) = self._prepare_vector_encoding_metadata(embedding)
            insert = insert.insert(
                ParameterValue(_operation_codes[embedding["operation"]]),
                ParameterValue(topic_name),
                ParameterValue(embedding["id"]),
                ParameterValue(embedding_bytes),
                ParameterValue(encoding),
                ParameterValue(metadata),
            )
            id_to_idx[embedding["id"]] = len(id_to_idx)
        with self.tx() as cur:
            sql, params = get_sql(insert, self.parameter_format())
            # The returning clause does not guarantee order, so we need to do reorder
            # the results. https://www.sqlite.org/lang_returning.html
            sql = f"{sql} RETURNING seq_id, id"  # Pypika doesn't support RETURNING
            results = cur.execute(sql, params).fetchall()
            # Reorder the results
            seq_ids = [cast(SeqId, None)] * len(
                results
            )  # Lie to mypy: https://stackoverflow.com/questions/76694215/python-type-casting-when-preallocating-list
            embedding_records = []
            for seq_id, id in results:
                seq_ids[id_to_idx[id]] = seq_id
                submit_embedding_record = embeddings[id_to_idx[id]]
                # We allow notifying consumers out of order relative to one call to
                # submit_embeddings so we do not reorder the records before submitting them
                embedding_record = EmbeddingRecord(
                    id=id,
                    seq_id=seq_id,
                    embedding=submit_embedding_record["embedding"],
                    encoding=submit_embedding_record["encoding"],
                    metadata=submit_embedding_record["metadata"],
                    operation=submit_embedding_record["operation"],
                )
                embedding_records.append(embedding_record)
            self._notify_all(topic_name, embedding_records)
            return seq_ids

    @override
    def subscribe(
        self,
        topic_name: str,
        consume_fn: ConsumerCallbackFn,
        start: Optional[SeqId] = None,
        end: Optional[SeqId] = None,
        id: Optional[UUID] = None,
    ) -> UUID:
        if not self._running:
            raise RuntimeError("Component not running")

        subscription_id = id or uuid.uuid4()
        start, end = self._validate_range(start, end)

        subscription = self.Subscription(
            subscription_id, topic_name, start, end, consume_fn
        )

        # Backfill first, so if it errors we do not add the subscription
        self._backfill(subscription)
        self._subscriptions[topic_name].add(subscription)

        return subscription_id

    @override
    def unsubscribe(self, subscription_id: UUID) -> None:
        for topic_name, subscriptions in self._subscriptions.items():
            for subscription in subscriptions:
                if subscription.id == subscription_id:
                    subscriptions.remove(subscription)
                    if len(subscriptions) == 0:
                        del self._subscriptions[topic_name]
                    return

    @override
    def min_seqid(self) -> SeqId:
        return -1

    @override
    def max_seqid(self) -> SeqId:
        return 2**63 - 1

    @property
    @override
    def max_batch_size(self) -> int:
        if self._max_batch_size is None:
            with self.tx() as cur:
                cur.execute("PRAGMA compile_options;")
                compile_options = cur.fetchall()

                for option in compile_options:
                    if "MAX_VARIABLE_NUMBER" in option[0]:
                        # The pragma returns a string like 'MAX_VARIABLE_NUMBER=999'
                        self._max_batch_size = int(option[0].split("=")[1]) // (
                            self.VARIABLES_PER_RECORD
                        )

                if self._max_batch_size is None:
                    # This value is the default for sqlite3 versions < 3.32.0
                    # It is the safest value to use if we can't find the pragma for some
                    # reason
                    self._max_batch_size = 999 // self.VARIABLES_PER_RECORD
        return self._max_batch_size

    def _prepare_vector_encoding_metadata(
        self, embedding: SubmitEmbeddingRecord
    ) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        if embedding["embedding"]:
            encoding_type = cast(ScalarEncoding, embedding["encoding"])
            encoding = encoding_type.value
            embedding_bytes = encode_vector(embedding["embedding"], encoding_type)
        else:
            embedding_bytes = None
            encoding = None
        metadata = json.dumps(embedding["metadata"]) if embedding["metadata"] else None
        return embedding_bytes, encoding, metadata

    def _backfill(self, subscription: Subscription) -> None:
        """Backfill the given subscription with any currently matching records in the
        DB"""
        t = Table("embeddings_queue")
        q = (
            self.querybuilder()
            .from_(t)
            .where(t.topic == ParameterValue(subscription.topic_name))
            .where(t.seq_id > ParameterValue(subscription.start))
            .where(t.seq_id <= ParameterValue(subscription.end))
            .select(t.seq_id, t.operation, t.id, t.vector, t.encoding, t.metadata)
            .orderby(t.seq_id)
        )
        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            cur.execute(sql, params)
            rows = cur.fetchall()
            for row in rows:
                if row[3]:
                    encoding = ScalarEncoding(row[4])
                    vector = decode_vector(row[3], encoding)
                else:
                    encoding = None
                    vector = None
                self._notify_one(
                    subscription,
                    [
                        EmbeddingRecord(
                            seq_id=row[0],
                            operation=_operation_codes_inv[row[1]],
                            id=row[2],
                            embedding=vector,
                            encoding=encoding,
                            metadata=json.loads(row[5]) if row[5] else None,
                        )
                    ],
                )

    def _validate_range(
        self, start: Optional[SeqId], end: Optional[SeqId]
    ) -> Tuple[int, int]:
        """Validate and normalize the start and end SeqIDs for a subscription using this
        impl."""
        start = start or self._next_seq_id()
        end = end or self.max_seqid()
        if not isinstance(start, int) or not isinstance(end, int):
            raise ValueError("SeqIDs must be integers for sql-based EmbeddingsDB")
        if start >= end:
            raise ValueError(f"Invalid SeqID range: {start} to {end}")
        return start, end

    def _next_seq_id(self) -> int:
        """Get the next SeqID for this database."""
        t = Table("embeddings_queue")
        q = self.querybuilder().from_(t).select(functions.Max(t.seq_id))
        with self.tx() as cur:
            cur.execute(q.get_sql())
            return int(cur.fetchone()[0]) + 1

    def _notify_all(self, topic: str, embeddings: Sequence[EmbeddingRecord]) -> None:
        """Send a notification to each subscriber of the given topic."""
        if self._running:
            for sub in self._subscriptions[topic]:
                self._notify_one(sub, embeddings)

    def _notify_one(
        self, sub: Subscription, embeddings: Sequence[EmbeddingRecord]
    ) -> None:
        """Send a notification to a single subscriber."""
        # Filter out any embeddings that are not in the subscription range
        should_unsubscribe = False
        filtered_embeddings = []
        for embedding in embeddings:
            if embedding["seq_id"] <= sub.start:
                continue
            if embedding["seq_id"] > sub.end:
                should_unsubscribe = True
                break
            filtered_embeddings.append(embedding)

        # Log errors instead of throwing them to preserve async semantics
        # for consistency between local and distributed configurations
        try:
            if len(filtered_embeddings) > 0:
                sub.callback(filtered_embeddings)
            if should_unsubscribe:
                self.unsubscribe(sub.id)
        except BaseException as e:
            logger.error(
                f"Exception occurred invoking consumer for subscription {sub.id.hex}"
                + f"to topic {sub.topic_name} %s",
                str(e),
            )
            if _called_from_test:
                raise e

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
from typing import Tuple, Optional, Dict, Set, cast
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

    def __init__(self, system: System):
        self._subscriptions = defaultdict(set)
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

        if embedding["embedding"]:
            encoding_type = cast(ScalarEncoding, embedding["encoding"])
            encoding = encoding_type.value
            embedding_bytes = encode_vector(embedding["embedding"], encoding_type)

        else:
            embedding_bytes = None
            encoding = None
        metadata = json.dumps(embedding["metadata"]) if embedding["metadata"] else None

        t = Table("embeddings_queue")
        insert = (
            self.querybuilder()
            .into(t)
            .columns(t.operation, t.topic, t.id, t.vector, t.encoding, t.metadata)
            .insert(
                ParameterValue(_operation_codes[embedding["operation"]]),
                ParameterValue(topic_name),
                ParameterValue(embedding["id"]),
                ParameterValue(embedding_bytes),
                ParameterValue(encoding),
                ParameterValue(metadata),
            )
        )
        with self.tx() as cur:
            sql, params = get_sql(insert, self.parameter_format())
            sql = f"{sql} RETURNING seq_id"  # Pypika doesn't support RETURNING
            seq_id = int(cur.execute(sql, params).fetchone()[0])
            embedding_record = EmbeddingRecord(
                id=embedding["id"],
                seq_id=seq_id,
                embedding=embedding["embedding"],
                encoding=embedding["encoding"],
                metadata=embedding["metadata"],
                operation=embedding["operation"],
            )
            self._notify_all(topic_name, embedding_record)
            return seq_id

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
                    EmbeddingRecord(
                        seq_id=row[0],
                        operation=_operation_codes_inv[row[1]],
                        id=row[2],
                        embedding=vector,
                        encoding=encoding,
                        metadata=json.loads(row[5]) if row[5] else None,
                    ),
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

    def _notify_all(self, topic: str, embedding: EmbeddingRecord) -> None:
        """Send a notification to each subscriber of the given topic."""
        if self._running:
            for sub in self._subscriptions[topic]:
                self._notify_one(sub, embedding)

    def _notify_one(self, sub: Subscription, embedding: EmbeddingRecord) -> None:
        """Send a notification to a single subscriber."""
        if embedding["seq_id"] > sub.end:
            self.unsubscribe(sub.id)
            return

        if embedding["seq_id"] <= sub.start:
            return

        # Log errors instead of throwing them to preserve async semantics
        # for consistency between local and distributed configurations
        try:
            sub.callback([embedding])
        except BaseException as e:
            id = embedding.get("id", embedding.get("delete_id"))
            logger.error(
                f"Exception occurred invoking consumer for subscription {sub.id}"
                + f"to topic {sub.topic_name} for embedding id {id} ",
                e,
            )

from chromadb.db.base import SqlDB, ParameterValue, get_sql
from chromadb.ingest import (
    Producer,
    Consumer,
    encode_vector,
    decode_vector,
    ConsumerCallbackFn,
)
from chromadb.types import (
    InsertEmbeddingRecord,
    EmbeddingRecord,
    DeleteEmbeddingRecord,
    SeqId,
    ScalarEncoding,
)
from chromadb.config import System
from overrides import override
from collections import defaultdict
from typing import Tuple, Optional, Dict, Set, Union
from uuid import UUID
import uuid
import json


class EmbeddingsDB(SqlDB, Producer, Consumer):
    """A SQL database that stores embeddings, allowing a traditional RDBMS to be used as
    the primary ingest queue and satisfying the top level Producer/Consumer interfaces.

    Note that this class is only suitable for use cases where the producer and consumer
    are in the same process.

    This is because notifiaction of new embeddings happens solely in-process, this
    implementation does not poll the database for new records added by other processes.
    """

    class Subscription:
        id: UUID
        topic_name: str
        start: int
        end: int
        callback: ConsumerCallbackFn

        def __init__(
            self, topic_name: str, start: int, end: int, callback: ConsumerCallbackFn
        ):
            self.topic_name = topic_name
            self.start = start
            self.end = end
            self.callback = callback

    _subscriptions: Dict[str, Set[Subscription]]
    _encoding_ids: Dict[ScalarEncoding, int]

    def __init__(self, system: System):
        self._subscriptions = defaultdict(set)

    @override
    def create_topic(self, topic_name: str) -> None:
        # Topic creation is implicit for this impl
        pass

    @override
    def delete_topic(self, topic_name: str) -> None:
        q = (
            self.querybuilder()
            .from_("embeddings")
            .where("topic", ParameterValue(topic_name))
            .delete()
        )
        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            cur.execute(sql, params)

    @override
    def submit_embedding(
        self, topic_name: str, embedding: InsertEmbeddingRecord, sync: bool = False
    ) -> None:
        embedding_bytes = encode_vector(embedding["embedding"], embedding["encoding"])
        metadata = json.dumps(embedding["metadata"]) if embedding["metadata"] else None

        insert = (
            self.querybuilder()
            .into("embeddings_log")
            .columns("topic", "id", "is_delete", "embedding", "encoding", "metadata")
            .insert(
                ParameterValue(topic_name),
                ParameterValue(embedding["id"]),
                False,
                ParameterValue(embedding_bytes),
                ParameterValue(embedding["encoding"].value),
                ParameterValue(metadata),
            )
        )
        with self.tx() as cur:
            sql, params = get_sql(insert, self.parameter_format())
            sql = f"{sql} RETURNING seq_id"  # Pypika doesn't support RETURNING
            seq_id = int(cur.execute(sql, params).fetchone()[0])
            # Notify within transaction, so if a synchronous notification
            # fails, the transaction will be rolled back.
            self._notify(
                topic_name,
                EmbeddingRecord(
                    id=embedding["id"],
                    seq_id=seq_id,
                    embedding=embedding["embedding"],
                    encoding=embedding["encoding"],
                    metadata=embedding["metadata"],
                ),
            )

    @override
    def submit_embedding_delete(
        self,
        topic_name: str,
        delete_embedding: DeleteEmbeddingRecord,
        sync: bool = False,
    ) -> None:
        insert = (
            self.querybuilder()
            .into("embeddings_log")
            .columns("topic", "id", "is_delete")
            .insert(
                ParameterValue(topic_name), ParameterValue(delete_embedding["id"]), True
            )
        )
        with self.tx() as cur:
            sql, params = get_sql(insert, self.parameter_format())
            cur.execute(sql, params)
            self._notify(topic_name, delete_embedding)

    @override
    def subscribe(
        self,
        topic_name: str,
        consume_fn: ConsumerCallbackFn,
        start: Optional[SeqId] = None,
        end: Optional[SeqId] = None,
        id: Optional[UUID] = None,
    ) -> UUID:
        subscription_id = id or uuid.uuid4()
        start, end = self._validate_range(start, end)

        subscription = self.Subscription(topic_name, start, end, consume_fn)
        self._subscriptions[topic_name].add(subscription)

        self._backfill(subscription)

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

    def _backfill(self, subscription: Subscription) -> None:
        """Backfill the given subscription with any currently matching records in the
        DB"""
        q = (
            self.querybuilder()
            .from_("embeddings_log")
            .where("topic", ParameterValue(subscription.topic_name))
            .where("seq_id", ">=", ParameterValue(subscription.start))
            .where("seq_id", "<=", ParameterValue(subscription.end))
            .select("seq_id", "id", "is_delete", "embedding", "encoding", "metadata")
            .order_by("seq_id")
        )
        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            cur.execute(sql, params)
            for row in cur.fetchall():
                if row[1]:
                    self._notify(
                        subscription.topic_name,
                        DeleteEmbeddingRecord(seq_id=row[0], id=row[1]),
                    )
                else:
                    encoding = ScalarEncoding(row[4])
                    self._notify(
                        subscription.topic_name,
                        EmbeddingRecord(
                            seq_id=row[0],
                            id=row[1],
                            embedding=decode_vector(row[3], encoding),
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
        end = end or 2**63 - 1
        if not isinstance(start, int) or not isinstance(end, int):
            raise ValueError("SeqIDs must be integers for sql-based EmbeddingsDB")
        if start <= end:
            raise ValueError(f"Invalid SeqID range: {start} to {end}")
        return start, end

    def _next_seq_id(self) -> int:
        """Get the next SeqID for this database."""
        q = self.querybuilder().select("max(seq_id)").from_("embeddings_log")
        with self.tx() as cur:
            cur.execute(q.get_sql())
            return int(cur.fetchone()[0]) + 1

    def _notify(
        self,
        topic: str,
        embedding: Union[EmbeddingRecord, DeleteEmbeddingRecord],
    ) -> None:
        for sub in self._subscriptions[topic]:
            if embedding["seq_id"] > sub.end:
                self.unsubscribe(sub.id)
                continue

            if embedding["seq_id"] < sub.start:
                continue

            sub.callback([embedding])

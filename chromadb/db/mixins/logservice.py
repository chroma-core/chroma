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
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from overrides import override
from collections import defaultdict
from typing import Sequence, Tuple, Optional, Dict, Set, cast
from uuid import UUID
from pypika import Table, functions
import uuid
import json
import logging

logger = logging.getLogger(__name__)


class LogService(Producer, Consumer):
    """
    Distributed Chroma Log Service
    """

    class Subscription:
        callback: ConsumerCallbackFn

        def __init__(
            self,
            callback: ConsumerCallbackFn,
        ):
            self.callback = callback

    _subscriptions: Dict[str, Set[Subscription]]
    _max_batch_size: Optional[int]

    def __init__(self, system: System):
        self._subscriptions = defaultdict(set)
        self._opentelemetry_client = system.require(OpenTelemetryClient)
        super().__init__(system)

    @trace_method("LogService.reset_state", OpenTelemetryGranularity.ALL)
    @override
    def reset_state(self) -> None:
        super().reset_state()
        self._subscriptions = defaultdict(set)

    @override
    def create_topic(self, topic_name: str) -> None:
        raise RuntimeError("create topic not supported for LogService")

    @trace_method("LogService.delete_topic", OpenTelemetryGranularity.ALL)
    @override
    def delete_topic(self, topic_name: str) -> None:
        raise RuntimeError("delete topic not supported for LogService")

    @trace_method("LogService.submit_embedding", OpenTelemetryGranularity.ALL)
    @override
    def submit_embedding(
        self, topic_name: str, embedding: SubmitEmbeddingRecord
    ) -> SeqId:
        if not self._running:
            raise RuntimeError("Component not running")

        return self.submit_embeddings(topic_name, [embedding])[0]

    @trace_method("LogService.submit_embeddings", OpenTelemetryGranularity.ALL)
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

        # TODO: push records to the log service

        # TODO:why need seq id?
        return 0

    @trace_method("LogService.subscribe", OpenTelemetryGranularity.ALL)
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

    @trace_method("LogService.unsubscribe", OpenTelemetryGranularity.ALL)
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
    @trace_method("LogService.max_batch_size", OpenTelemetryGranularity.ALL)
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

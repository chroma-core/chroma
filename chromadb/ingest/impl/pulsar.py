from __future__ import annotations
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple
import uuid
from chromadb.config import Settings, System
from chromadb.ingest import Consumer, ConsumerCallbackFn, Producer
from overrides import overrides, EnforceOverrides
from uuid import UUID
from chromadb.ingest.impl.pulsar_admin import PulsarAdmin
from chromadb.ingest.impl.utils import create_pulsar_connection_str
from chromadb.proto.convert import from_proto_submit, to_proto_submit
import chromadb.proto.chroma_pb2 as proto
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.types import SeqId, SubmitEmbeddingRecord
import pulsar
from concurrent.futures import wait, Future

from chromadb.utils.messageid import int_to_pulsar, pulsar_to_int


class PulsarProducer(Producer, EnforceOverrides):
    # TODO: ensure trace context propagates
    _connection_str: str
    _topic_to_producer: Dict[str, pulsar.Producer]
    _opentelemetry_client: OpenTelemetryClient
    _client: pulsar.Client
    _admin: PulsarAdmin
    _settings: Settings

    def __init__(self, system: System) -> None:
        pulsar_host = system.settings.require("pulsar_broker_url")
        pulsar_port = system.settings.require("pulsar_broker_port")
        self._connection_str = create_pulsar_connection_str(pulsar_host, pulsar_port)
        self._topic_to_producer = {}
        self._settings = system.settings
        self._admin = PulsarAdmin(system)
        self._opentelemetry_client = system.require(OpenTelemetryClient)
        super().__init__(system)

    @overrides
    def start(self) -> None:
        self._client = pulsar.Client(self._connection_str)
        super().start()

    @overrides
    def stop(self) -> None:
        self._client.close()
        super().stop()

    @overrides
    def create_topic(self, topic_name: str) -> None:
        self._admin.create_topic(topic_name)

    @overrides
    def delete_topic(self, topic_name: str) -> None:
        self._admin.delete_topic(topic_name)

    @trace_method("PulsarProducer.submit_embedding", OpenTelemetryGranularity.ALL)
    @overrides
    def submit_embedding(
        self, topic_name: str, embedding: SubmitEmbeddingRecord
    ) -> SeqId:
        """Add an embedding record to the given topic. Returns the SeqID of the record."""
        producer = self._get_or_create_producer(topic_name)
        proto_submit: proto.SubmitEmbeddingRecord = to_proto_submit(embedding)
        # TODO: batch performance / async
        msg_id: pulsar.MessageId = producer.send(proto_submit.SerializeToString())
        return pulsar_to_int(msg_id)

    @trace_method("PulsarProducer.submit_embeddings", OpenTelemetryGranularity.ALL)
    @overrides
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

        producer = self._get_or_create_producer(topic_name)
        protos_to_submit = [to_proto_submit(embedding) for embedding in embeddings]

        def create_producer_callback(
            future: Future[int],
        ) -> Callable[[Any, pulsar.MessageId], None]:
            def producer_callback(res: Any, msg_id: pulsar.MessageId) -> None:
                if msg_id:
                    future.set_result(pulsar_to_int(msg_id))
                else:
                    future.set_exception(
                        Exception(
                            "Unknown error while submitting embedding in producer_callback"
                        )
                    )

            return producer_callback

        futures = []
        for proto_to_submit in protos_to_submit:
            future: Future[int] = Future()
            producer.send_async(
                proto_to_submit.SerializeToString(),
                callback=create_producer_callback(future),
            )
            futures.append(future)

        wait(futures)

        results: List[SeqId] = []
        for future in futures:
            exception = future.exception()
            if exception is not None:
                raise exception
            results.append(future.result())

        return results

    @property
    @overrides
    def max_batch_size(self) -> int:
        # For now, we use 1,000
        # TODO: tune this to a reasonable value by default
        return 1000

    def _get_or_create_producer(self, topic_name: str) -> pulsar.Producer:
        if topic_name not in self._topic_to_producer:
            producer = self._client.create_producer(topic_name)
            self._topic_to_producer[topic_name] = producer
        return self._topic_to_producer[topic_name]

    @overrides
    def reset_state(self) -> None:
        if not self._settings.require("allow_reset"):
            raise ValueError(
                "Resetting the database is not allowed. Set `allow_reset` to true in the config in tests or other non-production environments where reset should be permitted."
            )
        for topic_name in self._topic_to_producer:
            self._admin.delete_topic(topic_name)
        self._topic_to_producer = {}
        super().reset_state()


class PulsarConsumer(Consumer, EnforceOverrides):
    class PulsarSubscription:
        id: UUID
        topic_name: str
        start: int
        end: int
        callback: ConsumerCallbackFn
        consumer: pulsar.Consumer

        def __init__(
            self,
            id: UUID,
            topic_name: str,
            start: int,
            end: int,
            callback: ConsumerCallbackFn,
            consumer: pulsar.Consumer,
        ):
            self.id = id
            self.topic_name = topic_name
            self.start = start
            self.end = end
            self.callback = callback
            self.consumer = consumer

    _connection_str: str
    _client: pulsar.Client
    _opentelemetry_client: OpenTelemetryClient
    _subscriptions: Dict[str, Set[PulsarSubscription]]
    _settings: Settings

    def __init__(self, system: System) -> None:
        pulsar_host = system.settings.require("pulsar_broker_url")
        pulsar_port = system.settings.require("pulsar_broker_port")
        self._connection_str = create_pulsar_connection_str(pulsar_host, pulsar_port)
        self._subscriptions = defaultdict(set)
        self._settings = system.settings
        self._opentelemetry_client = system.require(OpenTelemetryClient)
        super().__init__(system)

    @overrides
    def start(self) -> None:
        self._client = pulsar.Client(self._connection_str)
        super().start()

    @overrides
    def stop(self) -> None:
        self._client.close()
        super().stop()

    @trace_method("PulsarConsumer.subscribe", OpenTelemetryGranularity.ALL)
    @overrides
    def subscribe(
        self,
        topic_name: str,
        consume_fn: ConsumerCallbackFn,
        start: Optional[SeqId] = None,
        end: Optional[SeqId] = None,
        id: Optional[UUID] = None,
    ) -> UUID:
        """Register a function that will be called to recieve embeddings for a given
        topic. The given function may be called any number of times, with any number of
        records, and may be called concurrently.

        Only records between start (exclusive) and end (inclusive) SeqIDs will be
        returned. If start is None, the first record returned will be the next record
        generated, not including those generated before creating the subscription. If
        end is None, the consumer will consume indefinitely, otherwise it will
        automatically be unsubscribed when the end SeqID is reached.

        If the function throws an exception, the function may be called again with the
        same or different records.

        Takes an optional UUID as a unique subscription ID. If no ID is provided, a new
        ID will be generated and returned."""
        if not self._running:
            raise RuntimeError("Consumer must be started before subscribing")

        subscription_id = (
            id or uuid.uuid4()
        )  # TODO: this should really be created by the coordinator and stored in sysdb

        start, end = self._validate_range(start, end)

        def wrap_callback(consumer: pulsar.Consumer, message: pulsar.Message) -> None:
            msg_data = message.data()
            msg_id = pulsar_to_int(message.message_id())
            submit_embedding_record = proto.SubmitEmbeddingRecord()
            proto.SubmitEmbeddingRecord.ParseFromString(
                submit_embedding_record, msg_data
            )
            embedding_record = from_proto_submit(submit_embedding_record, msg_id)
            consume_fn([embedding_record])
            consumer.acknowledge(message)
            if msg_id == end:
                self.unsubscribe(subscription_id)

        consumer = self._client.subscribe(
            topic_name,
            subscription_id.hex,
            message_listener=wrap_callback,
        )

        subscription = self.PulsarSubscription(
            subscription_id, topic_name, start, end, consume_fn, consumer
        )
        self._subscriptions[topic_name].add(subscription)

        # NOTE: For some reason the seek() method expects a shadowed MessageId type
        # which resides in _msg_id.
        consumer.seek(int_to_pulsar(start)._msg_id)

        return subscription_id

    def _validate_range(
        self, start: Optional[SeqId], end: Optional[SeqId]
    ) -> Tuple[int, int]:
        """Validate and normalize the start and end SeqIDs for a subscription using this
        impl."""
        start = start or pulsar_to_int(pulsar.MessageId.latest)
        end = end or self.max_seqid()
        if not isinstance(start, int) or not isinstance(end, int):
            raise TypeError("SeqIDs must be integers")
        if start >= end:
            raise ValueError(f"Invalid SeqID range: {start} to {end}")
        return start, end

    @overrides
    def unsubscribe(self, subscription_id: UUID) -> None:
        """Unregister a subscription. The consume function will no longer be invoked,
        and resources associated with the subscription will be released."""
        for topic_name, subscriptions in self._subscriptions.items():
            for subscription in subscriptions:
                if subscription.id == subscription_id:
                    subscription.consumer.close()
                    subscriptions.remove(subscription)
                    if len(subscriptions) == 0:
                        del self._subscriptions[topic_name]
                    return

    @overrides
    def min_seqid(self) -> SeqId:
        """Return the minimum possible SeqID in this implementation."""
        return pulsar_to_int(pulsar.MessageId.earliest)

    @overrides
    def max_seqid(self) -> SeqId:
        """Return the maximum possible SeqID in this implementation."""
        return 2**192 - 1

    @overrides
    def reset_state(self) -> None:
        if not self._settings.require("allow_reset"):
            raise ValueError(
                "Resetting the database is not allowed. Set `allow_reset` to true in the config in tests or other non-production environments where reset should be permitted."
            )
        for topic_name, subscriptions in self._subscriptions.items():
            for subscription in subscriptions:
                subscription.consumer.close()
        self._subscriptions = defaultdict(set)
        super().reset_state()

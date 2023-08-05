from typing import Dict, Optional
import uuid
from chromadb.config import System
from chromadb.ingest import Consumer, ConsumerCallbackFn, Producer
from overrides import overrides, EnforceOverrides
from uuid import UUID
from chromadb.ingest.impl.pulsar_admin import PulsarAdmin
from chromadb.ingest.impl.utils import create_pulsar_connection_str
from chromadb.proto.convert import from_proto_submit, to_proto_submit
import chromadb.proto.chroma_pb2 as proto
from chromadb.types import SeqId, SubmitEmbeddingRecord
import pulsar

from chromadb.utils.messageid import pulsar_to_int


class PulsarProducer(Producer, EnforceOverrides):
    _connection_str: str
    _topic_to_producer: Dict[str, pulsar.Producer]
    _client: pulsar.Client
    _admin: PulsarAdmin

    def __init__(self, system: System) -> None:
        pulsar_host = system.settings.require("pulsar_broker_url")
        pulsar_port = system.settings.require("pulsar_broker_port")
        self._connection_str = create_pulsar_connection_str(pulsar_host, pulsar_port)
        self._topic_to_producer = {}
        self._admin = PulsarAdmin(system)
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
        # TODO: determine how to implement this given that pulsar doesn't support deleting topics without
        # using admin api
        pass

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

    def _get_or_create_producer(self, topic_name: str) -> pulsar.Producer:
        if topic_name not in self._topic_to_producer:
            producer = self._client.create_producer(topic_name)
            self._topic_to_producer[topic_name] = producer
        return self._topic_to_producer[topic_name]


class PulsarConsumer(Consumer, EnforceOverrides):
    _connection_str: str
    _client: pulsar.Client

    def __init__(self, system: System) -> None:
        pulsar_host = system.settings.require("pulsar_broker_url")
        pulsar_port = system.settings.require("pulsar_broker_port")
        self._connection_str = create_pulsar_connection_str(pulsar_host, pulsar_port)
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
        print("subscribing")
        if not self._running:
            raise RuntimeError("Consumer must be started before subscribing")

        subscription_id = (
            id or uuid.uuid4()
        )  # TODO: this should really be created by the coordinator and stored in sysdb

        # start, end = self._validate_range(start, end)

        # subscription = self.Subscription(
        #     subscription_id, topic_name, start, end, consume_fn
        # )

        # # Backfill first, so if it errors we do not add the subscription
        # self._backfill(subscription)
        # self._subscriptions[topic_name].add(subscription)

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
                consumer.close()

        consumer = self._client.subscribe(
            topic_name,
            subscription_id.hex,
            message_listener=wrap_callback,
        )
        if start is None:
            # TODO: race condition between subscribing and seeking?
            consumer.seek(pulsar.MessageId.earliest)

        return subscription_id

    @overrides
    def unsubscribe(self, subscription_id: UUID) -> None:
        """Unregister a subscription. The consume function will no longer be invoked,
        and resources associated with the subscription will be released."""
        pass

    @overrides
    def min_seqid(self) -> SeqId:
        """Return the minimum possible SeqID in this implementation."""
        return -1

    @overrides
    def max_seqid(self) -> SeqId:
        """Return the maximum possible SeqID in this implementation."""
        return 2**191 - 1

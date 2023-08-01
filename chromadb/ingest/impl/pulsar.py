from typing import Dict
from chromadb.config import System
from chromadb.ingest import Producer
from overrides import overrides, EnforceOverrides
from chromadb.proto.convert import to_proto_submit
import chromadb.proto.chroma_pb2 as proto
from chromadb.types import SeqId, SubmitEmbeddingRecord
import pulsar

from chromadb.utils.messageid import pulsar_to_int


class PulsarProducer(Producer, EnforceOverrides):
    _connection_str: str
    _topic_to_producer: Dict[str, pulsar.Producer]
    _client: pulsar.Client

    def __init__(self, system: System) -> None:
        pulsar_host = system.settings.require("pulsar_broker_url")
        pulsar_port = system.settings.require("pulsar_broker_port")
        self._connection_str = _create_pulsar_connection_str(pulsar_host, pulsar_port)
        self._topic_to_producer = {}
        super().__init__(system)

    @overrides
    def start(self) -> None:
        self._client = pulsar.Client(self._connection_str)

    @overrides
    def stop(self) -> None:
        self._client.close()

    @overrides
    def create_topic(self, topic_name: str) -> None:
        self._get_or_create_producer(topic_name)

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
        producer = self._topic_to_producer[topic_name]
        proto_submit: proto.SubmitEmbeddingRecord = to_proto_submit(embedding)
        # TODO: batch performance?
        msg_id: pulsar.MessageId = producer.send(proto_submit.SerializeToString())
        return pulsar_to_int(msg_id)

    def _get_or_create_producer(self, topic_name: str) -> pulsar.Producer:
        if topic_name not in self._topic_to_producer:
            producer = self._client.create_producer(topic_name)
            self._topic_to_producer[topic_name] = producer
        return self._topic_to_producer[topic_name]


def _create_pulsar_connection_str(host: str, port: str) -> str:
    return f"pulsar://{host}:{port}"

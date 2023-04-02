from chromadb.types import Topic, EmbeddingRecord, InsertEmbeddingRecord
from chromadb.ingest import Producer, proto_insert, proto_delete
import pulsar
import pulsar.schema as schema


class PulsarIngest(Producer):
    def __init__(self, settings) -> None:
        settings.validate("pulsar_host")
        settings.validate("pulsar_port")
        self._settings = settings
        self._client = pulsar.Client(f"pulsar://{settings.pulsar_host}:{settings.pulsar_port}")
        self._producers = {}

    def create_topic(self, topic: Topic) -> None:
        # Topic creation can be implicit, for now
        pass

    def delete_topic(self, topic_name: str) -> None:
        pass

    def submit_embedding(self, topic_name: str, embedding: InsertEmbeddingRecord) -> None:
        pb = proto_insert(embedding)
        self._producer(topic_name).send(pb.SerializeToString())

    def submit_embedding_delete(self, topic_name: str, id: str) -> None:
        pb = proto_delete(id)
        self._producer(topic_name).send(pb.SerializeToString())

    def _producer(self, topic):
        if topic not in self._producers:
            self._producers[topic] = self._client.create_producer(topic)
        return self._producers[topic]

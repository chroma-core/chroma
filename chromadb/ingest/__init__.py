from abc import ABC, abstractmethod
import pulsar.schema as schema


class AddEmbedding(schema.Record):
    _avro_namespace_ = 'chromadb.ingest'
    id = schema.String()
    embedding = schema.Array(schema.Float())
    metadata = schema.Map(schema.String(), schema.String())
    update = schema.Boolean()


class DeleteEmbedding(schema.Record):
    _avro_namespace_ = 'chromadb.ingest'
    id = schema.String()


class Message(schema.Record):
    _avro_namespace_ = 'chromadb.ingest'
    messages = Schema.Array(schema.Union([AddEmbedding, DeleteEmbedding]))


class Stream(ABC):
    """Base class for all ingest stream types"""

    @abstractmethod
    def submit(self, topic: str, message: Message):
        """Add the message to the given topic. Returns True if the
        messages were successfully submitted, or throws an execption
        otherwise."""


from abc import ABC, abstractmethod
from typing import Union
from chromadb.segment import InsertType
import pulsar.schema as schema


class InsertEmbedding(schema.Record):
    _avro_namespace_ = 'chromadb.ingest'
    id = schema.String()
    embedding = schema.Array(schema.Float())
    metadata = schema.Map(schema.String(), schema.String())
    insert_type = schema.CustomEnum(InsertType, default=InsertType.ADD_OR_UPDATE)


class DeleteEmbedding(schema.Record):
    _avro_namespace_ = 'chromadb.ingest'
    id = schema.String()


class Stream(ABC):
    """Base class for all ingest stream types"""

    @abstractmethod
    def submit(self, topic: str, message: Union[InsertEmbedding, DeleteEmbedding]):
        """Add the message to the given topic. Returns if the messages
        were successfully submitted, or throws an execption
        otherwise.
        """


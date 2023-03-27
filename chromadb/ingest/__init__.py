from abc import ABC, abstractmethod
from typing import Union, Optional
from chromadb.types import Topic, EmbeddingRecord, InsertType, ScalarEncoding, Vector
import pulsar.schema as schema
import array


class InsertEmbedding(schema.Record):
    _avro_namespace_ = "chromadb.ingest"
    id = schema.String()
    embedding = schema.Bytes()
    dimension = schema.Integer()
    encoding = schema.CustomEnum(ScalarEncoding)
    metadata = schema.Map(schema.String())
    insert_type = schema.CustomEnum(InsertType, default=InsertType.ADD_OR_UPDATE)


class DeleteEmbedding(schema.Record):
    _avro_namespace_ = "chromadb.ingest"
    id = schema.String()


def encode_vector(vector: Vector, encoding: ScalarEncoding = ScalarEncoding.FLOAT32) -> bytes:
    """Encode a vector into a byte array."""

    if encoding == ScalarEncoding.FLOAT32:
        return array.array("f", vector).tobytes()
    elif encoding == ScalarEncoding.INT32:
        return array.array("i", vector).tobytes()
    else:
        raise ValueError(f"Unsupported encoding: {encoding.value}")


def avro_insert(
    embedding: EmbeddingRecord, insert_type: InsertType, encoding: Optional[ScalarEncoding] = None
) -> InsertEmbedding:
    """Return an Avro record for an embedding insert."""

    if encoding is None:
        if isinstance(embedding["embedding"][0], int):
            encoding = ScalarEncoding.INT32
        elif isinstance(embedding["embedding"][0], float):
            encoding = ScalarEncoding.FLOAT32
        else:
            raise ValueError(
                f"Unsupported scalar type for embedding: {type(embedding['embedding'][0])}"
            )

    return InsertEmbedding(
        id=embedding["id"],
        embedding=encode_vector(embedding["embedding"], encoding=encoding),
        dimension=len(embedding["embedding"]),
        encoding=encoding,
        metadata=embedding["metadata"],
        insert_type=insert_type,
    )


def avro_delete(id: str) -> DeleteEmbedding:
    """Return an Avro record for an embedding delete."""

    return DeleteEmbedding(id=id)


class Ingest(ABC):
    """Base class for all ingest types"""

    @abstractmethod
    def create_topic(self, topic: Topic) -> None:
        pass

    @abstractmethod
    def delete_topic(self, topic_name: str) -> None:
        pass

    @abstractmethod
    def submit_embedding(
        self, topic_name: str, embedding: EmbeddingRecord, insert_type: InsertType
    ) -> None:
        """Add an embedding record to the given topic."""
        pass

    @abstractmethod
    def submit_embedding_delete(self, topic_name: str, id: str) -> None:
        """Add an embedding deletion record (soft delete) to the given topic."""
        pass

    @abstractmethod
    def reset(self):
        """Delete all topics and data. For testing only, implementations intended for production
        may throw an exception instead of implementing this method."""
        pass

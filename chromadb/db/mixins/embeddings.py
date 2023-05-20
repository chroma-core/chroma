from chromadb.db.base import SqlDB, ParameterValue, get_sql
from chromadb.ingest import Producer, Consumer, get_encoding, encode_vector
from chromadb.types import InsertEmbeddingRecord
from overrides import override
from typing import Any, List, Tuple


class EmbeddingsDB(SqlDB, Producer, Consumer):
    """A SQL database that stores embeddings, allowing a traditional RDBMS to be used as
    the primary ingest queue and satisfying the top level Producer/Consumer interfaces.
    """

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
        self._write_embedding(topic_name, embedding)

    @override
    def submit_embedding_delete(
        self, topic_name: str, id: str, sync: bool = False
    ) -> None:
        self._delete_embedding(topic_name, id)

    def _write_embedding(
        self, topic_name: str, embedding: InsertEmbeddingRecord
    ) -> int:
        embedding_bytes = encode_vector(embedding["embedding"], get_encoding(embedding))

        insert = (
            self.querybuilder()
            .into("embeddings")
            .columns("topic", "id", "embedding", "metadata")
            .insert(
                ParameterValue(topic_name),
                ParameterValue(embedding["id"]),
                ParameterValue(embedding_bytes),
            )
            .returning("seq_id")
        )

        insert_metdatadata = (
            self.querybuilder()
            .into("embedding_metadata")
            .columns("embedding", "key", "value_string", "value_int", "value_float")
            .insert(
                self.param(0),
                self.param(1),
                self.param(2),
                self.param(3),
                self.param(4),
            )
        )

        with self.tx() as cur:
            sql, params = get_sql(insert, self.parameter_format())
            seq_id = int(cur.execute(sql, params).fetchone()[0])

            if embedding["metadata"]:
                metadata_params: List[Tuple[Any, ...]] = []
                for k, v in embedding["metadata"].items():
                    if isinstance(v, str):
                        metadata_params.append((seq_id, k, v, None, None))
                    elif isinstance(v, int):
                        metadata_params.append((seq_id, k, None, v, None))
                    elif isinstance(v, float):
                        metadata_params.append((seq_id, k, None, None, v))

                cur.executemany(insert_metdatadata.get_sql(), metadata_params)

            return seq_id

    def _delete_embedding(self, topic_name: str, id: str) -> None:
        q = (
            self.querybuilder()
            .from_("embeddings")
            .where("topic", ParameterValue(topic_name))
            .where("id", ParameterValue(id))
            .delete()
        )
        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            cur.execute(sql, params)

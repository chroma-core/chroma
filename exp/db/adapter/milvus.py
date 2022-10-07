import numpy

from collections import defaultdict
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility
from sqlalchemy import desc

from embedding import Embedding


class Milvus:
    training_embeddings = defaultdict(list)
    prod_embeddings = []

    stage = defaultdict(list)

    def init_db(self, scratch_path):
        connections.connect(alias="default", host="localhost", port=19530)

        for collection in utility.list_collections():
            print(f"Drop collection: {collection}")
            utility.drop_collection(collection)

    def ingest_prod(self, embedding):
        self.prod_embeddings.append(embedding)

    def ingest_training(self, embedding:Embedding):
        self.stage[embedding.model].append(embedding)

    def commit(self):
        stage = self.stage
        self.stage = {}

        for model, embeddings in stage.items():
            collection = self.embeddings_collection(model=model, width=embeddings[0].width)
            data = [
                [str(e.inference) for e in embeddings],
                [0.0] * len(embeddings),
                [e.data for e in embeddings],
            ]
            try:
                result = collection.insert(data, "train")
            except Exception as e:
                print(f"Embedding: {embeddings[0]}")
                print(f"Data: {data[0]}")
                print(f"Collection: {collection.schema}")
                print(f"Failed insert: {e}")
                raise
            if result.insert_count != len(data[0]):
                print(f"Failed insert: {result}")

    def training_counts(self):
        return [(cat, len(embeds)) for cat, embeds in self.training_embeddings.items()]

    def categories(self):
        return self.training_embeddings.keys()

    def embeddings_for_category(self, category):
        return [e.data for e in self.training_embeddings[category]]

    def embeddings_collection(self, model:str, width:int):
        if utility.has_collection(model):
            return Collection(model)

        collection = Collection(
            name=model,
            schema=CollectionSchema(
                fields=[
                    FieldSchema(
                        name="embedding_id",
                        dtype=DataType.INT64,
                        is_primary=True,
                        auto_id=True,
                    ),
                    FieldSchema(
                        name="inference",
                        dtype=DataType.VARCHAR,
                        max_length=200,
                    ),
                    FieldSchema(
                        name="distance",
                        dtype=DataType.DOUBLE,
                    ),
                    FieldSchema(
                        name="embeddings",
                        dtype=DataType.FLOAT_VECTOR,
                        dim=width,
                    ),
                ],
                description=f"Embeddings {model} {width}"
            ),
            using='default',
            shards_num=2,
        )
        collection.create_partition("train")
        collection.create_partition("prod")
        return collection


if __name__ == "__main__":
    test = Milvus()
    test.init_db("/tmp")
    print("init")
    collection = test.embeddings_collection(name="model1", width=3)
    data = [
        ['inf'],
        [1.1],
        [[1.1, 2.2, 3.3]], 
    ]
    mr = collection.insert(data, "train")
    print(f"mr: {mr}")
    # test.ingest_training()

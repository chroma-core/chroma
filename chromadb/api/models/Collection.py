# collection class
class Collection:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def __repr__(self):
        return f"Collection(name={self.name})"

    def __dict__(self):
        return {
            "name": self.name,
        }

    def count(self):
        return self.client.count(collection_name=self.name)

    def add(self, embeddings, metadatas=None, documents=None, ids=None):
        return self.client.add(self.name, embeddings, metadatas, documents, ids)

    def get(self, ids=None, where=None, sort=None, limit=None, offset=None):
        return self.client.get(self.name, ids, where, sort, limit, offset)

    def peek(self, limit=None):
        return self.client.peek(self.name, limit)

    def query(self, query_embeddings, n_results=10, where={}):
        return self.client.query(
            collection_name=self.name,
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
        )

    def delete(self, ids=None, where=None):
        return self.client.delete(self.name, ids, where)

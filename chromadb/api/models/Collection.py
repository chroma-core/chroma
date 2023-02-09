# collection class
class Collection:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def __repr__(self):
        return f"Collection(name={self.name})"

    def count(self):
        return self.client.count(collection_name=self.name)

    def add(self, embeddings, metadatas=None):
        return self.client.add(self.name,embeddings,metadatas=None)
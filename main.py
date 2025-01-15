import chromadb

if __name__ == '__main__':
    client = chromadb.Client()
    col = client.create_collection(name="test", metadata={"hnsw:search_ef": 100, "hnsw:construction_ef": 1000})

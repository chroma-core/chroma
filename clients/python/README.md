<p align="center">
  <a href="https://trychroma.com"><img src="https://user-images.githubusercontent.com/891664/227103090-6624bf7d-9524-4e05-9d2c-c28d5d451481.png" alt="Chroma logo"></a>
</p>

<p align="center">
    <b>Chroma - the open-source embedding database</b>. <br />
    This package is for the Python HTTP client-only library for Chroma. This client connects to the Chroma Server. If that it not what you are looking for, you might want to check out the <a href="https://github.com/chroma-core/chroma ">full library</a>.
</p>


```bash
pip install chromadb-client # python http-client only library
```

To connect to your server and perform operations using the client only library, you can do the following:

```python
import chromadb
# Example setup of the client to connect to your chroma server
client = chromadb.HttpClient(host="localhost", port=8000)

collection = client.create_collection("all-my-documents")

collection.add(
    documents=["This is document1", "This is document2"],
    metadatas=[{"source": "notion"}, {"source": "google-docs"}], # filter on these!
    ids=["doc1", "doc2"], # unique for each doc
    embeddings = [[1.2, 2.1, ...], [1.2, 2.1, ...]]
)

results = collection.query(
    query_texts=["This is a query document"],
    n_results=2,
    # where={"metadata_field": "is_equal_to_this"}, # optional filter
    # where_document={"$contains":"search_string"}  # optional filter
)
```
## License

[Apache 2.0](./LICENSE)

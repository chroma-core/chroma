# Query and Get Data from Chroma Collections

Chroma collections can be queried in a variety of ways, using the `.query` method.

You can query by a set of `query embeddings`.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    n_results=10,
    where={"metadata_field": "is_equal_to_this"},
    where_document={"$contains":"search_string"},
    ids=["id1", "id2", ...]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
const result = await collection.query({
    queryEmbeddings: [[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    nResults: 10,
    where: {"metadata_field": "is_equal_to_this"},
    whereDocument: {"$contains": "search_string"},
    ids: ["id1", "id2", ...]
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

The query will return the `n results` closest matches to each `query embedding`, in order.
An optional `where` filter dictionary can be supplied to filter by the `metadata` associated with each document.
Additionally, an optional `where document` filter dictionary can be supplied to filter by contents of the document.
An optional `ids` list can be provided to filter results to only include documents with those specific IDs before performing the query.

If the supplied `query embeddings` are not the same dimension as the collection, an exception will be raised.

You can also query by a set of `query texts`. Chroma will first embed each `query text` with the collection's embedding function, and then perform the query with the generated embedding.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.query(
    query_texts=["doc10", "thus spake zarathustra", ...],
    n_results=10,
    where={"metadata_field": "is_equal_to_this"},
    where_document={"$contains":"search_string"},
    ids=["id1", "id2", ...]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.query({
    queryTexts: ["doc10", "thus spake zarathustra", ...],
    nResults: 10,
    where: {"metadata_field": "is_equal_to_this"},
    whereDocument: {"$contains": "search_string"},
    ids: ["id1", "id2", ...]
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

You can also retrieve items from a collection by `id` using `.get`.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.get(
	ids=["id1", "id2", "id3", ...],
	where={"style": "style1"}
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.get( {
    ids: ["id1", "id2", "id3", ...],
    where: {"style": "style1"}
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

`.get` also supports the `where` and `where document` filters. If no `ids` are supplied, it will return all items in the collection that match the `where` and `where document` filters.

### Choosing Which Data is Returned

When using get or query you can use the `include` parameter to specify which data you want returned - any of `embeddings`, `documents`, `metadatas`, and for query, `distances`. By default, Chroma will return the `documents`, `metadatas` and in the case of query, the `distances` of the results. `embeddings` are excluded by default for performance and the `ids` are always returned. You can specify which of these you want returned by passing an array of included field names to the includes parameter of the query or get method. Note that embeddings will be returned as a 2-d numpy array in `.get` and a python list of 2-d numpy arrays in `.query`.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Only get documents and ids
collection.get(
    include=["documents"]
)

collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    include=["documents"]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Only get documents and ids
await collection.get({
    include: ["documents"]
})

await collection.query({
    query_embeddings: [[11.1, 12.1, 13.1], [1.1, 2.3, 3.2], ...],
    include: ["documents"]
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

# Adding Data to Chroma Collections

Add data to Chroma with `.add`.

Raw documents:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.add(
    documents=["lorem ipsum...", "doc2", "doc3", ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    ids=["id1", "id2", "id3", ...]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.add({
    ids: ["id1", "id2", "id3", ...],
    metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents: ["lorem ipsum...", "doc2", "doc3", ...],
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

If Chroma is passed a list of `documents`, it will automatically tokenize and embed them with the collection's embedding function (the default will be used if none was supplied at collection creation). Chroma will also store the `documents` themselves. If the documents are too large to embed using the chosen embedding function, an exception will be raised.

Each document must have a unique associated `id`. Trying to `.add` the same ID twice will result in only the initial value being stored. An optional list of `metadata` dictionaries can be supplied for each document, to store additional information and enable filtering.

Alternatively, you can supply a list of document-associated `embeddings` directly, and Chroma will store the associated documents without embedding them itself.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.add(
    documents=["doc1", "doc2", "doc3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    ids=["id1", "id2", "id3", ...]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.add({
    ids: ["id1", "id2", "id3", ...],
    embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents: ["lorem ipsum...", "doc2", "doc3", ...],
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

If the supplied `embeddings` are not the same dimension as the collection, an exception will be raised.

You can also store documents elsewhere, and just supply a list of `embeddings` and `metadata` to Chroma. You can use the `ids` to associate the embeddings with your documents stored elsewhere.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.add(
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    ids=["id1", "id2", "id3", ...]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.add({
    ids: ["id1", "id2", "id3", ...],
    embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

---
id: collections-add
name: Add Data
---

# Adding Data to Chroma Collections

Add data to a Chroma collection with the `.add` method. It takes a list of unique string `ids`, and a list of `documents`. Chroma will embed these documents for you using the collection's [embedding function](../embeddings/embedding-functions). It will also store the documents themselves. You can optionally provide a metadata dictionary for each document you add.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.add(
    ids=["id1", "id2", "id3", ...],
    documents=["lorem ipsum...", "doc2", "doc3", ...],
    metadatas=[{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.add({
    ids: ["id1", "id2", "id3", ...],
    documents: ["lorem ipsum...", "doc2", "doc3", ...],
    metadatas: [{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

If you add a record with an ID that already exists in the collection, it will be ignored and no exception will be raised. This means that if a batch add operation fails, you can safely run it again.

Alternatively, you can supply a list of document-associated `embeddings` directly, and Chroma will store the associated documents without embedding them itself. Note that in this case there will be no guarantee that the embedding is mapped to the document associated with it.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.add(
    ids=["id1", "id2", "id3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    documents=["doc1", "doc2", "doc3", ...],
    metadatas=[{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],

)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.add({
    ids: ["id1", "id2", "id3", ...],
    embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    documents: ["lorem ipsum...", "doc2", "doc3", ...],
    metadatas: [{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
})
```

{% /Tab %}

{% /TabbedCodeBlock %}

If the supplied `embeddings` are not the same dimension as the embeddings already indexed in the collection, an exception will be raised.

You can also store documents elsewhere, and just supply a list of `embeddings` and `metadata` to Chroma. You can use the `ids` to associate the embeddings with your documents stored elsewhere.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.add(
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
    ids=["id1", "id2", "id3", ...]
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.add({
    ids: ["id1", "id2", "id3", ...],
    embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas: [{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
})
```

{% /Tab %}

{% /TabbedCodeBlock %}

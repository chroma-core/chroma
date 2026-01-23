---
id: collections-update
name: Update Data
---

# Updating Data in Chroma Collections

Any property of records in a collection can be updated with `.update`:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.update(
    ids=["id1", "id2", "id3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
    documents=["doc1", "doc2", "doc3", ...],
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.update({
    ids: ["id1", "id2", "id3", ...],
    embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas: [{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
    documents: ["doc1", "doc2", "doc3", ...]
})
```

{% /Tab %}

{% Tab label="go" %}

```go
err := collection.Update(ctx,
    chroma.WithIDs("id1", "id2", "id3"),
    chroma.WithEmbeddings(
        []float32{1.1, 2.3, 3.2},
        []float32{4.5, 6.9, 4.4},
        []float32{1.1, 2.3, 3.2},
    ),
    chroma.WithMetadatas(
        chroma.NewDocumentMetadata(chroma.NewIntAttribute("chapter", 3), chroma.NewIntAttribute("verse", 16)),
        chroma.NewDocumentMetadata(chroma.NewIntAttribute("chapter", 3), chroma.NewIntAttribute("verse", 5)),
        chroma.NewDocumentMetadata(chroma.NewIntAttribute("chapter", 29), chroma.NewIntAttribute("verse", 11)),
    ),
    chroma.WithTexts("doc1", "doc2", "doc3"),
)
```

{% /Tab %}

{% /TabbedCodeBlock %}

If an `id` is not found in the collection, an error will be logged and the update will be ignored. If `documents` are supplied without corresponding `embeddings`, the embeddings will be recomputed with the collection's embedding function.

If the supplied `embeddings` are not the same dimension as the collection, an exception will be raised.

Chroma also supports an `upsert` operation, which updates existing items, or adds them if they don't yet exist.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.upsert(
    ids=["id1", "id2", "id3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": 3, "verse": 16}, {"chapter": 3, "verse": 5}, {"chapter": 29, "verse": 11}, ...],
    documents=["doc1", "doc2", "doc3", ...],
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.upsert({
  ids: ["id1", "id2", "id3"],
  embeddings: [
    [1.1, 2.3, 3.2],
    [4.5, 6.9, 4.4],
    [1.1, 2.3, 3.2],
  ],
  metadatas: [
    { chapter: "3", verse: "16" },
    { chapter: "3", verse: "5" },
    { chapter: "29", verse: "11" },
  ],
  documents: ["doc1", "doc2", "doc3"],
});
```

{% /Tab %}

{% Tab label="go" %}

```go
err := collection.Upsert(ctx,
    chroma.WithIDs("id1", "id2", "id3"),
    chroma.WithEmbeddings(
        []float32{1.1, 2.3, 3.2},
        []float32{4.5, 6.9, 4.4},
        []float32{1.1, 2.3, 3.2},
    ),
    chroma.WithMetadatas(
        chroma.NewDocumentMetadata(chroma.NewStringAttribute("chapter", "3"), chroma.NewStringAttribute("verse", "16")),
        chroma.NewDocumentMetadata(chroma.NewStringAttribute("chapter", "3"), chroma.NewStringAttribute("verse", "5")),
        chroma.NewDocumentMetadata(chroma.NewStringAttribute("chapter", "29"), chroma.NewStringAttribute("verse", "11")),
    ),
    chroma.WithTexts("doc1", "doc2", "doc3"),
)
```

{% /Tab %}

{% /TabbedCodeBlock %}

If an `id` is not present in the collection, the corresponding items will be created as per `add`. Items with existing `id`s will be updated as per `update`.

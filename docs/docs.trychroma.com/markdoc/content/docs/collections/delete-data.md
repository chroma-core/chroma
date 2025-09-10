---
id: collections-delete
name: Delete Data
---

# Deleting Data from Chroma Collections

Chroma supports deleting items from a collection by `id` using `.delete`. The embeddings, documents, and metadata associated with each item will be deleted.

{% Banner type="warn" %}
Naturally, this is a destructive operation, and cannot be undone.
{% /Banner %}

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.delete(
    ids=["id1", "id2", "id3",...],
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.delete({
    ids: ["id1", "id2", "id3",...],
})
```

{% /Tab %}

{% /TabbedCodeBlock %}

`.delete` also supports the `where` filter. If no `ids` are supplied, it will delete all items in the collection that match the `where` filter.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.delete(
    ids=["id1", "id2", "id3",...],
	where={"chapter": "20"}
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.delete({
    ids: ["id1", "id2", "id3",...], //ids
    where: {"chapter": "20"} //where
})
```

{% /Tab %}

{% /TabbedCodeBlock %}

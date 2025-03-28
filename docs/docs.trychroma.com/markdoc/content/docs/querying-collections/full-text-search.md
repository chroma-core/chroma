# Full Text Search

In order to filter on document contents, you must supply a `where_document` filter dictionary to the query. We support two filtering keys: `$contains` and `$not_contains`. The dictionary must have the following structure:

```python
# Filtering for a search_string
{
    "$contains": "search_string"
}

# Filtering for not contains
{
    "$not_contains": "search_string"
}
```

You can combine full-text search with Chroma's metadata filtering.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.query(
    query_texts=["doc10", "thus spake zarathustra", ...],
    n_results=10,
    where={"metadata_field": "is_equal_to_this"},
    where_document={"$contains":"search_string"}
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.query({
    queryTexts: ["doc10", "thus spake zarathustra", ...],
    nResults: 10,
    where: {"metadata_field": "is_equal_to_this"},
    whereDocument: {"$contains": "search_string"}
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

#### Using logical operators

You can also use the logical operators `$and` and `$or` to combine multiple filters

```python
{
    "$and": [
        {"$contains": "search_string_1"},
        {"$not_contains": "search_string_2"},
    ]
}
```

An `$or` operator will return results that match any of the filters in the list
```python
{
    "$or": [
        {"$contains": "search_string_1"},
        {"$not_contains": "search_string_2"},
    ]
}
```

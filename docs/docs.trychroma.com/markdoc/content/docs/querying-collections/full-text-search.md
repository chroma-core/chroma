---
id: fts-regex
name: FTS and Regex
---

# Full Text Search and Regex

{% Tabs %}

{% Tab label="python" %}

The `where_document` argument in `get` and `query` is used to filter records based on their document content.

We support full-text search with the `$contains` and `$not_contains` operators. We also support [regular expression](https://regex101.com) pattern matching with the `$regex` and `$not_regex` operators.

For example, here we get all records whose document contains a search string:

```python
collection.get(
   where_document={"$contains": "search string"}
)
```

_Note_: Full-text search is case-sensitive.

Here we get all records whose documents matches the regex pattern for an email address:

```python
collection.get(
   where_document={
       "$regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
   }
)
```

## Using Logical Operators

You can also use the logical operators `$and` and `$or` to combine multiple filters.

An `$and` operator will return results that match all the filters in the list:

```python
collection.query(
    query_texts=["query1", "query2"],
    where_document={
        "$and": [
            {"$contains": "search_string_1"},
            {"$regex": "[a-z]+"},
        ]
    }
)
```

An `$or` operator will return results that match any of the filters in the list:

```python
collection.query(
    query_texts=["query1", "query2"],
    where_document={
        "$or": [
            {"$contains": "search_string_1"},
            {"$not_contains": "search_string_2"},
        ]
    }
)
```

## Combining with Metadata Filtering

`.get` and `.query` can handle `where_document` search combined with [metadata filtering](./metadata-filtering):

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

The `whereDocument` argument in `get` and `query` is used to filter records based on their document content.

We support full-text search with the `$contains` and `$not_contains` operators. We also support [regular expression](https://regex101.com) pattern matching with the `$regex` and `$not_regex` operators.

For example, here we get all records whose document contains a search string:

```typescript
await collection.get({
  whereDocument: { $contains: "search string" },
});
```

Here we get all records whose documents matches the regex pattern for an email address:

```typescript
await collection.get({
  whereDocument: {
    $regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
  },
});
```

## Using Logical Operators

You can also use the logical operators `$and` and `$or` to combine multiple filters.

An `$and` operator will return results that match all the filters in the list:

```typescript
await collection.query({
  queryTexts: ["query1", "query2"],
  whereDocument: {
    $and: [{ $contains: "search_string_1" }, { $regex: "[a-z]+" }],
  },
});
```

An `$or` operator will return results that match any of the filters in the list:

```typescript
await collection.query({
  queryTexts: ["query1", "query2"],
  whereDocument: {
    $or: [
      { $contains: "search_string_1" },
      { $not_contains: "search_string_2" },
    ],
  },
});
```

## Combining with Metadata Filtering

`.get` and `.query` can handle `whereDocument` search combined with [metadata filtering](./metadata-filtering):

```typescript
await collection.query({
    queryTexts: ["doc10", "thus spake zarathustra", ...],
    nResults: 10,
    where: { metadata_field: "is_equal_to_this" },
    whereDocument: { "$contains": "search_string" }
})
```

{% /Tab %}

{% /Tabs %}

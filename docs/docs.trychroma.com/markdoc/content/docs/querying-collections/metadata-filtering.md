---
id: metadata-filtering
name: Metadata Filtering
---

# Metadata Filtering

The `where` argument in `get` and `query` is used to filter records by their metadata. For example, in this `query` operation, Chroma will only query records that have the `page` metadata field with the value `10`:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.query(
    query_texts=["first query", "second query"],
    where={"page": 10}
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.query({
  queryTexts: ["first query", "second query"],
  where: { page: 10 },
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

In order to filter on metadata, you must supply a `where` filter dictionary to the query. The dictionary must have the following structure:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
{
    "metadata_field": {
        <Operator>: <Value>
    }
}
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
{
    metadata_field: {
        <Operator>: <Value>
    }
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

Using the `$eq` operator is equivalent to using the metadata field directly in your `where` filter.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
{
    "metadata_field": "search_string"
}

# is equivalent to

{
    "metadata_field": {
        "$eq": "search_string"
    }
}
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
{
    metadata_field: "search_string"
}

// is equivalent to

{
    metadata_field: {
        "$eq":"search_string"
    }
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

For example, here we query all records whose `page` metadata field is greater than 10:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.query(
    query_texts=["first query", "second query"],
    where={"page": { "$gt": 10 }}
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.query({
  queryTexts: ["first query", "second query"],
  where: { page: { $gt: 10 } },
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

## Using Logical Operators

You can also use the logical operators `$and` and `$or` to combine multiple filters.

An `$and` operator will return results that match all the filters in the list.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
{
    "$and": [
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        },
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        }
    ]
}
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
{
    "$and": [
        {
            metadata_field: { <Operator>: <Value> }
        },
        {
            metadata_field: { <Operator>: <Value> }
        }
    ]
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

For example, here we query all records whose `page` metadata field is between 5 and 10:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.query(
    query_texts=["first query", "second query"],
    where={
        "$and": [
            {"page": {"$gte": 5 }},
            {"page": {"$lte": 10 }},
        ]
    }
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.query({
  queryTexts: ["first query", "second query"],
  where: {
    $and: [{ page: { $gte: 5 } }, { page: { $lte: 10 } }],
  },
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

An `$or` operator will return results that match any of the filters in the list.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
{
    "or": [
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        },
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        }
    ]
}
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
{
    "or": [
        {
            metadata_field: { <Operator>: <Value> }
        },
        {
            metadata_field: { <Operator>: <Value> }
        }
    ]
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

For example, here we get all records whose `color` metadata field is `red` or `blue`:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.get(
    where={
        "or": [
            {"color": "red"},
            {"color": "blue"},
        ]
    }
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.get({
  where: {
    or: [{ color: "red" }, { color: "blue" }],
  },
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

## Using Inclusion Operators

The following inclusion operators are supported:

- `$in` - a value is in predefined list (string, int, float, bool)
- `$nin` - a value is not in predefined list (string, int, float, bool)

An `$in` operator will return results where the metadata attribute is part of a provided list:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
{
  "metadata_field": {
    "$in": ["value1", "value2", "value3"]
  }
}
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
{
    metadata_field: {
        "$in": ["value1", "value2", "value3"]
    }
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

An `$nin` operator will return results where the metadata attribute is not part of a provided list (or the attribute's key is not present):

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
{
  "metadata_field": {
    "$nin": ["value1", "value2", "value3"]
  }
}
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
{
    metadata_field: {
        "$nin": ["value1", "value2", "value3"]
    }
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

For example, here we get all records whose `author` metadata field is in a list of possible values:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.get(
    where={
       "author": {"$in": ["Rowling", "Fitzgerald", "Herbert"]}
    }
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.get({
  where: {
    author: { $in: ["Rowling", "Fitzgerald", "Herbert"] },
  },
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

## Combining with Document Search

`.get` and `.query` can handle metadata filtering combined with [document search](./full-text-search):

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
    where: { metadata_field: "is_equal_to_this" },
    whereDocument: { "$contains": "search_string" }
})
```

{% /Tab %}

{% /TabbedCodeBlock %}

---
id: collection-go
name: Collection (Go)
---

# Collection

## Properties

- `Name() string` - The name of the collection.
- `ID() string` - The unique identifier of the collection.
- `Metadata() CollectionMetadata` - The metadata associated with the collection.
- `Dimension() int` - The dimension of embeddings in the collection.
- `Tenant() Tenant` - The tenant the collection belongs to.
- `Database() Database` - The database the collection belongs to.

## Methods

### Add

- `Add(ctx context.Context, opts ...CollectionAddOption) error`

Add items to the collection.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithIDs(ids...)` | `...DocumentID` | Unique identifiers for the records (required unless using IDGenerator). |
| `WithTexts(texts...)` | `...string` | Document text (will be embedded if embeddings not provided). |
| `WithEmbeddings(embeddings...)` | `...Embedding` | Pre-computed embeddings. |
| `WithMetadatas(metadatas...)` | `...DocumentMetadata` | Metadata for each record. |
| `WithIDGenerator(gen)` | `IDGenerator` | Auto-generate IDs for records. |

#### Returns

`error` - Returns nil on success.

**Example**

```go
err := collection.Add(ctx,
    chroma.WithIDs("id1", "id2"),
    chroma.WithTexts("document 1", "document 2"),
    chroma.WithMetadatas(
        chroma.NewDocumentMetadata(chroma.NewStringAttribute("source", "web")),
        chroma.NewDocumentMetadata(chroma.NewStringAttribute("source", "file")),
    ),
)
```

### Upsert

- `Upsert(ctx context.Context, opts ...CollectionAddOption) error`

Upsert items to the collection (inserts new records or updates existing ones).

#### Options

Same options as `Add`.

#### Returns

`error` - Returns nil on success.

**Example**

```go
err := collection.Upsert(ctx,
    chroma.WithIDs("id1", "id2"),
    chroma.WithTexts("updated document 1", "updated document 2"),
)
```

### Update

- `Update(ctx context.Context, opts ...CollectionUpdateOption) error`

Update items in the collection.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithIDsUpdate(ids...)` | `...DocumentID` | IDs of records to update (required). |
| `WithTextsUpdate(texts...)` | `...string` | New document text. |
| `WithEmbeddingsUpdate(embeddings...)` | `...Embedding` | New embedding vectors. |
| `WithMetadatasUpdate(metadatas...)` | `...DocumentMetadata` | New metadata. |

#### Returns

`error` - Returns nil on success.

**Example**

```go
err := collection.Update(ctx,
    chroma.WithIDsUpdate("id1"),
    chroma.WithTextsUpdate("new document content"),
    chroma.WithMetadatasUpdate(
        chroma.NewDocumentMetadata(chroma.NewStringAttribute("updated", "true")),
    ),
)
```

### Delete

- `Delete(ctx context.Context, opts ...CollectionDeleteOption) error`

Delete items from the collection.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithIDsDelete(ids...)` | `...DocumentID` | Specific record IDs to delete. |
| `WithWhereDelete(where)` | `WhereFilter` | Metadata-based filtering for deletion. |
| `WithWhereDocumentDelete(where)` | `WhereDocumentFilter` | Document content-based filtering for deletion. |

#### Returns

`error` - Returns nil on success.

**Example**

```go
// Delete by IDs
err := collection.Delete(ctx,
    chroma.WithIDsDelete("id1", "id2"),
)

// Delete by filter
err := collection.Delete(ctx,
    chroma.WithWhereDelete(chroma.EqString(chroma.K("source"), "deprecated")),
)
```

### Get

- `Get(ctx context.Context, opts ...CollectionGetOption) (GetResult, error)`

Get items from the collection.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithIDsGet(ids...)` | `...DocumentID` | Specific record IDs to retrieve. |
| `WithWhereGet(where)` | `WhereFilter` | Metadata-based filtering. |
| `WithWhereDocumentGet(where)` | `WhereDocumentFilter` | Document content-based filtering. |
| `WithIncludeGet(include...)` | `...Include` | Fields to include: `IncludeDocuments`, `IncludeMetadatas`, `IncludeEmbeddings`. |
| `WithLimitGet(limit)` | `int` | Maximum records to return. |
| `WithOffsetGet(offset)` | `int` | Records to skip. |

#### Returns

`GetResult` - The query results containing IDs, documents, metadatas, and embeddings.

**Example**

```go
result, err := collection.Get(ctx,
    chroma.WithIDsGet("id1", "id2"),
    chroma.WithIncludeGet(chroma.IncludeDocuments, chroma.IncludeMetadatas),
)

// Access results
for i, id := range result.IDs {
    fmt.Printf("ID: %s, Document: %s\n", id, result.Documents[i])
}
```

### Query

- `Query(ctx context.Context, opts ...CollectionQueryOption) (QueryResult, error)`

Query the collection for similar items.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithQueryTexts(texts...)` | `...string` | Query text to embed and search. |
| `WithQueryEmbeddings(embeddings...)` | `...Embedding` | Pre-computed query embeddings. |
| `WithNResults(n)` | `int` | Maximum results per query. Default: `10`. |
| `WithWhereQuery(where)` | `WhereFilter` | Metadata-based filtering. |
| `WithWhereDocumentQuery(where)` | `WhereDocumentFilter` | Document content-based filtering. |
| `WithIncludeQuery(include...)` | `...Include` | Fields to include. Default: `["metadatas", "documents", "distances"]`. |
| `WithIDsQuery(ids...)` | `...DocumentID` | Filter to specific record IDs (Chroma >= 1.0.3). |

#### Returns

`QueryResult` - The query results containing IDs, documents, metadatas, embeddings, and distances.

**Example**

```go
// Query by text
result, err := collection.Query(ctx,
    chroma.WithQueryTexts("search query"),
    chroma.WithNResults(5),
    chroma.WithWhereQuery(chroma.EqString(chroma.K("category"), "science")),
    chroma.WithIncludeQuery(chroma.IncludeDocuments, chroma.IncludeDistances),
)

// Access results (results are nested by query)
for i, ids := range result.IDs {
    for j, id := range ids {
        fmt.Printf("Query %d, Result %d: ID=%s, Distance=%f\n",
            i, j, id, result.Distances[i][j])
    }
}
```

### Search

- `Search(ctx context.Context, opts ...SearchCollectionOption) (SearchResult, error)`

Performs hybrid search on the collection using expression builders. Provides a flexible API for complex queries with filtering, ranking, and result selection.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithSearchRequest(req)` | `*SearchRequest` | The search request built using expression builders. |

#### Returns

`SearchResult` - Search results. Use `.Rows()` to convert to row-major format.

**Example**

```go
// Basic search with KNN ranking
result, err := collection.Search(ctx,
    chroma.WithSearchRequest(
        chroma.NewSearchRequest().
            Where(chroma.EqString(chroma.K("category"), "science")).
            Rank(chroma.WithKnnRank(chroma.KnnRank{
                QueryText: "machine learning",
                Limit:     10,
            })).
            Limit(5).
            Select(chroma.K.DOCUMENT, chroma.K.SCORE, chroma.K("title")),
    ),
)

// Iterate over results
for _, row := range result.Rows() {
    fmt.Printf("ID: %s, Score: %f\n", row.ID, row.Score)
}
```

### Count

- `Count(ctx context.Context) (int, error)`

Count the number of items in the collection.

#### Returns

`int` - The number of items in the collection.

**Example**

```go
count, err := collection.Count(ctx)
```

### ModifyName

- `ModifyName(ctx context.Context, newName string) error`

Modify the collection name.

**Example**

```go
err := collection.ModifyName(ctx, "new_collection_name")
```

### ModifyMetadata

- `ModifyMetadata(ctx context.Context, newMetadata CollectionMetadata) error`

Modify the collection metadata.

**Example**

```go
err := collection.ModifyMetadata(ctx,
    chroma.NewMetadata(chroma.NewStringAttribute("version", "2.0")),
)
```

### Fork

- `Fork(ctx context.Context, newName string) (Collection, error)`

Creates a fork of the collection with a new name.

#### Returns

`Collection` - The forked collection.

**Example**

```go
forkedCollection, err := collection.Fork(ctx, "collection_backup")
```

### IndexingStatus

- `IndexingStatus(ctx context.Context) (*IndexingStatus, error)`

Returns the indexing status of the collection. Requires Chroma >= 1.4.1.

#### Returns

`*IndexingStatus` containing:
- `NumIndexedOps` - Number of indexed operations
- `NumUnindexedOps` - Number of unindexed operations
- `TotalOps` - Total operations
- `OpIndexingProgress` - Indexing progress (0.0 to 1.0)

**Example**

```go
status, err := collection.IndexingStatus(ctx)
fmt.Printf("Indexing progress: %.2f%%\n", status.OpIndexingProgress * 100)
```

### Close

- `Close() error`

Closes the collection and releases resources.

**Example**

```go
err := collection.Close()
```

## Filter Helpers

The Go client provides helper functions for building metadata filters:

### Comparison Operators

```go
// Equals
chroma.EqString(chroma.K("field"), "value")
chroma.EqInt(chroma.K("count"), 10)
chroma.EqFloat(chroma.K("score"), 0.95)
chroma.EqBool(chroma.K("active"), true)

// Not equals
chroma.NeString(chroma.K("field"), "value")

// Greater than / Less than
chroma.GtInt(chroma.K("count"), 5)
chroma.GteInt(chroma.K("count"), 5)
chroma.LtInt(chroma.K("count"), 10)
chroma.LteInt(chroma.K("count"), 10)

// In / Not In
chroma.InString(chroma.K("category"), "a", "b", "c")
chroma.NinString(chroma.K("category"), "x", "y")
```

### Logical Operators

```go
// AND
chroma.And(
    chroma.EqString(chroma.K("type"), "article"),
    chroma.GtInt(chroma.K("views"), 100),
)

// OR
chroma.Or(
    chroma.EqString(chroma.K("status"), "published"),
    chroma.EqString(chroma.K("status"), "featured"),
)
```

### Document Filters

```go
// Contains
chroma.Contains("search term")

// Not contains
chroma.NotContains("excluded term")

// Logical combinations
chroma.AndDoc(chroma.Contains("python"), chroma.Contains("tutorial"))
chroma.OrDoc(chroma.Contains("beginner"), chroma.Contains("introduction"))
```

---
id: query-and-get
name: Query and Get
---

# Query and Get Data from Chroma Collections

{% Note type="info" %}
**New Search API Available**: Chroma Cloud users can now use the powerful [Search API](/cloud/search-api/overview) for advanced hybrid search capabilities with better filtering, ranking, and batch operations.
{% /Note %}

{% Tabs %}

{% Tab label="python" %}
You can query a Chroma collection to run a similarity search using the `.query` method:

```python
collection.query(
    query_texts=["thus spake zarathustra", "the oracle speaks"]
)
```

Chroma will use the collection's [embedding function](../embeddings/embedding-functions) to embed your text queries, and use the output to run a vector similarity search against your collection.

Instead of provided `query_texts`, you can provide query embeddings directly. You will be required to do so if you also [added](../collections/add-data) embeddings directly to your collection, instead of using its embedding function. If the provided query embeddings are not of the same dimensions as those in your collection, an exception will be raised.

```python
collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...]
)
```

By default, Chroma will return 10 results per input query. You can modify this number using the `n_results` argument:

```python
collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    n_results=5
)
```

The `ids` argument lets you constrain the search only to records with the IDs from the provided list:

```python
collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    n_results=5,
    ids=["id1", "id2"]
)
```

You can also retrieve records from a collection by using the `.get` method. It supports the following arguments:

- `ids` - get records with IDs from this list. If not provided, the first 100 records will be retrieved, in the order of their addition to the collection.
- `limit` - the number of records to retrieve. The default value is 100.
- `offset` - The offset to start returning results from. Useful for paging results with limit. The default value is 0.

```python
collection.get(ids=["id1", "ids2", ...])
```

Both `query` and `get` have the `where` argument for [metadata filtering](./metadata-filtering) and `where_document` for [full-text search and regex](./full-text-search):

```python
collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    n_results=5,
    where={"page": 10}, # query records with metadata field 'page' equal to 10
    where_document={"$contains": "search string"} # query records with the search string in the records' document
)
```

## Results Shape

Chroma returns `.query` and `.get` results in columnar form. You will get a results object containing lists of `ids`, `embeddings`, `documents`, and `metadatas` of the records that matched your `.query` or `get` requests. Embeddings are returned as 2D-numpy arrays.

```python
class QueryResult(TypedDict):
    ids: List[IDs]
    embeddings: Optional[List[Embeddings]],
    documents: Optional[List[List[Document]]]
    metadatas: Optional[List[List[Metadata]]]
    distances: Optional[List[List[float]]]
    included: Include

class GetResult(TypedDict):
    ids: List[ID]
    embeddings: Optional[Embeddings],
    documents: Optional[List[Document]],
    metadatas: Optional[List[Metadata]]
    included: Include
```

`.query` results also contain a list of `distances`. These are the distances of each of the results from your input queries. `.query` results are also indexed by each of your input queries. For example, `results["ids"][0]` contains the list of records IDs for the results of the first input query.

```python
results = collection.query(query_texts=["first query", "second query"])
```

## Choosing Which Data is Returned

By default, `.query` and `.get` always return the `documents` and `metadatas`. You can use the `include` argument to modify what gets returned. `ids` are always returned:

```python
collection.query(query_texts=["my query"]) # 'ids', 'documents', and 'metadatas' are returned

collection.get(include=["documents"]) # Only 'ids' and 'documents' are returned

collection.query(
    query_texts=["my query"],
    include=["documents", "metadatas", "embeddings"]
) # 'ids', 'documents', 'metadatas', and 'embeddings' are returned
```

{% /Tab %}

{% Tab label="typescript" %}
You can query a Chroma collection to run a similarity search using the `.query` method:

```typescript
await collection.query({
  queryTexts: ["thus spake zarathustra", "the oracle speaks"],
});
```

Chroma will use the collection's [embedding function](../embeddings/embedding-functions) to embed your text queries, and use the output to run a vector similarity search against your collection.

Instead of provided `queryTexts`, you can provide query embeddings directly. You will be required to do so if you also [added](../collections/add-data) embeddings directly to your collection, instead of using its embedding function. If the provided query embeddings are not of the same dimensions as those in your collection, an exception will be raised.

```typescript
await collection.query({
    queryEmbeddings: [[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...]
})
```

By default, Chroma will return 10 results per input query. You can modify this number using the `nResults` argument:

```typescript
await collection.query({
    queryEmbeddings: [[11.1, 12.1, 13.1], [1.1, 2.3, 3.2], ...],
    nResults: 5
})
```

The `ids` argument lets you constrain the search only to records with the IDs from the provided list:

```typescript
await collection.query({
    queryEmbeddings: [[11.1, 12.1, 13.1], [1.1, 2.3, 3.2], ...],
    nResults: 5,
    ids: ["id1", "id2"]
})
```

You can also retrieve records from a collection by using the `.get` method. It supports the following arguments:

- `ids` - get records with IDs from this list. If not provided, the first 100 records will be retrieved, in the order of their addition to the collection.
- `limit` - the number of records to retrieve. The default value is 100.
- `offset` - The offset to start returning results from. Useful for paging results with limit. The default value is 0.

```typescript
await collection.get({ids: ["id1", "ids2", ...]})
```

Both `query` and `get` have the `where` argument for [metadata filtering](./metadata-filtering) and `whereDocument` for [full-text search and regex](./full-text-search):

```typescript
await collection.query({
    queryEmbeddings: [[11.1, 12.1, 13.1], [1.1, 2.3, 3.2], ...],
    nResults: 5,
    where: { page: 10 }, // query records with metadata field 'page' equal to 10
    whereDocument: { "$contains": "search string" } // query records with the search string in the records' document
})
```

## Results Shape

Chroma returns `.query` and `.get` results in columnar form. You will get a results object containing lists of `ids`, `embeddings`, `documents`, and `metadatas` of the records that matched your `.query` or `get` requests.

```typescript
class QueryResult {
  public readonly distances: (number | null)[][];
  public readonly documents: (string | null)[][];
  public readonly embeddings: (number[] | null)[][];
  public readonly ids: string[][];
  public readonly include: Include[];
  public readonly metadatas: (Record<
    string,
    string | number | boolean
  > | null)[][];
}

class GetResult {
  public readonly documents: (string | null)[];
  public readonly embeddings: number[][];
  public readonly ids: string[];
  public readonly include: Include[];
  public readonly metadatas: (Record<
    string,
    string | number | boolean
  > | null)[];
}
```

`.query` results also contain a list of `distances`. These are the distances of each of the results from your input queries. `.query` results are also indexed by each of your input queries. For example, `results.ids[0]` contains the list of records IDs for the results of the first input query.

```typescript
const results = await collection.query({
  queryTexts: ["first query", "second query"],
});
```

On `.query` and `.get` results, you can use the `.rows()` method, to get them in row-based format. That is, you will get an array of records, each with its `id`, `document`, `metdata` (etc.) fields.

```typescript
const results = await collection.get({ ids: ["id1", "id2", ...]});
const records = results.rows();
records.forEach((record) => {
    console.log(record.id, record.document);
})
```

You can also pass to `.get` and `.query` type arguments for the shape of your metadata. This will give you type inferrence for you metadata objects:

```typescript
const results = await collection.get<{page: number; title: string}>({
    ids: ["id1", "id2", ...]
});

const records = results.rows();
records.forEach((record) => {
    console.log(record.id, record.metadata?.page);
})
```

## Choosing Which Data is Returned

By default, `.query` and `.get` always return the `documents` and `metadatas`. You can use the `include` argument to modify what gets returned. `ids` are always returned:

```typescript
await collection.query({ queryTexts: ["my query"] }); // 'ids', 'documents', and 'metadatas' are returned

await collection.get({ include: ["documents"] }); // Only 'ids' and 'documents' are returned

await collection.query({
  queryTexts: ["my query"],
  include: ["documents", "metadatas", "embeddings"],
}); // 'ids', 'documents', 'metadatas', and 'embeddings' are returned
```

{% /Tab %}

{% Tab label="go" %}
You can query a Chroma collection to run a similarity search using the `.Query` method:

```go
results, err := collection.Query(ctx,
    chroma.WithQueryTexts("thus spake zarathustra", "the oracle speaks"),
)
```

Chroma will use the collection's [embedding function](../embeddings/embedding-functions) to embed your text queries, and use the output to run a vector similarity search against your collection.

Instead of providing `query_texts`, you can provide query embeddings directly:

```go
results, err := collection.Query(ctx,
    chroma.WithQueryEmbeddings(
        []float32{11.1, 12.1, 13.1},
        []float32{1.1, 2.3, 3.2},
    ),
)
```

By default, Chroma will return 10 results per input query. You can modify this using `WithNResults`:

```go
results, err := collection.Query(ctx,
    chroma.WithQueryTexts("search query"),
    chroma.WithNResults(5),
)
```

The `WithIDs` option lets you constrain the search only to records with the IDs from the provided list:

```go
results, err := collection.Query(ctx,
    chroma.WithQueryTexts("search query"),
    chroma.WithNResults(5),
    chroma.WithIDs("id1", "id2"),
)
```

You can also retrieve records from a collection by using the `.Get` method:

```go
results, err := collection.Get(ctx, chroma.WithIDs("id1", "id2"))
```

Both `Query` and `Get` have the `where` argument for [metadata filtering](./metadata-filtering) and `where_document` for [full-text search and regex](./full-text-search):

```go
results, err := collection.Query(ctx,
    chroma.WithQueryTexts("search query"),
    chroma.WithNResults(5),
    chroma.WithWhere(chroma.EqInt("page", 10)),
    chroma.WithWhereDocument(chroma.Contains("search string")),
)
```

## Results Shape

Chroma returns `.Query` and `.Get` results in columnar form. Query results are grouped by input query.

```go
// QueryResult methods:
results.GetIDsGroups()         // [][]string - IDs grouped by query
results.GetDocumentsGroups()   // [][]string - Documents grouped by query
results.GetMetadatasGroups()   // Metadata grouped by query
results.GetDistancesGroups()   // [][]float32 - Distances grouped by query

// GetResult methods:
results.GetIDs()        // []string - List of IDs
results.GetDocuments()  // []string - List of documents
results.GetMetadatas()  // List of metadata
```

`.Query` results are indexed by each of your input queries. For example, `results.GetIDsGroups()[0]` contains the list of record IDs for the results of the first input query.

Use `.Rows()` for easy iteration over results:

```go
for _, row := range results.Rows() {
    fmt.Printf("ID: %s, Document: %s, Score: %f\n", row.ID, row.Document, row.Score)
}
```

## Choosing Which Data is Returned

By default, `.Query` and `.Get` always return the `documents` and `metadatas`. You can use the `include` options to modify what gets returned:

```go
// Default query - returns ids, documents, metadatas
results, _ := collection.Query(ctx, chroma.WithQueryTexts("my query"))

// Get with only documents
results, _ := collection.Get(ctx, chroma.WithInclude(chroma.IncludeDocuments))

// Query with specific includes
results, _ := collection.Query(ctx,
    chroma.WithQueryTexts("my query"),
    chroma.WithInclude(chroma.IncludeDocuments, chroma.IncludeMetadatas, chroma.IncludeEmbeddings),
)
```

{% /Tab %}

{% /Tabs %}

---
id: collection-js-ts
name: Collection (JS/TS)
---

# Class: Collection

## Properties

- `id: string`
- `metadata: CollectionMetadata`
- `name: string`

## Methods

### add

- `add(params): Promise<void>`

Add items to the collection

#### Parameters

| Name     | Type               | Description                   |
| :------- | :----------------- | :---------------------------- |
| `params` | `AddRecordsParams` | The parameters for the query. |

#### Returns

`Promise<void>`

- The response from the API.

**Example**

```typescript
const response = await collection.add({
  ids: ["id1", "id2"],
  embeddings: [
    [1, 2, 3],
    [4, 5, 6],
  ],
  metadatas: [{ key: "value" }, { key: "value" }],
  documents: ["document1", "document2"],
});
```

### count

- `count(): Promise<number>`

Count the number of items in the collection

#### Returns

`Promise<number>`

- The number of items in the collection.

**Example**

```typescript
const count = await collection.count();
```

### delete

- `delete(params?): Promise<string[]>`

Deletes items from the collection.

#### Parameters

| Name     | Type           | Description                                            |
| :------- | :------------- | :----------------------------------------------------- |
| `params` | `DeleteParams` | The parameters for deleting items from the collection. |

#### Returns

`Promise<string[]>`

A promise that resolves to the IDs of the deleted items.

**Throws**

If there is an issue deleting items from the collection.

**Example**

```typescript
const results = await collection.delete({
  ids: "some_id",
  where: { name: { $eq: "John Doe" } },
  whereDocument: { $contains: "search_string" },
});
```

### get

- `get(params?): Promise<MultiGetResponse>`

Get items from the collection

#### Parameters

| Name     | Type            | Description                   |
| :------- | :-------------- | :---------------------------- |
| `params` | `BaseGetParams` | The parameters for the query. |

#### Returns

`Promise<MultiGetResponse>`

The response from the server.

**Example**

```typescript
const response = await collection.get({
  ids: ["id1", "id2"],
  where: { key: "value" },
  limit: 10,
  offset: 0,
  include: ["embeddings", "metadatas", "documents"],
  whereDocument: { $contains: "value" },
});
```

### modify

- `modify(params): Promise<CollectionParams>`

Modify the collection name or metadata

#### Parameters

| Name               | Type                 | Description                               |
| :----------------- | :------------------- | :---------------------------------------- |
| `params`           | `Object`             | The parameters for the query.             |
| `params.metadata?` | `CollectionMetadata` | Optional new metadata for the collection. |
| `params.name?`     | `string`             | Optional new name for the collection.     |

#### Returns

`Promise<CollectionParams>`

The response from the API.

**Example**

```typescript
const response = await client.updateCollection({
  name: "new name",
  metadata: { key: "value" },
});
```

### peek

- `peek(params?): Promise<MultiGetResponse>`

Peek inside the collection

#### Parameters

| Name     | Type         | Description                   |
| :------- | :----------- | :---------------------------- |
| `params` | `PeekParams` | The parameters for the query. |

#### Returns

`Promise<MultiGetResponse>`

A promise that resolves to the query results.

**Throws**

If there is an issue executing the query.

**Example**

```typescript
const results = await collection.peek({
  limit: 10,
});
```

### query

- `query(params): Promise<MultiQueryResponse>`

Performs a query on the collection using the specified parameters.

#### Parameters

| Name     | Type                 | Description                   |
| :------- | :------------------- | :---------------------------- |
| `params` | `QueryRecordsParams` | The parameters for the query. |

#### Returns

`Promise<MultiQueryResponse>`

A promise that resolves to the query results.

**Throws**

If there is an issue executing the query.

**Example**

```typescript
// Query the collection using embeddings
const embeddingsResults = await collection.query({
  queryEmbeddings: [[0.1, 0.2, ...], ...],
  ids: ["id1", "id2", ...],
  nResults: 10,
  where: {"name": {"$eq": "John Doe"}},
  include: ["metadata", "document"]
});

// Query the collection using query text
const textResults = await collection.query({
    queryTexts: "some text",
    ids: ["id1", "id2", ...],
    nResults: 10,
    where: {"name": {"$eq": "John Doe"}},
    include: ["metadata", "document"]
});
```

### update

- `update(params): Promise<void>`

Update items in the collection

#### Parameters

| Name     | Type                  | Description                   |
| :------- | :-------------------- | :---------------------------- |
| `params` | `UpdateRecordsParams` | The parameters for the query. |

#### Returns

`Promise<void>`

**Example**

```typescript
const response = await collection.update({
  ids: ["id1", "id2"],
  embeddings: [
    [1, 2, 3],
    [4, 5, 6],
  ],
  metadatas: [{ key: "value" }, { key: "value" }],
  documents: ["document1", "document2"],
});
```

### upsert

- `upsert(params): Promise<void>`

Upsert items to the collection

#### Parameters

| Name     | Type               | Description                   |
| :------- | :----------------- | :---------------------------- |
| `params` | `AddRecordsParams` | The parameters for the query. |

#### Returns

`Promise<void>`

**Example**

```typescript
const response = await collection.upsert({
  ids: ["id1", "id2"],
  embeddings: [
    [1, 2, 3],
    [4, 5, 6],
  ],
  metadatas: [{ key: "value" }, { key: "value" }],
  documents: ["document1", "document2"],
});
```

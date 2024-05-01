---
title: JS Collection
---

# Class: Collection

## Properties

### id

• **id**: `string`

---

### metadata

• **metadata**: `undefined` \| `CollectionMetadata`

---

### name

• **name**: `string`

## Methods

### add

▸ **add**(`params`): `Promise`<`AddResponse`\>

Add items to the collection

**`Example`**

```javascript
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

#### Parameters

| Name                 | Type                        | Description                              |
| :------------------- | :-------------------------- | :--------------------------------------- |
| `params`             | `Object`                    | The parameters for the query.            |
| `params.documents?`  | `string` \| `Documents`     | Optional documents of the items to add.  |
| `params.embeddings?` | `Embedding` \| `Embeddings` | Optional embeddings of the items to add. |
| `params.ids`         | `string` \| `IDs`           | IDs of the items to add.                 |
| `params.metadatas?`  | `Metadata` \| `Metadatas`   | Optional metadata of the items to add.   |

#### Returns

`Promise`<`AddResponse`\>

- The response from the API. True if successful.

---

### count

▸ **count**(): `Promise`<`number`\>

Count the number of items in the collection

**`Example`**

```javascript
const response = await collection.count();
```

#### Returns

`Promise`<`number`\>

- The response from the API.

---

### delete

▸ **delete**(`params?`): `Promise`<`string`[]\>

Deletes items from the collection.

**`Throws`**

If there is an issue deleting items from the collection.

**`Example`**

```javascript
const results = await collection.delete({
  ids: "some_id",
  where: {"$and": ["name": {"$eq": "John Doe"}, "age": {"$gte": 30}]},
  whereDocument: {"$contains":"search_string"}
});
```

#### Parameters

| Name                    | Type              | Description                                                                   |
| :---------------------- | :---------------- | :---------------------------------------------------------------------------- |
| `params`                | `Object`          | The parameters for deleting items from the collection.                        |
| `params.ids?`           | `string` \| `IDs` | Optional ID or array of IDs of items to delete.                               |
| `params.where?`         | `Where`           | Optional query condition to filter items to delete based on metadata values.  |
| `params.whereDocument?` | `WhereDocument`   | Optional query condition to filter items to delete based on document content. |

#### Returns

`Promise`<`string`[]\>

A promise that resolves to the IDs of the deleted items.

---

### get

▸ **get**(`params?`): `Promise`<`GetResponse`\>

Get items from the collection

**`Example`**

```javascript
const response = await collection.get({
  ids: ["id1", "id2"],
  where: { key: "value" },
  limit: 10,
  offset: 0,
  include: ["embeddings", "metadatas", "documents"],
  whereDocument: { $contains: "value" },
});
```

#### Parameters

| Name                    | Type              | Description                                        |
| :---------------------- | :---------------- | :------------------------------------------------- |
| `params`                | `Object`          | The parameters for the query.                      |
| `params.ids?`           | `string` \| `IDs` | Optional IDs of the items to get.                  |
| `params.include?`       | `IncludeEnum`[]   | Optional list of items to include in the response. |
| `params.limit?`         | `number`          | Optional limit on the number of items to get.      |
| `params.offset?`        | `number`          | Optional offset on the items to get.               |
| `params.where?`         | `Where`           | Optional where clause to filter items by.          |
| `params.whereDocument?` | `WhereDocument`   | Optional where clause to filter items by.          |

#### Returns

`Promise`<`GetResponse`\>

- The response from the server.

---

### modify

▸ **modify**(`params?`): `Promise`<`void`\>

Modify the collection name or metadata

**`Example`**

```javascript
const response = await collection.modify({
  name: "new name",
  metadata: { key: "value" },
});
```

#### Parameters

| Name               | Type                 | Description                               |
| :----------------- | :------------------- | :---------------------------------------- |
| `params`           | `Object`             | The parameters for the query.             |
| `params.metadata?` | `CollectionMetadata` | Optional new metadata for the collection. |
| `params.name?`     | `string`             | Optional new name for the collection.     |

#### Returns

`Promise`<`void`\>

- The response from the API.

---

### peek

▸ **peek**(`params?`): `Promise`<`GetResponse`\>

Peek inside the collection

**`Throws`**

If there is an issue executing the query.

**`Example`**

```javascript
const results = await collection.peek({
  limit: 10,
});
```

#### Parameters

| Name            | Type     | Description                                           |
| :-------------- | :------- | :---------------------------------------------------- |
| `params`        | `Object` | The parameters for the query.                         |
| `params.limit?` | `number` | Optional number of results to return (default is 10). |

#### Returns

`Promise`<`GetResponse`\>

A promise that resolves to the query results.

---

### query

▸ **query**(`params`): `Promise`<`QueryResponse`\>

Performs a query on the collection using the specified parameters.

**`Throws`**

If there is an issue executing the query.

**`Example`**

```javascript
// Query the collection using embeddings
const results = await collection.query({
  queryEmbeddings: [[0.1, 0.2, ...], ...],
  nResults: 10,
  where: {"$and": ["name": {"$eq": "John Doe"}, "age": {"$gte": 30}]},
  include: ["metadata", "document"]
});
```

**`Example`**

```js
// Query the collection using query text
const results = await collection.query({
  queryTexts: "some text",
  nResults: 10,
  where: { $and: [("name": { $eq: "John Doe" }), ("age": { $gte: 30 })] },
  include: ["metadata", "document"],
});
```

#### Parameters

| Name                      | Type                        | Description                                                                           |
| :------------------------ | :-------------------------- | :------------------------------------------------------------------------------------ |
| `params`                  | `Object`                    | The parameters for the query.                                                         |
| `params.include?`         | `IncludeEnum`[]             | Optional array of fields to include in the result, such as "metadata" and "document". |
| `params.nResults?`        | `number`                    | Optional number of results to return (default is 10).                                 |
| `params.queryEmbeddings?` | `Embedding` \| `Embeddings` | Optional query embeddings to use for the search.                                      |
| `params.queryTexts?`      | `string` \| `string`[]      | Optional query text(s) to search for in the collection.                               |
| `params.where?`           | `Where`                     | Optional query condition to filter results based on metadata values.                  |
| `params.whereDocument?`   | `WhereDocument`             | Optional query condition to filter results based on document content.                 |

#### Returns

`Promise`<`QueryResponse`\>

A promise that resolves to the query results.

---

### update

▸ **update**(`params`): `Promise`<`boolean`\>

Update the embeddings, documents, and/or metadatas of existing items

**`Example`**

```javascript
const response = await collection.update({
  ids: ["id1", "id2"],
  embeddings: [
    [1, 2, 3],
    [4, 5, 6],
  ],
  metadatas: [{ key: "value" }, { key: "value" }],
  documents: ["new document 1", "new document 2"],
});
```

#### Parameters

| Name                 | Type                        | Description                     |
| :------------------- | :-------------------------- | :------------------------------ |
| `params`             | `Object`                    | The parameters for the query.   |
| `params.documents?`  | `string` \| `Documents`     | Optional documents to update.   |
| `params.embeddings?` | `Embedding` \| `Embeddings` | Optional embeddings to update.  |
| `params.ids`         | `string` \| `IDs`           | The IDs of the items to update. |
| `params.metadatas?`  | `Metadata` \| `Metadatas`   | Optional metadatas to update.   |

#### Returns

`Promise`<`boolean`\>

- The API Response. True if successful. Else, error.

---

### upsert

▸ **upsert**(`params`): `Promise`<`boolean`\>

Upsert items to the collection

**`Example`**

```javascript
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

#### Parameters

| Name                 | Type                        | Description                              |
| :------------------- | :-------------------------- | :--------------------------------------- |
| `params`             | `Object`                    | The parameters for the query.            |
| `params.documents?`  | `string` \| `Documents`     | Optional documents of the items to add.  |
| `params.embeddings?` | `Embedding` \| `Embeddings` | Optional embeddings of the items to add. |
| `params.ids`         | `string` \| `IDs`           | IDs of the items to add.                 |
| `params.metadatas?`  | `Metadata` \| `Metadatas`   | Optional metadata of the items to add.   |

#### Returns

`Promise`<`boolean`\>

- The response from the API. True if successful.

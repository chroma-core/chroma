---
title: JS Client
---

# Class: ChromaClient

[ChromaClient](../modules/ChromaClient.md).ChromaClient

## Constructors

### constructor

• **new ChromaClient**(`params?`)

Creates a new ChromaClient instance.

**Parameters**

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `ChromaClientParams` | The parameters for creating a new client |

**Example**

```typescript
const client = new ChromaClient({
  path: "http://localhost:8000"
});
```

## Methods

### countCollections

▸ **countCollections**(): `Promise`\<`number`\>

Counts all collections.

**Returns**

`Promise`\<`number`\>

A promise that resolves to the number of collections.

**Throws**

If there is an issue counting the collections.

**Example**

```typescript
const collections = await client.countCollections();
```

___

### createCollection

▸ **createCollection**(`params`): `Promise`\<[`Collection`](Collection.Collection.md)\>

Creates a new collection with the specified properties.

**Parameters**

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `CreateCollectionParams` | The parameters for creating a new collection. |

**Returns**

`Promise`\<[`Collection`](Collection.Collection.md)\>

A promise that resolves to the created collection.

**Throws**

If the client is unable to connect to the server.

**Throws**

If there is an issue creating the collection.

**Example**

```typescript
const collection = await client.createCollection({
  name: "my_collection",
  metadata: {
    "description": "My first collection"
  }
});
```

___

### deleteCollection

▸ **deleteCollection**(`params`): `Promise`\<`void`\>

Deletes a collection with the specified name.

**Parameters**

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `DeleteCollectionParams` | The parameters for deleting a collection. |

**Returns**

`Promise`\<`void`\>

A promise that resolves when the collection is deleted.

**Throws**

If there is an issue deleting the collection.

**Example**

```typescript
await client.deleteCollection({
 name: "my_collection"
});
```

___

### getCollection

▸ **getCollection**(`params`): `Promise`\<[`Collection`](Collection.Collection.md)\>

Gets a collection with the specified name.

**Parameters**

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `GetCollectionParams` | The parameters for getting a collection. |

**Returns**

`Promise`\<[`Collection`](Collection.Collection.md)\>

A promise that resolves to the collection.

**Throws**

If there is an issue getting the collection.

**Example**

```typescript
const collection = await client.getCollection({
  name: "my_collection"
});
```

___

### getOrCreateCollection

▸ **getOrCreateCollection**(`params`): `Promise`\<[`Collection`](Collection.Collection.md)\>

Gets or creates a collection with the specified properties.

**Parameters**

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `CreateCollectionParams` | The parameters for creating a new collection. |

**Returns**

`Promise`\<[`Collection`](Collection.Collection.md)\>

A promise that resolves to the got or created collection.

**Throws**

If there is an issue getting or creating the collection.

**Example**

```typescript
const collection = await client.getOrCreateCollection({
  name: "my_collection",
  metadata: {
    "description": "My first collection"
  }
});
```

___

### heartbeat

▸ **heartbeat**(): `Promise`\<`number`\>

Returns a heartbeat from the Chroma API.

**Returns**

`Promise`\<`number`\>

A promise that resolves to the heartbeat from the Chroma API.

**Throws**

If the client is unable to connect to the server.

**Example**

```typescript
const heartbeat = await client.heartbeat();
```

___

### listCollections

▸ **listCollections**(`«destructured»?`): `Promise`\<`CollectionParams`[]\>

Lists all collections.

**Parameters**

| Name | Type |
| :------ | :------ |
| `«destructured»` | `ListCollectionsParams` |

**Returns**

`Promise`\<`CollectionParams`[]\>

A promise that resolves to a list of collection names.

**Throws**

If there is an issue listing the collections.

**Example**

```typescript
const collections = await client.listCollections({
    limit: 10,
    offset: 0,
});
```

___

### reset

▸ **reset**(): `Promise`\<`boolean`\>

Resets the state of the object by making an API call to the reset endpoint.

**Returns**

`Promise`\<`boolean`\>

A promise that resolves when the reset operation is complete.

**Throws**

If the client is unable to connect to the server.

**Throws**

If the server experienced an error while the state.

**Example**

```typescript
await client.reset();
```

___

### version

▸ **version**(): `Promise`\<`string`\>

Returns the version of the Chroma API.

**Returns**

`Promise`\<`string`\>

A promise that resolves to the version of the Chroma API.

**Throws**

If the client is unable to connect to the server.

**Example**

```typescript
const version = await client.version();
```

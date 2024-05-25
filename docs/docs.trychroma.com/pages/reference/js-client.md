---
title: JS Client
---

# Class: ChromaClient

## Constructors

### constructor

• **new ChromaClient**(`params?`)

Creates a new ChromaClient instance.

##### Basic

```javascript
const client = new ChromaClient({
  path: "http://localhost:8000"
});
```

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `Object` | The parameters for creating a new client |
| `params.path?` | `string` | The base path for the Chroma API. |

## Methods

### createCollection

▸ **createCollection**(`params`): `Promise`<[`Collection`](Collection.md)\>

Creates a new collection with the specified properties.

**`Throws`**

If there is an issue creating the collection.

**`Example`**

```javascript
const collection = await client.createCollection({
  name: "my_collection",
  metadata: {
    description: "My first collection"
  }
});
```

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `Object` | The parameters for creating a new collection. |
| `params.embeddingFunction?` | [`IEmbeddingFunction`](../interfaces/IEmbeddingFunction.md) | Optional custom embedding function for the collection. |
| `params.metadata?` | `CollectionMetadata` | Optional metadata associated with the collection. |
| `params.name` | `string` | The name of the collection. |

#### Returns

`Promise`<[`Collection`](Collection.md)\>

A promise that resolves to the created collection.

___

### deleteCollection

▸ **deleteCollection**(`params`): `Promise`<`void`\>

Deletes a collection with the specified name.

**`Throws`**

If there is an issue deleting the collection.

**`Example`**

```javascript
await client.deleteCollection({
 name: "my_collection"
});
```

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `Object` | The parameters for deleting a collection. |
| `params.name` | `string` | The name of the collection. |

#### Returns

`Promise`<`void`\>

A promise that resolves when the collection is deleted.

___

### getCollection

▸ **getCollection**(`params`): `Promise`<[`Collection`](Collection.md)\>

Gets a collection with the specified name.

**`Throws`**

If there is an issue getting the collection.

**`Example`**

```javascript
const collection = await client.getCollection({
  name: "my_collection"
});
```

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `Object` | The parameters for getting a collection. |
| `params.embeddingFunction?` | [`IEmbeddingFunction`](../interfaces/IEmbeddingFunction.md) | Optional custom embedding function for the collection. |
| `params.name` | `string` | The name of the collection. |

#### Returns

`Promise`<[`Collection`](Collection.md)\>

A promise that resolves to the collection.

___

### getOrCreateCollection

▸ **getOrCreateCollection**(`params`): `Promise`<[`Collection`](Collection.md)\>

Gets or creates a collection with the specified properties.

**`Throws`**

If there is an issue getting or creating the collection.

**`Example`**

```javascript
const collection = await client.getOrCreateCollection({
  name: "my_collection",
  metadata: {
    description: "My first collection"
  }
});
```

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `params` | `Object` | The parameters for creating a new collection. |
| `params.embeddingFunction?` | [`IEmbeddingFunction`](../interfaces/IEmbeddingFunction.md) | Optional custom embedding function for the collection. |
| `params.metadata?` | `CollectionMetadata` | Optional metadata associated with the collection. |
| `params.name` | `string` | The name of the collection. |

#### Returns

`Promise`<[`Collection`](Collection.md)\>

A promise that resolves to the got or created collection.

___

### heartbeat

▸ **heartbeat**(): `Promise`<`number`\>

Returns a heartbeat from the Chroma API.

**`Example`**

```javascript
const heartbeat = await client.heartbeat();
```

#### Returns

`Promise`<`number`\>

A promise that resolves to the heartbeat from the Chroma API.

___

### listCollections

▸ **listCollections**(): `Promise`<`CollectionType`[]\>

Lists all collections.

**`Throws`**

If there is an issue listing the collections.

**`Example`**

```javascript
const collections = await client.listCollections();
```

#### Returns

`Promise`<`CollectionType`[]\>

A promise that resolves to a list of collection names.

___

### reset

▸ **reset**(): `Promise`<`Reset200Response`\>

Resets the state of the object by making an API call to the reset endpoint.

**`Throws`**

If there is an issue resetting the state.

**`Example`**

```javascript
await client.reset();
```

#### Returns

`Promise`<`Reset200Response`\>

A promise that resolves when the reset operation is complete.

___

### version

▸ **version**(): `Promise`<`string`\>

Returns the version of the Chroma API.

**`Example`**

```javascript
const version = await client.version();
```

#### Returns

`Promise`<`string`\>

A promise that resolves to the version of the Chroma API.

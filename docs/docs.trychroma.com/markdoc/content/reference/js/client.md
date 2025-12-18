---
id: client-js-ts
name: Client (JS/TS)
---

# JS Client

## Class: ChromaClient

### constructor

- `new ChromaClient(params?)`

Creates a new ChromaClient instance.

#### Parameters

| Name            | Type                 | Description                              |
| :-------------- | :------------------- | :--------------------------------------- |
| `params.host?`  | `string`             | The host address of the Chroma server. Defaults to `'localhost'`. |
| `params.port?`  | `number`             | The port number of the Chroma server. Defaults to `8000`. |
| `params.ssl?`   | `boolean`            | Whether to use SSL/HTTPS for connections. Defaults to `false`. |
| `params.tenant?` | `string`             | The tenant name in the Chroma server to connect to. |
| `params.database?` | `string`            | The database name to connect to. |
| `params.headers?` | `Record<string, string>` | Additional HTTP headers to send with requests. |
| `params.fetchOptions?` | `RequestInit`      | Additional fetch options for HTTP requests. |

**Example**

```typescript
const client = new ChromaClient({
  host: "localhost",
  port: 8000,
  ssl: false,
  tenant: "my_tenant",
  database: "my_database",
});
```

## Methods

### countCollections

- `countCollections(): Promise<number>`

Counts all collections.

#### Returns

`Promise<number>`

A promise that resolves to the number of collections.

**Throws**

If there is an issue counting the collections.

**Example**

```typescript
const collections = await client.countCollections();
```

### createCollection

- `createCollection(params): Promise<Collection>`

Creates a new collection with the specified properties.

#### Parameters

| Name                      | Type                        | Description                                   |
| :------------------------ | :-------------------------- | :-------------------------------------------- |
| `params.name`             | `string`                    | The name of the collection (required).        |
| `params.configuration?`   | `CreateCollectionConfiguration` | Optional collection configuration.         |
| `params.metadata?`        | `CollectionMetadata`       | Optional metadata for the collection.         |
| `params.embeddingFunction?` | `EmbeddingFunction \| null` | Optional embedding function to use. Defaults to `DefaultEmbeddingFunction` from @chroma-core/default-embed. |
| `params.schema?`          | `Schema`                    | Optional schema describing index configuration. |

#### Returns

`Promise<Collection>`

A promise that resolves to the created collection.

**Throws**

- If the client is unable to connect to the server.
- If there is an issue creating the collection.

**Example**

```typescript
const collection = await client.createCollection({
  name: "my_collection",
  metadata: {
    description: "My first collection",
  },
});
```

### deleteCollection

- `deleteCollection(params): Promise<void>`

Deletes a collection with the specified name.

#### Parameters

| Name         | Type     | Description                               |
| :----------- | :------- | :---------------------------------------- |
| `params.name` | `string` | The name of the collection to delete (required). |

#### Returns

`Promise<void>`

A promise that resolves when the collection is deleted.

**Throws**

If there is an issue deleting the collection.

**Example**

```typescript
await client.deleteCollection({
  name: "my_collection",
});
```

### getCollection

- `getCollection(params): Promise<Collection>`

Gets a collection with the specified name.

#### Parameters

| Name                      | Type                | Description                              |
| :------------------------ | :------------------ | :--------------------------------------- |
| `params.name`             | `string`            | The name of the collection to retrieve (required). |
| `params.embeddingFunction?` | `EmbeddingFunction` | Optional embedding function. Should match the one used to create the collection. |

#### Returns

`Promise<Collection>`

A promise that resolves to the collection.

**Throws**

If there is an issue getting the collection.

**Example**

```typescript
const collection = await client.getCollection({
  name: "my_collection",
});
```

### getOrCreateCollection

- `getOrCreateCollection(params): Promise<Collection>`

Gets or creates a collection with the specified properties.

#### Parameters

| Name                      | Type                        | Description                                   |
| :------------------------ | :-------------------------- | :-------------------------------------------- |
| `params.name`             | `string`                    | The name of the collection (required).        |
| `params.configuration?`   | `CreateCollectionConfiguration` | Optional collection configuration (used only if creating). |
| `params.metadata?`        | `CollectionMetadata`       | Optional metadata for the collection (used only if creating). |
| `params.embeddingFunction?` | `EmbeddingFunction \| null` | Optional embedding function to use. |
| `params.schema?`          | `Schema`                    | Optional schema describing index configuration (used only if creating). |



#### Returns

`Promise<Collection>`

A promise that resolves to the got or created collection.

**Throws**

If there is an issue getting or creating the collection.

**Example**

```typescript
const collection = await client.getOrCreateCollection({
  name: "my_collection",
  metadata: {
    description: "My first collection",
  },
});
```

### heartbeat

- `heartbeat(): Promise<number>`

Returns a heartbeat from the Chroma API.

#### Returns

`Promise<number>`

A promise that resolves to the heartbeat from the Chroma API.

**Throws**

If the client is unable to connect to the server.

**Example**

```typescript
const heartbeat = await client.heartbeat();
```

### listCollections

- `listCollections(params?): Promise<Collection[]>`

Lists all collections.

#### Parameters

| Name            | Type     | Description                                                      |
| :-------------- | :------- | :--------------------------------------------------------------- |
| `params.limit?` | `number` | Maximum number of collections to return. Default: `100`.          |
| `params.offset?` | `number` | Number of collections to skip. Default: `0`.                     |

#### Returns

`Promise<Collection[]>`

A promise that resolves to an array of Collection instances.

**Throws**

If there is an issue listing the collections.

**Example**

```typescript
const collections = await client.listCollections({
  limit: 10,
  offset: 0,
});
```

### reset

- `reset(): Promise<void>`

Resets the entire database, deleting all collections and data.

#### Returns

`Promise<void>`

A promise that resolves when the reset is complete.

**Throws**

- If the client is unable to connect to the server.
- If the server experienced an error while resetting.

**Example**

```typescript
await client.reset();
```

### version

- `version(): Promise<string>`

Returns the version of the Chroma API.

#### Returns

`Promise<string>`

A promise that resolves to the version of the Chroma API.

**Throws**

If the client is unable to connect to the server.

**Example**

```typescript
const version = await client.version();
```

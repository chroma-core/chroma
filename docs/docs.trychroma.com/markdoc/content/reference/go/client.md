---
id: client-go
name: Client (Go)
---

# Go Client

## Creating a Client

### NewHTTPClient

- `NewHTTPClient(options ...ClientOption) (Client, error)`

Creates a new HTTP client to connect to a Chroma server.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithBaseURL(url)` | `string` | The base URL of the Chroma server. Defaults to `"http://localhost:8000/api/v2"`. |
| `WithTenant(tenant)` | `string` | The tenant name to connect to. |
| `WithDatabaseAndTenant(db, tenant)` | `string, string` | Set both database and tenant. |
| `WithDefaultDatabaseAndTenant()` | - | Use default database and tenant. |
| `WithAuth(provider)` | `CredentialsProvider` | Authentication provider for the client. |
| `WithDefaultHeaders(headers)` | `map[string]string` | Additional HTTP headers to send with requests. |
| `WithTimeout(timeout)` | `time.Duration` | Request timeout. |
| `WithSSLCert(certPath)` | `string` | Path to custom SSL certificate (PEM format). |
| `WithInsecure()` | - | Skip SSL verification (not for production). |
| `WithLogger(logger)` | `logger.Logger` | Custom logger for the client. |

**Example**

```go
import chroma "github.com/chroma-core/chroma/clients/go"

client, err := chroma.NewHTTPClient(
    chroma.WithBaseURL("http://localhost:8000/api/v2"),
    chroma.WithDatabaseAndTenant("my_database", "my_tenant"),
)
```

### NewCloudClient

- `NewCloudClient(apiKey string, options ...ClientOption) (Client, error)`

Creates a client configured for Chroma Cloud.

**Example**

```go
import chroma "github.com/chroma-core/chroma/clients/go"

client, err := chroma.NewCloudClient(
    os.Getenv("CHROMA_API_KEY"),
    chroma.WithDatabaseAndTenant("my_database", "my_tenant"),
)
```

## Methods

### Heartbeat

- `Heartbeat(ctx context.Context) error`

Checks if the Chroma server is alive.

#### Returns

`error` - Returns nil if the server is reachable, error otherwise.

**Example**

```go
err := client.Heartbeat(ctx)
```

### GetVersion

- `GetVersion(ctx context.Context) (string, error)`

Returns the version of the Chroma server.

#### Returns

`string` - The server version string.

**Example**

```go
version, err := client.GetVersion(ctx)
```

### CreateCollection

- `CreateCollection(ctx context.Context, name string, options ...CreateCollectionOption) (Collection, error)`

Creates a new collection with the specified name and options.

#### Parameters

| Name | Type | Description |
| :--- | :--- | :---------- |
| `name` | `string` | The name of the collection (required). |

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithCollectionMetadataCreate(metadata)` | `CollectionMetadata` | Metadata for the collection. |
| `WithEmbeddingFunctionCreate(ef)` | `EmbeddingFunction` | Embedding function to use. |
| `WithHNSWSpaceCreate(metric)` | `DistanceMetric` | Distance metric (L2, Cosine, IP). |
| `WithHNSWMCreate(m)` | `int` | HNSW M parameter. |
| `WithHNSWConstructionEfCreate(ef)` | `int` | HNSW construction EF. |
| `WithHNSWSearchEfCreate(ef)` | `int` | HNSW search EF. |
| `WithSchemaCreate(schema)` | `*Schema` | Collection schema configuration. |
| `WithDatabaseCreate(database)` | `Database` | Create in a specific database. |

#### Returns

`Collection` - The created collection.

**Example**

```go
collection, err := client.CreateCollection(ctx, "my_collection",
    chroma.WithCollectionMetadataCreate(
        chroma.NewMetadata(chroma.NewStringAttribute("description", "My collection")),
    ),
)
```

### GetCollection

- `GetCollection(ctx context.Context, name string, opts ...GetCollectionOption) (Collection, error)`

Gets a collection with the specified name.

#### Parameters

| Name | Type | Description |
| :--- | :--- | :---------- |
| `name` | `string` | The name of the collection (required). |

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithEmbeddingFunctionGet(ef)` | `EmbeddingFunction` | Embedding function to use with the collection. |
| `WithDatabaseGet(database)` | `Database` | Get from a specific database. |

#### Returns

`Collection` - The retrieved collection.

**Example**

```go
collection, err := client.GetCollection(ctx, "my_collection")
```

### GetOrCreateCollection

- `GetOrCreateCollection(ctx context.Context, name string, options ...CreateCollectionOption) (Collection, error)`

Gets a collection with the specified name, or creates it if it doesn't exist.

#### Parameters

| Name | Type | Description |
| :--- | :--- | :---------- |
| `name` | `string` | The name of the collection (required). |

#### Options

Same options as `CreateCollection`.

#### Returns

`Collection` - The retrieved or created collection.

**Example**

```go
collection, err := client.GetOrCreateCollection(ctx, "my_collection",
    chroma.WithCollectionMetadataCreate(
        chroma.NewMetadata(chroma.NewStringAttribute("description", "My collection")),
    ),
)
```

### DeleteCollection

- `DeleteCollection(ctx context.Context, name string, options ...DeleteCollectionOption) error`

Deletes a collection with the specified name.

#### Parameters

| Name | Type | Description |
| :--- | :--- | :---------- |
| `name` | `string` | The name of the collection to delete (required). |

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `WithDatabaseDelete(database)` | `Database` | Delete from a specific database. |

#### Returns

`error` - Returns nil on success.

**Example**

```go
err := client.DeleteCollection(ctx, "my_collection")
```

### ListCollections

- `ListCollections(ctx context.Context, opts ...ListCollectionsOption) ([]Collection, error)`

Lists all collections.

#### Options

| Name | Type | Description |
| :--- | :--- | :---------- |
| `ListWithLimit(limit)` | `int` | Maximum number of collections to return. Default: `100`. |
| `ListWithOffset(offset)` | `int` | Number of collections to skip. Default: `0`. |
| `WithDatabaseList(database)` | `Database` | List from a specific database. |

#### Returns

`[]Collection` - Array of collections.

**Example**

```go
collections, err := client.ListCollections(ctx,
    chroma.ListWithLimit(10),
    chroma.ListWithOffset(0),
)
```

### CountCollections

- `CountCollections(ctx context.Context, opts ...CountCollectionsOption) (int, error)`

Counts all collections.

#### Returns

`int` - The number of collections.

**Example**

```go
count, err := client.CountCollections(ctx)
```

### Reset

- `Reset(ctx context.Context) error`

Resets the database, deleting all collections and data. Requires `ALLOW_RESET=true` on the server.

#### Returns

`error` - Returns nil on success.

**Example**

```go
err := client.Reset(ctx)
```

### UseTenant

- `UseTenant(ctx context.Context, tenant Tenant) error`

Sets the current tenant for subsequent operations.

**Example**

```go
err := client.UseTenant(ctx, chroma.NewTenant("my_tenant"))
```

### UseDatabase

- `UseDatabase(ctx context.Context, database Database) error`

Sets the current database for subsequent operations.

**Example**

```go
err := client.UseDatabase(ctx, chroma.NewDatabase("my_database", chroma.NewTenant("my_tenant")))
```

### CreateTenant

- `CreateTenant(ctx context.Context, tenant Tenant) (Tenant, error)`

Creates a new tenant.

**Example**

```go
tenant, err := client.CreateTenant(ctx, chroma.NewTenant("my_tenant"))
```

### CreateDatabase

- `CreateDatabase(ctx context.Context, db Database) (Database, error)`

Creates a new database.

**Example**

```go
db, err := client.CreateDatabase(ctx, chroma.NewDatabase("my_database", chroma.NewTenant("my_tenant")))
```

### Close

- `Close() error`

Closes the client and releases resources.

**Example**

```go
err := client.Close()
```

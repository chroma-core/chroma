---
title: Client
---


## configure

```python
def configure(**kwargs) -> None
```

Override Chroma's default settings, environment variables or .env files

## EphemeralClient

```python
def EphemeralClient(settings: Optional[Settings] = None,
                    tenant: str = DEFAULT_TENANT,
                    database: str = DEFAULT_DATABASE) -> ClientAPI
```

Creates an in-memory instance of Chroma. This is useful for testing and
development, but not recommended for production use.

**Arguments**:

- `tenant` - The tenant to use for this client. Defaults to the default tenant.
- `database` - The database to use for this client. Defaults to the default database.

## PersistentClient

```python
def PersistentClient(path: str = "./chroma",
                     settings: Optional[Settings] = None,
                     tenant: str = DEFAULT_TENANT,
                     database: str = DEFAULT_DATABASE) -> ClientAPI
```

Creates a persistent instance of Chroma that saves to disk. This is useful for
testing and development, but not recommended for production use.

**Arguments**:

- `path` - The directory to save Chroma's data to. Defaults to "./chroma".
- `tenant` - The tenant to use for this client. Defaults to the default tenant.
- `database` - The database to use for this client. Defaults to the default database.

## HttpClient

```python
def HttpClient(host: str = "localhost",
               port: int = 8000,
               ssl: bool = False,
               headers: Optional[Dict[str, str]] = None,
               settings: Optional[Settings] = None,
               tenant: str = DEFAULT_TENANT,
               database: str = DEFAULT_DATABASE) -> ClientAPI
```

Creates a client that connects to a remote Chroma server. This supports
many clients connecting to the same server, and is the recommended way to
use Chroma in production.

**Arguments**:

- `host` - The hostname of the Chroma server. Defaults to "localhost".
- `port` - The port of the Chroma server. Defaults to "8000".
- `ssl` - Whether to use SSL to connect to the Chroma server. Defaults to False.
- `headers` - A dictionary of headers to send to the Chroma server. Defaults to {}.
- `settings` - A dictionary of settings to communicate with the chroma server.
- `tenant` - The tenant to use for this client. Defaults to the default tenant.
- `database` - The database to use for this client. Defaults to the default database.

## AsyncHttpClient

```python
async def AsyncHttpClient(host: str = "localhost",
                          port: int = 8000,
                          ssl: bool = False,
                          headers: Optional[Dict[str, str]] = None,
                          settings: Optional[Settings] = None,
                          tenant: str = DEFAULT_TENANT,
                          database: str = DEFAULT_DATABASE) -> AsyncClientAPI
```

Creates an async client that connects to a remote Chroma server. This supports
many clients connecting to the same server, and is the recommended way to
use Chroma in production.

**Arguments**:

- `host` - The hostname of the Chroma server. Defaults to "localhost".
- `port` - The port of the Chroma server. Defaults to "8000".
- `ssl` - Whether to use SSL to connect to the Chroma server. Defaults to False.
- `headers` - A dictionary of headers to send to the Chroma server. Defaults to {}.
- `settings` - A dictionary of settings to communicate with the chroma server.
- `tenant` - The tenant to use for this client. Defaults to the default tenant.
- `database` - The database to use for this client. Defaults to the default database.

## CloudClient

```python
def CloudClient(tenant: str,
                database: str,
                api_key: Optional[str] = None,
                settings: Optional[Settings] = None,
                *,
                cloud_host: str = "api.trychroma.com",
                cloud_port: int = 8000,
                enable_ssl: bool = True) -> ClientAPI
```

Creates a client to connect to a tennant and database on the Chroma cloud.

**Arguments**:

- `tenant` - The tenant to use for this client.
- `database` - The database to use for this client.
- `api_key` - The api key to use for this client.

## Client

```python
def Client(settings: Settings = __settings,
           tenant: str = DEFAULT_TENANT,
           database: str = DEFAULT_DATABASE) -> ClientAPI
```

Return a running chroma.API instance

tenant: The tenant to use for this client. Defaults to the default tenant.
database: The database to use for this client. Defaults to the default database.

## AdminClient

```python
def AdminClient(settings: Settings = Settings()) -> AdminAPI
```

Creates an admin client that can be used to create tenants and databases.


# BaseClient Methods

```python
class BaseAPI(ABC)
```

## heartbeat

```python
def heartbeat() -> int
```

Get the current time in nanoseconds since epoch.
Used to check if the server is alive.

**Returns**:

- `int` - The current time in nanoseconds since epoch

## count\_collections

```python
def count_collections() -> int
```

Count the number of collections.

**Returns**:

- `int` - The number of collections.


**Examples**:

    ```python
    client.count_collections()
    # 1
    ```

## delete\_collection

```python
def delete_collection(name: str) -> None
```

Delete a collection with the given name.

**Arguments**:

- `name` - The name of the collection to delete.


**Raises**:

- `ValueError` - If the collection does not exist.


**Examples**:

    ```python
    client.delete_collection("my_collection")
    ```

## reset

```python
def reset() -> bool
```

Resets the database. This will delete all collections and entries.

**Returns**:

- `bool` - True if the database was reset successfully.

## get\_version

```python
def get_version() -> str
```

Get the version of Chroma.

**Returns**:

- `str` - The version of Chroma

## get\_settings

```python
def get_settings() -> Settings
```

Get the settings used to initialize.

**Returns**:

- `Settings` - The settings used to initialize.

## get\_max\_batch\_size

```python
def get_max_batch_size() -> int
```

Return the maximum number of records that can be created or mutated in a single call.

# ClientClient Methods

```python
class ClientAPI(BaseAPI, ABC)
```

## list\_collections

```python
def list_collections(limit: Optional[int] = None,
                     offset: Optional[int] = None) -> Sequence[Collection]
```

List all collections.

**Arguments**:

- `limit` - The maximum number of entries to return. Defaults to None.
- `offset` - The number of entries to skip before returning. Defaults to None.


**Returns**:

- `Sequence[Collection]` - A list of collections


**Examples**:

    ```python
    client.list_collections()
    # [collection(name="my_collection", metadata={})]
    ```

## create\_collection

```python
def create_collection(name: str,
                      configuration: Optional[CollectionConfiguration] = None,
                      metadata: Optional[CollectionMetadata] = None,
                      embedding_function: Optional[EmbeddingFunction[
                          Embeddable]] = ef.DefaultEmbeddingFunction(),
                      data_loader: Optional[DataLoader[Loadable]] = None,
                      get_or_create: bool = False) -> Collection
```

Create a new collection with the given name and metadata.

**Arguments**:

- `name` - The name of the collection to create.
- `metadata` - Optional metadata to associate with the collection.
- `embedding_function` - Optional function to use to embed documents.
  Uses the default embedding function if not provided.
- `get_or_create` - If True, return the existing collection if it exists.
- `data_loader` - Optional function to use to load records (documents, images, etc.)


**Returns**:

- `Collection` - The newly created collection.


**Raises**:

- `ValueError` - If the collection already exists and get_or_create is False.
- `ValueError` - If the collection name is invalid.


**Examples**:

    ```python
    client.create_collection("my_collection")
    # collection(name="my_collection", metadata={})

    client.create_collection("my_collection", metadata={"foo": "bar"})
    # collection(name="my_collection", metadata={"foo": "bar"})
    ```

## get\_collection

```python
def get_collection(
        name: str,
        id: Optional[UUID] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]] = ef.DefaultEmbeddingFunction(),
        data_loader: Optional[DataLoader[Loadable]] = None) -> Collection
```

Get a collection with the given name.

**Arguments**:

- `id` - The UUID of the collection to get. Id and Name are simultaneously used for lookup if provided.
- `name` - The name of the collection to get
- `embedding_function` - Optional function to use to embed documents.
  Uses the default embedding function if not provided.
- `data_loader` - Optional function to use to load records (documents, images, etc.)


**Returns**:

- `Collection` - The collection


**Raises**:

- `ValueError` - If the collection does not exist


**Examples**:

    ```python
    client.get_collection("my_collection")
    # collection(name="my_collection", metadata={})
    ```

## get\_or\_create\_collection

```python
def get_or_create_collection(
        name: str,
        configuration: Optional[CollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]] = ef.DefaultEmbeddingFunction(),
        data_loader: Optional[DataLoader[Loadable]] = None) -> Collection
```

Get or create a collection with the given name and metadata.

**Arguments**:

- `name` - The name of the collection to get or create
- `metadata` - Optional metadata to associate with the collection. If
  the collection alredy exists, the metadata will be updated if
  provided and not None. If the collection does not exist, the
  new collection will be created with the provided metadata.
- `embedding_function` - Optional function to use to embed documents
- `data_loader` - Optional function to use to load records (documents, images, etc.)


**Returns**:

  The collection


**Examples**:

    ```python
    client.get_or_create_collection("my_collection")
    # collection(name="my_collection", metadata={})
    ```

## set\_tenant

```python
def set_tenant(tenant: str, database: str = DEFAULT_DATABASE) -> None
```

Set the tenant and database for the client. Raises an error if the tenant or
database does not exist.

**Arguments**:

- `tenant` - The tenant to set.
- `database` - The database to set.

## set\_database

```python
def set_database(database: str) -> None
```

Set the database for the client. Raises an error if the database does not exist.

**Arguments**:

- `database` - The database to set.

## clear\_system\_cache

```python
@staticmethod
def clear_system_cache() -> None
```

Clear the system cache so that new systems can be created for an existing path.
This should only be used for testing purposes.

# AdminClient Methods

```python
class AdminAPI(ABC)
```

## create\_database

```python
def create_database(name: str, tenant: str = DEFAULT_TENANT) -> None
```

Create a new database. Raises an error if the database already exists.

**Arguments**:

- `database` - The name of the database to create.

## get\_database

```python
def get_database(name: str, tenant: str = DEFAULT_TENANT) -> Database
```

Get a database. Raises an error if the database does not exist.

**Arguments**:

- `database` - The name of the database to get.
- `tenant` - The tenant of the database to get.

## create\_tenant

```python
def create_tenant(name: str) -> None
```

Create a new tenant. Raises an error if the tenant already exists.

**Arguments**:

- `tenant` - The name of the tenant to create.

## get\_tenant

```python
def get_tenant(name: str) -> Tenant
```

Get a tenant. Raises an error if the tenant does not exist.

**Arguments**:

- `tenant` - The name of the tenant to get.

# ServerClient Methods

```python
class ServerAPI(BaseAPI, AdminAPI, Component)
```

An API instance that extends the relevant Base API methods by passing
in a tenant and database. This is the root component of the Chroma System


---
id: client-params-python
name: Python SDK Reference (Client Params)
---

# Python SDK Reference — Client & Common Parameter Objects

This page documents the common parameter objects and arguments used by the Python SDK. It is intended as a concise, copy-pasteable reference for developers who want to know which keys and values the SDK accepts (client config, collection parameters, insert/upsert parameters, and query parameters).

> Notes

- This reference is intentionally "loose": it lists the common keys, typical types, and example values. If a parameter has strict validation or required shape at runtime, the example or note will call it out.
- Type names referenced here (e.g., `IDs`, `Documents`, `Embeddings`, `Metadatas`, `Schema`) are defined in the codebase under `chromadb.api.types` and `chromadb.types`.

## Table of contents
- Client initialization (aka ChromaClientParams)
- Settings (common Settings keys used by the client)
- Collection parameters (create/update)
- Insert / Add / Upsert parameters
- Get / Peek parameters
- Query / Search parameters
- Example workflows (create client, collection, insert, query)
- Quick notes & links

## Client initialization (ChromaClientParams)

The Python client is typically created with `chromadb.api.client.Client` (or helper factories). The most common constructor arguments are:

| Parameter | Type | Required | Default | Example | Notes |
|-----------|------|----------|---------|---------|-------|
| `tenant` | `str` | No | `"default_tenant"` | `"my-tenant"` | Logical tenant namespace. See `DEFAULT_TENANT` in `config.py`. |
| `database` | `str` | No | `"default_database"` | `"my-database"` | Database name scoped under tenant. |
| `settings` | `chromadb.config.Settings` | No | `Settings()` (defaults) | `Settings(chroma_server_host="localhost", chroma_server_http_port=8000)` | Full runtime/config object — see Settings section below. |

Short example:

```python
from chromadb.api.client import Client
from chromadb.config import Settings

settings = Settings(
    chroma_server_host="localhost",
    chroma_server_http_port=8000,
    chroma_server_ssl_enabled=False,
)
client = Client(tenant="default_tenant", database="default_database", settings=settings)
```

Notes:
- In many workflows you can supply only `tenant`/`database` or none, and the client will use defaults or values from the `settings`/auth provider.
- The `settings` object centralizes connection info and client-mode configuration (host, port, SSL, headers, etc.).

## Settings — common keys used by clients

The `Settings` class (see `chromadb.config.Settings`) includes many application and client configuration keys. The most commonly used by client consumers are:

| Key | Type | Default | Example | Notes |
|-----|------|---------|---------|-------|
| `chroma_server_host` | `Optional[str]` | `None` | `"localhost"` | Host name or IP of remote Chroma server. |
| `chroma_server_http_port` | `Optional[int]` | `None` | `8000` | Port to reach the HTTP server. |
| `chroma_server_ssl_enabled` | `Optional[bool]` | `False` | `True` | Use HTTPS when True. |
| `chroma_server_headers` | `Optional[Dict[str,str]]` | `None` | `{"Authorization":"Bearer <token>"}` | Custom HTTP headers for client-server calls. |
| `chroma_server_api_default_path` | `APIVersion` | `APIVersion.V2` | `APIVersion.V2` | API path version (v1 / v2). |
| `chroma_client_auth_provider` | `Optional[str]` | `None` | `"chromadb.auth.BasicAuthClientProvider"` | Configure auth provider if using remote servers. |

Short example (create Settings then Client):

```python
from chromadb.config import Settings
from chromadb.api.client import Client

settings = Settings(chroma_server_host="chroma.example.com", chroma_server_http_port=443, chroma_server_ssl_enabled=True)
client = Client(settings=settings)
```

## Collection create & update parameters

Method reference (client-level):
- `Client.create_collection(name: str, schema: Optional[Schema] = None, configuration: Optional[CreateCollectionConfiguration] = None, metadata: Optional[CollectionMetadata] = None, embedding_function: Optional[EmbeddingFunction] = DefaultEmbeddingFunction(), data_loader: Optional[DataLoader] = None, get_or_create: bool = False) -> Collection`
- `Client.get_or_create_collection(...)` has similar args.

| Parameter | Type | Required | Default | Example | Notes |
|-----------|------|----------|---------|---------|-------|
| `name` | `str` | Yes | — | `"my_collection"` | Unique collection name. |
| `schema` | `Optional[Schema]` | No | `None` | `{"title": {"type":"string"}}` | Optional schema object — see schema docs for structure. |
| `configuration` | `Optional[CreateCollectionConfiguration]` | No | `{}` | `{"embedding_function": {"name": "local_simple_hash", "config":{"dim":16}}}` | Collection index/config options. If `embedding_function` is provided here and in the function call, conflict validation occurs. |
| `metadata` | `Optional[CollectionMetadata]` | No | `None` | `{"owner":"team-x"}` | Free-form JSON metadata for collection. |
| `embedding_function` | `Optional[EmbeddingFunction]` | No | `DefaultEmbeddingFunction()` | `SimpleHashEmbeddingFunction(dim=16)` or `config_to_embedding_function(...)` | If provided, used for client-side embedding of documents. |
| `data_loader` | `Optional[DataLoader]` | No | `None` | Custom loader object | Optional loader for advanced ingestion. |
| `get_or_create` | `bool` | No | `False` | `True` | If true, returns an existing collection if it exists. |

Example:

```python
client.create_collection(
    name="news",
    schema={"title": {"type": "string"}},
    metadata={"domain": "news"},
    configuration={"embedding_function": {"name": "local_simple_hash", "config": {"dim": 16}}},
)
```

Notes:
- When both the configuration's `embedding_function` and the `embedding_function` argument are present, the code validates for conflicts (see `validate_embedding_function_conflict_on_create`).

## Insert / Add / Upsert parameters (records)

These parameters are used for inserting or updating document records.

Common method signatures call into API internals like:
- `_add(ids, collection_id, embeddings, metadatas=None, documents=None, uris=None)`
- `_upsert(collection_id, ids, embeddings, metadatas=None, documents=None, uris=None)`
- `_update(collection_id, ids, embeddings=None, metadatas=None, documents=None, uris=None)`

| Parameter | Type | Required | Default | Example | Notes |
|-----------|------|----------|---------|---------|-------|
| `ids` | `IDs` (`List[str]` or `str`) | Yes | — | `["id1","id2"]` | Unique identifiers for each record. Must match length of other list fields. |
| `embeddings` | `Embeddings` (`List[List[float]]` or numpy arrays) | Sometimes | — | `[[0.1, 0.2, ...], [0.2, 0.3, ...]]` | If provided, must be numeric vectors; helper `normalize_embeddings` allows multiple input shapes. |
| `metadatas` | `Optional[Metadatas]` (`List[dict]`) | No | `None` | `[{"author":"x"}, {"author":"y"}]` | Per-record arbitrary metadata. |
| `documents` | `Optional[Documents]` (`List[str]`) | No | `None` | `["text1", "text2"]` | Textual documents; used for retrieval/indexing. |
| `uris` | `Optional[URIs]` (`List[str]`) | No | `None` | `['/path/1', '/path/2']` | Optional URI references. |

Important validation rules:
- At least one of `embeddings`, `documents`, `images`, or `uris` must be provided for insert-like calls.
- List lengths of `ids`, `embeddings`, `documents`, and `metadatas` must match; empty lists are invalid.
- `normalize_insert_record_set` and `validate_insert_record_set` are used internally to normalize and validate these inputs.

Short example (upsert):
```python
client._upsert(
    collection_id=collection_id,
    ids=["doc1","doc2"],
    embeddings=[[0.1,0.2,...],[0.3,0.1,...]],
    metadatas=[{"source":"web"}, {"source":"db"}],
    documents=["Hello world", "Second doc"],
)
```

## Get / Peek parameters

Get methods return records and support filters and include options.

Common signature:
- `_get(collection_id, ids=None, where=None, limit=None, offset=None, where_document=None, include=IncludeMetadataDocuments) -> GetResult`
- `_peek(collection_id, n: int = 10) -> GetResult`

| Parameter | Type | Required | Default | Example | Notes |
|-----------|------|----------|---------|---------|-------|
| `collection_id` | `UUID` | Yes | — | `UUID("...")` | Internal collection identifier. |
| `ids` | `Optional[IDs]` | No | `None` | `["id1","id2"]` | Return only these IDs. |
| `where` | `Optional[Where]` | No | `None` | `{"author":"alice"}` | Structured metadata filters (see `Where` type). |
| `limit` | `Optional[int]` | No | `None` | `100` | Max number of returned records. |
| `offset` | `Optional[int]` | No | `None` | `0` | Result offset for pagination. |
| `where_document` | `Optional[WhereDocument]` | No | `None` | Full-text/document filters | Document-specific filters. |
| `include` | `Include` enum | No | `IncludeMetadataDocuments` | `IncludeMetadataDocumentsDistances` | Controls returned fields (embeddings, distances, documents, metadata, etc.). |

Short example:
```python
res = client._get(
    collection_id=collection_id,
    where={"category":"science"},
    limit=20,
    include=IncludeMetadataDocuments
)
```

## Query / Search parameters

Similarity search / nearest neighbor queries:

Common signature:
- `_query(collection_id, query_embeddings, ids=None, n_results=10, where=None, where_document=None, include=IncludeMetadataDocumentsDistances) -> QueryResult`

| Parameter | Type | Required | Default | Example | Notes |
|-----------|------|----------|---------|---------|-------|
| `collection_id` | `UUID` | Yes | — | `UUID("...")` | Target collection. |
| `query_embeddings` | `Embeddings` | Yes | — | `[[0.1, 0.2, ...]]` | Query vector(s). Multiple queries allowed. |
| `ids` | `Optional[IDs]` | No | `None` | Limit search to given IDs. |
| `n_results` | `int` | No | `10` | `5` | Number of nearest results per query. |
| `where` | `Optional[Where]` | No | `None` | Filter by metadata (e.g., `{"lang":"en"}`). |
| `where_document` | `Optional[WhereDocument]` | No | `None` | Document-level filters. |
| `include` | `Include` enum | No | `IncludeMetadataDocumentsDistances` | Controls included fields (ids, distances, embeddings, documents, metadata). |

Short example:
```python
result = client._query(
    collection_id=collection_id,
    query_embeddings=[[0.01, 0.02, ...]],
    n_results=5,
    where={"topic": "ai"},
)
# `result` contains ids, distances, and optionally documents/metadatas depending on include
```

## Common type quick-reference

These types appear frequently across client methods (see `chromadb.api.types` for full definitions).

- `IDs` — `List[str]` — list of record IDs.
- `Documents` — `List[str]` — textual content for each record.
- `Embeddings` — `List[List[float]]` or `List[np.ndarray]` — numeric vectors.
- `Metadatas` — `List[dict]` — per-record metadata objects.
- `Schema` — structured type describing collection schema (fields & types).
- `Where` — structured metadata filters (logical operators etc.).
- `Include` — enum controlling which fields are returned by `get`/`query`.

## Example end-to-end (create client, collection, upsert, query)

```python
from chromadb.config import Settings
from chromadb.api.client import Client
from uuid import UUID

# 1) Configure client
settings = Settings(chroma_server_host="localhost", chroma_server_http_port=8000)
client = Client(settings=settings)

# 2) Create a collection
collection = client.create_collection(
    name="articles",
    schema={"title": {"type": "string"}},
    metadata={"team": "docs"},
    configuration={"embedding_function": {"name": "local_simple_hash", "config": {"dim": 16}}},
)

# 3) Upsert data (ids must align with other lists)
client._upsert(
    collection_id=collection.id,  # internal UUID
    ids=["a1", "a2"],
    embeddings=[[0.1,0.2,...], [0.0,0.3,...]],
    metadatas=[{"author":"alice"}, {"author":"bob"}],
    documents=["Intro to Chroma", "Advanced guide"],
)

# 4) Query by vector
results = client._query(
    collection_id=collection.id,
    query_embeddings=[[0.1, 0.2, ...]],
    n_results=3,
    include=None  # choose include flags as needed
)
```

## Notes, caveats and troubleshooting

- This document provides a practical, loose reference. The canonical, strict types and validation logic are implemented in `chromadb.api.types` and the API implementations (FastAPI / Rust / Local). Consult those files for exact runtime validation.
- If you provide both an `embedding_function` on `create_collection(...)` and an `embedding_function` inside the `configuration` dict, the code will validate and raise on conflict — prefer one source of truth.
- `Settings` contains many environment-configurable options. For remote servers with auth, set `chroma_server_host`, `chroma_server_http_port`, and `chroma_server_headers` (e.g., Authorization header).
- Use `normalize_insert_record_set` and friends to help shape inputs when preparing bulk inserts.

---

If you'd like stricter typing (exact types and defaults extracted from code) I can prepare a follow-up patch that pulls type signatures and default values directly from the source code.

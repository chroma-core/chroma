# Chroma Ruby Client

A first-class Ruby client for Chroma's HTTP API.

## Install

```bash
bundle add chromadb
# or
ruby -e "system('gem install chromadb')"
```

## Quick Start (Local)

```ruby
require "chromadb"

client = Chroma::HttpClient.new(host: "localhost", port: 8000)
collection = client.get_or_create_collection(name: "docs")

collection.add(
  ids: ["a", "b"],
  documents: ["hello world", "goodbye"],
  embeddings: [[0.1, 0.2, 0.3], [0.0, 0.1, 0.0]],
)

results = collection.query(
  query_embeddings: [[0.1, 0.2, 0.25]],
  n_results: 2
)

pp results.to_h
```

## Cloud Client

```ruby
require "chromadb"

Chroma.configure do |config|
  config.cloud_api_key = ENV.fetch("CHROMA_API_KEY")
  # Optional when your API key spans multiple databases:
  # config.cloud_tenant = ENV["CHROMA_TENANT"]
  # config.cloud_database = ENV["CHROMA_DATABASE"]
end

client = Chroma::CloudClient.new

client.heartbeat
identity = client.get_user_identity
pp identity
```

Notes:
- Cloud clients only require an API key. Host/port overrides are for non-default deployments.

## Embedding Functions (Explicit)

There is no default embedding function. Provide embeddings directly or pass an embedding function.

### Cloud Qwen (dense)

```ruby
qwen = Chroma::EmbeddingFunctions::ChromaCloudQwenEmbeddingFunction.new(
  model: "Qwen/Qwen3-Embedding-0.6B",
  task: "nl_to_code"
)

collection = client.get_or_create_collection(
  name: "dense",
  embedding_function: qwen
)

collection.add(
  ids: ["doc-1"],
  documents: ["ruby code snippets"],
)
```

### Cloud Splade (sparse)

```ruby
splade = Chroma::EmbeddingFunctions::ChromaCloudSpladeEmbeddingFunction.new(include_tokens: true)

schema = Chroma::Schema.new
schema.create_index(
  config: Chroma::SparseVectorIndexConfig.new(
    embedding_function: splade,
    source_key: Chroma::DOCUMENT_KEY
  ),
  key: "sparse_embedding"
)

collection = client.get_or_create_collection(
  name: "sparse",
  schema: schema
)
```

### Chroma BM25 (local sparse)

```ruby
bm25 = Chroma::EmbeddingFunctions::ChromaBm25EmbeddingFunction.new

schema = Chroma::Schema.new
schema.create_index(
  config: Chroma::SparseVectorIndexConfig.new(
    embedding_function: bm25,
    source_key: Chroma::DOCUMENT_KEY,
    bm25: true
  ),
  key: "bm25_sparse"
)

collection = client.get_or_create_collection(name: "bm25", schema: schema)
```

## Search DSL (Cloud)

```ruby
search = Chroma::Search::Search.new
  .where(Chroma::Search::K["type"].eq("doc"))
  .rank(Chroma::Search.Knn(query: "ruby", key: "#embedding", limit: 10))
  .limit(10)
  .select_all

results = collection.search(search)
pp results.rows
```

## Rails initializer

Add a Rails initializer to configure the cloud API key once for your app:

```ruby
# config/initializers/chromadb.rb
Chroma.configure do |config|
  config.cloud_api_key = Rails.application.credentials.dig(:chroma, :api_key) || ENV["CHROMA_API_KEY"]
  config.cloud_tenant = Rails.application.credentials.dig(:chroma, :tenant)
  config.cloud_database = Rails.application.credentials.dig(:chroma, :database)
end
```

Then initialize a client anywhere in your app:

```ruby
client = Chroma::CloudClient.new
```

## Testing

### Single-node integration tests

Runs the Ruby test suite against a local single-node Chroma server.

```bash
bin/ruby-single-node-integration-test.sh
```

### Cloud integration tests

Hits hosted Chroma and requires an API key plus explicit opt-in.

```bash
CHROMA_API_KEY=... bin/ruby-cloud-integration-test.sh
```

Notes:
- The script accepts `RUBY_INTEGRATION_TEST_CHROMA_API_KEY` (CI secret) and maps it to `CHROMA_API_KEY`.
- Cloud tests only run when `CHROMA_CLOUD_INTEGRATION_TESTS=1` (set by the script).
- Optional overrides: `CHROMA_CLOUD_HOST`, `CHROMA_CLOUD_PORT`, `CHROMA_CLOUD_SSL`.

## Low-level OpenAPI client

The generated OpenAPI client is available for direct access to transport models and endpoints.

```ruby
require "chromadb/openapi"

Chromadb.configure do |config|
  config.host = "localhost"
  config.scheme = "http"
  config.base_path = "/api/v2"
end

api = Chromadb::DefaultApi.new
pp api.heartbeat
```

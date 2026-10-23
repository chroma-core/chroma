# Chroma

This crate provides the official Chroma Rust client.  Chroma is an open-source AI-native search
database that makes it easy to get private, offline, and real-time data that large language models
were not trained on into their context.  Where the language models provide reasoning, Chroma focuses
on search, enabling your application to customize the search methods it needs most.

Specifically, there are multiple modes of search supported by Chroma.

- Chroma supports dense embeddings for similarity search.  Briefly, embeddings give a numeric score
  for the difference between two strings.  For example, the string, "I like apples" is significantly
  closer to "I love apples" than it is to "I'm using Chroma to compare apples to apples."  Chroma
  automatically indexes your data so that you may query for similar text.
- Chroma supports sparse embeddings like BM25 or SPLADE-v3.  Briefly, sparse embeddings also give a
  numeric score for the difference between two strings.  More so than dense embeddings, sparse
  embeddings are sensitive to the literal words in documents.
- Full-text search enables applications to find literal matches in code.
- Metadata search allows for a variety of queries against the metadata for a record.

Chroma natively supports hybrid search of all search modes via its `search` endpoint, which can do a
weighted hybrid search across all modes of search, enabling applications to mix and match search
strategies.

## Quick-Start

Already know what Chroma is?  Get started fast:

1.  Get started with Chroma.  The easiest route is to sign up for [Chroma Cloud](https://trychroma.com/signup).  If the cloud isn't yet your thing, `pip install chromadb` and `chroma run` will get you much of the same experience.

2.  Add Chroma to your Rust project.

```console
cargo add chroma
```

3.  Initiate a ChromaHttpClient.

```rust
let client = ChromaHttpClient::cloud()?;
```

This will automatically read the following environment variables to set up a Chroma client:
- `CHROMA_ENDPOINT` sets the URL for Chroma.  For Chroma Cloud this is `https://api.trychroma.com`.
  There is no need to set this environment variable if you want to work with Cloud directly.
- `CHROMA_API_KEY` sets the API key to authenticate to Chroma.  This should be a Chroma-provided API
  key.  To generate a key, log in to [the Chroma dashboard](https://trychroma.com), create or select
  a database, and look for how to connect to your database under "settings".
- `CHROMA_TENANT` sets the tenant.  If you provide an API key, this will be inferred automatically
  on start.
- `CHROMA_DATABASE` sets the database.  If you provide an API key scoped to a single database, this
  will be automatically inferred.

If you're developing with Chroma locally, you can use the following code instead:

```rust
let client = ChromaHttpClient::from_env()?;
```

This is sufficient for developing from localhost.

For more complex configurations see `ChromaHttpClientOptions` in this crate by visiting [the
documentation](https://docs.rs/chroma/latest/chroma/).

## Client Features

The Chroma client is designed for production use and includes the following features:
- Optional automatic handling of rate limiting and backoff/retry for Chroma Cloud and compatible
  implementations.
- Support via the `metrics` feature for the OpenTelemetry standard.

## Learn More

[Learn more about Chroma](https://github.com/chroma-core/chroma)

- [💬 Community Discord](https://discord.gg/MMeYNTmh3x)
- [📖 Documentation](https://docs.trychroma.com/)
- [🏠 Homepage](https://www.trychroma.com/)

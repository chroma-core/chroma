# Clickhouse Architecture

## Context

The current prototype of Chroma Server uses DuckDB and Parquet files for
persistence. Although the simplicity and batch data retrieval
characteristics of this are attractive, we determine that this is
suboptimal for three primary reasons:

- Chroma's primary mode of ingesting data is a stream of small batches
  of embeddings. DuckDB and Parquet are not well optimized for
  streaming input. In fact, it's impossible to append to a Parquet
  file; the entire file must be re-written or additional files
  created.
- DuckDB explicitly does not support multiple writer processes, which
  we will likely want in the medium term.
- DuckDB + Parquet requires an explicit flush or write operation to
  persist data. This adds an element of "state management" and is
  complexity that we would rather not expose to the client.

Therefore, we are looking for an architecture with the following quantities:

- Efficient streaming ingest
- Efficient bulk read to pull data into memory for processing (OLAP)
- Low volume transactional CRUD operations (e.g datasets and metadata)
- Low administrative overhead, to present as small a client API as
  possible. We want to avoid exposing any methods aside from those
  that define Chroma as a product, for a focused user experience.

## Decision

We will use Clickhouse as the persistence layer. For now it will be
the only persistence mechanism used by Chroma.

Instances of Chroma Server will be stateless, aside from caching for
performance.

The MVP will run in a simple `docker-compose` configuration with a
single Clickhouse and a single Chroma Server.

The Chroma Server will, when required to service a read operation,
pull entire datasets from Clickhouse into memory and keep them cached
in order to perform algorithmic work on demand.

![Clickhouse Architecture](./2022-11-01-clickhouse-architecture/diagram.png "Clickhouse Architecture")

## Consequences

- The MVP is actually less complex than the previous DuckDB based
  solution.
- We can scale horizontally by adding more Chroma Server instances in
  a cluster.
- We can scale vertically by using a larger instance of Clickhouse or
  moving to clustered Clickhouse as workloads grow.
- At some point in the future, we will likely need to add an OLTP
  database, when the system contains enough transactional data that
  Clickhouse starts to perform poorly for row-based updates.
- We maintain separation of concerns, and can make future changes to
  the data persistence mechanisms without disrupting the backend
  protocol between Chroma Client and Chroma Server, or the user-facing
  API.

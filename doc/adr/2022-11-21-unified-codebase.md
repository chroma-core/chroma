# Unified Codebase

## Context

We would like users to have a "10 second" getting started experience,
for example via a Google Collab notebook. However, Google notebooks do
not support running Docker or other long-running subprocesses, making
a mandatory client+server model non-viable.

## Decision

We will combine the `chroma-server` and `chroma-client` projects into
a single codebase.

The codebase can be consumed as a library, or the server entry-point
can be invoked to run a server to which other clients can connect.

The customer-facing Python API is 100% identical, whether running in
client+server or client-only mode.

We will split the project's dependencies so packages that are only
required for the server components will use a separate
`chroma[server]` or `chroma[all]` PIP dependency.

## Python API Refactoring

We will create abstract interfaces (using Python's `abc` module as
described in [PEP 3119](https://peps.python.org/pep-3119/))
representing key parts of Chroma's internal structure.

Concrete singleton implementations of each functionality will be
obtained by invoking argument-free factory functions that return a
concrete type which depends on context and system wide configuration
(e.g. environment variables.) This serves as a rudimentary but
functional form of dependency injection.

Most code should be written only against the abstract interfaces and
strongly avoid requiring or importing any concrete implementations;
this will result in more lighweight runtime and allow users to
entirely omit dependencies they don't plan on using.

#### `chroma.API`

Defines Chroma's primary customer-facing API.

Implementations:

- `chroma.api.Local` - Client implemented via direct calls to
  algorithm or DB classes.
- `chroma.api.Celery` - Extension of `chroma.api.Local`, which
  delegates some potentially long-running operations to a Celery
  worker pool.
- `chroma.api.FastAPI` - Client implementation backed by requests to
   a remote `chroma.server.FastAPI` instance (see below.)
- `chroma.api.ArrowFlight` - Client implementation backed by requests to
   a remote `chroma.server.ArrowFlight` instance (see below.)

#### `chroma.DB`

Define's Chroma's data strorage and persistence layer.

Implementations:

- `chroma.db.Clickhouse` - Clickhouse database implementation.
- `chroma.db.DuckDB` - In-memory DuckDB implementation
- `chroma.db.PersistentDuckDB` - Extension of `chroma.db.DuckDB` that
  persists data in a local directory.

#### `chroma.Server`

A class which takes an instance of `chroma.API` and exposes it for
remote access.

Implementations:

- `chroma.server.FastAPI` - Run a FastAPI/ASGI webserver.
- `chroma.server.ArrowFlight` - Run an ArrowFlight gRPC server.

## Consequences

- Chroma can run in a Collab notebook or dev machine, with no
  configuration required beyond `pip install`.
- The Chroma PIP package will be slightly more heavyweight.
- The Chroma project structure (considered as a whole) will be less
  complex.
- A well-factored class structure will allow us degrees of freedom in
  the future for exploring and comparing alternative protocols,
  storage mechanisms, and locations of computation. It also creates a
  path to multi-teir architectures where some computation or storage
  is delegated to a remote SaaS product.


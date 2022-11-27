# Chroma Data Model

## Context

The AI/ML industry is evolving rapidly and ontologies for the subject
matter are not yet well defined. Practice differs between companies,
and between industry and academia. Furthermore, the field is evolving
so rapidly that even if a standard terminology could be established,
it would likely be obselete in the short term and therefore useless.

This presents a challenge for Chroma as a product. Customers think of
their domain in a certain way; "models", "model versions", "layers",
"retrainings", "training data", "test data", "validation data",
"projects", "pipelines", etc. But each customer defines these slightly
(or significantly) differently.

Ideally, Chroma would present an API to customers that reflected the
customer's own ontology: it is certainly out of scope for Chroma to
present its own preferred ontology and impose that upon customers.

But given the diversity of the target market, that is difficult to
do. Chroma would be better served to be as un-opinionated as possible
about the semantic and intent of any industry terms, and instead focus
on its own value proposition and what it needs to function.


## Decision

The central type in Chroma's data model is the *dataset*. A dataset is
a set (technically a
[multiset or bag](https://en.wikipedia.org/wiki/Multiset) since
duplicates are allowed) of *embeddings* that share a dimensionality
and are meaningful to operate on as a group. Every dataset has a UUID
that uniquely identifies it.

An *embedding* is a N-dimensional vector of single-precision (32-bit)
floating point numbers, along with a string identifier. The format of
the identifier is not specified, but is presumed to be sufficiently
unique to allow a client to retrieve the original data associated with
the embedding. Common choices will be UUIDs, URIs or hashes of binary
data.

The intended semantics of a dataset are left up to the
client. Typically, they represent a model trained with a specific data
set, or production inputs to a model over some interval, or a
particular layer or version of a model. Regardless, Chroma itself does
not care what semantics are ascribed to these concepts; only that a
dataset represents a set of embeddings.

Therefore, Chroma delegates the task of assigning "meaning" to a
dataset to the client. It does so via *metadata* on datasets. Every
dataset is associated with a JSON metadata object, fully managed
managed by the client. Clients can query Chroma to list or retrieve
specific datasets by pattern matching thier JSON metadata.

Chroma's operations are functions of one or more datasets which return
one or more collections of embeddings, annotated with Chroma specific
metadata. For the purposes of Chroma's data model, the set of operations
that Chroma can perform is open, but includes:

- `dump` - Return all the embeddings in a dataset.
- `*` - Other core Chroma operations. TODO: Include list from Anton's presentation.

### Backend API Operations

- Create dataset. Inputs is metadata JSON object, output is a new
  dataset UUID.
- Add embeddings to dataset. Input is dataset UUID and N embeddings,
  output is success or failure.
- Find dataset. Input is metadata pattern, output is one or more
  dataset UUIDs.
- Delete dataset. Input is dataset UUID, output is success or failure.
- Update dataset metadata. Input is dataset UUID and a new metadata
  JSON object, output is success or failure.
- Query. Input is one or more dataset UUIDs and the algorithm
  ID. Output is a collection of collections of embeddings, each with
  descriptive metadata specific to the algorithm.

## Consequences

- Chroma's data model can accomodate the full scope of desired user
  experiences. The indicated data model supports both early trial
  users who only want one dataset with metadata `{"name":
  "training"}`, to sophisticated production users integrating and
  testing multiple models, versions and layers over time.
- This data model is sufficient for the MVP but not complete -- as a
  full product Chroma will also need a model for users, permissions,
  etc.
- Metadata on individual embedding records is explicitly not
  supported. Chroma only requires the ability to store and return an
  embedding's unique identifier; storing additional information about
  the original datum is the client's responsibility. We can revisit
  this if it would grant significant utility to potential customers.
- At the storage layer, datasets themselves are fundamentally "OLAP"
  shaped; they ingest streaming data in bulk, and query algorithms
  must typically operate across the entire dataset to obtain
  results. However, dataset metadata (as well as users/permissions
  when we add those) are fundamentally "OLTP": datasets are typically
  queried for or updated one at a time (i.e, row-level.)  In the short
  term, we can use the same OLAP database for both as long as the
  metadata table is small, however, we may need to consider both an
  OLAP and an OLTP database as the product grows.
- This data model specifically does not contemplate long running
  background jobs or tasks. Not because we will not need them, but
  because they add a whole new layer of complexity to manage
  execution, allow clients to see job status, store or cache job
  results, etc. However, the model described in this ADR does not vary
  based on the existence of these systems (should they be
  required)... they can be added as additional functionality and API
  operations without affecting this core.

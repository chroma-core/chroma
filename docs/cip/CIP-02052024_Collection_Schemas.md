## CIP-02052024: Collection Schemas

## Status

Current Status: `Under Discussion`

Applies to:

- Single-node Chroma
- PersistentClient

## Motivation

Why do we want to add schema support for collections? What is the practical problem that we are trying to solve?

Today Chroma allows metadata to be defined pretty freely and even with conflicting types of the same metadata keys (e.g.
strings and ints). This flexibility, while powerful, can lead to issues with indexing and querying. The latter two
problems are only an immediate problem, however the horizon of this PR is a bit more ambitious than that. Our goal for
this CIP is to enable self-query and introspection of collections and their metadata. We believe that both of the latter
concepts will enable a much deeper and richer integration with AI-enabled apps and services.

## Public Interfaces

We propose the introduction of two new Collection properties, both of which are optional:

- Schema - A dictionary of metadata keys and their types
- Schema Validation type - An enumeration of the following values: `strict`, `lax`, `warn`, `none`
- Validating Party - An enumeration that represents the party that is responsible for validating the schema and its
  validation type. The following values are supported: `server` or `both`

The `Schema Validation type` value semantics are as follows:

- `strict` - This will enforce the schema on the metadata. If a metadata key is not present in the schema, it will be
  rejected. If a metadata key is present but of the wrong type, it will be rejected.
- `lax` - This will enforce the schema on the metadata. If a metadata key is not present in the schema, it will be
  accepted. If a metadata key is present but of the wrong type, it will be rejected.
- `warn` - This will enforce the schema on the metadata. If a metadata key is not present in the schema, it will be
  accepted. If a metadata key is present but of the wrong type, it will be accepted but a warning will be logged.
- `none` - This will not enforce the schema on the metadata.

Validating Party semantics are as follows:

- `server` - The server is responsible for validating the schema (default)
- `both` - Both the client and the server are responsible for validating the schema. This is useful in cases where the
  user wants to prevent the client from sending invalid requests (save on bandwidth and computer).

The above two properties will be passed as part of the existing collection creation, modification and query APIs.

### Dynamic Schema Introspection

To further facilitate external integrations of existing datasets in Chroma, we are also willing to consider adding an
endpoint to allow for dynamic schema introspection. The premise of this endpoint is that it will not make the
assumptions a schema exists and instead will generate a schema from existing collection metadata with all necessary
warnings and errors that may arise from schema validation.

We suggest the following API:

- `/api/v1/collections/{collection_name}/introspect` - an endpoint that returns introspection information including the
  dynamic schema and any validation errors that may exist in the collection metadata.

Notes on building the schema:

The schema will be built by walking the collection metadata and grouping the keys by their types. If a key has multiple
types, the schema will be built with the most common type (the one where most entries exist). A subsequent pass will
identify the offending values and log them as aggregate warnings or errors.

## Proposed Changes

We propose that the above-mentioned properties are stored as collection metadata with special prefix (similar to how
collection documents are stored).

- `chroma:schema` - A JSON string that represents the schema
- `chroma:schema_validation_type` - A string that represents the schema validation type
- `chroma:schema_validating_party` - A string that represents the validating party

### Rational for the Schema Validation Type

We realize that the proposed schema validation will not be a one-size-fits-all solution, but we firmly believe as the
number of deployments and use cases for Chroma grows, so will the demand for enforcing constraints within the system.
Therefore, we believe that schema validation with a range of validation options is a good starting point.

### Implications for Distributed Chroma

The schema and its validation are entirely decoupled from the storage which makes this work forward compatible with any
storage backend that Chroma may support in the future.

On the other hand the introspection endpoint will require coupling with the storage backend and as such will necessitate
further consideration for distributed Chroma.

### Future Work

Custom validators - a concept to allow users to more flexibly validate both metadata and documents. Examples of these
are regex-based validators, custom function-based validators, and more.

Enhance the existing LangChain integration with Chroma
for [self-query](https://python.langchain.com/docs/modules/data_connection/retrievers/self_query/), by removing the
manual self-query schema definition and replacing it with a schema-based introspection.

Dynamic OpenAPI Query Interface for GPTs - a concept where Chroma can expose a dynamic OpenAPI-based endpoint that
allows for OpenAI GPTs and similar to directly integrate with Chroma for queries without the need of an intermediate
translation layer.

## Compatibility, Deprecation, and Migration Plan

This change is backward compatible with previous releases of Chroma. The schema, the validation mechanism and
introspection will work with all prior 0.4.x releases. To benefit from the new functionality users will have to upgrade
to the version of Chroma where collections schemas feature is introduced first.

## Test Plan

We plan to modify unit tests to accommodate the change and use system tests to verify
this API change is backward compatible.

## Rejected Alternatives

- An alternative solution was explored where schema and its validation were implemented as tooling and utilities outside
  of core Chroma. We decided that this approach while comparable to the one above would be breaking to the developer and
  user experience Chroma has become known for.

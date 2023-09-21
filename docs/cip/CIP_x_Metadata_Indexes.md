# CIP-X Metadata Indexes

## WARNING: Challenges to discuss

- One of the main challenges with the below proposal is that indices are not per collection and defining them at
  collection level feels counter-intuitive.
- Another challenge is that string_value also contains the actual documents which means that the index will be very
  large and will not be very efficient.

## Status

Current Status: `Under Discussion`

## Motivation

Currently, some users are experiencing extreme slowness when querying with metadata filters, more frequently occurring
for large datasets. This behaviour is due to the full table scan that metadata filters (WHERE clauses) must perform to
locate the relevant documents.

## Public Interfaces

The proposal suggests the introduction of the following public API interfaces:

- `/api/v1/collection/{collection_id}/indices` - (POST, GET, DELETE) - allows users to define indices on the collection
  and get a list of all defined indices on the collection.
  - `POST` - allows users to define indices on the collection
  - `GET` - allows users to get a list of all defined indices on the collection
  - `DELETE` - allows users to drop all indices from the collection
- `/api/v1/collection/{collection_id}/indices/{index_name}` where:
  - `PUT` - allows users to update the index
  - `DELETE` - allows users to drop the index
  - `PATCH` - allows the user to perform an operation on the index e.g. rebuild

In addition to the API changes above we propose the following `Collection` interface changes:

- `Collection.indices.add()` - allows users to define indices on the collection - this is a convenience method that
  delegates to the API endpoint above (`POST /api/v1/collection/{collection_id}/indices`)
- `Collection.indices.list()` - allows users to get a list of all defined indices on the collection - this is a convenience
  method that delegates to the API endpoint above (`GET /api/v1/collection/{collection_id}/indices`)
- `Collection.indices.drop_all()` - allows users to drop all indices from the collection - this is a convenience method that
  delegates to the API endpoint above (`DELETE /api/v1/collection/{collection_id}/indices`)
- `Collection.indices.get('index_name').drop()` - allows users to drop the index - this is a convenience method that
  delegates to the API endpoint above (`DELETE /api/v1/collection/{collection_id}/indices/{index_name}`)
- `Collection.indices.get('index_name').update()` - allows users to update the index - this is a convenience method that
  delegates to the API endpoint above (`PUT /api/v1/collection/{collection_id}/indices/{index_name}`)
- `Collection.indices.get('index_name').rebuild()` - allows the user to perform an operation on the index e.g. rebuild  -
  this is a convenience method that delegates to the API endpoint above (`PATCH /api/v1/collection/{collection_id}/indices/{index_name}`)


## Proposed Changes

This proposal introduces the following domain models:

- `Index` - represents an index on a collection

### The `Index` model

> Note: The constructs suggested below are for illustration purposes. Final names may differ.

```python
from enum import Enum
from typing import Set
from typing_extensions import Literal, TypedDict

AllowedIndexColumns = Literal["string_value", "int_value", "float_value", "key"]


class IndexType(Enum,str):
    METADATA = "metadata"
    EMBEDDING = "embedding"


class Index(NamedTuple):
    name: str
    columns: Set[AllowedIndexColumns]
    index_type: IndexType = IndexType.METADATA
```

### Examples

#### Add indices

```python
collection.indices.add([Index(name="my_index", columns={"string_value", "int_value"})])
```

#### Update indices

```python
collection.indices.update([Index(name="my_index", columns={"string_value", "int_value","bool_value"})])
```

#### Rebuild indices

```python
collection.indices.get("my_index").rebuild()
```

#### Drop indices

```python
collection.indices.drop(["my_index", "my_index2"])
```

#### Get indices

```python
collection.indices.list()
```


### Additional Notes

It is advantageous to use `Index` objects instead of dictionaries or other simple type based objects because it allows
us to further extend the index utility in the future. For example, we can add a `unique` property to the `Index` object.

### Implementation Details

We suggest that all indices created by means described in this CIP are prefixed with `custom_idx_` to avoid any
potential conflicts with the indices created by the system.

### Known Limitations

Indices will be applied to all collections in the database due to how `embedding_database` table is structured.

## Compatibility, Deprecation, and Migration Plan

This feature does not introduce breaking changes and legacy code should continue to work as expected.

## Test Plan

We plan to introduce a new set of property tests to validate the behaviour of the new extension.

## Rejected Alternatives

The following alternatives were considered:

- Brute-force indexes on all columns - this solution may seam like a simple and straightforward but has the distinct
  drawbacks of not being very flexible with user requirements e.g. composite indexes where column orders matters.
- Allow users to define indices manually on the SQLite directly - this solution while being probably the simplest is not
  very developer friendly, some users do not have indepth knowledge of SQLite and it's indexing capabilities.
- Implement a mechanism to allow users to configure indices at startup - this solution is only a part of the suggested
  approach above and can only be implemented server-side and is less flexible due to indices being defined at startup.

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
for
large datasets. This behaviour is due to the full table scan that metadata filters (WHERE clauses) must perform to
locate the relevant documents.

## Public Interfaces

The proposal suggests the introduction of the following public interfaces:

- `Collection.modify(indices = Index(name='<name>',columns={'col'},operation=ADD|DROP|UPDATE|REBUILD))` - Allows users
  to
  define indices on the collection.
- `Collection` - introduces a new property `indices` that returns a list of all defined indices on the collection.

## Proposed Changes

We propose the introduction of a new function parameter to `Collection.modify` (`indices`) that will allow users to
define indices on the collection. The indices will be defined list of Index objects. The indices will be created on the
SQLite database level.

Index Object:

```python
AllowedColumns = Literal["string_value", "int_value", "float_value", "key"]


class Index(BaseModel):
    name: str
    columns: Optional[Set[AllowedColumns]] = None
    operation: Optional[CollectionIndexOperation] = CollectionIndexOperation.ADD
```

Where `CollectionIndexOperation` is an enum with the following values:

- `ADD` - add the specified indices to the collection
- `UPDATE` - update the specified indices on the collection
- `REBUILD` - rebuild the specified indices on the collection
- `DROP` - drop all indices from the collection

Furthermore, we propose that `Collection` object adds a propeertry `indices` that will return a dictionary of all:

- `Collection.indices` - returns a dictionary of all defined indices on the collection

### Examples

#### Add indices

```python
collection.modify(indices=[Index(name="my_index", columns={"string_value", "int_value"})])
```

#### Update indices

```python
collection.modify(
    indices=[Index(name="my_index", columns={"int_value", "string_value"}, operation=CollectionIndexOperation.UPDATE)])
```

#### Rebuild indices

```python
collection.modify(indices=[Index(name="my_index", operation=CollectionIndexOperation.REBUILD)])
```

#### Drop indices

```python
collection.modify(indices=[Index(name="my_index", operation=CollectionIndexOperation.DROP)])
```

#### Get indices

```python
collection.get().indices
```

Returns:

```python
{
    "my_index": {
        "columns": ["string_value", "int_value"],
    }
}
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

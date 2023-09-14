# CIP-1 Allow Filtering for Collections

## Status

Current Status: Under Discussion

## Motivation

Currently operations on getting collections does not yet support filtering based on its
metadata, as a result, users have to perform filtering after getting the collection.
This is inconvenient to the users as they have to perform the filtering in the
application and inefficient as extra bandwidth are consumed when transferring data
between client and server.

We should allow for getting a collection based on a filtering of its metadata. For
example, users could handle cases like wanting to get all collections belonging to a
specific id or a specific collection metadata field value.

## Public Interfaces

The public facing change is on the `list_collection` API. Specifically, we would like to
change the following API to add an optional `where` parameter in the API class.

```python
def list_collections(self) -> Sequence[Collection]: # original
def list_collections(self, where: Optional[Where] = {}) # after the change
```

## Proposed Changes

The proposed changes are mentioned in the public interfaces.

## Compatibility, Deprecation, and Migration Plan

This change is backward compatible.

## Test Plan

We plan to modify unit tests to accommodate the change and use system tests to verify
this API change is backward compatible.

## Rejected Alternatives

- An alternative solution would be adding new APIs similar to

```python
def get_collection(
self,
name: str,
embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
) -> Collection:
```

We decided to not go with it to reduce the user's burden to learn new APIs.

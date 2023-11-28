# CIP-28112023 Collection Unload

## Status

Current Status: Under Discussion

## Motivation

Currently, Chroma has no way to unload a collection from memory, unless it is restarted. This limits certain use cases for short-lived collection on a memory budget.

While this is a good feature to have it puts the onus on the user to reason about memory management, which in the many cases users do not need to do. 
If implemented in its current form there should be a warning to the users that memory management using the `unload` API will have side effects and can lead to OOM is assumptions are not met.

## Public Interfaces

The proposed CIP would introduce the following changes:

- API: `POST /api/v1/collections/<collection_id/unload` (with empty body)
- New method `Collection.unload()` that will either integrate with SegmentAPI change or the above API (client/server).


## Proposed Changes

The proposed changes span across the following components:

- FastAPI server
- SegmentAPI
- SegmentManager
- PersistentLocalHnswSegment
- BasAPI
- ServerAPI
- Client
- FastAPI client
- Collection

The crux of the implementation is in the SegmentManager and PersistentLocalHnswSegment where we unload the vector segment from memory and remove references to both MetadataReader and VectorReader. 
Rest of the changes are just to propagate the API call to the SegmentManager.

### Future work

As future work we anticipate the addition of LRU cache for collections which will allow us to automatically unload collections that are not read for a predefined global interval, configurable by the user via server config.

In addition, an interesting feature would be to allow the user to specify a TTL for a collection, which will automatically unload the collection after the TTL has expired. 
This is quite similar to the LRU cache, but allows the user to specify the TTL on a per-collection basis.

## Compatibility, Deprecation, and Migration Plan

The changes in a client/server mode will not be compatible between newer clients and older servers versions.

## Test Plan

We provide basic API tests to ensure the API works as expected.

> Note: While

## Rejected Alternatives



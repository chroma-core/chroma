# CIP-5: Large Batch Handling Improvements Proposal

## Status

Current Status: `Under Discussion`

## **Motivation**

As users start putting Chroma in its paces and storing ever-increasing datasets, we must ensure that errors
related to significant and potentially expensive batches are handled gracefully. This CIP proposes to add a new
setting, `max_batch_size` API, on the local segment API and use it to split large batches into smaller ones.

## **Public Interfaces**

The following interfaces are impacted:

- New Server API endpoint - `/pre-flight-checks`
- New `max_batch_size` property on the `API` interface
- Updated `_add`, `_update` and `_upsert` methods on `chromadb.api.segment.SegmentAPI`
- Updated `_add`, `_update` and `_upsert` methods on `chromadb.api.fastapi.FastAPI`
- New utility library `batch_utils.py`
- New exception raised when batch size exceeds `max_batch_size`

## **Proposed Changes**

We propose the following changes:

- The new `max_batch_size` property is now available in the `API` interface. The property relies on the
  underlying `Producer` class
  to fetch the actual value. The property will be implemented by both `chromadb.api.segment.SegmentAPI`
  and `chromadb.api.fastapi.FastAPI`
- `chromadb.api.segment.SegmentAPI` will implement the `max_batch_size` property by fetching the value from the
  `Producer` class.
- `chromadb.api.fastapi.FastAPI` will implement the `max_batch_size` by fetching it from a new `/pre-flight-checks`
  endpoint on the Server.
- New `/pre-flight-checks` endpoint on the Server will return a dictionary with pre-flight checks the client must
  fulfil to integrate with the server side. For now, we propose using this only for `max_batch_size`, but we can
  add more checks in the future. The pre-flight checks will be only fetched once per client and cached for the duration
  of the client's lifetime.
- Updated `_add`, `_update` and `_upsert` method on `chromadb.api.segment.SegmentAPI` to validate batch size.
- Updated `_add`, `_update` and `_upsert` method on `chromadb.api.fastapi.FastAPI`  to validate batch size (client-side
  validation)
- New utility library `batch_utils.py` will contain the logic for splitting batches into smaller ones.

## **Compatibility, Deprecation, and Migration Plan**

The change will be fully compatible with existing implementations. The changes will be transparent to the user.

## **Test Plan**

New tests:

- Batch splitting tests for `chromadb.api.segment.SegmentAPI`
- Batch splitting tests for `chromadb.api.fastapi.FastAPI`
- Tests for `/pre-flight-checks` endpoint

## **Rejected Alternatives**

N/A

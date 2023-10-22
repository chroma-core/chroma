# CIP-10082023 Expose Collection Dimensionality

## **Status**

Current status: **Draft**

## **Motivation**

Currently validation of embedding dimensionality is done on the server-side (or local persistent/ephemeral clients). We believe that by exposing the dimensionality of a collection to the client, we can improve the user experience by allowing the client to validate the dimensionality of the embedding before sending it to the server.

## **Public Interfaces**

The following changes are proposed:

- New server API endpoint: `GET /api/v1/collections/{collection}/dimensionality`
- New method `chromadb.api.API._dimensions` is added to the API class with corresponding FastAPI and SegmentAPI implementations.
- New `chromadb.api.models.Collection.dimensions` method is added.

## **Proposed Changes**

See [Public Interfaces](#public-interfaces).

## **Compatibility, Deprecation, and Migration Plan**

Newer clients will not be compatible with older server versions.
Older clients will be compatible with newer server versions.

## **Test Plan**

New tests will be added to the `chromadb.api` module.

## **Rejected Alternatives**

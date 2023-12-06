# CIP-10082023 Expose Collection Dimensionality

## **Status**

Current status: **Draft**

## **Motivation**

Currently, validation of embedding dimensionality is done on the server-side (or local persistent/ephemeral clients).
We believe that by exposing the dimensionality of a collection to the client, we can improve the user experience by
allowing the client to validate the dimensionality of the embedding before sending it to the server.

## **Public Interfaces**

The following changes are proposed:

- New server API endpoint: `GET /api/v1/collections/{collection}/describe` - returns a JSON object with detailed collection information (for now just dimensionality) with the following structure:
    ```json
    {
      "dimensionality": 768
    }
    ```
- New method `chromadb.api.API._describe` is added to the API class with corresponding FastAPI and SegmentAPI implementations.
- New `chromadb.api.models.Collection.describe` method is added.
- A new type `chromadb.api.types.CollectionInfo` containing the collection information is added.

In addition to providing the dimensionality of the collection, the `descirbe` method will serve as an enabled for future
collection information to be exposed to the client.

## **Proposed Changes**

See [Public Interfaces](#public-interfaces).

## **Compatibility, Deprecation, and Migration Plan**

Newer clients will not be compatible with older server versions.
Older clients will be compatible with newer server versions.

## **Test Plan**

New tests will be added to `chromadb/test/test_api.py`

## **Rejected Alternatives**

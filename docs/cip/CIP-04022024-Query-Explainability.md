# CIP-04022024: Query Explainability

## Status

Current Status: `Under Discussion`

## Motivation

As users roll out production apps it has become increasingly important to understand why certain queries are either slow
or return a set the results they return. This is especially important for users who are new to the system and are trying
to understand how to optimize their queries. This CIP proposes a new feature that will allow users to understand how
their queries in Chroma work.

We want to keep of the CIP small so the focus is only query explainability, although admittedly this can be applied to
other API calls.

The goal is to mimic most SQL databases, where you can use EXPLAIN on a query to understand its execution.

## Public Interfaces

We propose that a new API attribute - `explain` is added to `collection.query()` method. When set to `True`, the query
will return an explanation of how the query was executed. The explanation will include the following:

- Collection ID
- Number of documents in the collection
- Collection HNSW configuration with explanations of each attribute's effect on the query
- Pre-filtering steps
    - Underlying DB query - verbatim with parameters
    - Number of results of the query
    - Time taken to execute the query
- Vector query:
    - Time taken to execute the query
    - Number of results in the query
- Post-filtering steps
    - The underlying DB query - verbatim with parameters
    - Time taken to execute the query
    - Number of results in the query
    - Time taken to combine the results into a response

The proposed API changes will affect both persistent (local) and http client(s).

In order to keep things backward compatible we suggest that the query explanation payload is returned as an additional (
optional) attribute to the QueryResult object.

The query explanation can be done as a separate API call, but this comes with a
trade-off of increased API surface and code complexity.

## Proposed Changes

The proposed changes are mentioned in the public interfaces.

## Compatibility, Deprecation, and Migration Plan

The change is not backward compatible from client's perspective as the lack of the feature in prior clients will cause
an error when passing the new settings parameter. Server-side is not affected by this change.

## Test Plan

TBD

## Rejected Alternatives

N/A

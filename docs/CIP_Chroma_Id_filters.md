# CIP-5: Id Filters Proposal

## Status

Current Status: `Under Discussion`

## **Motivation**

Currently, Chroma does not provide a way to pre-filter embeddings with Id values. Doing so would enable users to perform pre-query processes for more complex and/or efficient queries. This has been requested by [community members](https://discord.com/channels/1073293645303795742/1074711446589542552/1088185430035411006).

## **Public Interfaces**

The changes will affect the following public interfaces:

- `collection.query()`

## **Proposed Changes**

We suggest the introduction of a new `where_id` argument to `query()`.

We suggest the following new definition:

```python

WhereId = Dict[InclusionExclusionOperator, List[string]]

```

And a change in the `query()` function:

```python
def query(
        query_embeddings: Optional[OneOrMany[Embedding]] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents",
                            "distances"],
        # new
        where_id=Optional[WhereId] = None

) -> QueryResult
```

An example of a query using the new parameter would be:

```python
collection.query(query_texts=query,
                 where_id={"$in": ['1', '2', '3']},
                 n_results=3)
```

## **Compatibility, Deprecation, and Migration Plan**

The change is compatible with existing release 0.4.x.

## **Test Plan**

Cases utilizing the new filter will be added to exisitng `query()` tests.

## **Rejected Alternatives**

N/A

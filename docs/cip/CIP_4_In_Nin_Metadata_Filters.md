# CIP-4: In and Not In Metadata Filters Proposal

## Status

Current Status: `Under Discussion`

## **Motivation**

Currently, Chroma does not provide a way to filter metadata through `in` and `not in`. This appears to be a frequent ask
from community members.

## **Public Interfaces**

The changes will affect the following public interfaces:

- `Where` and `OperatorExpression`
  classes - https://github.com/chroma-core/chroma/blob/48700dd07f14bcfd8b206dc3b2e2795d5531094d/chromadb/types.py#L125-L129
- `collection.get()`
- `collection.query()`

## **Proposed Changes**

We suggest the introduction of two new operators `$in` and `$nin` that will be used to filter metadata. We call these
operators `InclusionExclusionOperator`.

We suggest the following new operator definition:

```python
InclusionExclusionOperator = Union[Literal["$in"], Literal["$nin"]]
```

Additionally, we suggest that those operators are added to `OperatorExpression` for seamless integration with
existing `Where` semantics:

```python
OperatorExpression = Union[
    Dict[Union[WhereOperator, LogicalOperator], LiteralValue],
    Dict[InclusionExclusionOperator, List[LiteralValue]],
]
```

An example of a query using the new operators would be:

```python
collection.query(query_texts=query,
                 where={"$and": [{"author": {'$in': ['john', 'jill']}}, {"article_type": {"$eq": "blog"}}]},
                 n_results=3)
```

## **Compatibility, Deprecation, and Migration Plan**

The change is compatible with existing release 0.4.x.

## **Test Plan**

Property tests will be updated to ensure boundary conditions are covered as well as interoperability with existing `Where`
operators.

## **Rejected Alternatives**

N/A

- https://chat.openai.com/share/82173746-d590-427c-b40e-537bea57ab99

# CIP-6: Pythonic Metadata Filters

## Status

Current Status: `Under Discussion`

## **Motivation**

The main motivation for fluid filter is to promote the use of filters use through improved developer experience.

## **Public Interfaces**

This change does not involve any changes to public interfaces.

## **Proposed Changes**

We propose this change to ship as a utility library that can be used instead of the standard filter syntax.

We propose the following syntax:

```python
collection.get(where=Filter.where("category" == "chroma" and ("author" == "john" or "author" == "jack")))
```

Compared to existing approach:

```python
collection.get(where={"$and": [{"category": "chroma"}, {"$or": [{"author": "john"}, {"author": "jack"}]}]})
```

We believe that the proposed approach is easier to read and understand (with minor exception of the quoted attribute
names).

## **Compatibility, Deprecation, and Migration Plan**

The proposed change is fully compatible with the current implementation and does not require any migration.

## **Test Plan**

New property unit tests to be added to the existing test suite.

## **Rejected Alternatives**

1. Filter.where(attr("k") == "10").And((attr("p") == "x").Or(attr("p") == "y"))
2. Filter.where(attr.k == "10").And((attr.p == "x").Or(attr.p == "y"))
3. attr("k").Eq(10).And((attr("p").Eq("x")).Or(attr("p").Eq("y")))

### Filter.where(attr("k") == "10").And((attr("p") == "x").Or(attr("p") == "y"))

Developer Experience: Moderate; need to remember where, And, Or.
Pythonic: Not really, too many custom methods.
Fluidity: Moderate; relies on chained methods.
Ease of Use: Moderate; can get complex quickly.

Reason for rejection: While this is a good approach, it still is not frictionless and requires the use of for
attributes.

### Filter.where(attr.k == "10").And((attr.p == "x").Or(attr.p == "y"))

Developer Experience: Better due to attribute-style access.
Pythonic: More Pythonic than the first; leverages attributes.
Fluidity: Good; fluent style.
Ease of Use: Good; easier due to attributes.

[Reason for rejection: While this is a good approach, it still is not frictionless and requires the use of for
]()attributes. Additionally chaining of logical operators is difficult to follow.

### attr("k").Eq(10).And((attr("p").Eq("x")).Or(attr("p").Eq("y")))

Developer Experience: Okay; need to remember Eq, And, Or.
Pythonic: Not really; too many custom methods.
Fluidity: Okay; fluent but verbose.
Ease of Use: Moderate; verbose.

Rejection reason: While ok with the use of methods for the operators, it is not very easy to read and understand. Can
get messy with more complicated expressions.

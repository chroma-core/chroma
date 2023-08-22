# CIP: Pagination for list_collections and get_collections

## Motivation:

As Chroma's user base and data volume grow, the `list_collections` and `get_collections` are returning increasingly large sets of data. This affects performance and user. Adding lightweight pagination allows users to retrieve data in smaller chunks, improving performance and user experience.

## Impact:

This change impacts a large portion ( > 80%) of the user base either directly or indirectly. Add efficient pagination to `list_collections` requires changes to `get_collections`, which is used throughout the code base.

## Proposed Change:

Introduce two new parameters to the `list_collections` and `get_collections`: limit and offset.

- `limit`: The number of documents to return. Default: `None`
- `offset`: The offset to start returning results from. Default: `None`

## New or Changed Public Interfaces:

Same as above ^^ (list_collections and get_collections)

## Migration Plan and Compatibility:

By keeping the default values of limit and offset as None, we can mitigate the impact of this change on existing users and the rest of the code base. In the future we could consider changing the default values to 50 as @jeffchuber [suggested](https://github.com/chroma-core/chroma/issues/374#issuecomment-1682319084), although more changes would be required.

## Rejected Alternatives:

- Add pagination only for list_collections. This would be a half-baked solution as get_collections would still be returning large sets of data.
- Using a window function and a where clause in to the sqlite the limit the ammout of data queried. This would be a more efficient solution, but would be different than the `get` pagenation impelmentation and would be thrown out after the migration away from sqlite post alpha.

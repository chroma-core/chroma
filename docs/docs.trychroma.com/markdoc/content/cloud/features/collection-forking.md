---
id: forking
name: Collection Forking
---

# Collection Forking

**Instant copy-on-write collection forking in Chroma Cloud.**

Forking lets you create a new collection from an existing one instantly, using copy-on-write under the hood. The forked collection initially shares its data with the source and only incurs additional storage for incremental changes you make afterward.

{% Banner type="tip" %}
**Forking is available in Chroma Cloud only.** The storage engine on single-node Chroma does not support forking.
{% /Banner %}

## How it works

- **Copy-on-write**: Forks share data blocks with the source collection. New writes to either branch allocate new blocks; unchanged data remains shared.
- **Instant**: Forking a collection of any size completes quickly.
- **Isolation**: Changes to a fork do not affect the source, and vice versa.

## Try it

- **Cloud UI**: Open any collection and click the "Fork" button.
- **SDKs**: Use the fork API from Python or JavaScript.

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
source_collection = client.get_collection(name="main-repo-index")

# Create a forked collection. Name must be unique within the database.
forked_collection = source_collection.fork(name="main-repo-index-pr-1234")

# Forked collection is immediately queryable; changes are isolated
forked_collection.add(documents=["new content"], ids=["doc-pr-1"])  # billed as incremental storage
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
const sourceCollection = await client.getCollection({
  name: "main-repo-index",
});

// Create a forked collection. Name must be unique within the database.
const forkedCollection = await sourceCollection.fork({
  name: "main-repo-index-pr-1234",
});

await forkedCollection.add({
  ids: ["doc-pr-1"],
  documents: ["new content"], // billed as incremental storage
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

[In this notebook](https://github.com/chroma-core/chroma/blob/main/examples/advanced/forking.ipynb) you can find a comprehensive demo, where we index a codebase in a Chroma collection, and use forking to efficiently create collections for new branches.

## Pricing

- **$0.03 per fork call**
- **Storage**: You only pay for incremental blocks written after the fork (copy-on-write). Unchanged data remains shared across branches.

## Quotas and errors

Chroma limits the number of fork edges in your fork tree. Every time you call "fork", a new edge is created from the parent to the child. The count includes edges created by forks on the root collection and on any of its descendants; see the diagram below. The current default limit is **4,096** edges per tree. If you delete a collection, its edge remains in the tree and still counts.

If you exceed the limit, the request returns a quota error for the `NUM_FORKS` rule. In that case, create a new collection with a full copy to start a fresh root.

{% MarkdocImage lightSrc="/fork-edges-light.png" darkSrc="/fork-edges-dark.png" alt="Fork edges diagram" /%}

## When to use forking

- **Data versioning/checkpointing**: Maintain consistent snapshots as your data evolves.
- **Git-like workflows**: For example, index a branch by forking from its divergence point, then apply the diff to the fork. This saves both write and storage costs compared to re-ingesting the entire dataset.

## Notes

- Your forked collections will belong to the same database as the source collection.

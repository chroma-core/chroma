# Collection Forking

**Collection forking enables instant, zero-copy collection branching in Chroma Cloud.**

Forking lets you create a new collection from an existing one instantly, using copy-on-write under the hood. The forked collection initially shares its data with the source and only incurs additional storage for incremental changes you make afterward.

{% Banner type="tip" %}
Forking is available in Chroma Cloud only. The file system on single-node Chroma does not support forking — see [Single-Node Chroma: Performance and Limitations](../guides/deploy/performance). Chroma Cloud uses block storage that enables true copy-on-write semantics.
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
const sourceCollection = await client.getCollection({ name: "main-repo-index" });

// Create a forked collection. Name must be unique within the database.
const forkedCollection = await sourceCollection.fork({ name: "main-repo-index-pr-1234" });

await forkedCollection.add({
  ids: ["doc-pr-1"],
  documents: ["new content"], // billed as incremental storage
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

In this notebook you can find a comprehensive demo, where we index a codebase in a Chroma collection, and use forking to efficiently create collections for new branches: [Forking notebook](https://github.com/chroma-core/chroma/blob/main/examples/advanced/forking.ipynb).

## Pricing

- **$0.03 per fork call**
- **Storage**: You only pay for incremental blocks written after the fork (copy-on-write). Unchanged data remains shared across branches.

## Quotas and errors

Forking is subject to a limit on the total number of fork edges from the root. This counts every edge in the fork graph from the root collection (e.g., A→B→C is 2; A→[B, C], B→D is 3). The current default limit is **4,096**. If you exceed it, the fork request returns a quota error for the `NUM_FORKS` rule — catch it and fall back to creating a new collection with a full copy.

## When to use forking

- **Data versioning/checkpointing**: Maintain consistent snapshots as your data evolves.
- **Git-like workflows**: For example, index a pull request by forking the main repository’s collection, then apply the diff to the fork. This saves both write and storage costs compared to re-ingesting the entire dataset.
 - **Git-like workflows**: For example, index a branch by forking from its divergence point, then apply the diff to the fork. This saves both write and storage costs compared to re-ingesting the entire codebase.

## Notes

- Your forked collections will belong to the same database as the source collection.



---
id: pricing
name: Pricing
---

# Pricing

Chroma Cloud uses a simple, transparent, usage-based pricing model. You pay for what you use across **writes**, **reads**, and **storage**—with no hidden fees or tiered feature gating.

Need an estimate? Try our [pricing calculator](https://trychroma.com/pricing).

## Writes

Chroma Cloud charges **$2.50 per logical GiB** written via an add, update, or upsert.

- A _logical GiB_ is the raw, uncompressed size of the data you send to Chroma—regardless of how it's stored or indexed internally.
- You are only billed once per write, not for background compactions or reindexing.

## Forking

- Forking a collection costs **$0.03 per fork request**.
- Forks are copy-on-write. You only pay for incremental storage written after the fork; unchanged data remains shared.
- Forking is available on Chroma Cloud. Learn more on the [Collection Forking](./collection-forking) page.

## Reads

Read costs are based on both the amount of data scanned and the volume of data returned:

- **$0.0075 per TiB scanned**
- **$0.09 per GiB returned**

**How queries are counted:**

- A single vector similarity query counts as one query.
- Each metadata or full-text predicate in a query counts as an additional query.
- Full-text and regex filters are billed as _(N – 2)_ queries, where _N_ is the number of characters in the search string.

**Example:**

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.query(
   query_embeddings=[[1.0, 2.3, 1.1, ...]],
   where_document={"$contains": "hello world"}
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.query(
   queryEmbeddings=[[1.0, 2.3, 1.1, ...]],
   whereDocument={"$contains": "hello world"}
)
```

{% /Tab %}

{% /TabbedCodeBlock %}

For the query above (a single vector search and a 10-character full-text search), querying against 10 GiB of data incurs:

- 10,000 queries × 10 units (1 vector + 9 full-text) = 100,000 query units
- 10 GiB = 0.01 TiB scanned → 100,000 × 0.01 TiB × $0.0075 = **$7.50**

## Storage

Storage is billed at **$0.33 per GiB per month**, prorated by the hour:

- Storage usage is measured in **GiB-hours** to account for fluctuations over time.
- Storage is billed based on the logical amount of data written.
- All caching, including SSD caches used internally by Chroma, are not billed to you.

## Frequently Asked Questions

**Is there a free tier?**

We offer $5 in credits to new users.

**How is multi-tenancy handled for billing?**

Billing is account-based. All data across your collections and tenants within a Chroma Cloud account is aggregated for pricing.

**Can I deploy Chroma in my own VPC?**

Yes. We offer a BYOC (bring your own cloud) option for single-tenant deployments. [Contact us](mailto:support@trychroma.com) for more details.

**Do I get charged for background indexing?**

No. You’re only billed for the logical data you write and the storage you consume. Background jobs like compaction or reindexing do not generate additional write or read charges.

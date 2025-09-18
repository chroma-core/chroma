---
id: data-model
name: Data Model
---

# Chroma Data Model

Chroma’s data model is designed to balance simplicity, flexibility, and scalability. It introduces a few core abstractions—**Tenants**, **Databases**, and **Collections**—that allow you to organize, retrieve, and manage data efficiently across environments and use cases.

### Collections

A **collection** is the fundamental unit of storage and querying in Chroma. Each collection contains a set of items, where each item consists of:

- An ID uniquely identifying the item
- An **embedding vector**
- Optional **metadata** (key-value pairs)
- A document that belongs to the provided embedding

Collections are independently indexed and are optimized for fast retrieval using **vector similarity**, **full-text search**, and **metadata filtering**. In distributed deployments, collections can be sharded or migrated across nodes as needed; the system transparently manages paging them in and out of memory based on access patterns.

### Databases

Collections are grouped into **databases**, which serve as a logical namespace. This is useful for organizing collections by purpose—for example, separating environments like "staging" and "production", or grouping applications under a common schema.

Each database contains multiple collections, and each collection name must be unique within a database.

### Tenants

At the top level of the model is the **tenant**, which represents a single user, team, or account. Tenants provide complete isolation. No data or metadata, is shared across tenants. All access control, quota enforcement, and billing are scoped to the tenant level.

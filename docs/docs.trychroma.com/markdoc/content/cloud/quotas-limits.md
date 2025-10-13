---
id: quotas-and-limits
name: Quotas & Limits
---

# Quotas & Limits

To ensure the stability and fairness in a multi-tenant environment, Chroma Cloud enforces input and query quotas across all user-facing operations. These limits are designed to strike a balance between performance, reliability, and ease of use for the majority of workloads.

{% Banner type="tip" %}
Most quotas can be increased upon request. If your application requires higher limits, please [contact us](mailto:support@trychroma.com).
{% /Banner %}

| **Quota**                                           | **Value**   |
| --------------------------------------------------- | ----------- |
| Maximum embedding dimensions                        | 4,096       |
| Maximum document bytes                              | 16,384      |
| Maximum URI bytes                                   | 256         |
| Maximum ID size bytes                               | 128         |
| Maximum database name size bytes                    | 128         |
| Maximum collection name size bytes                  | 128         |
| Maximum record metadata value size bytes            | 4,096       |
| Maximum collection metadata value size bytes        | 256         |
| Maximum metadata key size bytes                     | 36          |
| Maximum number of record metadata keys              | 32          |
| Maximum number of collection metadata keys          | 32          |
| Maximum number of where predicates                  | 8           |
| Maximum size of full text search or regex search    | 256         |
| Maximum number of results returned                  | 300         |
| Maximum number of concurrent reads per collection   | 5           |
| Maximum number of concurrent writes per collection  | 5           |
| Maximum number of collections                       | 1,000,000   |
| Maximum number of records per collection            | 5,000,000   |
| Maximum fork edges from root                        | 4,096       |
| Maximum number of records per write                 | 300         |

These limits apply per request or per collection as appropriate. For example, concurrent read/write limits are tracked independently per collection, and full-text query limits apply to the length of the input string, not the number of documents searched.

For details about the fork edges limit and quota error handling when forking, see [Collection Forking](./collection-forking).

If you expect to approach these limits, we recommend reaching out early so we can ensure your account is configured accordingly.

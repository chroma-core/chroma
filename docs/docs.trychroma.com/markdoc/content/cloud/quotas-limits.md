# Quotas & Limits

To ensure the stability and fairness in a multi-tenant environment, Chroma Cloud enforces input and query quotas across all user-facing operations. These limits are designed to strike a balance between performance, reliability, and ease of use for the majority of workloads.

Most quotas can be increased upon request, once a clear need has been demonstrated. If your application requires higher limits, please [contact us](mailto:support@trychroma.com). We are happy to help.

| **Quota** | **Value** |
| --- | --- |
| Maximum embedding dimensions | 3072 |
| Maximum document bytes | 16,384 |
| Maximum uri bytes | 128 |
| Maximum ID size bytes  | 128 |
| Maximum metadata value size bytes | 256 |
| Maximum metadata key size bytes | 36 |
| Maximum number of metadata keys | 16 |
| Maximum number of where predicates  | 8 |
| Maximum size of full text search or regex search | 256 |
| Maximum number of results returned | 100 |
| Maximum number of concurrent reads per collection | 5 |
| Maximum number of concurrent writes per collection | 5 |
| Maximum number of collections | 1,000,000 |

These limits apply per request or per collection as appropriate. For example, concurrent read/write limits are tracked independently per collection, and full-text query limits apply to the length of the input string, not the number of documents searched.

If you expect to approach these limits, we recommend reaching out early so we can ensure your account is configured accordingly.

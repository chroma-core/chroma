---
title: CLI
---

## Vacuuming

Vacuuming shrinks and optimizes your database.

Vacuuming after upgrading from a version of Chroma below 0.6 will greatly reduce the size of your database and enable continuous database pruning. A warning is logged during server startup if this is necessary.

In most other cases, vacuuming is unnecessary. **It does not need to be run regularly**.

Vacuuming blocks all reads and writes to your database while it's running, so we recommend shutting down your Chroma server before vacuuming (although it's not strictly required).

To vacuum your database, run:

```bash
chroma utils vacuum --path <your-data-directory>
```

For large databases, expect this to take up to a few minutes.

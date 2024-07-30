---
title: CLI
---

## Vacuuming

Vacuuming shrinks and optimizes your database. **It does not need to be run regularly**. It blocks all reads and writes to your database while it's running, so it should only be run when absolutely necessary. In most cases, vacuuming will save very little disk space. We recommend shutting down your Chroma server before vacuuming (although it's not strictly required).

To vacuum your database, run:

```bash
chroma utils vacuum --path <your-data-directory>
```

For large databases, expect this to take up to a few minutes.

If you recently upgraded from an older version of Chroma (<0.6), you should vacuum once to greatly reduce the size of your database and enable continuous database pruning. A warning is logged during server startup if this is necessary.

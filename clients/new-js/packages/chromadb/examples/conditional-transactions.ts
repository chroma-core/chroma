/**
 * Collection-scoped conditional transaction example.
 *
 * Run against a Chroma server that supports conditional transactions:
 *
 *   cd clients/new-js
 *   npx tsx packages/chromadb/examples/conditional-transactions.ts
 *
 * Optional environment:
 *
 *   CHROMA_ENDPOINT=http://localhost:8000
 *   CHROMA_TENANT=default_tenant
 *   CHROMA_DATABASE=default_database
 *   CHROMA_API_KEY=...
 */

import { ChromaClient } from "../src/index.ts";

const COLLECTION_NAME = "transactional_chroma_js_example";
const RECORD_ID = "txn-doc";

const endpoint = new URL(
  process.env.CHROMA_ENDPOINT ?? "http://localhost:8000",
);
const apiKey = process.env.CHROMA_API_KEY;

const client = new ChromaClient({
  ssl: endpoint.protocol === "https:",
  host: endpoint.hostname,
  port:
    endpoint.port !== ""
      ? Number(endpoint.port)
      : endpoint.protocol === "https:"
      ? 443
      : 80,
  tenant: process.env.CHROMA_TENANT ?? "default_tenant",
  database: process.env.CHROMA_DATABASE ?? "default_database",
  headers: apiKey ? { "x-chroma-token": apiKey } : undefined,
});

const metadata = (status: string, version: number) => ({
  status,
  version,
});

const main = async () => {
  try {
    await client.deleteCollection({ name: COLLECTION_NAME });
  } catch {
    // Ignore missing collections so the example is repeatable.
  }

  const collection = await client.getOrCreateCollection({
    name: COLLECTION_NAME,
    embeddingFunction: null,
  });

  const outcome = await collection.conditional().run(
    async (txn) => {
      const existing = await txn.get({
        ids: [RECORD_ID],
        include: ["documents", "metadatas"],
      });

      if (existing.ids.length === 0) {
        await txn.add({
          ids: [RECORD_ID],
          embeddings: [[1.0, 0.0, 0.0]],
          metadatas: [metadata("created-by-run", 1)],
        });
        return "created";
      }

      await txn.update({
        ids: [RECORD_ID],
        metadatas: [metadata("updated-by-run", 1)],
      });
      return "updated";
    },
    { maxRetries: 3 },
  );
  console.log(`run() transaction ${outcome} "${RECORD_ID}"`);

  const txn = collection.conditional();
  const before = await txn.get({
    ids: [RECORD_ID],
    include: ["documents", "metadatas"],
  });
  if (before.ids.length === 0) {
    throw new Error(`"${RECORD_ID}" disappeared before manual commit`);
  }

  await txn.update({
    ids: [RECORD_ID],
    metadatas: [metadata("updated-by-manual-commit", 2)],
  });
  const committed = await txn.commit();
  console.log(`manual commit wrote ${committed.record_count} record(s)`);

  const after = await collection.get({
    ids: [RECORD_ID],
    include: ["metadatas"],
  });
  console.log("final metadata:", after.metadatas);
};

await main();

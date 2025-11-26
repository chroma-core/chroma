import { ChromaClient, CloudClient } from "chromadb";

export function getChromaClient(): ChromaClient {
  if (!process.env.CHROMA_API_KEY) {
    throw new Error(
      "Missing Chroma API key. Set your CHROMA_API_KEY environment variable",
    );
  }

  if (!process.env.CHROMA_TENANT) {
    throw new Error(
      "Missing Chroma tenant information. Set your CHROMA_TENANT environment variable",
    );
  }

  if (!process.env.CHROMA_DATABASE) {
    throw new Error(
      "Missing Chroma DB name. Set your CHROMA_DATABASE environment variable",
    );
  }

  if (process.env.CHROMA_HOST) {
    return new ChromaClient({
      host: process.env.CHROMA_HOST,
      tenant: process.env.CHROMA_TENANT,
      database: process.env.CHROMA_DATABASE,
      headers: { 'x-chroma-token': process.env.CHROMA_API_KEY },
    })
  }

  return new CloudClient({
    tenant: process.env.CHROMA_TENANT,
    database: process.env.CHROMA_DATABASE,
  });
}

export async function getMoviesCollection() {
  const collectionName =
    process.env.CHROMA_COLLECTION ||
    "movies";

  const client = getChromaClient();
  return await client.getCollection({
    name: collectionName,
  });
}



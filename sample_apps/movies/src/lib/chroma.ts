import {
  ChromaClient,
  CloudClient,
  K,
  Knn,
  knownSparseEmbeddingFunctions,
  registerSparseEmbeddingFunction,
  Rrf,
  Search,
} from "chromadb";
import { ChromaBm25EmbeddingFunction } from "@chroma-core/all";

function getChromaClient(): ChromaClient {
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
    const hostUrl = new URL(process.env.CHROMA_HOST);
    return new ChromaClient({
      ssl: process.env.CHROMA_HOST.startsWith("https"),
      host: hostUrl.host,
      tenant: process.env.CHROMA_TENANT,
      database: process.env.CHROMA_DATABASE,
      headers: { "x-chroma-token": process.env.CHROMA_API_KEY },
    });
  }

  return new CloudClient({
    tenant: process.env.CHROMA_TENANT,
    database: process.env.CHROMA_DATABASE,
  });
}

export async function queryMovies(query: string) {
  const moviesCollection = await getChromaClient().getCollection({
    name: "movies",
  });

  if (!knownSparseEmbeddingFunctions.has("chroma-bm25")) {
    registerSparseEmbeddingFunction("chroma-bm25", ChromaBm25EmbeddingFunction);
  }

  const rank = Rrf({
    ranks: [
      Knn({
        query,
        returnRank: true,
        limit: 100,
      }),
      Knn({
        query,
        key: "bm25_sparse_vector",
        returnRank: true,
        limit: 100,
      }),
    ],
    weights: [1, 1],
  });

  const search = new Search()
    .rank(rank)
    .limit(10)
    .select(K.ID, K.DOCUMENT, K.METADATA, K.SCORE);

  const searchResult = await moviesCollection.search(search);
  const rows = searchResult.rows()[0] ?? [];

  const sanitizedRows = rows.map((item) => {
    const { metadata, ...rest } = item;
    if (!metadata) return item;

    // Drop bm25_sparse_vector from metadata before returning results
    const filteredMetadata = Object.fromEntries(
      Object.entries(metadata).filter(([key]) => key !== "bm25_sparse_vector"),
    );

    return { ...rest, metadata: filteredMetadata };
  });

  return {
    results: sanitizedRows,
  };
}

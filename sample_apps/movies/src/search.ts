import { Rrf, Knn, Search, K } from "chromadb";
import { getMoviesCollection } from "./client.js";


const DEFAULT_LIMIT = 5;

export async function searchMovies(query: string) {
  const collection = await getMoviesCollection();

  const rank = Rrf({
    ranks: [
      Knn({
        query,
        returnRank: true,
        limit: DEFAULT_LIMIT,
      }),
      Knn({
        query,
        key: "bm25_sparse_vector",
        returnRank: true,
        limit: DEFAULT_LIMIT,
      }),
    ],
    weights: [1, 1],
    k: DEFAULT_LIMIT,
  })

  const search = new Search()
    .rank(rank)
    .limit(DEFAULT_LIMIT)
    .select(K.ID, K.DOCUMENT, K.METADATA, K.SCORE);

  const searchResult = await collection.search(search);
  const row = searchResult.rows()[0] ?? []

  return {
    results: row,
    count: row.length,
  };
}

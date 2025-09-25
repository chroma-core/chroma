import { describe, expect, test } from "@jest/globals";
import { K, Knn, Search, SearchResult, Val, toSearch } from "../src";
import type { SearchResponse } from "../src/api";

describe("search expression DSL", () => {
  test("builder chain converts to API payload", () => {
    const search = new Search()
      .where(K("category").eq("science").and(K("score").gt(0.5)))
      .rank(Knn({ query: [0.1, 0.2], limit: 10 }).add(Val(0.2)))
      .limit({ limit: 5, offset: 2 })
      .select(K.DOCUMENT, K.SCORE, "title");

    const payload = search.toPayload();

    expect(payload.limit).toEqual({ offset: 2, limit: 5 });
    expect(payload.select).toEqual({ keys: ["#document", "#score", "title"] });
    expect(payload.filter).toEqual({
      where_clause: {
        $and: [
          { category: { $eq: "science" } },
          { score: { $gt: 0.5 } },
        ],
      },
    });
    expect(payload.rank).toEqual({
      $sum: [
        {
          $knn: {
            query: [0.1, 0.2],
            key: "#embedding",
            limit: 10,
          },
        },
        { $val: 0.2 },
      ],
    });
  });

  test("plain object inputs convert via toSearch", () => {
    const search = toSearch({
      where: { status: { $ne: "archived" } },
      rank: { $val: 0.75 },
      limit: 7,
      select: { keys: ["#document"] },
    });

    const payload = search.toPayload();

    expect(payload).toEqual({
      filter: { where_clause: { status: { $ne: "archived" } } },
      rank: { $val: 0.75 },
      limit: { offset: 0, limit: 7 },
      select: { keys: ["#document"] },
    });
  });

  test("selectAll helper includes predefined keys", () => {
    const payload = new Search().selectAll().toPayload();
    expect(payload.select).toEqual({
      keys: ["#document", "#embedding", "#metadata", "#score"],
    });
  });

  test("SearchResult rows flatten column-major data", () => {
    const response: SearchResponse = {
      ids: [
        ["id1", "id2"],
        ["id3"],
      ],
      documents: [
        ["doc1", null],
        null,
      ],
      embeddings: [
        [
          [1, 2, 3],
          null,
        ],
        [
          [4, 5, 6],
        ],
      ],
      metadatas: [
        [
          { topic: "science" },
          null,
        ],
        [
          { topic: "math" },
        ],
      ],
      scores: [
        [0.12, 0.34],
        [null],
      ],
      select: [
        ["Document", "Score"],
        [{ MetadataField: "topic" }],
      ],
    };

    const result = new SearchResult(response);

    expect(result.rows()).toEqual([
      [
        {
          id: "id1",
          document: "doc1",
          embedding: [1, 2, 3],
          metadata: { topic: "science" },
          score: 0.12,
        },
        {
          id: "id2",
          score: 0.34,
        },
      ],
      [
        {
          id: "id3",
          embedding: [4, 5, 6],
          metadata: { topic: "math" },
        },
      ],
    ]);
  });

  test("K helper maps metadata selections and operators", () => {
    const where = K("author").isIn(["alice", "bob"]).or(K.DOCUMENT.contains("quantum"));
    const payload = new Search({ where }).toPayload();

    expect(payload.filter).toEqual({
      where_clause: {
        $or: [
          { author: { $in: ["alice", "bob"] } },
          { "#document": { $contains: "quantum" } },
        ],
      },
    });
  });
});

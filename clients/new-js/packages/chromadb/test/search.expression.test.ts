import { describe, expect, jest, test } from "@jest/globals";
import { K, Knn, Rrf, Search, SearchResult, Val, toSearch } from "../src";
import type { SearchResponse, SparseVector } from "../src/api";
import { CollectionImpl } from "../src/collection";
import type { CollectionConfiguration } from "../src/collection-configuration";
import type { ChromaClient } from "../src/chroma-client";
import type { EmbeddingFunction, SparseEmbeddingFunction } from "../src/embedding-function";

class QueryMockEmbedding implements EmbeddingFunction {
  public readonly name = "query_mock";

  constructor(
    private readonly queryVector: number[] = [0.42, 0.24, 0.11],
    private readonly denseVector: number[] = [0.9, 0.8, 0.7],
  ) { }

  async generate(texts: string[]): Promise<number[][]> {
    return texts.map(() => this.denseVector.slice());
  }

  async generateForQueries(texts: string[]): Promise<number[][]> {
    return texts.map(() => this.queryVector.slice());
  }
}

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
      $and: [{ category: { $eq: "science" } }, { score: { $gt: 0.5 } }],
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
      filter: { status: { $ne: "archived" } },
      rank: { $val: 0.75 },
      limit: { offset: 0, limit: 7 },
      select: { keys: ["#document"] },
    });
  });

  test("direct construction with dicts (from Python examples)", () => {
    // Example from plan.py lines 52-58
    const search = new Search({
      where: { status: "active" },
      rank: { $knn: { query: [0.1, 0.2] } },
      limit: 10,
      select: ["#document", "#score"],
    });

    const payload = search.toPayload();

    expect(payload.filter).toEqual({
      status: { $eq: "active" },
    });
    // Check that rank has $knn with the query
    expect(payload.rank).toBeDefined();
    expect((payload.rank as any)?.$knn).toBeDefined();
    expect((payload.rank as any)?.$knn?.query).toEqual([0.1, 0.2]);
    expect(payload.limit).toEqual({ offset: 0, limit: 10 });
    expect(payload.select?.keys).toContain("#document");
    expect(payload.select?.keys).toContain("#score");
  });

  test("builder pattern with dicts (from Python examples)", () => {
    // Example from plan.py lines 67-72
    const search = new Search()
      .where({ status: "active" })
      .rank({ $knn: { query: [0.1, 0.2] } })
      .limit(10)
      .select(K.DOCUMENT, K.SCORE);

    const payload = search.toPayload();

    expect(payload.filter).toEqual({
      status: { $eq: "active" },
    });
    // Check that rank has $knn with the query
    expect(payload.rank).toBeDefined();
    expect((payload.rank as any)?.$knn).toBeDefined();
    expect((payload.rank as any)?.$knn?.query).toEqual([0.1, 0.2]);
    expect(payload.limit).toEqual({ offset: 0, limit: 10 });
    expect(payload.select?.keys).toContain("#document");
    expect(payload.select?.keys).toContain("#score");
  });

  test("filter by IDs (from Python examples)", () => {
    // Example from plan.py line 75
    const search = new Search().where(K.ID.isIn(["id1", "id2", "id3"]));

    const payload = search.toPayload();

    expect(payload.filter).toEqual({
      "#id": { $in: ["id1", "id2", "id3"] },
    });
  });

  test("combined ID and metadata filtering (from Python examples)", () => {
    // Example from plan.py line 78
    const search = new Search().where(
      K.ID.isIn(["id1", "id2"]).and(K("status").eq("active")),
    );

    const payload = search.toPayload();

    expect(payload.filter).toEqual({
      $and: [{ "#id": { $in: ["id1", "id2"] } }, { status: { $eq: "active" } }],
    });
  });

  test("empty Search with defaults (from Python examples)", () => {
    // Example from plan.py lines 80-84
    const search = new Search();

    const payload = search.toPayload();

    expect(payload.filter).toBeUndefined();
    expect(payload.rank).toBeUndefined();
    expect(payload.limit).toEqual({ offset: 0 });
    expect(payload.select).toEqual({ keys: [] });
  });

  test("complex where with $and operator", () => {
    const search = new Search({
      where: {
        $and: [{ category: { $eq: "science" } }, { score: { $gt: 0.5 } }],
      },
    });

    const payload = search.toPayload();

    expect(payload.filter).toEqual({
      $and: [{ category: { $eq: "science" } }, { score: { $gt: 0.5 } }],
    });
  });

  test("complex where with $or operator", () => {
    const search = new Search({
      where: {
        $or: [{ status: { $eq: "active" } }, { priority: { $eq: "high" } }],
      },
    });

    const payload = search.toPayload();

    expect(payload.filter).toEqual({
      $or: [{ status: { $eq: "active" } }, { priority: { $eq: "high" } }],
    });
  });

  test("all where operators as dict", () => {
    // Test all the operators from operator.py lines 72-83

    // $eq
    expect(
      new Search({ where: { field: { $eq: "value" } } }).toPayload().filter,
    ).toEqual({
      field: { $eq: "value" },
    });

    // $ne
    expect(
      new Search({ where: { field: { $ne: "value" } } }).toPayload().filter,
    ).toEqual({
      field: { $ne: "value" },
    });

    // $gt
    expect(
      new Search({ where: { field: { $gt: 5 } } }).toPayload().filter,
    ).toEqual({
      field: { $gt: 5 },
    });

    // $gte
    expect(
      new Search({ where: { field: { $gte: 5 } } }).toPayload().filter,
    ).toEqual({
      field: { $gte: 5 },
    });

    // $lt
    expect(
      new Search({ where: { field: { $lt: 5 } } }).toPayload().filter,
    ).toEqual({
      field: { $lt: 5 },
    });

    // $lte
    expect(
      new Search({ where: { field: { $lte: 5 } } }).toPayload().filter,
    ).toEqual({
      field: { $lte: 5 },
    });

    // $in
    expect(
      new Search({ where: { field: { $in: ["a", "b"] } } }).toPayload().filter,
    ).toEqual({
      field: { $in: ["a", "b"] },
    });

    // $nin
    expect(
      new Search({ where: { field: { $nin: ["a", "b"] } } }).toPayload().filter,
    ).toEqual({
      field: { $nin: ["a", "b"] },
    });

    // $contains
    expect(
      new Search({ where: { field: { $contains: "text" } } }).toPayload()
        .filter,
    ).toEqual({
      field: { $contains: "text" },
    });

    // $not_contains
    expect(
      new Search({ where: { field: { $not_contains: "text" } } }).toPayload()
        .filter,
    ).toEqual({
      field: { $not_contains: "text" },
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
      ids: [["id1", "id2"], ["id3"]],
      documents: [["doc1", null], null],
      embeddings: [[[1, 2, 3], null], [[4, 5, 6]]],
      metadatas: [[{ topic: "science" }, null], [{ topic: "math" }]],
      scores: [[0.12, 0.34], [null]],
      select: [["Document", "Score"], [{ MetadataField: "topic" }]],
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
    const where = K("author")
      .isIn(["alice", "bob"])
      .or(K.DOCUMENT.contains("quantum"));
    const payload = new Search({ where }).toPayload();

    expect(payload.filter).toEqual({
      $or: [
        { author: { $in: ["alice", "bob"] } },
        { "#document": { $contains: "quantum" } },
      ],
    });
  });

  test("Rrf rank expression serializes combined scores", () => {
    const search = new Search()
      .rank(
        Rrf({
          ranks: [Knn({ query: [0.1, 0.2], limit: 5 }), Val(0.4)],
          k: 20,
          weights: [2, 1],
        }),
      )
      .limit(5)
      .select(K.SCORE);

    const payload = search.toPayload();
    const rankPayload = payload.rank as any;

    expect(rankPayload?.$mul).toBeDefined();
    const mulOperands = rankPayload.$mul as any[];
    expect(Array.isArray(mulOperands)).toBe(true);
    expect(mulOperands.some((op) => op?.$sum)).toBe(true);
    expect(mulOperands.some((op) => op?.$val === -1)).toBe(true);

    const sumOperand = mulOperands.find((op) => op?.$sum);
    expect(sumOperand).toBeDefined();
    expect(Array.isArray(sumOperand.$sum)).toBe(true);
    expect(sumOperand.$sum.some((item: any) => item?.$div)).toBe(true);
  });

  test("search auto-embeds string knn queries before sending to API", async () => {
    const queryText = "semantic search request";
    const embeddedVector = [0.42, 0.24, 0.11];
    const embeddingFunction = new QueryMockEmbedding(
      embeddedVector,
      [0.9, 0.8, 0.7],
    );
    const generateSpy = jest.spyOn(embeddingFunction, "generate");
    const generateForQueriesSpy = jest.spyOn(
      embeddingFunction,
      "generateForQueries",
    );

    let capturedBody: any;
    const mockChromaClient = {
      getMaxBatchSize: jest.fn<() => Promise<number>>().mockResolvedValue(1000),
      supportsBase64Encoding: jest.fn<() => Promise<boolean>>().mockResolvedValue(false),
      _path: jest.fn<() => Promise<{ path: string; tenant: string; database: string }>>().mockResolvedValue({ path: "/api/v1", tenant: "default_tenant", database: "default_database" }),
    };

    const mockApiClient = {
      post: jest.fn().mockImplementation(async (options: any) => {
        capturedBody = options.body;
        return {
          data: {
            ids: [],
            documents: [],
            embeddings: [],
            metadatas: [],
            scores: [],
            select: [],
          } as SearchResponse,
        };
      }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "col-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined,
      embeddingFunction,
      schema: undefined,
    });

    await collection.search(new Search().rank(Knn({ query: queryText, limit: 7 })));

    expect(mockApiClient.post).toHaveBeenCalledTimes(1);
    expect(generateForQueriesSpy).toHaveBeenCalledTimes(1);
    expect(generateForQueriesSpy).toHaveBeenCalledWith([queryText]);
    expect(generateSpy).not.toHaveBeenCalled();

    expect(capturedBody).toBeDefined();
    expect(Array.isArray(capturedBody.searches)).toBe(true);
    expect(capturedBody.searches).toHaveLength(1);

    const knnPayload = capturedBody.searches[0].rank.$knn;
    expect(knnPayload.query).toEqual(embeddedVector);
    expect(knnPayload.key).toBe("#embedding");
    expect(knnPayload.limit).toBe(7);
  });

  test("search auto-embeds string knn queries with sparse embedding function", async () => {
    const queryText = "hello world";

    class DeterministicSparseEmbedding implements SparseEmbeddingFunction {
      public readonly name = "deterministic_sparse";

      constructor(private readonly label = "sparse") { }

      async generate(texts: string[]): Promise<SparseVector[]> {
        return texts.map((text) => {
          if (text === "hello world") {
            return { indices: [0], values: [11.0] };
          }
          return { indices: [], values: [] };
        });
      }

      getConfig(): Record<string, any> {
        return { label: this.label };
      }

      static buildFromConfig(config: Record<string, any>): DeterministicSparseEmbedding {
        return new DeterministicSparseEmbedding(config.label);
      }
    }

    const sparseEf = new DeterministicSparseEmbedding("sparse");
    const generateSpy = jest.spyOn(sparseEf, "generate");

    const { Schema, SparseVectorIndexConfig } = await import("../src/schema");
    const schema = new Schema().createIndex(
      new SparseVectorIndexConfig({
        sourceKey: "raw_text",
        embeddingFunction: sparseEf,
      }),
      "sparse_metadata",
    );

    let capturedBody: any;
    const mockChromaClient = {
      getMaxBatchSize: jest.fn<() => Promise<number>>().mockResolvedValue(1000),
      supportsBase64Encoding: jest.fn<() => Promise<boolean>>().mockResolvedValue(false),
      _path: jest.fn<() => Promise<{ path: string; tenant: string; database: string }>>().mockResolvedValue({ path: "/api/v1", tenant: "default_tenant", database: "default_database" }),
    };

    const mockApiClient = {
      post: jest.fn().mockImplementation(async (options: any) => {
        capturedBody = options.body;
        return {
          data: {
            ids: [],
            documents: [],
            embeddings: [],
            metadatas: [],
            scores: [],
            select: [],
          } as SearchResponse,
        };
      }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "col-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined,
      embeddingFunction: undefined,
      schema,
    });

    await collection.search(
      new Search().rank(Knn({ key: "sparse_metadata", query: queryText, limit: 10 })),
    );

    expect(mockApiClient.post).toHaveBeenCalledTimes(1);
    expect(generateSpy).toHaveBeenCalledTimes(1);
    expect(generateSpy).toHaveBeenCalledWith([queryText]);

    expect(capturedBody).toBeDefined();
    expect(Array.isArray(capturedBody.searches)).toBe(true);
    expect(capturedBody.searches).toHaveLength(1);

    const knnPayload = capturedBody.searches[0].rank.$knn;
    expect(knnPayload.query).toEqual({ indices: [0], values: [11.0] });
    expect(knnPayload.key).toBe("sparse_metadata");
    expect(knnPayload.limit).toBe(10);
  });
});

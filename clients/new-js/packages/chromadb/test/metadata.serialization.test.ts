import { afterEach, describe, expect, jest, test } from "@jest/globals";
import { RecordService } from "../src/api";
import { CollectionImpl } from "../src/collection";
import { serializeMetadata, serializeMetadatas } from "../src/utils";

afterEach(() => {
  jest.restoreAllMocks();
});

describe("metadata serialization", () => {
  test("preserves null field deletions and other metadata values", () => {
    const metadata = {
      remove: null,
      text: "",
      count: 0,
      enabled: false,
      tags: ["a", "b"],
      sparse: { indices: [1], values: [0.5] },
    };

    expect(serializeMetadata(metadata)).toEqual({
      remove: null,
      text: "",
      count: 0,
      enabled: false,
      tags: ["a", "b"],
      sparse: { "#type": "sparse_vector", indices: [1], values: [0.5] },
    });
    expect(metadata.sparse).not.toHaveProperty("#type");
    expect(serializeMetadatas([{ remove: null }])).toEqual([{ remove: null }]);
  });

  test("keeps absent metadata distinct from null metadata", () => {
    expect(serializeMetadata(undefined)).toBeUndefined();
    expect(serializeMetadata(null)).toBeNull();
    expect(serializeMetadatas(undefined)).toBeUndefined();
    expect(serializeMetadatas(null)).toBeNull();
  });

  test.each(["update", "upsert"] as const)(
    "sends null field deletions through collection.%s",
    async (operation) => {
      const service =
        operation === "update" ? "collectionUpdate" : "collectionUpsert";
      const spy = jest
        .spyOn(RecordService, service)
        .mockResolvedValue({} as any);
      const collection = new CollectionImpl({
        chromaClient: {
          getMaxBatchSize: jest.fn(async () => 100),
          supportsBase64Encoding: jest.fn(async () => false),
        } as any,
        apiClient: {} as any,
        id: "00000000-0000-0000-0000-000000000001",
        tenant: "tenant",
        database: "database",
        name: "test",
        configuration: {},
      });

      await collection[operation]({
        ids: ["record"],
        embeddings: [[1, 2]],
        metadatas: [{ remove: null, keep: "value" }],
      });

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.objectContaining({
            metadatas: [{ remove: null, keep: "value" }],
          }),
        }),
      );
    },
  );
});

import { describe, expect, jest, test } from "@jest/globals";
import { CollectionImpl } from "../src/collection";
import type { CollectionConfiguration } from "../src/collection-configuration";
import type { ChromaClient } from "../src/chroma-client";

describe("count with readLevel", () => {
  test("count passes readLevel option to API as query param", async () => {
    const { ReadLevel } = await import("../src/types");

    let capturedQuery: any;
    const mockChromaClient = {
      getMaxBatchSize: jest.fn<() => Promise<number>>().mockResolvedValue(1000),
      supportsBase64Encoding: jest
        .fn<() => Promise<boolean>>()
        .mockResolvedValue(false),
      _path: jest
        .fn<() => Promise<{ path: string; tenant: string; database: string }>>()
        .mockResolvedValue({
          path: "/api/v1",
          tenant: "default_tenant",
          database: "default_database",
        }),
    };

    const mockApiClient = {
      get: jest.fn().mockImplementation(async (options: any) => {
        capturedQuery = options.query;
        return { data: 42 };
      }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "col-id",
      name: "test",
      tenant: "default_tenant",
      database: "default_database",
      configuration: {} as CollectionConfiguration,
      metadata: undefined,
      embeddingFunction: undefined,
      schema: undefined,
    });

    // Test with INDEX_ONLY
    const count1 = await collection.count({
      readLevel: ReadLevel.INDEX_ONLY,
    });
    expect(count1).toBe(42);
    expect(mockApiClient.get).toHaveBeenCalledTimes(1);
    expect(capturedQuery).toBeDefined();
    expect(capturedQuery.read_level).toBe("index_only");

    // Test with INDEX_AND_WAL
    mockApiClient.get.mockClear();
    const count2 = await collection.count({
      readLevel: ReadLevel.INDEX_AND_WAL,
    });
    expect(count2).toBe(42);
    expect(mockApiClient.get).toHaveBeenCalledTimes(1);
    expect(capturedQuery.read_level).toBe("index_and_wal");

    // Test without readLevel (should not send query)
    mockApiClient.get.mockClear();
    const count3 = await collection.count();
    expect(count3).toBe(42);
    expect(mockApiClient.get).toHaveBeenCalledTimes(1);
    expect(capturedQuery).toBeUndefined();
  });
});

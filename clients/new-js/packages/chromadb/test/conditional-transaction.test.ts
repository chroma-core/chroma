import { afterEach, describe, expect, jest, test } from "@jest/globals";
import { chromaFetch } from "../src/chroma-fetch";
import { CollectionImpl } from "../src/collection";
import { RecordService } from "../src/api";
import {
  ChromaBackoffError,
  ChromaConditionalWriteConflictError,
  ChromaStaleReadError,
} from "../src/errors";

const collection = () =>
  new CollectionImpl({
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

afterEach(() => {
  jest.restoreAllMocks();
});

describe("conditional collection transactions", () => {
  test("tracks point reads and commits buffered adds", async () => {
    const getSpy = jest
      .spyOn(RecordService, "collectionConditionalGet")
      .mockResolvedValue({
        data: {
          ids: ["present"],
          documents: ["present document"],
          include: ["documents"],
          read_token: 42,
        },
      } as any);
    const commitSpy = jest
      .spyOn(RecordService, "collectionConditionalCommit")
      .mockResolvedValue({
        data: {
          first_inserted_record_offset: 7,
          record_count: 1,
        },
      } as any);

    const transaction = collection().conditional();
    const result = await transaction.get({
      ids: ["present", "absent"],
      include: ["documents"],
    });
    await transaction.add({ ids: ["absent"], embeddings: [[1, 2, 3]] });
    const committed = await transaction.commit();

    expect(result.ids).toEqual(["present"]);
    expect(result.documents).toEqual(["present document"]);
    expect(committed).toEqual({
      first_inserted_record_offset: 7,
      record_count: 1,
    });
    expect(getSpy.mock.calls[0][0]).toMatchObject({
      path: {
        tenant: "tenant",
        database: "database",
        collection_id: "00000000-0000-0000-0000-000000000001",
      },
      body: {
        ids: ["present", "absent"],
        include: ["documents"],
        read_token: null,
      },
    });
    expect(commitSpy.mock.calls[0][0]).toMatchObject({
      body: {
        read_token: 42,
        read_ids: ["absent", "present"],
        operations: [
          {
            operation: "add",
            payload: {
              ids: ["absent"],
              embeddings: [[1, 2, 3]],
            },
          },
        ],
      },
    });
  });

  test("commits write-only upserts without a read token", async () => {
    const commitSpy = jest
      .spyOn(RecordService, "collectionConditionalCommit")
      .mockResolvedValue({
        data: {
          first_inserted_record_offset: 11,
          record_count: 1,
        },
      } as any);

    const transaction = collection().conditional();
    await transaction.upsert({ ids: ["unknown"], embeddings: [[1]] });
    await transaction.commit();

    expect(commitSpy.mock.calls[0][0]).toMatchObject({
      body: {
        read_token: null,
        read_ids: [],
        operations: [
          {
            operation: "upsert",
            payload: {
              ids: ["unknown"],
              embeddings: [[1]],
            },
          },
        ],
      },
    });
  });

  test("rejects writes that do not satisfy transaction preconditions", async () => {
    const transaction = collection().conditional();

    await expect(
      transaction.add({ ids: ["id"], embeddings: [[1]] }),
    ).rejects.toThrow(
      'transactional add for id "id" requires a prior read proving the id is absent',
    );
  });

  test("rejects reads after a buffered write for the same id", async () => {
    const getSpy = jest.spyOn(RecordService, "collectionConditionalGet");
    const transaction = collection().conditional();

    await transaction.upsert({ ids: ["id"], embeddings: [[1]] });

    await expect(transaction.get({ ids: ["id"] })).rejects.toThrow(
      'cannot transactionally read id "id" after buffering a write for it',
    );
    expect(getSpy).not.toHaveBeenCalled();
  });

  test("requires positive limits for transactional filter reads", async () => {
    const transaction = collection().conditional();

    await expect(transaction.get({ where: { tag: "value" } })).rejects.toThrow(
      "transactional filter reads require a positive limit",
    );
  });

  test("run retries commit conflicts with fresh transactions", async () => {
    const commitSpy = jest
      .spyOn(RecordService, "collectionConditionalCommit")
      .mockRejectedValueOnce(
        new ChromaConditionalWriteConflictError("conditional write conflict"),
      )
      .mockResolvedValueOnce({
        data: {
          first_inserted_record_offset: 12,
          record_count: 1,
        },
      } as any);

    let attempts = 0;
    const result = await collection()
      .conditional()
      .run(
        async (transaction) => {
          attempts += 1;
          await transaction.upsert({
            ids: [`id-${attempts}`],
            embeddings: [[attempts]],
          });
          return `attempt-${attempts}`;
        },
        { maxRetries: 1 },
      );

    expect(result).toBe("attempt-2");
    expect(attempts).toBe(2);
    expect(commitSpy).toHaveBeenCalledTimes(2);
    expect(
      (commitSpy.mock.calls[0][0] as any).body.operations[0].payload.ids,
    ).toEqual(["id-1"]);
    expect(
      (commitSpy.mock.calls[1][0] as any).body.operations[0].payload.ids,
    ).toEqual(["id-2"]);
  });

  test("run retries stale reads from transaction operations only", async () => {
    const getSpy = jest
      .spyOn(RecordService, "collectionConditionalGet")
      .mockRejectedValueOnce(new ChromaStaleReadError("stale read"))
      .mockResolvedValueOnce({
        data: {
          ids: ["id"],
          documents: ["document"],
          include: ["documents"],
          read_token: 43,
        },
      } as any);
    const commitSpy = jest.spyOn(RecordService, "collectionConditionalCommit");

    const result = await collection()
      .conditional()
      .run(
        async (transaction) => {
          const got = await transaction.get({
            ids: ["id"],
            include: ["documents"],
          });
          return got.ids[0];
        },
        { maxRetries: 1 },
      );

    expect(result).toBe("id");
    expect(getSpy).toHaveBeenCalledTimes(2);
    expect(commitSpy).not.toHaveBeenCalled();
  });

  test("run does not retry user-raised retryable errors", async () => {
    let attempts = 0;

    await expect(
      collection()
        .conditional()
        .run(
          () => {
            attempts += 1;
            throw new ChromaConditionalWriteConflictError(
              "user-raised conflict",
            );
          },
          { maxRetries: 1 },
        ),
    ).rejects.toThrow("user-raised conflict");

    expect(attempts).toBe(1);
  });

  test("run rejects explicit commits inside callbacks", async () => {
    await expect(
      collection()
        .conditional()
        .run(async (transaction) => {
          await transaction.commit();
        }),
    ).rejects.toThrow("txn.commit() cannot be called inside run()");
  });
});

describe("conditional transaction errors", () => {
  test("maps conditional write conflict responses", async () => {
    jest.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          error: "ConditionalWriteConflictError",
          message: "conditional write conflict",
        }),
        { status: 409 },
      ),
    );

    await expect(chromaFetch("http://localhost")).rejects.toThrow(
      ChromaConditionalWriteConflictError,
    );
  });

  test("maps stale read responses", async () => {
    jest.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          error: "StaleReadError",
          message: "read token is too old",
        }),
        { status: 412 },
      ),
    );

    await expect(chromaFetch("http://localhost")).rejects.toThrow(
      ChromaStaleReadError,
    );
  });

  test("maps backoff responses", async () => {
    jest.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          error: "Backoff",
          message: "Backoff and retry",
        }),
        { status: 429 },
      ),
    );

    await expect(chromaFetch("http://localhost")).rejects.toThrow(
      ChromaBackoffError,
    );
  });
});

import { beforeEach, describe, expect, jest, test } from "@jest/globals";
import { CloudClient } from "../src";
import type { UserIdentity } from "../src/types";

describe("_path() tenant/database initialization", () => {
  beforeEach(() => {
    // Ensure tenant/database are unset so _path() must resolve identity.
    delete process.env.CHROMA_TENANT;
    delete process.env.CHROMA_DATABASE;
  });

  const makeClient = () => new CloudClient({ apiKey: "test-api-key" });

  test("concurrent callers share a single identity resolution and get the same path", async () => {
    const chroma = makeClient();
    const resolvers: Array<(identity: UserIdentity) => void> = [];
    const identitySpy = jest
      .spyOn(chroma, "getUserIdentity")
      .mockImplementation(
        () =>
          new Promise<UserIdentity>((resolve) => {
            resolvers.push(resolve);
          }),
      );

    // Both callers enter _path() before identity resolution has completed.
    const firstPath = chroma._path();
    const secondPath = chroma._path();

    // Resolve every in-flight identity request with a distinct tenant. If the
    // client fired more than one request, the last writer would win on
    // _tenant/_database and the two callers would see inconsistent paths.
    resolvers.forEach((resolve, i) =>
      resolve({
        tenant: `tenant-${i}`,
        databases: [`database-${i}`],
        user_id: "test-user",
      }),
    );

    const [first, second] = await Promise.all([firstPath, secondPath]);

    expect(identitySpy).toHaveBeenCalledTimes(1);
    expect(first).toEqual({ tenant: "tenant-0", database: "database-0" });
    expect(second).toEqual(first);
    expect(chroma.tenant).toBe("tenant-0");
    expect(chroma.database).toBe("database-0");
  });

  test("does not re-fetch identity once tenant and database are resolved", async () => {
    const chroma = makeClient();
    const identitySpy = jest
      .spyOn(chroma, "getUserIdentity")
      .mockResolvedValue({
        tenant: "tenant-a",
        databases: ["database-a"],
        user_id: "test-user",
      });

    await expect(chroma._path()).resolves.toEqual({
      tenant: "tenant-a",
      database: "database-a",
    });
    await expect(chroma._path()).resolves.toEqual({
      tenant: "tenant-a",
      database: "database-a",
    });

    expect(identitySpy).toHaveBeenCalledTimes(1);
  });

  test("a failed identity resolution is retried on the next call", async () => {
    const chroma = makeClient();
    const identitySpy = jest
      .spyOn(chroma, "getUserIdentity")
      .mockRejectedValueOnce(new Error("identity fetch failed"))
      .mockResolvedValueOnce({
        tenant: "tenant-a",
        databases: ["database-a"],
        user_id: "test-user",
      });

    await expect(chroma._path()).rejects.toThrow("identity fetch failed");
    await expect(chroma._path()).resolves.toEqual({
      tenant: "tenant-a",
      database: "database-a",
    });

    expect(identitySpy).toHaveBeenCalledTimes(2);
  });
});

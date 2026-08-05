import { ChromaClient } from "../src/chroma-client";

/**
 * Regression for #7494: concurrent _path() callers must share a single
 * getUserIdentity resolution instead of racing.
 */
describe("ChromaClient._path TOCTOU", () => {
  it("dedupes concurrent getUserIdentity calls", async () => {
    let calls = 0;
    class TestClient extends ChromaClient {
      public async getUserIdentity() {
        calls += 1;
        // Yield so concurrent callers both enter before resolution.
        await new Promise((r) => setTimeout(r, 20));
        return { tenant: "t1", databases: ["db1"] } as any;
      }
    }

    const client = new TestClient({ host: "localhost", port: 8000 });
    // Force identity lookup path
    (client as any)._tenant = undefined;
    (client as any)._database = undefined;

    const [a, b] = await Promise.all([
      (client as any)._path(),
      (client as any)._path(),
    ]);

    expect(calls).toBe(1);
    expect(a).toEqual({ tenant: "t1", database: "db1" });
    expect(b).toEqual({ tenant: "t1", database: "db1" });
  });
});

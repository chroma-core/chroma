import { describe, it, expect } from "@jest/globals";
import { ChromaClient } from "../src";

describe("ChromaDB Client Integration", () => {
  it("should properly import and instantiate ChromaClient", () => {
    const client = new ChromaClient();
    expect(client).toBeDefined();
    expect(typeof client.createCollection).toBe("function");
    expect(typeof client.listCollections).toBe("function");
  });

  it("should be able to set client options", () => {
    const client = new ChromaClient({ path: "http://localhost:8000" });
    // We're just verifying the client initializes correctly with options
    expect(client).toBeDefined();
  });
});

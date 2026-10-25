import { describe, expect, test, beforeEach, afterEach } from "@jest/globals";
import { CloudClient } from "../src/CloudClient";

function basePathOf(client: any): string {
  // BaseAPI stores the resolved base path from the Configuration it is built
  // with. This is the URL every request is issued against.
  return client.api?.configuration?.basePath ?? client.api?.basePath;
}

describe("CloudClient", () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    process.env = { ...OLD_ENV };
  });

  afterEach(() => {
    process.env = OLD_ENV;
  });

  test("defaults to the Chroma Cloud host on the HTTPS port (443)", () => {
    const client = new CloudClient({ apiKey: "test-key" });

    // Chroma Cloud is served over HTTPS on port 443, matching the Python
    // client. It must not default to the local dev port 8000.
    expect(basePathOf(client)).toBe("https://api.trychroma.com:443");
  });

  test("does not point at the local dev port 8000 by default", () => {
    const client = new CloudClient({ apiKey: "test-key" });
    expect(basePathOf(client)).not.toContain(":8000");
  });

  test("honors explicit cloudHost and cloudPort overrides", () => {
    const client = new CloudClient({
      apiKey: "test-key",
      cloudHost: "http://localhost",
      cloudPort: "9000",
    });
    expect(basePathOf(client)).toBe("http://localhost:9000");
  });

  test("reads the API key from CHROMA_API_KEY when not provided", () => {
    process.env.CHROMA_API_KEY = "env-key";
    expect(() => new CloudClient({})).not.toThrow();
  });
});

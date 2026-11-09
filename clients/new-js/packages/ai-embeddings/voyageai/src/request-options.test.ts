import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";
import { createServer, Server } from "node:http";
import { AddressInfo } from "node:net";
import { VoyageAIEmbeddingFunction } from "./index";

describe("VoyageAI request options", () => {
  const nativeFetch = globalThis.fetch;
  let server: Server;
  let endpoint: string;
  let requests: Record<string, unknown>[];

  beforeAll(async () => {
    server = createServer(async (request, response) => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      requests.push(JSON.parse(Buffer.concat(chunks).toString()));
      response.setHeader("Content-Type", "application/json");
      response.end(JSON.stringify({ data: [{ embedding: [1, 2], index: 0 }] }));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });
  afterAll(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  });

  const captureRequests = () => {
    requests = [];
    // Keep the real SDK serializer and HTTP transport, but never contact Voyage.
    jest
      .spyOn(globalThis, "fetch")
      .mockImplementation((_, init) => nativeFetch(endpoint, init));
  };

  it.each(["query", "document"])(
    "sends %s input type and disables truncation",
    async (inputType) => {
      captureRequests();
      const embedder = new VoyageAIEmbeddingFunction({
        apiKey: "local-test-key",
        modelName: "voyage-3.5-lite",
        inputType,
        truncation: false,
      });

      await expect(embedder.generate(["hello"])).resolves.toEqual([[1, 2]]);
      expect(requests).toEqual([
        {
          input: ["hello"],
          model: "voyage-3.5-lite",
          input_type: inputType,
          truncation: false,
        },
      ]);
    },
  );

  it.each([undefined, true])(
    "uses truncation %s without a retrieval input type",
    async (truncation) => {
      captureRequests();
      const embedder = new VoyageAIEmbeddingFunction({
        apiKey: "local-test-key",
        modelName: "voyage-3.5-lite",
        truncation,
      });

      await embedder.generate(["hello"]);
      expect(requests).toEqual([
        {
          input: ["hello"],
          model: "voyage-3.5-lite",
          input_type: null,
          truncation: true,
        },
      ]);
    },
  );

  it("rejects unsupported input types before making a request", () => {
    captureRequests();
    expect(
      () =>
        new VoyageAIEmbeddingFunction({
          apiKey: "local-test-key",
          modelName: "voyage-3.5-lite",
          inputType: "unsupported",
        }),
    ).toThrow("Voyage input type must be query or document");
    expect(globalThis.fetch).not.toHaveBeenCalled();
  });

  it("preserves request options when rebuilding from config", async () => {
    captureRequests();
    const keyEnvVar = "CHROMA_VOYAGE_OPTIONS_TEST_KEY";
    const previousKey = process.env[keyEnvVar];
    process.env[keyEnvVar] = "local-test-key";
    try {
      const original = new VoyageAIEmbeddingFunction({
        apiKey: "local-test-key",
        apiKeyEnvVar: keyEnvVar,
        modelName: "voyage-3.5-lite",
        inputType: "document",
        truncation: false,
      });
      const restored = VoyageAIEmbeddingFunction.buildFromConfig(
        original.getConfig(),
      );

      await restored.generate(["hello"]);
      expect(requests[0]).toMatchObject({
        input_type: "document",
        truncation: false,
      });
    } finally {
      if (previousKey === undefined) delete process.env[keyEnvVar];
      else process.env[keyEnvVar] = previousKey;
    }
  });
});

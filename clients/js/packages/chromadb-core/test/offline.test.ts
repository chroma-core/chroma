import { expect, test } from "@jest/globals";
import { ChromaClient } from "../src/ChromaClient";
import { ChromaConnectionError } from "../src/Errors";

test("it fails with a nice error when offline", async () => {
  const chroma = new ChromaClient({ path: "http://example.invalid" });
  await expect(chroma.createCollection({ name: "test" })).rejects.toThrow(
    ChromaConnectionError,
  );
});

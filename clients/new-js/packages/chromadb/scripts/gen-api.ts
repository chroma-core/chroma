import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { rm, readFile, writeFile } from "node:fs/promises";
import { createClient } from "@hey-api/openapi-ts";
import { startChromaServer } from "./start-chroma.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const generateChromaApi = async () => {
  console.log("Starting Chroma server via Rust binary...");

  const server = await startChromaServer();
  console.log(`Server started at ${server.url}`);

  try {
    await createClient({
      input: `${server.url}/openapi.json`,
      output: join(__dirname, "../src/api"),
      plugins: [
        {
          name: "@hey-api/client-fetch",
          throwOnError: true,
          baseUrl: "http://localhost:8000",
        },
        { name: "@hey-api/sdk", asClass: true },
        "@hey-api/typescript",
      ],
    });

    console.log("✅ API client generated and normalized!");

    // Fix HashMap type definition
    const typesPath = join(__dirname, "../src/api/types.gen.ts");
    let typesContent = await readFile(typesPath, "utf-8");

    // Fix the HashMap type to include null, arrays, and remove duplicate number
    typesContent = typesContent.replace(
      /export type HashMap = \{\s*\[key: string\]: boolean \| number \| number \| string \| SparseVector[^}]*};/,
      "export type HashMap = {\n  [key: string]: boolean | number | string | SparseVector | Array<boolean> | Array<number> | Array<string> | null;\n};",
    );

    await writeFile(typesPath, typesContent);
    console.log("✅ Fixed HashMap type definition!");
  } finally {
    server.stop();
    console.log("Server stopped");

    try {
      await rm("./chroma", { recursive: true, force: true });
      console.log("✅ Cleaned up ./chroma directory");
    } catch (err) {
      console.warn("Warning: Could not delete ./chroma directory:", err);
    }
  }
};

const generateSyncApi = async () => {
  console.log("Generating Sync API client from sync.trychroma.com...");

  // Fetch and fix the OpenAPI spec (missing SourceTypeFilter and OrderBy schemas)
  const response = await fetch("https://sync.trychroma.com/openapi.json");
  if (!response.ok) {
    throw new Error(
      `Failed to fetch OpenAPI spec: ${response.status} ${response.statusText}`,
    );
  }
  const spec = (await response.json()) as Record<string, any>;

  if (!spec.components) spec.components = {};
  if (!spec.components.schemas) spec.components.schemas = {};

  if (!spec.components.schemas.SourceTypeFilter) {
    spec.components.schemas.SourceTypeFilter = {
      type: "string",
      enum: ["github", "web_scrape", "s3"],
    };
  }
  if (!spec.components.schemas.OrderBy) {
    spec.components.schemas.OrderBy = {
      type: "string",
      enum: ["ASC", "DESC"],
      default: "DESC",
    };
  }

  // Fix list_sources response: the spec incorrectly types it as Vec (Job[])
  // but the API actually returns Source[].
  const listSourcesGet = spec.paths?.["/api/v1/sources"]?.get;
  if (listSourcesGet?.responses?.["200"]?.content?.["application/json"]) {
    listSourcesGet.responses["200"].content["application/json"].schema = {
      type: "array",
      items: { $ref: "#/components/schemas/Source" },
    };
  }

  const fixedSpecPath = join(__dirname, "../src/sync-api/openapi.json");
  try {
    await writeFile(fixedSpecPath, JSON.stringify(spec, null, 2));

    await createClient({
      input: fixedSpecPath,
      output: join(__dirname, "../src/sync-api"),
      plugins: [
        {
          name: "@hey-api/client-fetch",
          throwOnError: true,
          baseUrl: "https://sync.trychroma.com",
        },
        { name: "@hey-api/sdk", asClass: true },
        "@hey-api/typescript",
      ],
    });
  } finally {
    // Clean up the temp spec file
    await rm(fixedSpecPath, { force: true });
  }

  console.log("✅ Sync API client generated!");
};

const main = async () => {
  const args = process.argv.slice(2);
  const target = args[0];

  if (target === "sync") {
    await generateSyncApi();
  } else if (target === "chromadb") {
    await generateChromaApi();
  } else {
    await generateChromaApi();
    await generateSyncApi();
  }
};

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

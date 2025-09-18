import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { rm, readFile, writeFile } from "node:fs/promises";
import { createClient } from "@hey-api/openapi-ts";
import { startChromaServer } from "./start-chroma.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const main = async () => {
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

    // Fix the HashMap type to include null and remove duplicate number
    typesContent = typesContent.replace(
      /export type HashMap = \{\s*\[key: string\]: boolean \| number \| number \| string \| SparseVector;\s*};/,
      "export type HashMap = {\n  [key: string]: boolean | number | string | SparseVector | null;\n};",
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

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

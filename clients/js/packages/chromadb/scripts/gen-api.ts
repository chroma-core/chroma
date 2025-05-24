import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { rm } from "node:fs/promises";
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
        { name: "@hey-api/client-fetch", throwOnError: true },
        { name: "@hey-api/sdk", asClass: true },
        "@hey-api/typescript",
      ],
    });

    console.log("✅ API client generated!");
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

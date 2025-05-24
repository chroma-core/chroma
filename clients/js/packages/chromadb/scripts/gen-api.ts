import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { rm, readFile, writeFile } from "node:fs/promises";
import { glob } from "glob";
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
          baseUrl: "http://localhost:8000"
        },
        { name: "@hey-api/sdk", asClass: true },
        "@hey-api/typescript",
      ],
    });

    // Post-process generated files to normalize URLs
    const apiDir = join(__dirname, "../src/api");
    const generatedFiles = await glob("*.ts", { cwd: apiDir });
    
    for (const file of generatedFiles) {
      const filePath = join(apiDir, file);
      const content = await readFile(filePath, "utf-8");
      const normalizedContent = content.replace(/http:\/\/127\.0\.0\.1:8000/g, "http://localhost:8000");
      await writeFile(filePath, normalizedContent, "utf-8");
    }

    console.log("✅ API client generated and normalized!");
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

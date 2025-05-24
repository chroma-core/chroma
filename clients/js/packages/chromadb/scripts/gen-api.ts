import waitOn from "wait-on";
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { rm } from "node:fs/promises";
import { createClient } from "@hey-api/openapi-ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const startServer = () => {
  const tsNodePath = join(process.cwd(), "node_modules", ".bin", "ts-node");
  const serverProcess = spawn(tsNodePath, [join(__dirname, "run-server.ts")], {
    stdio: "inherit",
  });

  process.on("exit", () => {
    try {
      serverProcess.kill();
    } catch (e) {}
  });

  serverProcess.on("error", (err) => {
    console.error("Failed to start server:", err);
    process.exit(1);
  });

  return serverProcess;
};

const main = async () => {
  const serverProcess = startServer();
  console.log("Server starting...");

  try {
    await waitOn({
      resources: ["http://localhost:8000/openapi.json"],
      timeout: 30_000,
    });
  } catch (err) {
    console.error("Server failed to start in time:", err);
    serverProcess.kill();
    process.exit(1);
  }

  try {
    await createClient({
      input: "http://localhost:8000/openapi.json",
      output: join(__dirname, "../src/api"),
      plugins: [
        { name: "@hey-api/client-fetch", throwOnError: true },
        { name: "@hey-api/sdk", asClass: true },
        "@hey-api/typescript",
      ],
    });

    console.log("✅ API client generated!");
  } finally {
    if (serverProcess) {
      serverProcess.kill();
      console.log("Server stopped");
    }

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

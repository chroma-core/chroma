import * as dotenv from "dotenv";
import { execSync } from "child_process";

dotenv.config({ path: "../../../.env" });

console.log("Starting Huggingface embeddings server container...");
try {
  execSync(
    "docker run --platform linux/amd64 -d -p 8080:80 --pull always --rm ghcr.io/huggingface/text-embeddings-inference:cpu-1.7 --model-id BAAI/bge-large-en-v1.5",
    { stdio: "inherit" },
  );
  console.log("Huggingface embeddings server started successfully");
} catch (error) {
  console.error("Failed to start Huggingface embeddings server:", error);
}

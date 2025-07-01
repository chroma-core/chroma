import { startChromaRustServer } from "./startChromaContainer";
import { execSync } from "child_process";
import { startOllamaContainer } from "./embeddings/startOllamaContainer";

export default async function testSetup() {
  const { url, stop } = await startChromaRustServer();
  process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
  (globalThis as any).stopChromaServer = stop;
  (globalThis as any).ollamaAvailable = false;
  try {
    execSync("npm ls ollama", { stdio: "ignore" });
    const { ollamaUrl, ollamaContainer } = await startOllamaContainer();
    process.env.OLLAMA_URL = ollamaUrl;
    (globalThis as any).chromaContainer = ollamaContainer;
    (globalThis as any).ollamaAvailable = true;
    console.log(
      "ollama is installed and Ollama container is running. Running tests...",
    );
  } catch (error) {
    console.log(
      "Ollama package not installed or failed to start ollama. Skipping tests: " +
        error,
    );
  }
}

import { startChromaContainer } from "./startChromaContainer";
import { execSync } from "child_process";
import { startOllamaContainer } from "./embeddings/startOllamaContainer";

export default async function testSetup() {
  const { container, url } = await startChromaContainer();
  process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
  (globalThis as any).chromaContainer = container;
  (globalThis as any).isLangchainInstalled = false;
  try {
    // check for deps first needed for LC Embeddings <--> Chroma EF tests
    execSync("npm ls @langchain/core", { stdio: "ignore" });
    execSync("npm ls @langchain/community", { stdio: "ignore" });
    execSync("npm ls @langchain/ollama", { stdio: "ignore" });
    //start ollama container
    const { ollamaUrl, ollamaContainer } = await startOllamaContainer();
    process.env.OLLAMA_URL = ollamaUrl;
    (globalThis as any).chromaContainer = ollamaContainer;
    (globalThis as any).isLangchainInstalled = true;
    console.log(
      "@langchain/core and @langchain/ollama are installed and Ollama container is running. Running tests...",
    );
  } catch (error) {
    console.log(
      "Package is not installed or failed to start ollama. Skipping tests: " +
        error,
    );
  }
}

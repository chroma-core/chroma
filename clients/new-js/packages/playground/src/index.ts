import { ChromaClient } from "chromadb";
import { DefaultEmbeddingFunction } from "@chroma-core/default-embed";

async function main() {
  const ef = new DefaultEmbeddingFunction();
  await ef.generate(["hello"]);
}

main().catch(console.error);

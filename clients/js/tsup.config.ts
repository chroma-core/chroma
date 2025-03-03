import { defineConfig, Options } from "tsup";
import fs from "fs";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: {
      chromadb: "src/index.ts",
      "embeddings/cohere": "src/embeddings/CohereEmbeddingFunction.ts",
      "embeddings/google": "src/embeddings/GoogleGeminiEmbeddingFunction.ts",
      "embeddings/huggingface":
        "src/embeddings/HuggingFaceEmbeddingServerFunction.ts",
      "embeddings/jina": "src/embeddings/JinaEmbeddingFunction.ts",
      "embeddings/ollama": "src/embeddings/OllamaEmbeddingFunction.ts",
      "embeddings/openai": "src/embeddings/OpenAIEmbeddingFunction.ts",
      "embeddings/transformers":
        "src/embeddings/TransformersEmbeddingFunction.ts",
      "embeddings/voyageai": "src/embeddings/VoyageAIEmbeddingFunction.ts",
    },
    sourcemap: true,
    dts: true,
    ...options,
  };

  return [
    {
      ...commonOptions,
      format: ["esm"],
      outExtension: () => ({ js: ".mjs" }),
      clean: true,
      async onSuccess() {
        // Support Webpack 4 by pointing `"module"` to a file with a `.js` extension
        fs.copyFileSync("dist/chromadb.mjs", "dist/chromadb.legacy-esm.js");
      },
    },
    {
      ...commonOptions,
      format: "cjs",
      outDir: "./dist/cjs/",
      outExtension: () => ({ js: ".cjs" }),
    },
  ];
});

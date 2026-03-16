import { defineConfig, Options } from "tsup";
import fs from "fs";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: {
      "chromadb-client": "src/index.ts",
    },
    sourcemap: true,
    dts: true,
    target: "es2020",
    // Only bundle the core, keep embedding packages as external
    noExternal: ["@internal/chromadb-core", "isomorphic-fetch", "cliui"],
    // Ensure all embedding packages remain external
    external: [
      "@google/generative-ai",
      "@xenova/transformers",
      "chromadb-default-embed",
      "cohere-ai",
      "openai",
      "voyageai",
      "ollama",
    ],
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
        fs.copyFileSync(
          "dist/chromadb-client.mjs",
          "dist/chromadb-client.legacy-esm.js",
        );
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

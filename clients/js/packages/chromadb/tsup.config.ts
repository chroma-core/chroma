import { defineConfig, Options } from "tsup";
import fs from "fs";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: {
      chromadb: "src/index.ts",
    },
    sourcemap: true,
    dts: true,
    target: "es2020",
    // Include core package and all embedding packages in the bundle for the thick client
    noExternal: [
      '@internal/chromadb-core',
      '@google/generative-ai',
      '@xenova/transformers',
      'chromadb-default-embed',
      'cohere-ai',
      'openai',
      'voyageai',
      'ollama',
      'isomorphic-fetch',
      'cliui'
    ],
    esbuildOptions: (options) => {
      // Improve handling of dynamic imports
      options.mainFields = ['module', 'main'];
      options.conditions = ['import', 'module', 'require', 'default'];
    },
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
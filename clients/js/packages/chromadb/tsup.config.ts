import { defineConfig, Options } from "tsup";
import fs from "fs";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: {
      chromadb: "src/index.ts",
      cli: "src/cli.ts",
    },
    sourcemap: true,
    dts: true,
    target: "es2020",
    external: [
      "chromadb-js-bindings-darwin-arm64",
      "chromadb-js-bindings-darwin-x64",
      "chromadb-js-bindings-linux-arm64-gnu",
      "chromadb-js-bindings-linux-x64-gnu",
      "chromadb-js-bindings-win32-arm64-msvc",
      "chromadb-js-bindings-win32-x64-msvc",
    ],
    // Include core package and all embedding packages in the bundle for the thick client
    noExternal: ["@internal/chromadb-core", "chromadb-default-embed"],
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

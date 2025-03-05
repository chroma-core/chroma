import { defineConfig, Options } from "tsup";
import * as fs from "fs";
import * as path from "path";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: {
      chromadb: "src/index.ts",
    },
    sourcemap: true,
    dts: true,
    target: "es2020",
    // Bundle @internal/chromadb-core into the output
    noExternal: ["@internal/chromadb-core"],
    treeshake: true,
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
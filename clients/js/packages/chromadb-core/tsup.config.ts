import { defineConfig, Options } from "tsup";
import * as fs from "fs";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: {
      "chromadb-core": "src/index.ts",
    },
    sourcemap: true,
    dts: true,
    target: "es2020",
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
        fs.copyFileSync("dist/chromadb-core.mjs", "dist/chromadb-core.legacy-esm.js");
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
import { defineConfig, Options } from "tsup";
import fs from "fs";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    sourcemap: true,
    dts: true,
    ...options,
  };

  return [
    {
      entry: {
        chromadb: "src/index.ts",
      },
      format: ["esm"],
      outExtension: () => ({ js: ".mjs" }),
      clean: true,
      async onSuccess() {
        // Support Webpack 4 by pointing `"module"` to a file with a `.js` extension
        fs.copyFileSync("dist/chromadb.mjs", "dist/chromadb.legacy-esm.js");
      },
      ...commonOptions,
    },
    {
      entry: {
        chromadb: "src/index.ts",
      },
      format: "cjs",
      outDir: "./dist/cjs/",
      outExtension: () => ({ js: ".cjs" }),
      ...commonOptions,
    },
    {
      entry: {
        cli: "src/cli.ts",
      },
      format: "cjs",
      outDir: "dist",
      outExtension: () => ({ js: ".js" }),
      banner: {
        js: "#!/usr/bin/env node",
      },
      sourcemap: false,
      clean: false,
      ...commonOptions,
    }
  ];
});

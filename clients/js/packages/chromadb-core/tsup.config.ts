import { defineConfig, Options } from "tsup";
import * as fs from "fs";
import * as path from "path";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: {
      "chromadb-core": "src/index.ts",
    },
    sourcemap: true,
    dts: true,
    target: "es2020",
    // Tell esbuild to bundle and handle CommonJS dependencies correctly
    platform: "node",
    // Ensure Node.js polyfills are included
    shims: true,
    // Handle dynamic requires
    banner: {
      js: `
        // Polyfill for punycode which is used by whatwg-url
        import { createRequire } from 'module';
        const require = createRequire(import.meta.url);
        globalThis.require = require;
      `,
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
        fs.copyFileSync(
          "dist/chromadb-core.mjs",
          "dist/chromadb-core.legacy-esm.js",
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

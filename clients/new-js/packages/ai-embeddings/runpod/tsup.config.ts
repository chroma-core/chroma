import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  dts: true,
  splitting: false,
  sourcemap: false,
  clean: true,
  treeshake: true,
  minify: false,
  target: "es2020",
  outDir: "dist",
  outExtension({ format }) {
    if (format === "cjs") {
      return {
        js: `.cjs`,
        dts: `.d.cts`,
      };
    }
    if (format === "esm") {
      return {
        js: `.mjs`,
        dts: `.d.ts`,
      };
    }
    return {
      js: `.legacy-esm.js`,
      dts: `.d.ts`,
    };
  },
}); 
import { defineConfig, type Options } from "tsup";
import * as fs from "fs";

export default defineConfig((options: Options) => {
    const commonOptions: Partial<Options> = {
        entry: {
            "chroma-bm25": "src/index.ts",
        },
        sourcemap: true,
        dts: {
            compilerOptions: {
                composite: false,
                declaration: true,
                emitDeclarationOnly: false,
            },
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
                    "dist/chroma-bm25.mjs",
                    "dist/chroma-bm25.legacy-esm.js",
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

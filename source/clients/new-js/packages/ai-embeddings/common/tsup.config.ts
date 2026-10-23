import { defineConfig, Options } from "tsup";

export default defineConfig((options: Options) => {
  const commonOptions: Partial<Options> = {
    entry: ["src/index.ts"],
    sourcemap: true,
    dts: {
      compilerOptions: {
        composite: false,
        declaration: true,
        emitDeclarationOnly: false,
      },
    },
    splitting: false,
    clean: true,
    ...options,
  };

  return [
    {
      ...commonOptions,
      format: ["esm"],
      outExtension: () => ({ js: ".mjs" }),
    },
    {
      ...commonOptions,
      format: ["cjs"],
    },
  ];
});

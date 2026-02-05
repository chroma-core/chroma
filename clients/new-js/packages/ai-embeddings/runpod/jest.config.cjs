module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  testMatch: ["**/*.test.ts"],
  transform: {
    "^.+\\.tsx?$": [
      "ts-jest",
      {
        useESM: true,
        tsconfig: {
          moduleResolution: "node",
          allowSyntheticDefaultImports: true,
          esModuleInterop: true,
        },
      },
    ],
  },
  extensionsToTreatAsEsm: [".ts"],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
    "^chromadb$": "<rootDir>/../../chromadb/src/index.ts",
    "^@chroma-core/ai-embeddings-common$": "<rootDir>/../common/src/index.ts",
  },
  setupFiles: ["./jest.setup.ts"],
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
  transformIgnorePatterns: ["node_modules/(?!(chromadb|@chroma-core)/)"],
};

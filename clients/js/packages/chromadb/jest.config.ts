export default {
  preset: "ts-jest",
  testEnvironment: "node",
  clearMocks: true,
  collectCoverage: false,
  testTimeout: 15000,
  transform: {
    "^.+\\.(ts|tsx)$": [
      "ts-jest",
      {
        tsconfig: "tsconfig.json",
      },
    ],
  },
  collectCoverageFrom: [
    "./src/**/*.{js,ts}",
    "!**/node_modules/**",
    "!**/vendor/**",
  ],
  moduleNameMapper: {
    "@internal/chromadb-core": "<rootDir>/../chromadb-core/src",
  },
};

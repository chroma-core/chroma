import type { Config } from "@jest/types";

const config: Config.InitialOptions = {
  preset: "ts-jest",
  testEnvironment: "jest-environment-node-single-context",
  clearMocks: true,
  collectCoverage: false,
  testTimeout: 30000, // Longer timeout for integration tests
  setupFiles: ["<rootDir>/test/setup-env.ts"],
  globalSetup: "../chromadb/test/utils/test-env-setup.ts",
  globalTeardown: "../chromadb/test/utils/test-env-teardown.ts",
  coverageDirectory: "./test/coverage",
  coverageReporters: ["json", "html", "lcov"],
  collectCoverageFrom: [
    "./test/**/*.{js,ts}",
    "!**/node_modules/**",
    "!**/vendor/**",
  ],
};
export default config;
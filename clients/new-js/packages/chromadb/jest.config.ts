import type { Config } from "@jest/types";

const config: Config.InitialOptions = {
  preset: "ts-jest",
  testEnvironment: "node",
  clearMocks: true,
  collectCoverage: false,
  testTimeout: 15000,
  globalSetup: "./test/utils/test-env-setup.ts",
  globalTeardown: "./test/utils/test-env-teardown.ts",
  coverageDirectory: "./test/coverage",
  coverageReporters: ["json", "html", "lcov"],
  collectCoverageFrom: [
    "./src/**/*.{js,ts}",
    "./src/**/*.unit.test.ts",
    "!**/node_modules/**",
    "!**/vendor/**",
    "!**/vendor/**",
  ],
  globals: {
    "ts-jest": {
      tsconfig: {
        module: "esnext",
        target: "es2020"
      }
    }
  },
};
export default config;

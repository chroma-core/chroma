import type { Config } from "@jest/types";

const config: Config.InitialOptions = {
  preset: "ts-jest",
  testEnvironment: "node",
  clearMocks: true,
  collectCoverage: false,
  testTimeout: 15000,
  coverageDirectory: "./test/coverage",
  coverageReporters: ["json", "html", "lcov"],
  collectCoverageFrom: [
    "./src/**/*.{js,ts}",
    "./src/**/*.unit.test.ts",
    "!**/node_modules/**",
    "!**/vendor/**",
    "!**/vendor/**",
  ],
};
export default config;

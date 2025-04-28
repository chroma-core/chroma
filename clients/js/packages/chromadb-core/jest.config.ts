export default {
  preset: "ts-jest",
  testEnvironment: "node",
  clearMocks: true,
  collectCoverage: false,
  testTimeout: 15000,
  // Docker bindings for tests
  globalSetup: "./test/testEnvSetup.ts",
  globalTeardown: "./test/testEnvTeardown.ts",
  coverageDirectory: "./test/coverage",
  coverageReporters: ["json", "html", "lcov"],
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
  // Make tests find the src modules
  moduleNameMapper: {
    "^@src/(.*)$": "<rootDir>/src/$1",
  },
  testPathIgnorePatterns: ["node_modules/"],
};

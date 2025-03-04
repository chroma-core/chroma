export default {
  preset: "ts-jest",
  testEnvironment: "node",
  clearMocks: true,
  collectCoverage: false,
  testTimeout: 15000,
  // Commenting out globalSetup and globalTeardown for now since they require Docker
  globalSetup: "./test/testEnvSetup.ts",
  globalTeardown: "./test/testEnvTeardown.ts",
  coverageDirectory: "./test/coverage",
  coverageReporters: ["json", "html", "lcov"],
  transform: {
    "^.+\\.(ts|tsx)$": ["ts-jest", {
      tsconfig: "tsconfig.json"
    }]
  },
  collectCoverageFrom: [
    "./src/**/*.{js,ts}",
    "!**/node_modules/**",
    "!**/vendor/**",
  ],
};
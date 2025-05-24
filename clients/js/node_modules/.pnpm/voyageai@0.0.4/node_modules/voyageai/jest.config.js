/** @type {import('jest').Config} */
module.exports = {
    preset: "ts-jest",
    testEnvironment: "node",
    coverageThreshold: {
      "global": {
        "branches": 70,
        "functions": 80,
        "lines": 85,
        "statements": 85,
      }
    }
};

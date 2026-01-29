import * as dotenv from "dotenv";

dotenv.config({ path: "../../../.env" });

// Ensure we have the API key for tests
if (!process.env.RUNPOD_API_KEY) {
  console.warn("⚠️  RUNPOD_API_KEY not set - some tests will be skipped");
}

// Set up test environment
process.env.NODE_ENV = "test";

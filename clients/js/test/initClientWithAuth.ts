import { ChromaClient } from "../src/ChromaClient";
import { CloudClient } from "../src/CloudClient";

const PORT = process.env.PORT || "8000";
const URL = "http://localhost:" + PORT;
export const chromaBasic = () =>
  new ChromaClient({
    path: URL,
    auth: { provider: "basic", credentials: "admin:admin" },
  });
export const chromaTokenDefault = () =>
  new ChromaClient({
    path: URL,
    auth: { provider: "token", credentials: "test-token" },
  });
export const chromaTokenBearer = () =>
  new ChromaClient({
    path: URL,
    auth: {
      provider: "token",
      credentials: "test-token",
      tokenHeaderType: "AUTHORIZATION",
    },
  });
export const chromaTokenXToken = () =>
  new ChromaClient({
    path: URL,
    auth: {
      provider: "token",
      credentials: "test-token",
      tokenHeaderType: "X_CHROMA_TOKEN",
    },
  });
export const cloudClient = () =>
  new CloudClient({
    apiKey: "test-token",
    cloudPort: PORT,
    cloudHost: "http://localhost",
  });

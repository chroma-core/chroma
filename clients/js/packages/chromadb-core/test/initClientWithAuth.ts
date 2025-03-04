import { ChromaClient } from "../src/ChromaClient";
import { CloudClient } from "../src/CloudClient";

const PORT = process.env.PORT || "8000";
const URL = "http://localhost:" + PORT;
export const chromaBasic = (url?: string) =>
  new ChromaClient({
    path: url ?? URL,
    auth: { provider: "basic", credentials: "admin:admin" },
  });
export const chromaTokenDefault = (url?: string) =>
  new ChromaClient({
    path: url ?? URL,
    auth: { provider: "token", credentials: "test-token" },
  });
export const chromaTokenBearer = (url?: string) =>
  new ChromaClient({
    path: url ?? URL,
    auth: {
      provider: "token",
      credentials: "test-token",
      tokenHeaderType: "AUTHORIZATION",
    },
  });
export const chromaTokenXToken = (url?: string) =>
  new ChromaClient({
    path: url ?? URL,
    auth: {
      provider: "token",
      credentials: "test-token",
      tokenHeaderType: "X_CHROMA_TOKEN",
    },
  });
export const cloudClient = ({
  host = "http://localhost",
  port = PORT,
}: {
  host?: string;
  port?: string;
} = {}) =>
  new CloudClient({
    apiKey: "test-token",
    cloudPort: port,
    cloudHost: host,
  });

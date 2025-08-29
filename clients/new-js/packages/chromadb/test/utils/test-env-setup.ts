import { startChromaServer } from "../../scripts/start-chroma-jest";

const testEnvSetup = async () => {
  // Skip starting server if external server is provided
  process.env.EXTERNAL_CHROMA_SERVER = "http://localhost:8000";
  if (process.env.EXTERNAL_CHROMA_SERVER) {
    const url = process.env.EXTERNAL_CHROMA_SERVER;
    console.log("Using external Chroma server at", url);
    process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
    process.env.DEFAULT_CHROMA_INSTANCE_HOST = new URL(url).hostname;
    process.env.DEFAULT_CHROMA_INSTANCE_PORT = new URL(url).port;
    (globalThis as any).stopChromaServer = () => {}; // No-op since we didn't start it
  } else {
    const { url, host, stop, port } = await startChromaServer();
    process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
    process.env.DEFAULT_CHROMA_INSTANCE_HOST = host;
    process.env.DEFAULT_CHROMA_INSTANCE_PORT = `${port}`;
    (globalThis as any).stopChromaServer = stop;
  }
};

export default testEnvSetup;

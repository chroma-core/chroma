import { startChromaServer } from "../../scripts/start-chroma-jest";

const testEnvSetup = async () => {
  const { url, host, port, stop } = await startChromaServer();
  process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
  process.env.DEFAULT_CHROMA_INSTANCE_HOST = host;
  (globalThis as any).stopChromaServer = stop;
};

export default testEnvSetup;

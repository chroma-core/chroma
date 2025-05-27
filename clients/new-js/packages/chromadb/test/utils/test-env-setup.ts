import { startChromaServer } from "./start-chroma";

const testEnvSetup = async () => {
  const { url, stop } = await startChromaServer();
  process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
  (globalThis as any).stopChromaServer = stop;
};

export default testEnvSetup;

import { startChromaServer } from "../../scripts/start-chroma-jest";

const testEnvSetup = async () => {
  if (
    !process.env.DEFAULT_CHROMA_INSTANCE_URL ||
    !process.env.DEFAULT_CHROMA_INSTANCE_HOST
  ) {
    const { url, host, stop } = await startChromaServer();
    process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
    process.env.DEFAULT_CHROMA_INSTANCE_HOST = host;
    (globalThis as any).stopChromaServer = stop;
  } else {
    (globalThis as any).stopChromaServer = () => true;
  }
};

export default testEnvSetup;

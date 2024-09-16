import { startChromaContainer } from "./startChromaContainer";

export default async function testSetup() {
  const { container, url } = await startChromaContainer();
  process.env.DEFAULT_CHROMA_INSTANCE_URL = url;
  (globalThis as any).chromaContainer = container;
}

export default async function testTeardown() {
  await (globalThis as any).stopChromaServer();
}

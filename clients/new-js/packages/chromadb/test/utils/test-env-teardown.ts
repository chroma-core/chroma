const testTeardown = async () => {
  await (globalThis as any).stopChromaServer();
};

export default testTeardown;

import { DefaultEmbeddingFunction } from "./index";

const main = async () => {
  const e = new DefaultEmbeddingFunction();
  const x = await e.generate(["hello"]);
  console.log(x);
};

main().catch(console.error);

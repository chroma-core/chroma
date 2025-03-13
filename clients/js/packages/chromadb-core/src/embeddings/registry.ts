import type { EmbeddingFunctionConstructor } from "./IEmbeddingFunction";
import * as allEmbeddingFunctions from "./all";

const knownEmbeddingFunctions = new Map<string, EmbeddingFunctionConstructor>(
  Object.values(allEmbeddingFunctions).map((fn) => [fn.name, fn]),
);

export const registerEmbeddingFunction = (fn: EmbeddingFunctionConstructor) => {
  if (!fn.name) {
    throw new Error("Embedding function must have a name to be registered.");
  }

  knownEmbeddingFunctions.set(fn.name, fn);
};

export const getEmbeddingFunction = (name: string) => {
  const fn = knownEmbeddingFunctions.get(name);
  if (!fn) {
    throw new Error(`Embedding function ${name} not found`);
  }
  return fn;
};

import { EmbeddingFunctionConfiguration } from "./api";

export type EmbeddingFunctionSpace = "cosine" | "l2" | "ip";

export interface EmbeddingFunction {
  generate(texts: string[]): Promise<number[][]>;
  name?: string;
  defaultSpace?(): EmbeddingFunctionSpace;
  supportedSpaces?(): EmbeddingFunctionSpace[];
  buildFromConfig?(config: Record<string, any>): EmbeddingFunction;
  getConfig?(): Record<string, any>;
  validateConfigUpdate?(
    oldConfig: Record<string, any>,
    newConfig: Record<string, any>,
  ): void;
  validateConfig?(config: Record<string, any>): void;
}

class MalformedEmbeddingFunction implements EmbeddingFunction {
  public readonly name: string;

  constructor(collectionName: string, message: string) {
    this.name = `Failed to build embedding function for collection ${collectionName}: ${message}`;
    console.error(this.name);
  }

  generate(texts: string[]): Promise<number[][]> {
    throw new Error(this.name);
  }
}

export const knownEmbeddingFunctions = new Map<string, EmbeddingFunction>();

export const registerEmbeddingFunction = (fn: EmbeddingFunction) => {
  if (!fn.name) {
    throw new Error("Embedding function must have a name to be registered.");
  }
  if (knownEmbeddingFunctions.has(fn.name)) {
    throw new Error(
      `Embedding function with name ${fn.name} is already registered.`,
    );
  }
  knownEmbeddingFunctions.set(fn.name, fn);
};

export const getEmbeddingFunction = (
  collectionName: string,
  efConfig?: EmbeddingFunctionConfiguration,
) => {
  if (!efConfig) {
    return new MalformedEmbeddingFunction(
      collectionName,
      `Missing embedding function config`,
    );
  }

  let name: string;
  if (efConfig.type === "legacy") {
    name = "default";
  } else {
    name = efConfig.name;
  }

  const embeddingFunction = knownEmbeddingFunctions.get(name);
  if (!embeddingFunction) {
    return new MalformedEmbeddingFunction(
      collectionName,
      `Embedding function ${name} is not registered. Make sure that the @ai-embeddings/${name} package is installed`,
    );
  }

  let constructorConfig: Record<string, any> =
    efConfig.type === "known" ? (efConfig.config as Record<string, any>) : {};

  try {
    if (embeddingFunction.buildFromConfig) {
      return embeddingFunction.buildFromConfig(constructorConfig);
    }
    return new MalformedEmbeddingFunction(
      collectionName,
      `Embedding function ${name} does not define a 'buildFromConfig' function'`,
    );
  } catch (e) {
    return new MalformedEmbeddingFunction(
      collectionName,
      `Embedding function ${name} failed to build with config: ${constructorConfig}. Error: ${e}`,
    );
  }
};

export const serializeEmbeddingFunction = (
  ef?: EmbeddingFunction,
): EmbeddingFunctionConfiguration => {
  if (!ef) {
    return { type: "legacy" };
  }

  if (!ef.getConfig || !ef.name) {
    throw new Error(
      "Failed to serialize embedding function: missing 'getConfig' or 'name'",
    );
  }

  if (ef.validateConfig) ef.validateConfig(ef.getConfig());
  return {
    name: ef.name,
    type: "known",
    config: ef.getConfig(),
  };
};

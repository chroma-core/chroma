import { EmbeddingFunctionConfiguration } from "./api";
import { ChromaValueError } from "./errors";

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

export interface EmbeddingFunctionClass {
  new (...args: any[]): EmbeddingFunction;
  name: string;
  buildFromConfig(config: Record<string, any>): EmbeddingFunction;
}

class MalformedEmbeddingFunction implements EmbeddingFunction {
  public readonly name: string;

  constructor(collectionName: string, message: string) {
    this.name = `Failed to build embedding function for collection ${collectionName}: ${message}`;
    console.error(this.name);
  }

  generate(texts: string[]): Promise<number[][]> {
    throw new ChromaValueError(this.name);
  }
}

export const knownEmbeddingFunctions = new Map<
  string,
  EmbeddingFunctionClass
>();

export const registerEmbeddingFunction = (
  name: string,
  fn: EmbeddingFunctionClass,
) => {
  if (knownEmbeddingFunctions.has(name)) {
    throw new ChromaValueError(
      `Embedding function with name ${name} is already registered.`,
    );
  }
  knownEmbeddingFunctions.set(name, fn);
};

export const getEmbeddingFunction = async (
  collectionName: string,
  efConfig?: EmbeddingFunctionConfiguration,
) => {
  if (!efConfig) {
    efConfig = { type: "legacy" };
  }

  let name: string;
  if (efConfig.type === "legacy") {
    efConfig = await getDefaultEFConfig();
    name = "default";
  } else {
    name = efConfig.name;
  }

  const embeddingFunction = knownEmbeddingFunctions.get(name);
  if (!embeddingFunction) {
    return new MalformedEmbeddingFunction(
      collectionName,
      `Embedding function ${name} is not registered. Make sure that the @chroma-core/${name} package is installed`,
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
  ef: EmbeddingFunction,
): EmbeddingFunctionConfiguration => {
  if (!ef.getConfig || !ef.name || !ef.buildFromConfig) {
    return { type: "legacy" };
  }

  if (ef.validateConfig) ef.validateConfig(ef.getConfig());
  return {
    name: ef.name,
    type: "known",
    config: ef.getConfig(),
  };
};

export const getDefaultEFConfig =
  async (): Promise<EmbeddingFunctionConfiguration> => {
    try {
      const { DefaultEmbeddingFunction } = await import(
        "@chroma-core/default-embed"
      );
      if (!knownEmbeddingFunctions.has(new DefaultEmbeddingFunction().name)) {
        registerEmbeddingFunction("default", DefaultEmbeddingFunction);
      }
    } catch (e) {
      console.error(e);
      throw new Error(
        "Cannot instantiate a collection with the DefaultEmbeddingFunction. Please install @chroma-core/default-embed, or provide a different embedding function",
      );
    }
    return {
      name: "default",
      type: "known",
      config: {},
    };
  };

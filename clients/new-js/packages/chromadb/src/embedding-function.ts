import { EmbeddingFunctionConfiguration } from "./api";
import { ChromaValueError } from "./errors";

/**
 * Supported vector space types.
 */
export type EmbeddingFunctionSpace = "cosine" | "l2" | "ip";

/**
 * Interface for embedding functions.
 * Embedding functions transform text documents into numerical representations
 * that can be used for similarity search and other vector operations.
 */
export interface EmbeddingFunction {
  /**
   * Generates embeddings for the given texts.
   * @param texts - Array of text strings to embed
   * @returns Promise resolving to array of embedding vectors
   */
  generate(texts: string[]): Promise<number[][]>;
  /** Optional name identifier for the embedding function */
  name?: string;
  /** Returns the default vector space for this embedding function */
  defaultSpace?(): EmbeddingFunctionSpace;
  /** Returns all supported vector spaces for this embedding function */
  supportedSpaces?(): EmbeddingFunctionSpace[];
  /** Creates an instance from configuration object */
  buildFromConfig?(config: Record<string, any>): EmbeddingFunction;
  /** Returns the current configuration as an object */
  getConfig?(): Record<string, any>;
  /**
   * Validates that a configuration update is allowed.
   * @param newConfig - New configuration to validate
   */
  validateConfigUpdate?(newConfig: Record<string, any>): void;
  /**
   * Validates that a configuration object is valid.
   * @param config - Configuration to validate
   */
  validateConfig?(config: Record<string, any>): void;
}

/**
 * Interface for embedding function constructor classes.
 * Used for registering and instantiating embedding functions.
 */
export interface EmbeddingFunctionClass {
  /** Constructor for creating new instances */
  new(...args: any[]): EmbeddingFunction;
  /** Name identifier for the embedding function */
  name: string;
  /** Static method to build instance from configuration */
  buildFromConfig(config: Record<string, any>): EmbeddingFunction;
}

/**
 * Registry of available embedding functions.
 * Maps function names to their constructor classes.
 */
export const knownEmbeddingFunctions = new Map<
  string,
  EmbeddingFunctionClass
>();

/**
 * Registers an embedding function in the global registry.
 * @param name - Unique name for the embedding function
 * @param fn - Embedding function class to register
 * @throws ChromaValueError if name is already registered
 */
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

/**
 * Retrieves and instantiates an embedding function from configuration.
 * @param collectionName - Name of the collection (for error messages)
 * @param efConfig - Configuration for the embedding function
 * @returns Promise resolving to an EmbeddingFunction instance
 */
export const getEmbeddingFunction = async (
  collectionName: string,
  efConfig?: EmbeddingFunctionConfiguration,
) => {
  if (!efConfig) {
    console.warn(
      `No embedding function configuration found for collection ${collectionName}. 'add' and 'query' will fail unless you provide them embeddings directly.`,
    );
    return undefined;
  }

  if (efConfig.type === "legacy") {
    console.warn(
      `No embedding function configuration found for collection ${collectionName}. 'add' and 'query' will fail unless you provide them embeddings directly.`,
    );
    return undefined;
  }

  const name = efConfig.name;

  const embeddingFunction = knownEmbeddingFunctions.get(name);
  if (!embeddingFunction) {
    console.warn(
      `Collection ${collectionName} was created with the ${embeddingFunction} embedding function. However, the @chroma-core/${embeddingFunction} package is not install. 'add' and 'query' will fail unless you provide them embeddings directly, or install the @chroma-core/${embeddingFunction} package.`,
    );
    return undefined;
  }

  let constructorConfig: Record<string, any> =
    efConfig.type === "known" ? (efConfig.config as Record<string, any>) : {};

  try {
    if (embeddingFunction.buildFromConfig) {
      return embeddingFunction.buildFromConfig(constructorConfig);
    }

    console.warn(
      `Embedding function ${name} does not define a 'buildFromConfig' function. 'add' and 'query' will fail unless you provide them embeddings directly.`,
    );
    return undefined;
  } catch (e) {
    console.warn(
      `Embedding function ${name} failed to build with config: ${constructorConfig}. 'add' and 'query' will fail unless you provide them embeddings directly. Error: ${e}`,
    );
    return undefined;
  }
};

/**
 * Serializes an embedding function to configuration format.
 * @param embeddingFunction - User provided embedding function
 * @param configEmbeddingFunction - Collection config embedding function
 * @returns Configuration object that can recreate the function
 */
export const serializeEmbeddingFunction = ({
  embeddingFunction,
  configEmbeddingFunction,
}: {
  embeddingFunction?: EmbeddingFunction;
  configEmbeddingFunction?: EmbeddingFunction;
}): EmbeddingFunctionConfiguration | undefined => {
  if (embeddingFunction && configEmbeddingFunction) {
    throw new ChromaValueError(
      "Embedding function provided when already defined in the collection configuration",
    );
  }

  if (!embeddingFunction && !configEmbeddingFunction) {
    return undefined;
  }

  const ef = embeddingFunction || configEmbeddingFunction!;
  if (
    !ef.getConfig ||
    !ef.name ||
    !(ef.constructor as EmbeddingFunctionClass).buildFromConfig
  ) {
    return { type: "legacy" };
  }

  if (ef.validateConfig) ef.validateConfig(ef.getConfig());
  return {
    name: ef.name,
    type: "known",
    config: ef.getConfig(),
  };
};

/**
 * Gets the configuration for the default embedding function.
 * Dynamically imports and registers the default embedding function if needed.
 * @returns Promise resolving to default embedding function configuration
 * @throws Error if default embedding function cannot be loaded
 */
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

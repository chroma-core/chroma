import {
  IEmbeddingFunction,
  EmbeddingFunctionSpace,
} from "./embeddings/IEmbeddingFunction";
import { DefaultEmbeddingFunction } from "./embeddings/DefaultEmbeddingFunction";
import { Api } from "./generated";
export type HnswSpace = EmbeddingFunctionSpace;

export interface HNSWConfiguration {
  space?: HnswSpace;
  ef_construction?: number;
  max_neighbors?: number;
  ef_search?: number;
  num_threads?: number;
  batch_size?: number;
  sync_threshold?: number;
  resize_factor?: number;
}

export interface CollectionConfiguration {
  hnsw?: HNSWConfiguration | null;
  embedding_function?: IEmbeddingFunction | null;
}

// Known embedding functions registry (replace with actual implementation if needed)
const knownEmbeddingFunctions: Record<
  string,
  { build_from_config: (config: any) => IEmbeddingFunction }
> = {};

// TODO: make warnings prettier and add link to migration docs
export function loadCollectionConfigurationFromJson(
  jsonMap: Record<string, any>,
): CollectionConfiguration {
  let hnswConfig: HNSWConfiguration | null | undefined = jsonMap.hnsw;
  let embeddingFunction: IEmbeddingFunction | null | undefined = undefined;

  if (jsonMap.embedding_function) {
    const efConfig = jsonMap.embedding_function;
    if (efConfig.type === "legacy") {
      console.warn(
        "Legacy embedding function config detected.",
        "DeprecationWarning",
      );
      embeddingFunction = null; // Treat legacy as null/default for now
    } else if (
      efConfig.type === "known" &&
      knownEmbeddingFunctions[efConfig.name]
    ) {
      const efBuilder = knownEmbeddingFunctions[efConfig.name];
      try {
        embeddingFunction = efBuilder.build_from_config(efConfig.config);
      } catch (e) {
        console.error("Error building embedding function from config:", e);
        embeddingFunction = null; // Fallback if build fails
      }
    } else {
      console.warn(
        `Unknown embedding function type or name: ${efConfig.type}, ${efConfig.name}`,
      );
      embeddingFunction = null;
    }
  }

  return {
    hnsw: hnswConfig,
    embedding_function: embeddingFunction,
  };
}

export function loadCollectionConfigurationFromJsonStr(
  jsonStr: string,
): CollectionConfiguration {
  try {
    const jsonMap = JSON.parse(jsonStr);
    return loadCollectionConfigurationFromJson(jsonMap);
  } catch (e) {
    console.error("Error parsing JSON string for collection configuration:", e);
    throw new Error("Invalid JSON string for collection configuration");
  }
}

export function collectionConfigurationToJson(
  config: CollectionConfiguration,
): Record<string, any> {
  let hnswConfig = config.hnsw;
  let ef = config.embedding_function;
  let efConfig: Record<string, any> | null = null;

  // Basic validation/casting attempt
  if (hnswConfig && typeof hnswConfig !== "object") {
    throw new Error("Invalid HNSW config provided");
  }

  if (ef === null || ef === undefined) {
    // Assuming null/undefined EF means legacy or default - adjust as per actual logic
    efConfig = { type: "legacy" };
  } else {
    try {
      // Add an is_legacy method to IEmbeddingFunction or handle differently
      // if ((ef as any).is_legacy?.()) {
      //     efConfig = { type: "legacy" };
      // } else
      if (ef.getConfig && ef.name) {
        // Assuming non-legacy functions have getConfig and name
        efConfig = {
          name: ef.name,
          type: "known",
          config: ef.getConfig(),
        };
        // Assuming register_embedding_function equivalent is handled elsewhere or not needed
        // register_embedding_function(type(ef));
      } else {
        console.warn(
          "Could not serialize embedding function - missing getConfig or name method.",
        );
        efConfig = { type: "legacy" }; // Fallback to legacy
      }
    } catch (e) {
      console.warn(
        "Error processing embedding function for serialization, falling back to legacy:",
        e,
        "DeprecationWarning",
      );
      efConfig = { type: "legacy" };
    }
  }

  validateCreateHnswConfig(hnswConfig, ef);

  return {
    hnsw: hnswConfig,
    embedding_function: efConfig,
  };
}

export function collectionConfigurationToJsonStr(
  config: CollectionConfiguration,
): string {
  try {
    const jsonObj = collectionConfigurationToJson(config);
    return JSON.stringify(jsonObj);
  } catch (e) {
    console.error("Error serializing collection configuration to JSON:", e);
    throw new Error("Could not serialize collection configuration");
  }
}

// Interfaces for creating configurations
export interface CreateHNSWConfiguration extends HNSWConfiguration {}

export interface CreateCollectionConfiguration {
  hnsw?: CreateHNSWConfiguration | null;
  embedding_function?: IEmbeddingFunction | null;
}

// Interfaces for updating configurations
export interface UpdateHNSWConfiguration {
  ef_search?: number;
  num_threads?: number;
  batch_size?: number;
  sync_threshold?: number;
  resize_factor?: number;
}

export interface UpdateCollectionConfiguration {
  hnsw?: UpdateHNSWConfiguration | null;
  embedding_function?: IEmbeddingFunction | null;
}

// --- Create Configuration Helpers ---

export class InvalidConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidConfigurationError";
  }
}

export function validateCreateHnswConfig(
  config?: CreateHNSWConfiguration | null,
  ef?: IEmbeddingFunction | null,
): void {
  if (!config) return;

  if (config.batch_size !== undefined && config.sync_threshold !== undefined) {
    if (config.batch_size > config.sync_threshold) {
      throw new InvalidConfigurationError(
        "batch_size must be less than or equal to sync_threshold",
      );
    }
  }
  if (config.num_threads !== undefined) {
    if (config.num_threads <= 0) {
      throw new InvalidConfigurationError("num_threads must be greater than 0");
    }
  }
  if (config.resize_factor !== undefined) {
    if (config.resize_factor <= 0) {
      throw new InvalidConfigurationError(
        "resize_factor must be greater than 0",
      );
    }
  }
  if (config.space) {
    if (!["l2", "cosine", "ip"].includes(config.space)) {
      throw new InvalidConfigurationError(
        `space must be one of: l2, cosine, ip`,
      );
    }
    if (ef?.supportedSpaces) {
      const supported = ef.supportedSpaces();
      if (!supported.includes(config.space)) {
        throw new InvalidConfigurationError(
          `space '${
            config.space
          }' must be supported by the embedding function (${supported.join(
            ", ",
          )})`,
        );
      }
    }
  }
  if (config.ef_construction !== undefined && config.ef_construction <= 0) {
    throw new InvalidConfigurationError(
      "ef_construction must be greater than 0",
    );
  }
  if (config.max_neighbors !== undefined && config.max_neighbors <= 0) {
    throw new InvalidConfigurationError("max_neighbors must be greater than 0");
  }
  if (config.ef_search !== undefined && config.ef_search <= 0) {
    throw new InvalidConfigurationError("ef_search must be greater than 0");
  }
}

export function loadApiCollectionConfigurationFromCreateCollectionConfiguration(
  config: CreateCollectionConfiguration,
): Api.CollectionConfiguration {
  return createCollectionConfigurationToJson(
    config,
  ) as Api.CollectionConfiguration;
}

// TODO: make warnings prettier and add link to migration docs
export function createCollectionConfigurationToJson(
  config: CreateCollectionConfiguration,
): Record<string, any> {
  let hnswConfig = config.hnsw;
  let ef = config.embedding_function;
  let efConfig: Record<string, any> | null = null;

  // Basic validation/casting attempt
  if (hnswConfig && typeof hnswConfig !== "object") {
    throw new Error(
      "Invalid HNSW config provided in CreateCollectionConfiguration",
    );
  }

  if (ef === null || ef === undefined) {
    efConfig = { type: "legacy" };
  } else {
    try {
      // Assuming similar logic as collectionConfigurationToJson
      // Add is_legacy check if needed
      if (ef.getConfig && ef.name) {
        efConfig = {
          name: ef.name,
          type: "known",
          config: ef.getConfig(),
        };
        // register_embedding_function equivalent?
      } else {
        console.warn(
          "Could not serialize embedding function - missing getConfig or name method.",
        );
        efConfig = { type: "legacy" };
      }
    } catch (e) {
      console.warn(
        "Error processing embedding function for serialization, falling back to legacy:",
        e,
        "DeprecationWarning",
      );
      efConfig = { type: "legacy" };
    }
  }

  // Perform validation before returning the JSON object
  validateCreateHnswConfig(hnswConfig, ef);

  return {
    hnsw: hnswConfig,
    embedding_function: efConfig,
  };
}

// --- Update Configuration Helpers ---

export function jsonToUpdateHnswConfiguration(
  jsonMap: Record<string, any>,
): UpdateHNSWConfiguration {
  const config: UpdateHNSWConfiguration = {};
  if ("ef_search" in jsonMap) config.ef_search = jsonMap.ef_search;
  if ("num_threads" in jsonMap) config.num_threads = jsonMap.num_threads;
  if ("batch_size" in jsonMap) config.batch_size = jsonMap.batch_size;
  if ("sync_threshold" in jsonMap)
    config.sync_threshold = jsonMap.sync_threshold;
  if ("resize_factor" in jsonMap) config.resize_factor = jsonMap.resize_factor;
  return config;
}

export function validateUpdateHnswConfig(
  config?: UpdateHNSWConfiguration | null,
): void {
  if (!config) return;

  if (config.ef_search !== undefined && config.ef_search <= 0) {
    throw new InvalidConfigurationError("ef_search must be greater than 0");
  }
  if (config.num_threads !== undefined) {
    if (config.num_threads <= 0) {
      throw new InvalidConfigurationError("num_threads must be greater than 0");
    }
  }
  if (config.resize_factor !== undefined && config.resize_factor <= 0) {
    throw new InvalidConfigurationError("resize_factor must be greater than 0");
  }
}

export function updateCollectionConfigurationToJson(
  config: UpdateCollectionConfiguration,
): Record<string, any> {
  let hnswConfig = config.hnsw;
  let ef = config.embedding_function;
  let efConfig: Record<string, any> | null | undefined = undefined; // Initialize as undefined

  // Validate HNSW config if present
  if (hnswConfig) {
    if (typeof hnswConfig !== "object") {
      throw new Error(
        "Invalid HNSW config provided in UpdateCollectionConfiguration",
      );
    }
    validateUpdateHnswConfig(hnswConfig);
  }

  // Handle embedding function serialization only if explicitly provided
  if (ef !== undefined) {
    if (ef === null) {
      // Explicitly setting to legacy/null
      efConfig = { type: "legacy" };
    } else {
      try {
        if (ef.getConfig && ef.name) {
          efConfig = {
            name: ef.name,
            type: "known",
            config: ef.getConfig(),
          };
          // register_embedding_function equivalent?
        } else {
          console.warn(
            "Could not serialize embedding function for update - missing getConfig or name method.",
          );
          efConfig = { type: "legacy" }; // Fallback
        }
      } catch (e) {
        console.warn(
          "Error processing embedding function for update serialization, falling back to legacy:",
          e,
          "DeprecationWarning",
        );
        efConfig = { type: "legacy" };
      }
    }
  }

  // Construct the result object, only including defined fields
  const result: Record<string, any> = {};
  if (hnswConfig !== undefined) {
    // Check if hnsw was provided
    result.hnsw = hnswConfig;
  }
  if (efConfig !== undefined) {
    // Check if efConfig was processed
    result.embedding_function = efConfig;
  }

  return result;
}

export function loadApiUpdateCollectionConfigurationFromUpdateCollectionConfiguration(
  config: UpdateCollectionConfiguration,
): Api.UpdateCollectionConfiguration {
  return updateCollectionConfigurationToJson(
    config,
  ) as Api.UpdateCollectionConfiguration;
}

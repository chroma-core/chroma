import {
  IEmbeddingFunction,
  EmbeddingFunctionSpace,
} from "./embeddings/IEmbeddingFunction";
import { DefaultEmbeddingFunction } from "./embeddings/DefaultEmbeddingFunction";
import { Api } from "./generated";
export type HnswSpace = EmbeddingFunctionSpace;

// --- Common Interfaces ---

export class InvalidConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidConfigurationError";
  }
}

// --- HNSW Configuration ---

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

export interface CreateHNSWConfiguration extends HNSWConfiguration {}

export interface UpdateHNSWConfiguration {
  ef_search?: number;
  num_threads?: number;
  batch_size?: number;
  sync_threshold?: number;
  resize_factor?: number;
}

// --- SPANN Configuration ---

export interface SpannConfiguration {
  space?: EmbeddingFunctionSpace; // Re-using HnswSpace which is EmbeddingFunctionSpace
  search_nprobe?: number;
  write_nprobe?: number;
  ef_construction?: number;
  max_neighbors?: number;
  ef_search?: number;
  reassign_neighbor_count?: number;
  split_threshold?: number;
  merge_threshold?: number;
}

export interface CreateSpannConfiguration extends SpannConfiguration {}

export interface UpdateSpannConfiguration {
  search_nprobe?: number;
  ef_search?: number;
}

// --- Collection Configuration ---

export interface CollectionConfiguration {
  hnsw?: HNSWConfiguration | null;
  spann?: SpannConfiguration | null;
  embedding_function?: IEmbeddingFunction | null;
}

export interface CreateCollectionConfiguration {
  hnsw?: CreateHNSWConfiguration | null;
  spann?: CreateSpannConfiguration | null;
  embedding_function?: IEmbeddingFunction | null;
}

export interface UpdateCollectionConfiguration {
  hnsw?: UpdateHNSWConfiguration | null;
  spann?: UpdateSpannConfiguration | null;
  embedding_function?: IEmbeddingFunction | null;
}

// --- Known Embedding Functions Registry ---
// Known embedding functions registry (replace with actual implementation if needed)
const knownEmbeddingFunctions: Record<
  string,
  { build_from_config: (config: any) => IEmbeddingFunction }
> = {};

// --- Validation Helpers ---

function validateSpace(space?: string, ef?: IEmbeddingFunction | null): void {
  if (!space) return;
  if (!["l2", "cosine", "ip"].includes(space)) {
    throw new InvalidConfigurationError(`space must be one of: l2, cosine, ip`);
  }
  if (ef?.supportedSpaces) {
    const supported = ef.supportedSpaces();
    if (!supported.includes(space as EmbeddingFunctionSpace)) {
      // Cast needed as we checked inclusion above
      throw new InvalidConfigurationError(
        `space '${space}' must be supported by the embedding function (${supported.join(
          ", ",
        )})`,
      );
    }
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
  if (config.num_threads !== undefined && config.num_threads <= 0) {
    throw new InvalidConfigurationError("num_threads must be greater than 0");
  }
  if (config.resize_factor !== undefined && config.resize_factor <= 0) {
    throw new InvalidConfigurationError("resize_factor must be greater than 0");
  }
  validateSpace(config.space, ef);
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

export function validateUpdateHnswConfig(
  config?: UpdateHNSWConfiguration | null,
): void {
  if (!config) return;

  if (config.ef_search !== undefined && config.ef_search <= 0) {
    throw new InvalidConfigurationError("ef_search must be greater than 0");
  }
  if (config.num_threads !== undefined && config.num_threads <= 0) {
    throw new InvalidConfigurationError("num_threads must be greater than 0");
  }
  // Note: Python version checks batch_size > sync_threshold only if both are present in the update.
  // This TS version doesn't have access to the existing config to do that check fully here.
  // It's primarily validated server-side anyway.
  if (config.resize_factor !== undefined && config.resize_factor <= 0) {
    throw new InvalidConfigurationError("resize_factor must be greater than 0");
  }
}

export function validateCreateSpannConfig(
  config?: CreateSpannConfiguration | null,
  ef?: IEmbeddingFunction | null,
): void {
  if (!config) return;

  validateSpace(config.space, ef);

  if (config.search_nprobe !== undefined && config.search_nprobe <= 0) {
    throw new InvalidConfigurationError("search_nprobe must be greater than 0");
  }
  if (config.write_nprobe !== undefined && config.write_nprobe <= 0) {
    throw new InvalidConfigurationError("write_nprobe must be greater than 0");
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
  if (
    config.reassign_neighbor_count !== undefined &&
    config.reassign_neighbor_count <= 0
  ) {
    throw new InvalidConfigurationError(
      "reassign_neighbor_count must be greater than 0",
    );
  }
  if (config.split_threshold !== undefined && config.split_threshold <= 0) {
    throw new InvalidConfigurationError(
      "split_threshold must be greater than 0",
    );
  }
  if (config.merge_threshold !== undefined && config.merge_threshold <= 0) {
    throw new InvalidConfigurationError(
      "merge_threshold must be greater than 0",
    );
  }
}

export function validateUpdateSpannConfig(
  config?: UpdateSpannConfiguration | null,
): void {
  if (!config) return;

  if (config.search_nprobe !== undefined && config.search_nprobe <= 0) {
    throw new InvalidConfigurationError("search_nprobe must be greater than 0");
  }
  if (config.ef_search !== undefined && config.ef_search <= 0) {
    throw new InvalidConfigurationError("ef_search must be greater than 0");
  }
}

// --- JSON Conversion Helpers ---

function serializeEmbeddingFunction(
  ef: IEmbeddingFunction | null | undefined,
): Record<string, any> | null {
  let efConfig: Record<string, any> | null = null;
  if (ef === null || ef === undefined) {
    efConfig = { type: "legacy" };
  } else {
    try {
      if (ef.getConfig && ef.name) {
        if (ef.validateConfig) {
          ef.validateConfig(ef.getConfig());
        }
        efConfig = {
          name: ef.name,
          type: "known",
          config: ef.getConfig(),
        };
      } else {
        console.warn(
          "Could not serialize embedding function - missing getConfig or name method.",
        );
        efConfig = { type: "legacy" }; // Fallback
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
  return efConfig;
}

function deserializeEmbeddingFunction(
  efConfig: Record<string, any> | null | undefined,
): IEmbeddingFunction | null | undefined {
  if (!efConfig) return undefined; // Or null? Python seems to map missing to None/null EF.

  if (efConfig.type === "legacy") {
    console.warn(
      "Legacy embedding function config detected during load.",
      "DeprecationWarning",
    );
    return null; // Treat legacy as null/default
  } else if (
    efConfig.type === "known" &&
    knownEmbeddingFunctions[efConfig.name]
  ) {
    const efBuilder = knownEmbeddingFunctions[efConfig.name];
    try {
      return efBuilder.build_from_config(efConfig.config);
    } catch (e) {
      console.error("Error building embedding function from config:", e);
      return null; // Fallback if build fails
    }
  } else {
    console.warn(
      `Unknown embedding function type or name: ${efConfig.type}, ${efConfig.name}`,
    );
    return null;
  }
}

// TODO: make warnings prettier and add link to migration docs
export function loadCollectionConfigurationFromJson(
  jsonMap: Record<string, any>,
): CollectionConfiguration {
  if (jsonMap.hnsw && jsonMap.spann) {
    throw new InvalidConfigurationError(
      "Cannot specify both 'hnsw' and 'spann' configurations.",
    );
  }
  let hnswConfig: HNSWConfiguration | null | undefined = jsonMap.hnsw; // Assume structure matches HNSWConfiguration
  let spannConfig: SpannConfiguration | null | undefined = jsonMap.spann; // Assume structure matches SpannConfiguration
  let embeddingFunction: IEmbeddingFunction | null | undefined =
    deserializeEmbeddingFunction(jsonMap.embedding_function);

  return {
    hnsw: hnswConfig,
    spann: spannConfig,
    embedding_function: embeddingFunction,
  };
}

export function loadCollectionConfigurationFromJsonStr(
  jsonStr: string,
): CollectionConfiguration {
  try {
    const jsonMap = JSON.parse(jsonStr);
    return loadCollectionConfigurationFromJson(jsonMap);
  } catch (e: any) {
    if (e instanceof InvalidConfigurationError) throw e;
    console.error("Error parsing JSON string for collection configuration:", e);
    throw new Error("Invalid JSON string for collection configuration");
  }
}

export function collectionConfigurationToJson(
  config: CollectionConfiguration,
): Record<string, any> {
  if (config.hnsw && config.spann) {
    throw new InvalidConfigurationError(
      "Cannot specify both 'hnsw' and 'spann' configurations.",
    );
  }
  let hnswConfig = config.hnsw;
  let spannConfig = config.spann;
  let ef = config.embedding_function;
  let efConfig = serializeEmbeddingFunction(ef);

  // Basic validation/casting attempt (already done in create/update, but maybe check types?)
  if (hnswConfig && typeof hnswConfig !== "object") {
    throw new Error("Invalid HNSW config provided");
  }
  if (spannConfig && typeof spannConfig !== "object") {
    throw new Error("Invalid SPANN config provided");
  }

  // Note: Validation (like validateCreateHnswConfig) is tied to creation/update actions
  // not necessarily to retrieving/displaying the existing config.

  return {
    hnsw: hnswConfig,
    spann: spannConfig,
    embedding_function: efConfig,
  };
}

export function collectionConfigurationToJsonStr(
  config: CollectionConfiguration,
): string {
  try {
    const jsonObj = collectionConfigurationToJson(config);
    return JSON.stringify(jsonObj);
  } catch (e: any) {
    if (e instanceof InvalidConfigurationError) throw e;
    console.error("Error serializing collection configuration to JSON:", e);
    throw new Error("Could not serialize collection configuration");
  }
}

// --- Create Configuration Helpers ---

export function loadApiCollectionConfigurationFromCreateCollectionConfiguration(
  config: CreateCollectionConfiguration,
): Api.CollectionConfiguration {
  // Cast needed because the generated Api type might not be perfectly aligned
  // with our internal Create* types, but the structure should match after JSON conversion.
  return createCollectionConfigurationToJson(
    config,
  ) as Api.CollectionConfiguration;
}

// TODO: make warnings prettier and add link to migration docs
export function createCollectionConfigurationToJson(
  config: CreateCollectionConfiguration,
): Record<string, any> {
  if (config.hnsw && config.spann) {
    throw new InvalidConfigurationError(
      "Cannot specify both 'hnsw' and 'spann' configurations during creation.",
    );
  }
  let hnswConfig = config.hnsw;
  let spannConfig = config.spann;
  let ef = config.embedding_function;
  let efConfig = serializeEmbeddingFunction(ef);

  // Basic validation/casting attempt
  if (hnswConfig && typeof hnswConfig !== "object") {
    throw new Error(
      "Invalid HNSW config provided in CreateCollectionConfiguration",
    );
  }
  if (spannConfig && typeof spannConfig !== "object") {
    throw new Error(
      "Invalid SPANN config provided in CreateCollectionConfiguration",
    );
  }

  // Perform validation before returning the JSON object
  validateCreateHnswConfig(hnswConfig, ef);
  validateCreateSpannConfig(spannConfig, ef);

  return {
    hnsw: hnswConfig,
    spann: spannConfig,
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

export function jsonToUpdateSpannConfiguration(
  jsonMap: Record<string, any>,
): UpdateSpannConfiguration {
  const config: UpdateSpannConfiguration = {};
  if ("search_nprobe" in jsonMap) config.search_nprobe = jsonMap.search_nprobe;
  if ("ef_search" in jsonMap) config.ef_search = jsonMap.ef_search;
  return config;
}

export function updateCollectionConfigurationToJson(
  config: UpdateCollectionConfiguration,
): Record<string, any> {
  if (config.hnsw && config.spann) {
    throw new InvalidConfigurationError(
      "Cannot specify both 'hnsw' and 'spann' configurations during update.",
    );
  }
  let hnswConfig = config.hnsw;
  let spannConfig = config.spann;
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

  // Validate SPANN config if present
  if (spannConfig) {
    if (typeof spannConfig !== "object") {
      throw new Error(
        "Invalid SPANN config provided in UpdateCollectionConfiguration",
      );
    }
    validateUpdateSpannConfig(spannConfig);
  }

  // Handle embedding function serialization only if explicitly provided (ef !== undefined)
  if (ef !== undefined) {
    efConfig = serializeEmbeddingFunction(ef);
  }

  // Construct the result object, only including defined fields
  const result: Record<string, any> = {};
  if (hnswConfig !== undefined) result.hnsw = hnswConfig;
  if (spannConfig !== undefined) result.spann = spannConfig;
  if (efConfig !== undefined) result.embedding_function = efConfig;

  // Check if the result is empty, which means no valid update fields were provided
  if (Object.keys(result).length === 0) {
    throw new InvalidConfigurationError(
      "No valid configuration fields provided for update.",
    );
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

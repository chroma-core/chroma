import { ChromaValueError } from "./errors";
import {
  EmbeddingFunctionConfiguration,
  HnswConfiguration as ApiHnswConfiguration,
  SpannConfiguration,
  UpdateCollectionConfiguration as ApiUpdateCollectionConfiguration,
} from "./api";
import {
  EmbeddingFunction,
  getDefaultEFConfig,
  getEmbeddingFunction,
  serializeEmbeddingFunction,
} from "./embedding-function";

export interface CollectionConfiguration {
  embeddingFunction?: EmbeddingFunctionConfiguration | null;
  hnsw?: HNSWConfiguration | null;
  spann?: SpannConfiguration | null;
}

export type HNSWConfiguration = ApiHnswConfiguration & {
  batch_size?: number | null;
  num_threads?: number | null;
};

export type CreateCollectionConfiguration = Omit<
  CollectionConfiguration,
  "embeddingFunction"
> & { embeddingFunction?: EmbeddingFunction };

export interface UpdateCollectionConfiguration {
  embeddingFunction?: EmbeddingFunction;
  hnsw?: UpdateHNSWConfiguration;
  spann?: UpdateSPANNConfiguration;
}

export interface UpdateHNSWConfiguration {
  batch_size?: number;
  ef_search?: number;
  num_threads?: number;
  resize_factor?: number;
  sync_threshold?: number;
}

export interface UpdateSPANNConfiguration {
  search_nprobe?: number;
  ef_search?: number;
}

/**
 * Validate user provided collection configuration and embedding function. Returns a
 * CollectionConfiguration to be used in collection creation.
 */
export const processCreateCollectionConfig = async ({
  configuration,
  embeddingFunction,
}: {
  configuration?: CreateCollectionConfiguration;
  embeddingFunction?: EmbeddingFunction;
}) => {
  if (configuration?.hnsw && configuration?.spann) {
    throw new ChromaValueError(
      "Cannot specify both HNSW and SPANN configurations",
    );
  }

  const embeddingFunctionConfiguration =
    serializeEmbeddingFunction({
      embeddingFunction,
      configEmbeddingFunction: configuration?.embeddingFunction,
    }) || (await getDefaultEFConfig());

  return {
    ...(configuration || {}),
    embedding_function: embeddingFunctionConfiguration,
  } as CollectionConfiguration;
};

/**
 *
 */
export const processUpdateCollectionConfig = async ({
  collectionName,
  currentConfiguration,
  currentEmbeddingFunction,
  newConfiguration,
}: {
  collectionName: string;
  currentConfiguration: CollectionConfiguration;
  currentEmbeddingFunction?: EmbeddingFunction;
  newConfiguration: UpdateCollectionConfiguration;
}): Promise<{
  updateConfiguration?: ApiUpdateCollectionConfiguration;
  updateEmbeddingFunction?: EmbeddingFunction;
}> => {
  if (newConfiguration.hnsw && typeof newConfiguration.hnsw !== "object") {
    throw new ChromaValueError(
      "Invalid HNSW config provided in UpdateCollectionConfiguration",
    );
  }

  if (newConfiguration.spann && typeof newConfiguration.spann !== "object") {
    throw new ChromaValueError(
      "Invalid SPANN config provided in UpdateCollectionConfiguration",
    );
  }

  const embeddingFunction =
    currentEmbeddingFunction ||
    (await getEmbeddingFunction(
      collectionName,
      currentConfiguration.embeddingFunction ?? undefined,
    ));

  const newEmbeddingFunction = newConfiguration.embeddingFunction;

  if (
    embeddingFunction &&
    embeddingFunction.validateConfigUpdate &&
    newEmbeddingFunction &&
    newEmbeddingFunction.getConfig
  ) {
    embeddingFunction.validateConfigUpdate(newEmbeddingFunction.getConfig());
  }

  return {
    updateConfiguration: {
      hnsw: newConfiguration.hnsw,
      spann: newConfiguration.spann,
      embedding_function:
        newEmbeddingFunction &&
        serializeEmbeddingFunction({ embeddingFunction: newEmbeddingFunction }),
    },
    updateEmbeddingFunction: newEmbeddingFunction,
  };
};

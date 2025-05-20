import { EmbeddingFunction, registerEmbeddingFunction } from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import process from "node:process";
import { CohereClient } from "cohere-ai";

export type CohereEmbedInputType =
  | "search_document"
  | "search_query"
  | "classification"
  | "clustering"
  | "image";

export type CohereEmbedTruncate = "NONE" | "START" | "END";

export type CohereEmbedEmbeddingType =
  | "float"
  | "int8"
  | "uint8"
  | "binary"
  | "ubinary";

const NAME = "cohere";

export type StoredConfig = {
  model_name: string;
  api_key_env_var: string;
  input_type?: CohereEmbedInputType;
  truncate?: CohereEmbedTruncate;
  embedding_type?: CohereEmbedEmbeddingType;
  image?: boolean;
};

interface CohereArgs {
  inputType?: CohereEmbedInputType;
  truncate?: CohereEmbedTruncate;
  embeddingType?: CohereEmbedEmbeddingType;
}

export interface CohereConfig extends CohereArgs {
  apiKey?: string;
  apiKeyEnvVar?: string;
  modelName?: string;
  image?: boolean;
}

export class CohereEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;

  private readonly client: CohereClient;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private readonly inputType: CohereEmbedInputType | undefined;
  private readonly truncate: CohereEmbedTruncate | undefined;
  private readonly embeddingType: CohereEmbedEmbeddingType | undefined;
  private readonly image: boolean;

  constructor(args: Partial<CohereConfig> = {}) {
    const {
      apiKeyEnvVar = "COHERE_API_KEY",
      modelName = "embed-english-v3.0",
      inputType = "search_document",
      truncate,
      embeddingType,
      image = false,
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `Cohere API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.client = new CohereClient({ token: apiKey });
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.modelName = modelName;
    this.inputType = inputType;
    this.truncate = truncate;
    this.embeddingType = embeddingType;
    this.image = image;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    if (this.image && texts.length > 1) {
      throw new Error("Cohere image embedding supports one image at a time");
    }

    if (!this.image && texts.length > 96) {
      throw new Error(
        "Cohere image embedding supports a maximum of 96 text inputs at a time",
      );
    }

    const response = await this.client.embed({
      model: this.modelName,
      inputType: this.image ? "image" : this.inputType,
      truncate: this.truncate,
      embeddingTypes: this.embeddingType ? [this.embeddingType] : undefined,
      images: this.image ? texts : undefined,
      texts: !this.image ? texts : undefined,
    });

    const embeddings = response.embeddings;
    if (Array.isArray(embeddings)) {
      return embeddings;
    } else if (
      this.embeddingType &&
      embeddings[this.embeddingType] &&
      Array.isArray(embeddings[this.embeddingType])
    ) {
      return embeddings[this.embeddingType] as number[][];
    } else if (embeddings["float"] && Array.isArray(embeddings["float"])) {
      return embeddings["float"];
    }
    throw new Error("Failed to generate embeddings");
  }

  public static buildFromConfig(config: StoredConfig): CohereEmbeddingFunction {
    return new CohereEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      inputType: config.input_type,
      truncate: config.truncate,
      embeddingType: config.embedding_type,
      image: config.image,
    });
  }

  getConfig(): StoredConfig {
    return {
      model_name: this.modelName,
      api_key_env_var: this.apiKeyEnvVar,
      input_type: this.inputType,
      truncate: this.truncate,
      embedding_type: this.embeddingType,
      image: this.image,
    };
  }

  public static validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, CohereEmbeddingFunction);

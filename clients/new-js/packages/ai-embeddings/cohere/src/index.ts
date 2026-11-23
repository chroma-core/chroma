import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import process from "node:process";
import { CohereClientV2 } from "cohere-ai";

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

// Models that share one embedding space. Documents embedded with one can be
// queried with another, e.g. index with Pro and query with Fast.
const EMBED_V5_MODELS = new Set(["embed-v5.0-pro", "embed-v5.0-fast"]);

const sharesSpace = (a: string, b: string): boolean =>
  a === b || (EMBED_V5_MODELS.has(a) && EMBED_V5_MODELS.has(b));

export interface CohereConfig {
  model_name: string;
  api_key_env_var: string;
  input_type?: CohereEmbedInputType;
  truncate?: CohereEmbedTruncate;
  embedding_type?: CohereEmbedEmbeddingType;
  image?: boolean;
  query_model_name?: string;
  output_dimension?: number;
}

export interface CohereArgs {
  apiKey?: string;
  apiKeyEnvVar?: string;
  modelName?: string;
  image?: boolean;
  inputType?: CohereEmbedInputType;
  truncate?: CohereEmbedTruncate;
  embeddingType?: CohereEmbedEmbeddingType;
  /** Model used for queries. Must share an embedding space with modelName. */
  queryModelName?: string;
  /** Embedding size for models with Matryoshka output (embed-v4.0 and newer). */
  outputDimension?: number;
}

export class CohereEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;

  private readonly client: CohereClientV2;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private readonly inputType: CohereEmbedInputType | undefined;
  private readonly truncate: CohereEmbedTruncate | undefined;
  private readonly embeddingType: CohereEmbedEmbeddingType | undefined;
  private readonly image: boolean;
  private readonly queryModelName: string | undefined;
  private readonly outputDimension: number | undefined;

  constructor(args: Partial<CohereArgs> = {}) {
    const {
      apiKeyEnvVar = "COHERE_API_KEY",
      modelName = "embed-v5.0-pro",
      inputType = "search_document",
      truncate,
      embeddingType,
      image = false,
      queryModelName,
      outputDimension,
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      console.warn(
        `Cohere API key is not set. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    if (queryModelName && !sharesSpace(modelName, queryModelName)) {
      throw new ChromaValueError(
        `queryModelName must share an embedding space with modelName. Supported pairs: ${[...EMBED_V5_MODELS].join(", ")}`,
      );
    }

    this.client = new CohereClientV2({ token: apiKey });
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.modelName = modelName;
    this.inputType = inputType;
    this.truncate = truncate;
    this.embeddingType = embeddingType;
    this.image = image;
    this.queryModelName = queryModelName;
    this.outputDimension = outputDimension;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    return this.embed(texts, this.modelName, this.inputType);
  }

  /**
   * Embeds query texts. When the embedding function is configured for
   * retrieval (the default "search_document" input type), queries are sent
   * as "search_query", which is what Cohere's retrieval models expect.
   */
  public async generateForQueries(texts: string[]): Promise<number[][]> {
    const inputType =
      this.inputType === "search_document" ? "search_query" : this.inputType;
    return this.embed(texts, this.queryModelName ?? this.modelName, inputType);
  }

  private async embed(
    texts: string[],
    model: string,
    inputType: CohereEmbedInputType | undefined,
  ): Promise<number[][]> {
    if (!this.image && texts.length > 96) {
      throw new Error(
        "Cohere embedding supports a maximum of 96 text inputs at a time",
      );
    }

    const embeddingType = this.embeddingType ?? "float";
    const response = await this.client.embed({
      model,
      inputType: this.image ? "image" : (inputType ?? "search_document"),
      truncate: this.truncate,
      embeddingTypes: [embeddingType],
      outputDimension: this.outputDimension,
      images: this.image ? texts : undefined,
      texts: !this.image ? texts : undefined,
    });

    const embeddings = response.embeddings[embeddingType];
    if (Array.isArray(embeddings)) {
      return embeddings as number[][];
    }
    throw new Error("Failed to generate embeddings");
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    if (this.modelName === "embed-multilingual-v2.0") {
      return "ip";
    }
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    switch (this.modelName) {
      case "embed-english-v3.0":
        return ["cosine", "l2", "ip"];
      case "embed-english-light-v3.0":
        return ["cosine", "ip", "l2"];
      case "embed-english-v2.0":
        return ["cosine"];
      case "embed-english-light-v2.0":
        return ["cosine"];
      case "embed-multilingual-v3.0":
        return ["cosine", "l2", "ip"];
      case "embed-multilingual-light-v3.0":
        return ["cosine", "l2", "ip"];
      case "embed-multilingual-v2.0":
        return ["ip"];
      default:
        return ["cosine", "l2", "ip"];
    }
  }

  public static buildFromConfig(config: CohereConfig): CohereEmbeddingFunction {
    return new CohereEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      inputType: config.input_type,
      truncate: config.truncate,
      embeddingType: config.embedding_type,
      image: config.image,
      queryModelName: config.query_model_name,
      outputDimension: config.output_dimension,
    });
  }

  getConfig(): CohereConfig {
    return {
      model_name: this.modelName,
      api_key_env_var: this.apiKeyEnvVar,
      input_type: this.inputType,
      truncate: this.truncate,
      embedding_type: this.embeddingType,
      image: this.image,
      query_model_name: this.queryModelName,
      output_dimension: this.outputDimension,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
    if (
      newConfig.output_dimension !== undefined &&
      newConfig.output_dimension !== this.outputDimension
    ) {
      throw new ChromaValueError("Output dimension cannot be updated");
    }
    if (
      newConfig.query_model_name &&
      !sharesSpace(this.modelName, newConfig.query_model_name)
    ) {
      throw new ChromaValueError(
        "queryModelName must share an embedding space with modelName",
      );
    }
  }

  public static validateConfig(config: CohereConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, CohereEmbeddingFunction);

import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import {
  validateConfigSchema,
  decodeBase64Embedding,
} from "@chroma-core/ai-embeddings-common";
import Perplexity from "@perplexity-ai/perplexity_ai";

const NAME = "perplexity";

export interface PerplexityConfig {
  api_key_env_var: string;
  model_name: string;
  dimensions?: number;
}

export interface PerplexityArgs {
  modelName?: string;
  apiKeyEnvVar?: string;
  apiKey?: string;
  dimensions?: number;
}

export class PerplexityEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private readonly dimensions?: number;
  private client: Perplexity;

  constructor(args: PerplexityArgs = {}) {
    const {
      apiKeyEnvVar = "PERPLEXITY_API_KEY",
      modelName = "pplx-embed-v1-0.6b",
      dimensions,
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `Perplexity API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.modelName = modelName;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.dimensions = dimensions;
    this.client = new Perplexity({ apiKey });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embeddings.create({
      input: texts,
      model: this.modelName as "pplx-embed-v1-0.6b" | "pplx-embed-v1-4b",
      dimensions: this.dimensions,
    });

    if (!response.data || !response.data.every((e) => e?.embedding)) {
      throw new Error("Failed to generate Perplexity embeddings");
    }

    return response.data.map((e) => decodeBase64Embedding(e.embedding!));
  }

  public static buildFromConfig(
    config: PerplexityConfig,
  ): PerplexityEmbeddingFunction {
    return new PerplexityEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      dimensions: config.dimensions,
    });
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public getConfig(): PerplexityConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model_name: this.modelName,
      dimensions: this.dimensions,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, unknown>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: PerplexityConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, PerplexityEmbeddingFunction);
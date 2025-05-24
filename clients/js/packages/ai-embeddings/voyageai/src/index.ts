import { EmbeddingFunction, registerEmbeddingFunction } from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import { VoyageAIClient } from "voyageai";

const NAME = "voyageai";

type StoredConfig = {
  api_key_env_var: string;
  model_name: string;
};

export interface VoyageAIConfig {
  modelName: string;
  apiKeyEnvVar?: string;
}

export interface VoyageAIArgs extends VoyageAIConfig {
  apiKey?: string;
}

export class VoyageAIEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private client: VoyageAIClient;

  constructor(args: VoyageAIArgs) {
    const { apiKeyEnvVar = "VOYAGE_API_KEY", modelName } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `Voyage API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.modelName = modelName;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.client = new VoyageAIClient({ apiKey });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embed({
      input: texts,
      model: this.modelName,
    });

    if (!response.data || !response.data.every((e) => e !== undefined)) {
      throw new Error("Failed to generate VoyageAI embeddings");
    }

    return response.data?.map((e) => e.embedding!);
  }

  public static buildFromConfig(
    config: StoredConfig,
  ): VoyageAIEmbeddingFunction {
    return new VoyageAIEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
    });
  }

  getConfig(): StoredConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model_name: this.modelName,
    };
  }

  public static validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, VoyageAIEmbeddingFunction);

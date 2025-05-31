import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import { Mistral } from "@mistralai/mistralai";

const NAME = "mistral";

export interface MistralConfig {
  api_key_env_var: string;
  model: string;
}

export interface MistralArgs {
  model?: string;
  apiKeyEnvVar?: string;
  apiKey?: string;
}

export class MistralEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly model: string;
  private client: Mistral;

  constructor(args: Partial<MistralArgs> = {}) {
    const { apiKeyEnvVar = "MISTRAL_API_KEY", model = "mistral-embed" } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `Mistral API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.model = model;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.client = new Mistral({ apiKey });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embeddings.create({
      model: this.model,
      inputs: texts,
    });

    if (!response.data || !response.data.every((e) => e !== undefined)) {
      throw new Error("Failed to generate Mistral embeddings");
    }

    return response.data?.map((e) => e.embedding!);
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(
    config: MistralConfig,
  ): MistralEmbeddingFunction {
    return new MistralEmbeddingFunction({
      model: config.model,
      apiKeyEnvVar: config.api_key_env_var,
    });
  }

  public getConfig(): MistralConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model: this.model,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model !== newConfig.model) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: MistralConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, MistralEmbeddingFunction);

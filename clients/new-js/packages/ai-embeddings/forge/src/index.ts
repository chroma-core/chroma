import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import OpenAI from "openai";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";

const NAME = "forge";
const DEFAULT_API_BASE = "https://api.voxell.ai/v1";

export interface ForgeConfig {
  api_key_env_var: string;
  model_name: string;
  api_base?: string;
  dimensions?: number;
}

export interface ForgeArgs {
  modelName?: string;
  apiKeyEnvVar?: string;
  apiKey?: string;
  apiBase?: string;
  dimensions?: number;
}

export class ForgeEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private readonly apiBase: string;
  private readonly dimensions?: number;
  private client: OpenAI;

  constructor(args: ForgeArgs = {}) {
    const {
      apiKeyEnvVar = "FORGE_API_KEY",
      modelName = "forge-pro",
      apiBase = DEFAULT_API_BASE,
      dimensions,
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      console.warn(
        `Forge API key is not set. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.modelName = modelName;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.apiBase = apiBase;
    this.dimensions = dimensions;
    this.client = new OpenAI({ apiKey, baseURL: apiBase });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embeddings.create({
      input: texts,
      model: this.modelName,
      ...(this.dimensions !== undefined && { dimensions: this.dimensions }),
    });

    return response.data.map((e) => e.embedding);
  }

  public static buildFromConfig(config: ForgeConfig): ForgeEmbeddingFunction {
    return new ForgeEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      apiBase: config.api_base,
      dimensions: config.dimensions,
    });
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public getConfig(): ForgeConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model_name: this.modelName,
      api_base: this.apiBase,
      dimensions: this.dimensions,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, unknown>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: ForgeConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, ForgeEmbeddingFunction);

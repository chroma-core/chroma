import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import OpenAI from "openai";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";

const NAME = "openai";

export interface OpenAIConfig {
  api_key_env_var?: string;
  model_name: string;
  organization_id?: string;
  dimensions?: number;
}

export interface OpenAIArgs {
  apiKeyEnvVar?: string;
  modelName: string;
  organizationId?: string;
  dimensions?: number;
  apiKey?: string;
}

export class OpenAIEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string | undefined;
  private readonly modelName: string;
  private readonly dimensions: number | undefined;
  private readonly organizationId: string | undefined;
  private client: OpenAI;

  constructor(args: OpenAIArgs) {
    const {
      apiKeyEnvVar = "OPENAI_API_KEY",
      modelName,
      dimensions,
      organizationId,
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];
    if (!apiKey) {
      throw new Error(
        `OpenAI API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.modelName = modelName;
    this.organizationId = organizationId;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.dimensions = dimensions;

    this.client = new OpenAI({ apiKey, organization: this.organizationId });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embeddings.create({
      input: texts,
      model: this.modelName,
      dimensions: this.dimensions,
    });
    return response.data.map((e) => e.embedding);
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(config: OpenAIConfig): OpenAIEmbeddingFunction {
    return new OpenAIEmbeddingFunction({
      apiKeyEnvVar: config.api_key_env_var,
      modelName: config.model_name,
      organizationId: config.organization_id,
      dimensions: config.dimensions,
    });
  }

  public getConfig(): OpenAIConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model_name: this.modelName,
      organization_id: this.organizationId,
      dimensions: this.dimensions,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: OpenAIConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, OpenAIEmbeddingFunction);

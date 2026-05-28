import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import OpenAI from "openai";

const NAME = "morph";

export interface MorphConfig {
  api_key_env_var: string;
  model_name: string;
  api_base?: string;
  encoding_format?: "float" | "base64";
}

export interface MorphArgs {
  apiKey?: string;
  apiKeyEnvVar?: string;
  modelName?: string;
  apiBase?: string;
  encodingFormat?: "float" | "base64";
}

export class MorphEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private readonly encodingFormat: "float" | "base64";
  private readonly apiBase: string;
  private client: OpenAI;

  constructor(args: MorphArgs = {}) {
    const {
      apiKeyEnvVar = "MORPH_API_KEY",
      modelName = "morph-embedding-v2",
      apiBase = "https://api.morphllm.com/v1",
      encodingFormat = "float",
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];
    if (!apiKey) {
      throw new Error(
        `API key not found. Please set ${apiKeyEnvVar} environment variable or provide apiKey in args.`,
      );
    }

    this.modelName = modelName;
    this.encodingFormat = encodingFormat;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.apiBase = apiBase;

    this.client = new OpenAI({
      apiKey,
      baseURL: apiBase,
    });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embeddings.create({
      model: this.modelName,
      input: texts,
      encoding_format: this.encodingFormat,
    });

    return response.data.map((item) => item.embedding);
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(config: MorphConfig): MorphEmbeddingFunction {
    return new MorphEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      apiBase: config.api_base,
      encodingFormat: config.encoding_format,
    });
  }

  public getConfig(): MorphConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model_name: this.modelName,
      api_base: this.apiBase,
      encoding_format: this.encodingFormat,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: MorphConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, MorphEmbeddingFunction);

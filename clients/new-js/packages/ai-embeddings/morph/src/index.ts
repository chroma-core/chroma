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
  encoding_format?: 'float' | 'base64';
}

export interface MorphEmbeddingFunctionConfig {
  api_key?: string;
  model_name?: string;
  api_base?: string;
  encoding_format?: 'float' | 'base64';
  api_key_env_var?: string;
}

export class MorphEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private readonly encodingFormat: 'float' | 'base64';
  private readonly apiBase: string;
  private client: OpenAI;

  constructor(config: MorphEmbeddingFunctionConfig = {}) {
    const {
      api_key,
      model_name = 'morph-embedding-v2',
      api_base = 'https://api.morphllm.com/v1',
      encoding_format = 'float',
      api_key_env_var = 'MORPH_API_KEY'
    } = config;

    // Get API key from config or environment
    const apiKey = api_key || process.env[api_key_env_var];
    if (!apiKey) {
      throw new Error(`API key not found. Please set ${api_key_env_var} environment variable or provide api_key in config.`);
    }

    this.modelName = model_name;
    this.encodingFormat = encoding_format;
    this.apiKeyEnvVar = api_key_env_var;
    this.apiBase = api_base;

    this.client = new OpenAI({
      apiKey,
      baseURL: api_base,
    });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embeddings.create({
      model: this.modelName,
      input: texts,
      encoding_format: this.encodingFormat,
    });

    return response.data.map(item => item.embedding);
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(config: MorphConfig): MorphEmbeddingFunction {
    return new MorphEmbeddingFunction({
      model_name: config.model_name,
      api_key_env_var: config.api_key_env_var,
      api_base: config.api_base,
      encoding_format: config.encoding_format,
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

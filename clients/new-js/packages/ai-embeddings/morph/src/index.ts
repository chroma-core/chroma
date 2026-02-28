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

export interface MorphArgs {
  apiKey?: string;
  modelName?: string;
  apiBase?: string;
  encodingFormat?: 'float' | 'base64';
  apiKeyEnvVar?: string;
}

/**
 * @deprecated Use MorphArgs instead. This interface will be removed in a future version.
 */
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

    // Support both camelCase (new) and snake_case (deprecated) for backwards compatibility
    const camelArgs = args as MorphArgs;
    const snakeArgs = args as MorphEmbeddingFunctionConfig;

    const apiKey = camelArgs.apiKey ?? snakeArgs.api_key;
    const modelName = camelArgs.modelName ?? snakeArgs.model_name ?? "morph-embedding-v2";
    const apiBase = camelArgs.apiBase ?? snakeArgs.api_base ?? "https://api.morphllm.com/v1";
    const encodingFormat = camelArgs.encodingFormat ?? snakeArgs.encoding_format ?? "float";
    const apiKeyEnvVar = camelArgs.apiKeyEnvVar ?? snakeArgs.api_key_env_var ?? "MORPH_API_KEY";

    // Get API key from config or environment
    const resolvedApiKey = apiKey || process.env[apiKeyEnvVar];
    if (!resolvedApiKey) {
      throw new Error(`API key not found. Please set ${apiKeyEnvVar} environment variable or provide apiKey in config.`);
    }

    this.modelName = modelName;
    this.encodingFormat = encodingFormat;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.apiBase = apiBase;

    this.client = new OpenAI({
      apiKey: resolvedApiKey,
      baseURL: apiBase,
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

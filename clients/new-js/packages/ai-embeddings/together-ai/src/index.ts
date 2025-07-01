import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import {
  snakeCase,
  validateConfigSchema,
} from "@chroma-core/ai-embeddings-common";
import Together from "together-ai";

const NAME = "together-ai";

export interface TogetherAIConfig {
  api_key_env_var: string;
  model_name: string;
}

export interface TogetherAIArgs {
  modelName: string;
  apiKeyEnvVar?: string;
  apiKey?: string;
}

export class TogetherAIEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private client: Together;

  constructor(args: TogetherAIArgs) {
    const { apiKeyEnvVar = "TOGETHER_API_KEY", modelName } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `TogetherAI API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.modelName = modelName;
    this.apiKeyEnvVar = apiKeyEnvVar;
    this.client = new Together({ apiKey });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await this.client.embeddings.create({
      input: texts,
      model: this.modelName,
    });
    return response.data.map((e) => e.embedding);
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(
    config: TogetherAIConfig,
  ): TogetherAIEmbeddingFunction {
    return new TogetherAIEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
    });
  }

  public getConfig(): TogetherAIConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model_name: this.modelName,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: TogetherAIConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, TogetherAIEmbeddingFunction);

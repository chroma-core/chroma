import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";

const NAME = "huggingface";

export interface HuggingfaceConfig {
  api_key_env_var: string;
  model_name: string;
}

export interface HuggingfaceArgs {
  apiKey?: string;
  apiKeyEnvVar?: string;
  modelName?: string;
}

/**
 * Embeddings via the hosted HuggingFace Inference API (feature-extraction
 * pipeline). Brings the JS client to parity with the Python
 * `HuggingFaceEmbeddingFunction`. For a self-hosted text-embeddings-inference
 * server, use `@chroma-core/huggingface-server` instead.
 */
export class HuggingfaceEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;

  private readonly apiKeyEnvVar: string;
  private readonly modelName: string;
  private readonly url: string;
  private readonly headers: { [key: string]: string };

  constructor(args: Partial<HuggingfaceArgs> = {}) {
    const {
      apiKeyEnvVar = "CHROMA_HUGGINGFACE_API_KEY",
      modelName = "sentence-transformers/all-MiniLM-L6-v2",
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];
    if (!apiKey) {
      throw new Error(
        `HuggingFace API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.modelName = modelName;
    this.apiKeyEnvVar = apiKeyEnvVar;
    // The legacy api-inference.huggingface.co host is deprecated; the hosted
    // Inference API is now served through the router (see chroma-core/chroma#6770,
    // #6907 for the equivalent Python migration).
    this.url = `https://router.huggingface.co/hf-inference/models/${modelName}/pipeline/feature-extraction`;
    this.headers = {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    };
  }

  public async generate(texts: string[]): Promise<number[][]> {
    try {
      const response = await fetch(this.url, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify({
          inputs: texts,
          options: { wait_for_model: true },
        }),
      });

      const data = (await response.json()) as
        | number[][]
        | { error?: string; message?: string };

      if (!response.ok || !Array.isArray(data)) {
        const message =
          (!Array.isArray(data) && (data.error || data.message)) ||
          response.statusText;
        throw new Error(message);
      }

      return data;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling HuggingFace API: ${error.message}`);
      }
      throw new Error(`Error calling HuggingFace API: ${error}`);
    }
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(
    config: HuggingfaceConfig,
  ): HuggingfaceEmbeddingFunction {
    return new HuggingfaceEmbeddingFunction({
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
    });
  }

  public getConfig(): HuggingfaceConfig {
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

  public static validateConfig(config: HuggingfaceConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, HuggingfaceEmbeddingFunction);

import {
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import * as process from "node:process";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";

const NAME = "huggingface-server";

export interface HuggingfaceServerConfig {
  api_key_env_var?: string;
  url: string;
}

export class HuggingfaceServerEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string | undefined;
  private readonly url: string;
  private readonly headers: Record<string, any> | undefined;

  constructor(args: { apiKey?: string; apiKeyEnvVar?: string; url: string }) {
    if (args.apiKeyEnvVar && !process.env[args.apiKeyEnvVar]) {
      throw new Error(`Could not find API key at ${args.apiKeyEnvVar}`);
    }

    this.apiKeyEnvVar = args.apiKeyEnvVar;
    this.url = args.url;

    const apiKey =
      args.apiKey || (this.apiKeyEnvVar && process.env[this.apiKeyEnvVar]);

    this.headers = {
      "Content-Type": "application/json",
      Authorization: apiKey ? `Bearer ${apiKey}` : undefined,
    };
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const response = await fetch(this.url, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({ inputs: texts }),
    });

    if (!response.ok) {
      throw new Error(`Failed to generate embeddings: ${response.statusText}`);
    }

    return await response.json();
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(
    config: HuggingfaceServerConfig,
  ): HuggingfaceServerEmbeddingFunction {
    return new HuggingfaceServerEmbeddingFunction({
      url: config.url,
      apiKeyEnvVar: config.api_key_env_var,
    });
  }

  public getConfig(): HuggingfaceServerConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      url: this.url,
    };
  }

  public static validateConfig(config: HuggingfaceServerConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, HuggingfaceServerEmbeddingFunction);

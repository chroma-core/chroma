import { EmbeddingFunction, registerEmbeddingFunction } from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import * as process from "node:process";

const NAME = "cloudflare-worker-ai";
const BASE_URL = "https://api.cloudflare.com/client/v4/accounts";
const GATEWAY_BASE_URL = "https://gateway.ai.cloudflare.com/v1";

type StoredConfig = {
  account_id: string;
  model_name: string;
  api_key_env_var: string;
  gateway_id?: string;
};

interface CloudflareWorkerAIConfig {
  apiKey?: string;
  accountId: string;
  modelName: string;
  apiKeyEnvVar?: string;
  gatewayId?: string;
}

export class CloudflareWorkerAIEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;

  private readonly accountId: string;
  private readonly modelName: string;
  private readonly gatewayId: string | undefined;
  private readonly apiKey: string;
  private readonly apiKeyEnvVar: string;
  private readonly apiUrl: string;
  private readonly headers: Record<string, any>;

  constructor(args: CloudflareWorkerAIConfig) {
    const {
      accountId,
      modelName,
      apiKeyEnvVar = "CLOUDFLARE_API_KEY",
      gatewayId,
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `Cloudflare API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.accountId = accountId;
    this.modelName = modelName;
    this.gatewayId = gatewayId;
    this.apiKey = apiKey;
    this.apiKeyEnvVar = apiKeyEnvVar;

    if (this.gatewayId) {
      this.apiUrl = `${GATEWAY_BASE_URL}/${this.accountId}/${this.gatewayId}/workers-ai/${this.modelName}`;
    } else {
      this.apiUrl = `${BASE_URL}/${this.accountId}/ai/run/${this.modelName}`;
    }

    this.headers = {
      Authorization: `Bearer ${this.apiKey}`,
      "Accept-Encoding": "identity",
      "Content-Type": "application/json",
    };
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const payload = { text: texts };
    let cloudFlareResult: { result?: { data?: number[][] }; detail?: string };

    try {
      const response = await fetch(this.apiUrl, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify(payload),
      });

      cloudFlareResult = await response.json();
    } catch (e) {
      throw new Error(`Error calling Cloudflare Workers AI API: ${e}`);
    }

    if (!cloudFlareResult.result || !cloudFlareResult.result.data) {
      throw new Error(
        cloudFlareResult.detail || "Error calling Cloudflare Workers AI API",
      );
    }

    return cloudFlareResult.result.data;
  }

  public static buildFromConfig(
    config: StoredConfig,
  ): CloudflareWorkerAIEmbeddingFunction {
    return new CloudflareWorkerAIEmbeddingFunction({
      accountId: config.account_id,
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      gatewayId: config.gateway_id,
    });
  }

  getConfig(): StoredConfig {
    return {
      account_id: this.accountId,
      model_name: this.modelName,
      api_key_env_var: this.apiKeyEnvVar,
      gateway_id: this.gatewayId,
    };
  }

  public static validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, CloudflareWorkerAIEmbeddingFunction);

import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import * as process from "node:process";

const NAME = "cloudflare-worker-ai";
const BASE_URL = "https://api.cloudflare.com/client/v4/accounts";
const GATEWAY_BASE_URL = "https://gateway.ai.cloudflare.com/v1";

export interface CloudflareWorkerAIConfig {
  account_id: string;
  model_name: string;
  api_key_env_var: string;
  gateway_id?: string;
}

export interface CloudflareWorkerAIArgs {
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

  constructor(args: CloudflareWorkerAIArgs) {
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

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "ip", "l2"];
  }

  public static buildFromConfig(
    config: CloudflareWorkerAIConfig,
  ): CloudflareWorkerAIEmbeddingFunction {
    return new CloudflareWorkerAIEmbeddingFunction({
      accountId: config.account_id,
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      gatewayId: config.gateway_id,
    });
  }

  public getConfig(): CloudflareWorkerAIConfig {
    return {
      account_id: this.accountId,
      model_name: this.modelName,
      api_key_env_var: this.apiKeyEnvVar,
      gateway_id: this.gatewayId,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: CloudflareWorkerAIConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, CloudflareWorkerAIEmbeddingFunction);

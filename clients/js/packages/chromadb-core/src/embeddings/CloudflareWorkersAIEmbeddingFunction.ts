import { IEmbeddingFunction } from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";

type StoredConfig = {
  account_id: string;
  model_name: string;
  api_key_env_var: string;
  gateway_id?: string;
};

const BASE_URL = "https://api.cloudflare.com/client/v4/accounts";
const GATEWAY_BASE_URL = "https://gateway.ai.cloudflare.com/v1";

export class CloudflareWorkersAIEmbeddingFunction
  implements IEmbeddingFunction
{
  name = "cloudflare_workers_ai";

  private account_id: string;
  private model_name: string;
  private api_key_env_var: string;
  private gateway_id?: string;
  private api_url: string;
  private headers: { [key: string]: string };

  constructor({
    cloudflare_api_key,
    model_name,
    account_id,
    api_key_env_var = "CHROMA_CLOUDFLARE_API_KEY",
    gateway_id = undefined,
  }: {
    cloudflare_api_key?: string;
    model_name: string;
    account_id: string;
    api_key_env_var: string;
    gateway_id?: string;
  }) {
    const apiKey = cloudflare_api_key ?? process.env[api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `Cloudflare API key is required. Please provide it in the constructor or set the environment variable ${api_key_env_var}.`,
      );
    }

    this.model_name = model_name;
    this.account_id = account_id;
    this.api_key_env_var = api_key_env_var;
    this.gateway_id = gateway_id;

    if (this.gateway_id) {
      this.api_url = `${GATEWAY_BASE_URL}/${this.account_id}/${this.gateway_id}/workers-ai/${this.model_name}`;
    } else {
      this.api_url = `${BASE_URL}/${this.account_id}/ai/run/${this.model_name}`;
    }

    this.headers = {
      Authorization: `Bearer ${apiKey}`,
      "Accept-Encoding": "identity",
      "Content-Type": "application/json",
    };
  }

  public async generate(texts: string[]) {
    try {
      const payload = {
        text: texts,
      };

      const response = await fetch(this.api_url, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify(payload),
      });

      const resp = await response.json();

      if (!resp.result || !resp.result.data) {
        throw new Error(resp.detail || "Unknown error");
      }

      return resp.result.data;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(
          `Error calling Cloudflare Workers AI API: ${error.message}`,
        );
      } else {
        throw new Error(`Error calling Cloudflare Workers AI API: ${error}`);
      }
    }
  }

  buildFromConfig(config: StoredConfig): CloudflareWorkersAIEmbeddingFunction {
    return new CloudflareWorkersAIEmbeddingFunction({
      model_name: config.model_name,
      account_id: config.account_id,
      api_key_env_var: config.api_key_env_var,
      gateway_id: config.gateway_id ?? undefined,
    });
  }

  getConfig(): StoredConfig {
    return {
      model_name: this.model_name,
      account_id: this.account_id,
      api_key_env_var: this.api_key_env_var,
      gateway_id: this.gateway_id ?? undefined,
    };
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "cloudflare_workers_ai");
  }
}

import { validateConfigSchema } from "../schemas/schemaUtils";
import { IEmbeddingFunction } from "./IEmbeddingFunction";

type StoredConfig = {
  url: string;
  api_key_env_var?: string;
};

export class HuggingFaceEmbeddingServerFunction implements IEmbeddingFunction {
  name = "huggingface_server";

  private url: string;
  private api_key_env_var?: string;
  private headers?: { [key: string]: string };
  constructor({
    api_key,
    url,
    api_key_env_var = "CHROMA_HUGGINGFACE_API_KEY",
  }: {
    url: string;
    api_key?: string;
    api_key_env_var?: string;
  }) {
    // we used to construct the client here, but we need to async import the types
    // for the openai npm package, and the constructor can not be async
    let apiKey: string | undefined;
    this.api_key_env_var = api_key_env_var;
    if (this.api_key_env_var) {
      apiKey = api_key || process.env[this.api_key_env_var];
    } else {
      apiKey = api_key;
    }
    this.url = url;
    if (apiKey) {
      this.headers = {
        Authorization: `Bearer ${apiKey}`,
      };
    }
  }

  public async generate(texts: string[]) {
    const response = await fetch(this.url, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({ inputs: texts }),
    });

    if (!response.ok) {
      throw new Error(`Failed to generate embeddings: ${response.statusText}`);
    }

    const data = await response.json();
    return data;
  }

  buildFromConfig(config: StoredConfig): HuggingFaceEmbeddingServerFunction {
    return new HuggingFaceEmbeddingServerFunction({
      url: config.url,
      api_key_env_var: config.api_key_env_var,
    });
  }

  getConfig(): StoredConfig {
    return {
      url: this.url,
      api_key_env_var: this.api_key_env_var,
    };
  }

  validateConfigUpdate(
    oldConfig: Record<string, any>,
    newConfig: Record<string, any>,
  ): void {
    if (oldConfig.url !== newConfig.url) {
      throw new Error("Changing the URL is not allowed.");
    }
  }

  validateConfig(config: Record<string, any>): void {
    validateConfigSchema(config, "huggingface_server");
  }
}

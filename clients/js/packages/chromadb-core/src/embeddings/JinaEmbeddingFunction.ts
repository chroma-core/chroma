import { IEmbeddingFunction } from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";
type StoredConfig = {
  api_key_env_var: string;
  model_name: string;
};

export class JinaEmbeddingFunction implements IEmbeddingFunction {
  name = "jina";

  private api_key_env_var: string;
  private model_name: string;
  private api_url: string;
  private headers: { [key: string]: string };

  constructor({
    jinaai_api_key,
    model_name = "jina-embeddings-v2-base-en",
    api_key_env_var = "JINAAI_API_KEY",
  }: {
    jinaai_api_key?: string;
    model_name?: string;
    api_key_env_var: string;
  }) {
    const apiKey = jinaai_api_key ?? process.env[api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `Jina AI API key is required. Please provide it in the constructor or set the environment variable ${api_key_env_var}.`,
      );
    }

    this.model_name = model_name;
    this.api_key_env_var = api_key_env_var;

    this.api_url = "https://api.jina.ai/v1/embeddings";
    this.headers = {
      Authorization: `Bearer ${jinaai_api_key}`,
      "Accept-Encoding": "identity",
      "Content-Type": "application/json",
    };
  }

  public async generate(texts: string[]) {
    try {
      const response = await fetch(this.api_url, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify({
          input: texts,
          model: this.model_name,
        }),
      });

      const data = (await response.json()) as { data: any[]; detail: string };
      if (!data || !data.data) {
        throw new Error(data.detail);
      }

      const embeddings: any[] = data.data;
      const sortedEmbeddings = embeddings.sort((a, b) => a.index - b.index);

      return sortedEmbeddings.map((result) => result.embedding);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling Jina AI API: ${error.message}`);
      } else {
        throw new Error(`Error calling Jina AI API: ${error}`);
      }
    }
  }

  buildFromConfig(config: StoredConfig): JinaEmbeddingFunction {
    return new JinaEmbeddingFunction({
      model_name: config.model_name,
      api_key_env_var: config.api_key_env_var,
    });
  }

  getConfig(): StoredConfig {
    return {
      api_key_env_var: this.api_key_env_var,
      model_name: this.model_name,
    };
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "jina");
  }
}

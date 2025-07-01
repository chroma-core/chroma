import { IEmbeddingFunction } from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";

type StoredConfig = {
  model_name: string;
  api_key_env_var: string;
};

const ENDPOINT = "https://api.together.xyz/v1/embeddings";

export class TogetherAIEmbeddingFunction implements IEmbeddingFunction {
  name = "together_ai";

  private model_name: string;
  private api_key_env_var: string;
  private headers: { [key: string]: string };

  constructor({
    together_ai_api_key,
    model_name,
    api_key_env_var = "CHROMA_TOGETHER_AI_API_KEY",
  }: {
    together_ai_api_key?: string;
    model_name: string;
    api_key_env_var: string;
  }) {
    const apiKey = together_ai_api_key ?? process.env[api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `Together AI API key is required. Please provide it in the constructor or set the environment variable ${api_key_env_var}.`,
      );
    }

    this.model_name = model_name;
    this.api_key_env_var = api_key_env_var;

    this.headers = {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      accept: "application/json",
    };
  }

  public async generate(texts: string[]): Promise<number[][]> {
    try {
      const payload = {
        model: this.model_name,
        input: texts,
      };

      const response = await fetch(ENDPOINT, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify(payload),
      });

      const resp = await response.json();

      if (!resp.data) {
        throw new Error("Invalid response format from Together AI API");
      }

      const embeddings = resp.data.map(
        (item: { embedding: number[] }) => item.embedding,
      );
      return embeddings;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling Together AI API: ${error.message}`);
      } else {
        throw new Error(`Error calling Together AI API: ${error}`);
      }
    }
  }

  buildFromConfig(config: StoredConfig): IEmbeddingFunction {
    return new TogetherAIEmbeddingFunction({
      model_name: config.model_name,
      api_key_env_var: config.api_key_env_var,
    });
  }

  getConfig(): StoredConfig {
    return {
      model_name: this.model_name,
      api_key_env_var: this.api_key_env_var,
    };
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "together_ai");
  }
}

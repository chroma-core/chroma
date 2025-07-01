import { IEmbeddingFunction } from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";
class VoyageAIAPI {
  private client: any;
  private apiKey: string;

  constructor(configuration: { apiKey: string }) {
    this.apiKey = configuration.apiKey;
  }

  private async loadClient() {
    if (this.client) return;
    //@ts-ignore
    const voyageai = await import("voyageai").then((voyageai) => {
      return voyageai;
    });
    // @ts-ignore
    this.client = new voyageai.VoyageAIClient({
      apiKey: this.apiKey,
    });
  }

  public async createEmbedding(params: {
    model: string;
    input: string[];
  }): Promise<number[][]> {
    await this.loadClient();
    return await this.client
      .embed({ input: params.input, model: params.model })
      .then((response: any) => {
        return response.data.map(
          (item: { embedding: number[] }) => item.embedding,
        );
      });
  }
}

type StoredConfig = {
  api_key_env_var: string;
  model_name: string;
};

export class VoyageAIEmbeddingFunction implements IEmbeddingFunction {
  name = "voyageai";

  private voyageAiApi?: VoyageAIAPI;
  private model: string;
  private apiKey: string;
  private apiKeyEnvVar: string;
  constructor({
    api_key,
    model,
    api_key_env_var = "CHROMA_VOYAGE_API_KEY",
  }: {
    api_key?: string;
    model: string;
    api_key_env_var: string;
  }) {
    const apiKey = api_key ?? process.env[api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `VoyageAI API key is required. Please provide it in the constructor or set the environment variable ${api_key_env_var}.`,
      );
    }
    this.apiKey = apiKey;
    this.model = model;
    this.apiKeyEnvVar = api_key_env_var;
  }

  private async initClient() {
    if (this.voyageAiApi) return;
    try {
      // @ts-ignore
      this.voyageAiApi = await import("voyageai").then((voyageai) => {
        // @ts-ignore
        return new VoyageAIAPI({ apiKey: this.apiKey });
      });
    } catch (e) {
      // @ts-ignore
      if (e.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the voyageai package to use the VoyageAIEmbeddingFunction, `npm install -S voyageai`",
        );
      }
      throw e;
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.initClient();
    // @ts-ignore
    return await this.voyageAiApi.createEmbedding({
      model: this.model,
      input: texts,
    });
  }

  buildFromConfig(config: StoredConfig): VoyageAIEmbeddingFunction {
    return new VoyageAIEmbeddingFunction({
      api_key_env_var: config.api_key_env_var,
      model: config.model_name,
    });
  }

  getConfig(): StoredConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      model_name: this.model,
    };
  }

  validateConfigUpdate(oldConfig: StoredConfig, newConfig: StoredConfig): void {
    if (oldConfig.model_name !== newConfig.model_name) {
      throw new Error("Cannot change the model of the embedding function.");
    }
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "voyageai");
  }
}

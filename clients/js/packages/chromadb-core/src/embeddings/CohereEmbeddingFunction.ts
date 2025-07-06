import type {
  EmbeddingFunctionSpace,
  IEmbeddingFunction,
} from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";
interface CohereAIAPI {
  createEmbedding: (params: {
    model: string;
    input: string[];
    isImage?: boolean;
  }) => Promise<number[][]>;
}

class CohereAISDK56 implements CohereAIAPI {
  private cohereClient: any;
  private apiKey: string;

  constructor(configuration: { apiKey: string }) {
    this.apiKey = configuration.apiKey;
  }

  private async loadClient() {
    if (this.cohereClient) return;
    //@ts-ignore
    const { default: cohere } = await import("cohere-ai");
    // @ts-ignore
    cohere.init(this.apiKey);
    this.cohereClient = cohere;
  }

  public async createEmbedding(params: {
    model: string;
    input: string[];
  }): Promise<number[][]> {
    await this.loadClient();
    return await this.cohereClient
      .embed({
        texts: params.input,
        model: params.model,
      })
      .then((response: any) => {
        return response.body.embeddings;
      });
  }
}

class CohereAISDK7 implements CohereAIAPI {
  private cohereClient: any;
  private apiKey: string;

  constructor(configuration: { apiKey: string }) {
    this.apiKey = configuration.apiKey;
  }

  private async loadClient() {
    if (this.cohereClient) return;
    //@ts-ignore
    const cohere = await import("cohere-ai").then((cohere) => {
      return cohere;
    });
    // @ts-ignore
    this.cohereClient = new cohere.CohereClient({
      token: this.apiKey,
    });
  }

  public async createEmbedding(params: {
    model: string;
    input: string[];
    isImage?: boolean;
  }): Promise<number[][]> {
    await this.loadClient();
    if (params.isImage) {
      return await this.cohereClient
        .embed({ images: params.input, model: params.model })
        .then((response: any) => {
          return response.embeddings;
        });
    } else {
      return await this.cohereClient
        .embed({ texts: params.input, model: params.model })
        .then((response: any) => {
          return response.embeddings;
        });
    }
  }
}

interface StoredConfig {
  model_name: string;
  api_key_env_var: string;
}

export class CohereEmbeddingFunction implements IEmbeddingFunction {
  name = "cohere";

  private cohereAiApi?: CohereAIAPI;
  private model: string;
  private isImage: boolean;
  private apiKey: string;
  private apiKeyEnvVar: string;

  constructor({
    cohere_api_key,
    model = "large",
    cohere_api_key_env_var = "CHROMA_COHERE_API_KEY",
    /**
     * If true, the input texts passed to `generate` are expected to be
     * base64 encoded PNG data URIs.
     */
    isImage = false,
  }: {
    cohere_api_key?: string;
    model?: string;
    cohere_api_key_env_var: string;
    /**
     * If true, the input texts passed to `generate` are expected to be
     * base64 encoded PNG data URIs.
     */
    isImage?: boolean;
  }) {
    this.model = model;
    this.isImage = isImage;

    const apiKey = cohere_api_key ?? process.env[cohere_api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `Cohere API key is required. Please provide it in the constructor or set the environment variable ${cohere_api_key_env_var}.`,
      );
    }

    this.apiKey = apiKey;
    this.apiKeyEnvVar = cohere_api_key_env_var;
  }

  private async initCohereClient() {
    if (this.cohereAiApi) return;
    try {
      // @ts-ignore
      this.cohereAiApi = await import("cohere-ai").then((cohere) => {
        // @ts-ignore
        if (cohere.CohereClient) {
          return new CohereAISDK7({ apiKey: this.apiKey });
        } else {
          return new CohereAISDK56({ apiKey: this.apiKey });
        }
      });
    } catch (e) {
      // @ts-ignore
      if (e.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the cohere-ai package to use the CohereEmbeddingFunction, `npm install -S cohere-ai`",
        );
      }
      throw e;
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.initCohereClient();
    // @ts-ignore
    return await this.cohereAiApi.createEmbedding({
      model: this.model,
      input: texts,
      isImage: this.isImage,
    });
  }

  buildFromConfig(config: StoredConfig): CohereEmbeddingFunction {
    return new CohereEmbeddingFunction({
      model: config.model_name,
      cohere_api_key_env_var: config.api_key_env_var,
    });
  }

  getConfig(): StoredConfig {
    return {
      model_name: this.model,
      api_key_env_var: this.apiKeyEnvVar,
    };
  }

  validateConfigUpdate(oldConfig: StoredConfig, newConfig: StoredConfig): void {
    if (oldConfig.model_name !== newConfig.model_name) {
      throw new Error(
        "CohereEmbeddingFunction model_name cannot be changed after initialization.",
      );
    }
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "cohere");
  }

  supportedSpaces(): EmbeddingFunctionSpace[] {
    if (this.model === "embed-english-v3.0") {
      return ["cosine", "l2", "ip"];
    }

    if (this.model === "embed-english-light-v3.0") {
      return ["cosine", "ip", "l2"];
    }

    if (this.model === "embed-english-v2.0") {
      return ["cosine"];
    }

    if (this.model === "embed-english-light-v2.0") {
      return ["cosine"];
    }

    if (this.model === "embed-multilingual-v3.0") {
      return ["cosine", "l2", "ip"];
    }

    if (this.model === "embed-multilingual-light-v3.0") {
      return ["cosine", "l2", "ip"];
    }

    if (this.model === "embed-multilingual-v2.0") {
      return ["ip"];
    }

    return ["cosine", "l2", "ip"];
  }

  defaultSpace(): EmbeddingFunctionSpace {
    if (this.model == "embed-multilingual-v2.0") {
      return "ip";
    }

    return "cosine";
  }
}

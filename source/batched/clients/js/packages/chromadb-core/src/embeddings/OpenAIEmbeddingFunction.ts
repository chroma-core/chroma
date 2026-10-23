import { IEmbeddingFunction } from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";
let OpenAIApi: any;
let openAiVersion = null;
let openAiMajorVersion = null;

interface OpenAIAPI {
  createEmbedding: (params: {
    model: string;
    input: string[];
    user?: string;
    dimensions?: number;
  }) => Promise<number[][]>;
}

class OpenAIAPIv3 implements OpenAIAPI {
  private readonly configuration: any;
  private openai: any;

  constructor(configuration: { organization: string; apiKey: string }) {
    this.configuration = new OpenAIApi.Configuration({
      organization: configuration.organization,
      apiKey: configuration.apiKey,
    });
    this.openai = new OpenAIApi.OpenAIApi(this.configuration);
  }

  public async createEmbedding(params: {
    model: string;
    input: string[];
    user?: string;
    dimensions?: number;
  }): Promise<number[][]> {
    const embeddings: number[][] = [];
    const response = await this.openai
      .createEmbedding({
        model: params.model,
        input: params.input,
      })
      .catch((error: any) => {
        throw error;
      });
    // @ts-ignore
    const data = response.data["data"];
    for (let i = 0; i < data.length; i += 1) {
      embeddings.push(data[i]["embedding"]);
    }
    return embeddings;
  }
}

class OpenAIAPIv4 implements OpenAIAPI {
  private readonly apiKey: any;
  private openai: any;

  constructor(apiKey: any) {
    this.apiKey = apiKey;
    this.openai = new OpenAIApi({
      apiKey: this.apiKey,
    });
  }

  public async createEmbedding(params: {
    model: string;
    input: string[];
    user?: string;
    dimensions?: number;
  }): Promise<number[][]> {
    const embeddings: number[][] = [];
    const response = await this.openai.embeddings.create(params);
    const data = response["data"];
    for (let i = 0; i < data.length; i += 1) {
      embeddings.push(data[i]["embedding"]);
    }
    return embeddings;
  }
}

type StoredConfig = {
  api_key_env_var: string;
  model_name: string;
  organization_id: string;
  dimensions: number;
};

export class OpenAIEmbeddingFunction implements IEmbeddingFunction {
  name = "openai";

  private api_key: string;
  private org_id: string;
  private model: string;
  private openaiApi?: OpenAIAPI;
  private dimensions?: number;

  constructor({
    openai_api_key,
    openai_model = "text-embedding-ada-002",
    openai_organization_id,
    openai_embedding_dimensions,
    openai_api_key_env_var = "CHROMA_OPENAI_API_KEY",
  }: {
    openai_api_key?: string;
    openai_model?: string;
    openai_organization_id?: string;
    openai_embedding_dimensions?: number;
    openai_api_key_env_var?: string;
  }) {
    const apiKey = openai_api_key ?? process.env[openai_api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `OpenAI API key is required. Please provide it in the constructor or set the environment variable ${openai_api_key_env_var}.`,
      );
    }
    this.api_key = apiKey;

    this.org_id = openai_organization_id ?? "";
    this.model = openai_model;
    this.dimensions = openai_embedding_dimensions ?? 1536;
  }

  private async loadClient() {
    // cache the client
    if (this.openaiApi) return;

    try {
      const { openai, version } = await OpenAIEmbeddingFunction.import();
      OpenAIApi = openai;
      let versionVar: string = version;
      openAiVersion = versionVar.replace(/[^0-9.]/g, "");
      openAiMajorVersion = parseInt(openAiVersion.split(".")[0]);
    } catch (_a) {
      // @ts-ignore
      if (_a.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the openai package to use the OpenAIEmbeddingFunction, e.g. `npm install openai`",
        );
      }
      throw _a; // Re-throw other errors
    }

    if (openAiMajorVersion > 3) {
      this.openaiApi = new OpenAIAPIv4(this.api_key);
    } else {
      this.openaiApi = new OpenAIAPIv3({
        organization: this.org_id,
        apiKey: this.api_key,
      });
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.loadClient();

    return await this.openaiApi!.createEmbedding({
      model: this.model,
      input: texts,
      dimensions: this.dimensions,
    }).catch((error: any) => {
      throw error;
    });
  }

  /** @ignore */
  static async import(): Promise<{
    // @ts-ignore
    openai: typeof import("openai");
    version: string;
  }> {
    try {
      // @ts-ignore
      const { default: openai } = await import("openai");
      // @ts-ignore
      const { VERSION } = await import("openai/version");
      // @ts-ignore
      return { openai, version: VERSION };
    } catch (e) {
      throw new Error(
        "Please install the openai package to use the OpenAIEmbeddingFunction, e.g. `npm install openai`",
      );
    }
  }

  buildFromConfig(config: StoredConfig): OpenAIEmbeddingFunction {
    return new OpenAIEmbeddingFunction({
      openai_api_key: config.api_key_env_var,
      openai_model: config.model_name,
      openai_organization_id: config.organization_id,
      openai_embedding_dimensions: config.dimensions,
    });
  }

  getConfig(): StoredConfig {
    return {
      api_key_env_var: this.api_key,
      model_name: this.model,
      organization_id: this.org_id,
      dimensions: this.dimensions ?? 1536,
    };
  }

  validateConfigUpdate(oldConfig: StoredConfig, newConfig: StoredConfig): void {
    if (oldConfig.model_name !== newConfig.model_name) {
      throw new Error("Cannot change model name.");
    }
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "openai");
  }
}

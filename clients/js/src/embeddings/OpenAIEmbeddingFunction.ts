import { IEmbeddingFunction } from "./IEmbeddingFunction";

let OpenAIApi: any;
let openAiVersion = null;
let openAiMajorVersion = null;

interface OpenAIAPI {
  createEmbedding: (params: {
    model: string;
    input: string[];
    user?: string;
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

class OpenAIAPIAzure implements OpenAIAPI {
  private openai: any;

  constructor(configuration: { apiKey: string; apiVersion?: string; apiBase?: string; deployment?: string }) {
    this.openai = new OpenAIApi.AzureOpenAI({
      apiKey: configuration.apiKey,
      apiBase: configuration.apiBase,
      apiVersion: configuration.apiVersion,
      deployment: configuration.deployment,
    });
  }

  public async createEmbedding(params: {
    model: string;
    input: string[];
    user?: string;
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

export class OpenAIEmbeddingFunction implements IEmbeddingFunction {
  private api_key: string;
  private org_id: string;
  private api_type?: string;
  private api_version?: string;
  private api_base?: string;
  private deployment?: string;
  private model: string;
  private openaiApi?: OpenAIAPI;

  constructor({
    openai_api_key,
    openai_model,
    openai_organization_id,
    api_type,
    api_base,
    api_version,
    deployment,
  }: {
    openai_api_key: string;
    openai_model?: string;
    openai_organization_id?: string;
    api_type?: string;
    api_base?: string;
    api_version?: string;
    deployment?: string;
  }) {
    // we used to construct the client here, but we need to async import the types
    // for the openai npm package, and the constructor can not be async
    this.api_key = openai_api_key;
    this.org_id = openai_organization_id || "";
    this.model = openai_model || "text-embedding-ada-002";
    this.api_type = api_type;
    this.api_base = api_base;
    this.api_version = api_version;
    this.deployment = deployment;
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
          "Please install the openai package to use the OpenAIEmbeddingFunction, `npm install -S openai`",
        );
      }
      throw _a; // Re-throw other errors
    }

    if (this.api_type === "azure") {
      this.openaiApi = new OpenAIAPIAzure({
        apiKey: this.api_key,
        apiBase: this.api_base,
        apiVersion: this.api_version,
        deployment: this.deployment
      });
    } else if (openAiMajorVersion > 3) {
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
      return { openai, version: VERSION };
    } catch (e) {
      throw new Error(
        "Please install openai as a dependency with, e.g. `yarn add openai`",
      );
    }
  }
}

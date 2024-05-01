import { IEmbeddingFunction } from "./IEmbeddingFunction";

let OpenAIApi: any;

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

class OpenAIAPIv4 implements OpenAIAPI {
  private openai: any;

  constructor(configuration: { organization: string; apiKey: string }) {
    this.openai = new OpenAIApi.OpenAI(configuration);
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
  private model: string;
  private openaiApi?: OpenAIAPI;

  constructor({
    openai_api_key,
    openai_model,
    openai_organization_id,
  }: {
    openai_api_key: string;
    openai_model?: string;
    openai_organization_id?: string;
  }) {
    // we used to construct the client here, but we need to async import the types
    // for the openai npm package, and the constructor can not be async
    this.api_key = openai_api_key;
    this.org_id = openai_organization_id || "";
    this.model = openai_model || "text-embedding-ada-002";
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.initOpenAIClient();

    return await this.openaiApi!.createEmbedding({
      model: this.model,
      input: texts,
    }).catch((error: any) => {
      throw error;
    });
  }
  private async initOpenAIClient() {
    if (this.openaiApi) return;
    try {
      this.openaiApi = await import("openai").then(async (openai) => {
        OpenAIApi = openai;
        let LIB_VERSION = "3";
        try {
          // @ts-ignore
          const { VERSION } = await import("openai/version");
          LIB_VERSION = VERSION;
        } catch (e) {
          // @ts-ignore
          if (e.code === "MODULE_NOT_FOUND") {
            LIB_VERSION = "3";
          }
        }
        if (LIB_VERSION.startsWith("4")) {
          return new OpenAIAPIv4({
            apiKey: this.api_key,
            organization: this.org_id,
          });
        } else if (LIB_VERSION.startsWith("3")) {
          return new OpenAIAPIv3({
            organization: this.org_id,
            apiKey: this.api_key,
          });
        } else {
          throw new Error("Unsupported OpenAI library version");
        }
      });
    } catch (e) {
      // @ts-ignore
      if (e.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the openai package to use the OpenAIEmbeddingFunction, `npm install -S openai`",
        );
      }
      throw e;
    }
  }
}

import { importOptionalModule } from "../utils";
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

async function loadOpenAIApi(): Promise<void> {
  return importOptionalModule("openai").then((module) => {
    OpenAIApi = module;
  });
}

export class OpenAIEmbeddingFunction implements IEmbeddingFunction {
  private api_key: string;
  private org_id: string;
  private model: string;
  private openaiApi: any;
  private isInitialized: boolean;

  constructor({
    openai_api_key,
    openai_model,
    openai_organization_id,
  }: {
    openai_api_key: string;
    openai_model?: string;
    openai_organization_id?: string;
  }) {
    this.api_key = openai_api_key;
    this.org_id = openai_organization_id || "";
    this.model = openai_model || "text-embedding-ada-002";
    this.isInitialized = false;

    loadOpenAIApi()
      .then(() => {
        let version = null;
        try {
          version = OpenAIApi.VERSION || "3.x";
          this.isInitialized = true;
        } catch (error) {
          version = "3.x";
        }

        const openAiVersion = version.replace(/[^0-9.]/g, "");
        const openAiMajorVersion = parseInt(openAiVersion.split(".")[0]);

        if (openAiMajorVersion > 3) {
          this.openaiApi = new OpenAIAPIv4(this.api_key);
        } else {
          this.openaiApi = new OpenAIAPIv3({
            organization: this.org_id,
            apiKey: this.api_key,
          });
        }
      })
      .catch((error) => {
        console.error("Could not load OpenAIApi:", error);
      });
  }

  public async generate(texts: string[]): Promise<number[][]> {
    if (!this.isInitialized) {
      throw new Error("OpenAI API is not initialized.");
    }

    return await this.openaiApi
      .createEmbedding({
        model: this.model,
        input: texts,
      })
      .catch((error: any) => {
        throw error;
      });
  }
}

import { IEmbeddingFunction } from "./IEmbeddingFunction";

interface CohereAIAPI {
  createEmbedding: (params: {
    model: string;
    input: string[];
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
  }): Promise<number[][]> {
    await this.loadClient();
    return await this.cohereClient
      .embed({ texts: params.input, model: params.model })
      .then((response: any) => {
        return response.embeddings;
      });
  }
}

export class CohereEmbeddingFunction implements IEmbeddingFunction {
  private cohereAiApi?: CohereAIAPI;
  private model: string;
  private apiKey: string;
  constructor({
    cohere_api_key,
    model,
  }: {
    cohere_api_key: string;
    model?: string;
  }) {
    this.model = model || "large";
    this.apiKey = cohere_api_key;
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
    });
  }
}

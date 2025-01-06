import { IEmbeddingFunction } from "./IEmbeddingFunction";

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
        return response.data.map((item: { embedding: number[]; }) => item.embedding);
      });
  }
}

export class VoyageAIEmbeddingFunction implements IEmbeddingFunction {
  private voyageAiApi?: VoyageAIAPI;
  private model: string;
  private apiKey: string;
  constructor({
    api_key,
    model,
  }: {
    api_key: string;
    model: string;
  }) {
    this.model = model;
    this.apiKey = api_key;
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
}

import { IEmbeddingFunction } from "./IEmbeddingFunction";

export enum InputType {
  DOCUMENT = "document",
  QUERY = "query"
}

export class VoyageAIEmbeddingFunction implements IEmbeddingFunction {
  private modelName: string;
  private apiUrl: string;
  private batchSize: number;
  private truncation?: boolean;
  private inputType?: InputType;
  private headers: { [key: string]: string };

  constructor({
    voyageaiApiKey,
    modelName,
    batchSize,
    truncation,
    inputType,
  }: {
    voyageaiApiKey: string;
    modelName: string;
    batchSize?: number;
    truncation?: boolean;
    inputType?: InputType;
  }) {
    this.apiUrl = "https://api.voyageai.com/v1/embeddings";
    this.headers = {
      Authorization: `Bearer ${voyageaiApiKey}`,
      "Content-Type": "application/json",
    };

    this.modelName = modelName;
    this.truncation = truncation;
    this.inputType = inputType;
    if (batchSize) {
      this.batchSize = batchSize;
    } else {
      if (modelName in ["voyage-2", "voyage-02"]) {
        this.batchSize = 72;
      } else {
        this.batchSize = 7;
      }
    }
  }

  public async generate(texts: string[]) {
    try {
      if(texts.length > this.batchSize) {
        throw new Error(`The number of texts to embed exceeds the maximum batch size of ${this.batchSize}`);
      }

      const response = await fetch(this.apiUrl, {
        method: 'POST',
        headers: this.headers,
        body: JSON.stringify({
          input: texts,
          model: this.modelName,
          truncation: this.truncation,
          input_type: this.inputType,
        }),
      });

      const data = (await response.json()) as { data: any[]; detail: string };
      if (!data || !data.data) {
        throw new Error(data.detail);
      }

      const embeddings: any[] = data.data;
      const sortedEmbeddings = embeddings.sort((a, b) => a.index - b.index);

      const embeddingsChunks = sortedEmbeddings.map((result) => result.embedding);

      return embeddingsChunks;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling VoyageAI API: ${error.message}`);
      } else {
        throw new Error(`Error calling VoyageAI API: ${error}`);
      }
    }
  }
}

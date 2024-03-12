import { IEmbeddingFunction } from "./IEmbeddingFunction";

export class VoyageAIEmbeddingFunction implements IEmbeddingFunction {
  private model_name: string;
  private api_url: string;
  private batch_size: number;
  private truncation?: boolean;
  private headers: { [key: string]: string };

  constructor({
    voyageai_api_key,
    model_name,
    batch_size,
    truncation,
  }: {
    voyageai_api_key: string;
    model_name: string;
    batch_size?: number;
    truncation?: boolean;
  }) {
    this.api_url = "https://api.voyageai.com/v1/embeddings";
    this.headers = {
      Authorization: `Bearer ${voyageai_api_key}`,
      "Content-Type": "application/json",
    };

    this.model_name = model_name;
    this.truncation = truncation;
    if (batch_size) {
      this.batch_size = batch_size;
    } else {
      if (model_name in ["voyage-2", "voyage-02"]) {
        this.batch_size = 72;
      } else {
        this.batch_size = 7;
      }
    }
  }

  public async generate(texts: string[]) {
    try {
      const result: number[][] = [];
      let index = 0;
    
      while (index < texts.length) {
        const response = await fetch(this.api_url, {
          method: 'POST',
          headers: this.headers,
          body: JSON.stringify({
            input: texts.slice(index, index + this.batch_size),
            model: this.model_name,
            truncation: this.truncation,
          }),
        });

        const data = (await response.json()) as { data: any[]; detail: string };
        if (!data || !data.data) {
          throw new Error(data.detail);
        }

        const embeddings: any[] = data.data;
        const sortedEmbeddings = embeddings.sort((a, b) => a.index - b.index);

        const embeddingsChunks = sortedEmbeddings.map((result) => result.embedding);
        result.push(...embeddingsChunks);
        index += this.batch_size;
      }
      return result;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling VoyageAI API: ${error.message}`);
      } else {
        throw new Error(`Error calling VoyageAI API: ${error}`);
      }
    }
  }
}

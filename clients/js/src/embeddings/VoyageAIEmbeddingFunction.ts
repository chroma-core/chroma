import { IEmbeddingFunction } from "./IEmbeddingFunction";

export class VoyageAIEmbeddingFunction implements IEmbeddingFunction {
  private model_name: string;
  private api_url: string;
  private truncation?: boolean;
  private headers: { [key: string]: string };

  constructor({ voyageai_api_key, model_name, truncation }: { voyageai_api_key: string; model_name: string; truncation?: boolean }) {
    this.api_url = 'https://api.voyageai.com/v1/embeddings';
    this.headers = {
      Authorization: `Bearer ${voyageai_api_key}`,
      'Content-Type': 'application/json',
    };

    this.model_name = model_name;
    this.truncation = truncation;
  }

  public async generate(texts: string[]) {
    try {
      const response = await fetch(this.api_url, {
        method: 'POST',
        headers: this.headers,
        body: JSON.stringify({
          input: texts,
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

      return sortedEmbeddings.map((result) => result.embedding);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling VoyageAI API: ${error.message}`);
      } else {
        throw new Error(`Error calling VoyageAI API: ${error}`);
      }
    }
  }
}

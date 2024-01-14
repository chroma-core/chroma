import { IEmbeddingFunction } from "./IEmbeddingFunction";

export class TogetherEmbeddingFunction implements IEmbeddingFunction {
  private model_name: string;
  private api_url: string;
  private headers: { [key: string]: string };

  constructor({ together_api_key, model_name }: { together_api_key: string; model_name?: string }) {
    this.model_name = model_name || 'togethercomputer/m2-bert-80M-8k-retrieval';
    this.api_url = 'https://api.together.xyz/api/v1/embeddings';
    this.headers = {
      Authorization: `Bearer ${together_api_key}`,
      'Content-Type': 'application/json',
    };
  }

  public async generate(texts: string[]) {
    try {
      const response = await fetch(this.api_url, {
        method: 'POST',
        headers: this.headers,
        body: JSON.stringify({
          input: texts,
          model: this.model_name,
        }),
      });

      const data = (await response.json()) as { data: any[]; detail: string };
      if (!data || !data.data) {
        throw new Error(data.detail);
      }
      const embeddingLists = data.data.map(embeddingObject => embeddingObject.embedding);
      return embeddingLists;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling Together AI API: ${error.message}`);
      } else {
        throw new Error(`Error calling Together AI API: ${error}`);
      }
    }
  }
}

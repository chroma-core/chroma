import { IEmbeddingFunction } from "./IEmbeddingFunction";

export class JinaEmbeddingFunction implements IEmbeddingFunction {
  private model_name: string;
  private api_url: string;
  private headers: { [key: string]: string };

  constructor({
    jinaai_api_key,
    model_name,
  }: {
    jinaai_api_key: string;
    model_name?: string;
  }) {
    this.model_name = model_name || "jina-embeddings-v2-base-en";
    this.api_url = "https://api.jina.ai/v1/embeddings";
    this.headers = {
      Authorization: `Bearer ${jinaai_api_key}`,
      "Accept-Encoding": "identity",
      "Content-Type": "application/json",
    };
  }

  public async generate(texts: string[]) {
    try {
      const response = await fetch(this.api_url, {
        method: "POST",
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

      const embeddings: any[] = data.data;
      const sortedEmbeddings = embeddings.sort((a, b) => a.index - b.index);

      return sortedEmbeddings.map((result) => result.embedding);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling Jina AI API: ${error.message}`);
      } else {
        throw new Error(`Error calling Jina AI API: ${error}`);
      }
    }
  }
}

import { IEmbeddingFunction } from "./IEmbeddingFunction";

let CohereAiApi: any;

export class HuggingFaceEmbeddingServerFunction implements IEmbeddingFunction {
  private url: string;

  constructor({ url }: { url: string }) {
    // we used to construct the client here, but we need to async import the types
    // for the openai npm package, and the constructor can not be async
    this.url = url;
  }

  public async generate(texts: string[]) {
    const response = await fetch(this.url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ inputs: texts }),
    });

    if (!response.ok) {
      throw new Error(`Failed to generate embeddings: ${response.statusText}`);
    }

    const data = await response.json();
    return data;
  }
}

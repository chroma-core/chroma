import { IEmbeddingFunction } from "./IEmbeddingFunction";
const DEFAULT_MODEL = "chroma/all-minilm-l6-v2-f32";
const DEFAULT_LOCAL_URL = "http://localhost:11434";
export class OllamaEmbeddingFunction implements IEmbeddingFunction {
  private readonly url?: string | undefined;
  private readonly model: string;
  private ollamaClient: any;

  constructor(
    {
      url = DEFAULT_LOCAL_URL,
      model = DEFAULT_MODEL,
    }: { url?: string; model?: string } = {
      url: DEFAULT_LOCAL_URL,
      model: DEFAULT_MODEL,
    },
  ) {
    // we used to construct the client here, but we need to async import the types
    // for the openai npm package, and the constructor can not be async
    if (url && url.endsWith("/api/embeddings")) {
      this.url = url.slice(0, -"/api/embeddings".length);
    } else {
      this.url = url || DEFAULT_LOCAL_URL;
    }
    this.model = model || DEFAULT_MODEL;
  }

  private async initClient() {
    if (this.ollamaClient) return;
    try {
      // @ts-ignore
      this.ollamaClient = await import("ollama/browser").then((ollama) => {
        // @ts-ignore
        return new ollama.Ollama({ host: this.url });
      });
    } catch (e) {
      // @ts-ignore
      if (e.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the ollama package to use the CohereEmbeddingFunction, `npm install -S ollama`",
        );
      }
      throw e;
    }
  }

  public async generate(texts: string[]) {
    await this.initClient();
    return await this.ollamaClient
      .embed({
        model: this.model,
        input: texts,
      })
      .then((response: any) => {
        return response.embeddings;
      });
    // let embeddings: number[][] = [];
    // for (let text of texts) {
    //   const response = await fetch(this.url, {
    //     method: "POST",
    //     headers: {
    //       "Content-Type": "application/json",
    //     },
    //     body: JSON.stringify({ model: this.model, prompt: text }),
    //   });
    //
    //   if (!response.ok) {
    //     throw new Error(
    //       `Failed to generate embeddings: ${response.status} (${response.statusText})`,
    //     );
    //   }
    //   let finalResponse = await response.json();
    //   embeddings.push(finalResponse["embedding"]);
    // }
    // return embeddings;
  }
}

import type { IEmbeddingFunction } from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";
type StoredConfig = {
  url: string;
  model_name: string;
};

export class OllamaEmbeddingFunction implements IEmbeddingFunction {
  name = "ollama";

  private readonly url: string;
  private readonly model: string;
  private ollamaClient?: any;

  constructor({
    url = "http://localhost:11434",
    model = "chroma/all-minilm-l6-v2-f32",
  }: { url?: string; model?: string } = {}) {
    // we used to construct the client here, but we need to async import the types
    // for the openai npm package, and the constructor can not be async
    if (url && url.endsWith("/api/embeddings")) {
      this.url = url.slice(0, -"/api/embeddings".length);
    } else {
      this.url = url;
    }
    this.model = model;
  }

  private async initClient() {
    if (this.ollamaClient) return;
    try {
      // @ts-ignore
      const { ollama } = await OllamaEmbeddingFunction.import();
      this.ollamaClient = new ollama.Ollama({ host: this.url });
    } catch (e) {
      // @ts-ignore
      if (e.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the ollama package to use the OllamaEmbeddingFunction, `npm install -S ollama`",
        );
      }
      throw e;
    }
  }

  /** @ignore */
  static async import(): Promise<{
    // @ts-ignore
    ollama: typeof import("ollama");
  }> {
    try {
      // @ts-ignore
      const { ollama } = await import("ollama").then((m) => ({ ollama: m }));
      // @ts-ignore
      return { ollama };
    } catch (e) {
      throw new Error(
        "Please install Ollama as a dependency with, e.g. `npm install ollama`",
      );
    }
  }

  public async generate(texts: string[]) {
    await this.initClient();
    return await this.ollamaClient!.embed({
      model: this.model,
      input: texts,
    }).then((response: any) => {
      return response.embeddings;
    });
  }

  buildFromConfig(config: StoredConfig): OllamaEmbeddingFunction {
    return new OllamaEmbeddingFunction({
      url: config.url,
      model: config.model_name,
    });
  }

  getConfig(): StoredConfig {
    return {
      url: this.url,
      model_name: this.model,
    };
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "ollama");
  }
}

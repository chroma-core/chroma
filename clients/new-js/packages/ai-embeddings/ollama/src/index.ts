import {
  isBrowser,
  validateConfigSchema,
} from "@chroma-core/ai-embeddings-common";
import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import type { Ollama as OllamaNode } from "ollama";
import type { Ollama as OllamaBrowser } from "ollama/browser";

const NAME = "ollama";

export interface OllamaConfig {
  url: string;
  model_name: string;
}

export class OllamaEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly url: string;
  private readonly model: string;
  private client: OllamaNode | OllamaBrowser | undefined;

  constructor(args: Partial<{ url?: string; model: string }> = {}) {
    const {
      url = "http://localhost:11434",
      model = "chroma/all-minilm-l6-v2-f32",
    } = args;
    this.url = url;
    this.model = model;
  }

  private async import() {
    if (isBrowser()) {
      const { Ollama } = await import("ollama/browser");
      this.client = new Ollama({ host: this.url });
    } else {
      const { Ollama } = await import("ollama");
      this.client = new Ollama({ host: this.url });
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.import();
    if (!this.client) {
      throw new Error("Failed to instantiate Ollama client");
    }
    const response = await this.client.embed({
      model: this.model,
      input: texts,
    });
    return response.embeddings;
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(config: OllamaConfig): OllamaEmbeddingFunction {
    return new OllamaEmbeddingFunction({
      model: config.model_name,
      url: config.url,
    });
  }

  public getConfig(): OllamaConfig {
    return {
      model_name: this.model,
      url: this.url,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
  }

  public static validateConfig(config: OllamaConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, OllamaEmbeddingFunction);

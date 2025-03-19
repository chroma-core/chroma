import { validateConfigSchema } from "../schemas/schemaUtils";
import { IEmbeddingFunction } from "./IEmbeddingFunction";

type StoredConfig = {
  url: string;
};

export class HuggingFaceEmbeddingServerFunction implements IEmbeddingFunction {
  name = "huggingface_server";

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

  buildFromConfig(config: StoredConfig): HuggingFaceEmbeddingServerFunction {
    return new HuggingFaceEmbeddingServerFunction(config);
  }

  getConfig(): StoredConfig {
    return {
      url: this.url,
    };
  }

  validateConfigUpdate(
    oldConfig: Record<string, any>,
    newConfig: Record<string, any>,
  ): void {
    if (oldConfig.url !== newConfig.url) {
      throw new Error("Changing the URL is not allowed.");
    }
  }

  validateConfig(config: Record<string, any>): void {
    validateConfigSchema(config, "huggingface_server");
  }
}

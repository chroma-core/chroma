import { importOptionalModule } from "../utils";
import { IEmbeddingFunction } from "./IEmbeddingFunction";

let CohereAiApi: any;

async function loadCohereAiApi() {
  return importOptionalModule("cohere-ai")
    .then((module) => {
      CohereAiApi = module;
      return true;
    })
    .catch(() => {
      throw new Error(
        "Please install the cohere-ai package to use the CohereEmbeddingFunction, `npm install -S cohere-ai`"
      );
    });
}

export class CohereEmbeddingFunction implements IEmbeddingFunction {
  private api_key: string;
  private model: string;
  private isInitialized = false;

  constructor({
    cohere_api_key,
    model,
  }: {
    cohere_api_key: string;
    model?: string;
  }) {
    this.api_key = cohere_api_key;
    this.model = model || "large";

    loadCohereAiApi()
      .then(() => {
        CohereAiApi.init(this.api_key);
        this.isInitialized = true;
      })
      .catch((error) => {
        console.error("Could not load CohereAiApi:", error);
      });
  }

  public async generate(texts: string[]) {
    if (!this.isInitialized) {
      throw new Error("Cohere API is not initialized.");
    }

    const response = await CohereAiApi.embed({
      texts: texts,
      model: this.model,
    });
    return response.body.embeddings;
  }
}

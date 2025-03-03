import { IEmbeddingFunction } from "./IEmbeddingFunction";

let googleGenAiApi: any;

export class GoogleGenerativeAiEmbeddingFunction implements IEmbeddingFunction {
  private api_key: string;
  private model: string;
  private googleGenAiApi?: any;
  private taskType: string;

  constructor({
    googleApiKey,
    model,
    taskType,
  }: {
    googleApiKey: string;
    model?: string;
    taskType?: string;
  }) {
    // we used to construct the client here, but we need to async import the types
    // for the openai npm package, and the constructor can not be async
    this.api_key = googleApiKey;
    this.model = model || "embedding-001";
    this.taskType = taskType || "RETRIEVAL_DOCUMENT";
  }

  private async loadClient() {
    if (this.googleGenAiApi) return;
    try {
      // eslint-disable-next-line global-require,import/no-extraneous-dependencies
      const { googleGenAi } =
        await GoogleGenerativeAiEmbeddingFunction.import();
      googleGenAiApi = googleGenAi;
      // googleGenAiApi.init(this.api_key);
      googleGenAiApi = new googleGenAiApi(this.api_key);
    } catch (_a) {
      // @ts-ignore
      if (_a.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the @google/generative-ai package to use the GoogleGenerativeAiEmbeddingFunction, `npm install @google/generative-ai`",
        );
      }
      throw _a; // Re-throw other errors
    }
    this.googleGenAiApi = googleGenAiApi;
  }

  public async generate(texts: string[]) {
    await this.loadClient();
    const model = this.googleGenAiApi.getGenerativeModel({ model: this.model });
    const response = await model.batchEmbedContents({
      requests: texts.map((t) => ({
        content: { parts: [{ text: t }] },
        taskType: this.taskType,
      })),
    });
    const embeddings = response.embeddings.map((e: any) => e.values);

    return embeddings;
  }

  /** @ignore */
  static async import(): Promise<{
    // @ts-ignore
    googleGenAi: typeof import("@google/generative-ai");
  }> {
    try {
      // @ts-ignore
      const { GoogleGenerativeAI } = await import("@google/generative-ai");
      const googleGenAi = GoogleGenerativeAI;
      // @ts-ignore
      return { googleGenAi };
    } catch (e) {
      throw new Error(
        "Please install @google/generative-ai as a dependency with, e.g. `npm install @google/generative-ai`",
      );
    }
  }
}

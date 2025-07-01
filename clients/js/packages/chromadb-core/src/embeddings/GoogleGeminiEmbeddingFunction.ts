import {
  EmbeddingFunctionSpace,
  IEmbeddingFunction,
} from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";
interface StoredConfig {
  api_key_env_var: string;
  model_name: string;
  task_type: string;
}

let googleGenAiApi: any;

export class GoogleGenerativeAiEmbeddingFunction implements IEmbeddingFunction {
  name = "google_generative_ai";

  private api_key: string;
  private api_key_env_var: string;
  private model: string;
  private googleGenAiApi?: any;
  private taskType: string;

  constructor({
    googleApiKey,
    model = "embedding-001",
    taskType = "RETRIEVAL_DOCUMENT",
    apiKeyEnvVar = "GOOGLE_API_KEY",
  }: {
    googleApiKey?: string;
    model?: string;
    taskType?: string;
    apiKeyEnvVar: string;
  }) {
    const apiKey = googleApiKey ?? process.env[apiKeyEnvVar];
    if (!apiKey) {
      throw new Error(
        `Google API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    this.api_key = apiKey;
    this.api_key_env_var = apiKeyEnvVar;
    this.model = model;
    this.taskType = taskType;
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

  buildFromConfig(config: StoredConfig): GoogleGenerativeAiEmbeddingFunction {
    return new GoogleGenerativeAiEmbeddingFunction({
      model: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      taskType: config.task_type,
    });
  }

  getConfig(): StoredConfig {
    return {
      api_key_env_var: this.api_key_env_var,
      model_name: this.model,
      task_type: this.taskType,
    };
  }

  validateConfigUpdate(
    oldConfig: Record<string, any>,
    newConfig: Record<string, any>,
  ): void {
    if (oldConfig.model_name !== newConfig.model_name) {
      throw new Error("The model name cannot be changed after initialization.");
    }

    if (oldConfig.taskType !== newConfig.taskType) {
      throw new Error("The task type cannot be changed after initialization.");
    }
  }

  validateConfig(config: Record<string, any>): void {
    validateConfigSchema(config, "google_generative_ai");
  }
}

import {
  IEmbeddingFunction,
  EmbeddingFunctionSpace,
} from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";

declare const process: any;

interface RunPodAPI {
  createEmbedding: (params: {
    model: string;
    input: string[];
    timeout?: number;
  }) => Promise<number[][]>;
}

class RunPodAPIImpl implements RunPodAPI {
  private readonly apiKey: string;
  private readonly endpointId: string;
  private runpod: any;
  private endpoint: any;

  constructor(apiKey: string, endpointId: string, runpodSdk: any) {
    this.apiKey = apiKey;
    this.endpointId = endpointId;

    // Initialize the RunPod SDK
    this.runpod = runpodSdk(this.apiKey);
    this.endpoint = this.runpod.endpoint(this.endpointId);
  }

  public async createEmbedding(params: {
    model: string;
    input: string[];
    timeout?: number;
  }): Promise<number[][]> {
    // Process all documents in parallel for better performance
    const embeddings = await Promise.all(
      params.input.map((document) =>
        this.createSingleEmbedding(document, params.model, params.timeout)
      )
    );

    return embeddings;
  }

  private async createSingleEmbedding(
    document: string,
    model: string,
    timeout?: number
  ): Promise<number[]> {
    try {
      // Prepare the input payload for RunPod
      const inputPayload = {
        input: {
          model: model,
          input: document,
        },
      };

      // Start the async request
      const runRequest = await this.endpoint.run(inputPayload);
      const jobId = runRequest.id;

      // Check initial status
      let status = runRequest.status;

      if (
        status === "FAILED" ||
        status === "CANCELLED" ||
        status === "TIMED_OUT"
      ) {
        throw new Error(
          `RunPod endpoint failed with status '${status}': ${JSON.stringify(
            runRequest,
          )}`,
        );
      }

      let finalStatus;
      if (status === "COMPLETED") {
        finalStatus = await this.endpoint.status(jobId);
      } else {
        const timeoutMs = (timeout || 300) * 1000;
        const startTime = Date.now();
        const pollInterval = 1000;

        while (Date.now() - startTime < timeoutMs) {
          finalStatus = await this.endpoint.status(jobId);

          if (finalStatus.status === "COMPLETED") {
            break;
          } else if (
            finalStatus.status === "FAILED" ||
            finalStatus.status === "CANCELLED" ||
            finalStatus.status === "TIMED_OUT"
          ) {
            throw new Error(
              `RunPod endpoint failed with status '${
                finalStatus.status
              }': ${JSON.stringify(finalStatus)}`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, pollInterval));
        }

        if (finalStatus?.status !== "COMPLETED") {
          throw new Error(
            `Request timed out after ${
              timeout || 300
            } seconds. Last status: ${finalStatus?.status || "UNKNOWN"}`,
          );
        }
      }

      // Extract embedding from the completed status
      const output = finalStatus?.output;
      if (output && "data" in output) {
        const dataList = output["data"];
        if (dataList.length > 0 && "embedding" in dataList[0]) {
          return dataList[0]["embedding"];
        } else {
          throw new Error(
            `No embedding found in response data: ${JSON.stringify(dataList)}`,
          );
        }
      } else {
        throw new Error(
          `Unexpected output format. Expected 'output.data[0].embedding', got: ${JSON.stringify(
            output,
          )}`,
        );
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      throw new Error(
        `Failed to generate embedding for document: ${errorMessage}`,
      );
    }
  }
}

type StoredConfig = {
  api_key_env_var: string;
  endpoint_id: string;
  model_name: string;
  timeout?: number;
};

export class RunPodEmbeddingFunction implements IEmbeddingFunction {
  name = "runpod";

  private api_key: string;
  private endpoint_id: string;
  private model: string;
  private timeout: number;
  private runpodApi?: RunPodAPI;

  private api_key_env_var: string;

  constructor({
    runpod_api_key,
    runpod_endpoint_id,
    runpod_model_name,
    runpod_timeout = 300,
    runpod_api_key_env_var = "RUNPOD_API_KEY",
  }: {
    runpod_api_key?: string;
    runpod_endpoint_id: string;
    runpod_model_name: string;
    runpod_timeout?: number;
    runpod_api_key_env_var?: string;
  }) {
    this.api_key_env_var = runpod_api_key_env_var;
    const apiKey = runpod_api_key ?? process.env[runpod_api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `RunPod API key is required. Please provide it in the constructor or set the environment variable ${runpod_api_key_env_var}.`,
      );
    }
    this.api_key = apiKey;

    if (!runpod_endpoint_id || !runpod_endpoint_id.trim()) {
      throw new Error("RunPod endpoint ID is required and cannot be empty.");
    }
    this.endpoint_id = runpod_endpoint_id;

    if (!runpod_model_name || !runpod_model_name.trim()) {
      throw new Error("RunPod model name is required and cannot be empty.");
    }
    this.model = runpod_model_name;

    this.timeout = runpod_timeout;
  }

  private async loadClient() {
    // cache the client
    if (this.runpodApi) return;

    try {
      const { runpodSdk } = await RunPodEmbeddingFunction.import();

      // Validate SDK installation
      if (!runpodSdk || typeof runpodSdk !== "function") {
        throw new Error("Invalid runpod-sdk installation detected");
      }

      this.runpodApi = new RunPodAPIImpl(
        this.api_key,
        this.endpoint_id,
        runpodSdk,
      );
    } catch (error: any) {
      if (error.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the runpod-sdk package to use the RunPodEmbeddingFunction, e.g. `npm install runpod-sdk`",
        );
      }
      throw error; // Re-throw other errors
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.loadClient();

    if (!texts || texts.length === 0) {
      return [];
    }

    return await this.runpodApi!.createEmbedding({
      model: this.model,
      input: texts,
      timeout: this.timeout,
    }).catch((error: any) => {
      throw error;
    });
  }

  /** @ignore */
  static async import(): Promise<{
    runpodSdk: any;
  }> {
    try {
      // @ts-ignore
      const runpodModule = await import("runpod-sdk");
      const runpodSdk = runpodModule.default || runpodModule;
      return { runpodSdk };
    } catch (e: any) {
      if (
        e.code === "ERR_MODULE_NOT_FOUND" ||
        e.message?.includes("Cannot resolve module")
      ) {
        throw new Error(
          "Please install the runpod-sdk package to use the RunPodEmbeddingFunction, e.g. `npm install runpod-sdk`",
        );
      }
      // For other errors, re-throw with more details
      throw new Error(`Failed to import runpod-sdk: ${e.message || String(e)}`);
    }
  }

  buildFromConfig(config: StoredConfig): RunPodEmbeddingFunction {
    return new RunPodEmbeddingFunction({
      runpod_api_key_env_var: config.api_key_env_var,
      runpod_endpoint_id: config.endpoint_id,
      runpod_model_name: config.model_name,
      runpod_timeout: config.timeout,
    });
  }

  getConfig(): StoredConfig {
    return {
      api_key_env_var: this.api_key_env_var,
      endpoint_id: this.endpoint_id,
      model_name: this.model,
      timeout: this.timeout,
    };
  }

  validateConfigUpdate(oldConfig: StoredConfig, newConfig: StoredConfig): void {
    if (oldConfig.model_name !== newConfig.model_name) {
      throw new Error("Cannot change model name.");
    }
    if (oldConfig.endpoint_id !== newConfig.endpoint_id) {
      throw new Error("Cannot change endpoint ID.");
    }
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "runpod");
  }

  defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }
}

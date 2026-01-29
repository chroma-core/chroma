import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";

const NAME = "runpod";

export interface RunPodConfig {
  api_key_env_var: string;
  endpoint_id: string;
  model_name: string;
  timeout?: number;
}

export interface RunPodArgs {
  endpointId: string;
  modelName: string;
  apiKeyEnvVar?: string;
  apiKey?: string;
  timeout?: number;
}

interface RunPodAPI {
  createEmbedding(payload: { model: string; input: string }): Promise<number[]>;
}

class RunPodAPIImpl implements RunPodAPI {
  private apiKey: string;
  private endpointId: string;
  private endpoint: any;
  private runpod: any;
  private timeout: number;

  constructor(
    apiKey: string,
    endpointId: string,
    runpodSdk: any,
    timeout: number = 300,
  ) {
    this.apiKey = apiKey;
    this.endpointId = endpointId;
    this.timeout = timeout;

    // Initialize the RunPod SDK
    this.runpod = runpodSdk(this.apiKey);
    this.endpoint = this.runpod.endpoint(this.endpointId);
  }

  async createEmbedding(payload: {
    model: string;
    input: string;
  }): Promise<number[]> {
    try {
      // Prepare the input payload for RunPod
      const input_payload = {
        input: {
          model: payload.model,
          input: payload.input,
        },
      };

      // Run the endpoint asynchronously
      const response = await this.endpoint.run(input_payload);
      const runId = response.id;

      // Poll for completion
      let status = response.status;
      let output: any = null;
      const startTime = Date.now();

      while (status === "IN_QUEUE" || status === "IN_PROGRESS") {
        if (Date.now() - startTime > this.timeout * 1000) {
          throw new Error("Request timed out");
        }

        await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1 second
        const statusResponse = await this.endpoint.status(runId);
        status = statusResponse.status;
        output = statusResponse.output;
      }

      // Handle different status cases
      if (
        status === "FAILED" ||
        status === "CANCELLED" ||
        status === "TIMED_OUT"
      ) {
        throw new Error(
          `RunPod endpoint failed with status '${status}': ${JSON.stringify(
            output || response,
          )}`,
        );
      }

      if (status !== "COMPLETED") {
        throw new Error(`Unexpected status from RunPod endpoint: ${status}`);
      }

      // Extract embedding from response
      if (output && "data" in output) {
        const data_list = output["data"];
        if (
          Array.isArray(data_list) &&
          data_list.length > 0 &&
          "embedding" in data_list[0]
        ) {
          return data_list[0]["embedding"];
        } else {
          throw new Error(
            `No embedding found in response data: ${JSON.stringify(data_list)}`,
          );
        }
      } else {
        throw new Error(
          `Unexpected output format. Expected 'output.data[0].embedding', got: ${JSON.stringify(output)}`,
        );
      }
    } catch (error: any) {
      if (
        error.message.includes("RunPod endpoint failed") ||
        error.message.includes("Request timed out") ||
        error.message.includes("Unexpected") ||
        error.message.includes("No embedding found")
      ) {
        throw error;
      }
      throw new Error(
        `RunPod endpoint failed with status '${
          error.status || "unknown"
        }': ${error.message || JSON.stringify(error)}`,
      );
    }
  }
}

export class RunPodEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;
  private readonly apiKeyEnvVar: string;
  private readonly apiKey: string;
  private readonly endpointId: string;
  private readonly modelName: string;
  private readonly timeout: number;
  private runpodApi?: RunPodAPI;
  private initPromise?: Promise<void>;

  constructor(args: RunPodArgs) {
    const {
      endpointId,
      modelName,
      apiKeyEnvVar = "RUNPOD_API_KEY",
      timeout = 300,
    } = args;

    const apiKey = args.apiKey ?? process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `RunPod API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    if (!endpointId || !endpointId.trim()) {
      throw new Error("RunPod endpoint ID is required and cannot be empty.");
    }

    if (!modelName || !modelName.trim()) {
      throw new Error("RunPod model name is required and cannot be empty.");
    }

    this.apiKey = apiKey;
    this.endpointId = endpointId;
    this.modelName = modelName;
    this.timeout = timeout;
    this.apiKeyEnvVar = apiKeyEnvVar;
  }

  private async initializeRunPodAPI(): Promise<void> {
    if (this.runpodApi) return;

    // Prevent concurrent initialization (race condition fix)
    if (this.initPromise) {
      return this.initPromise;
    }

    this.initPromise = this.doInitialize();
    try {
      await this.initPromise;
    } finally {
      this.initPromise = undefined;
    }
  }

  private async doInitialize(): Promise<void> {
    try {
      const { runpodSdk } = await RunPodEmbeddingFunction.import();

      // Validate SDK installation
      if (!runpodSdk || typeof runpodSdk !== "function") {
        throw new Error("Invalid runpod-sdk installation detected");
      }

      this.runpodApi = new RunPodAPIImpl(
        this.apiKey,
        this.endpointId,
        runpodSdk,
        this.timeout,
      );
    } catch (e: any) {
      if (e.message?.includes("Invalid runpod-sdk")) {
        throw e;
      }
      throw new Error(
        "Please install the runpod-sdk package to use the RunPodEmbeddingFunction, e.g. `npm install runpod-sdk`",
      );
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.initializeRunPodAPI();

    if (!texts || texts.length === 0) {
      return [];
    }

    // Process all texts in parallel for better performance
    const embeddings = await Promise.all(
      texts.map((text) =>
        this.runpodApi!.createEmbedding({
          model: this.modelName,
          input: text,
        })
      )
    );

    return embeddings;
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  static async import(): Promise<{
    runpodSdk: any;
  }> {
    try {
      const runpodModule = await import("runpod-sdk");
      const runpodSdk = runpodModule.default || runpodModule;
      return { runpodSdk };
    } catch (e: any) {
      throw new Error(
        `Failed to import runpod-sdk: ${e.message || String(e)}. Please install the runpod-sdk package to use the RunPodEmbeddingFunction, e.g. 'npm install runpod-sdk'`,
      );
    }
  }

  public static buildFromConfig(config: RunPodConfig): RunPodEmbeddingFunction {
    return new RunPodEmbeddingFunction({
      endpointId: config.endpoint_id,
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      timeout: config.timeout,
    });
  }

  public getConfig(): RunPodConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      endpoint_id: this.endpointId,
      model_name: this.modelName,
      timeout: this.timeout,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    const currentConfig = this.getConfig();

    if (
      Object.prototype.hasOwnProperty.call(newConfig, "model_name") &&
      currentConfig.model_name !== newConfig.model_name
    ) {
      throw new ChromaValueError("Model name cannot be updated");
    }

    if (
      Object.prototype.hasOwnProperty.call(newConfig, "endpoint_id") &&
      currentConfig.endpoint_id !== newConfig.endpoint_id
    ) {
      throw new ChromaValueError("Endpoint ID cannot be updated");
    }
  }

  public static validateConfig(config: RunPodConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, RunPodEmbeddingFunction);

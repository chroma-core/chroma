import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import {
  pipeline,
  FeatureExtractionPipeline,
  ProgressCallback,
} from "@huggingface/transformers";
import { env as TransformersEnv } from "@huggingface/transformers";

export type DType =
  "auto" | "fp32" | "fp16" | "q8" | "int8" | "uint8" | "q4" | "bnb4" | "q4f16";

export type Quantization = DType | Record<string, DType>;

export type EmbeddingFunctionSpace = "cosine" | "l2" | "ip";

export interface DefaultEmbeddingFunctionConfig {
  model_name?: string;
  revision?: string;
  dtype?: Quantization;
  quantized?: boolean;
  wasm?: boolean;
}

export interface DefaultEmbeddingFunctionArgs {
  modelName?: string;
  revision?: string;
  dtype?: Quantization;
  /** @deprecated Use 'dtype' instead. If set to true, dtype value will be 'uint8' */
  quantized?: boolean;
  wasm?: boolean;
}

// Loading a pipeline reads the model from disk and creates a new ONNX
// session, which is far more expensive than running it. Share one pipeline
// per model configuration across instances and calls. The cache is bounded
// and evicts the least recently used configuration, so a long-lived process
// using many configurations does not retain every loaded model.
export const MAX_CACHED_PIPELINES = 4;
const pipelineCache = new Map<string, Promise<FeatureExtractionPipeline>>();

export class DefaultEmbeddingFunction {
  public readonly name: string = "default";
  private readonly modelName: string;
  private readonly revision: string;
  private readonly dtype: Quantization | undefined;
  private readonly quantized: boolean;
  private readonly progressCallback: ProgressCallback | undefined = undefined;
  private readonly wasm: boolean;

  constructor(
    args: Partial<
      DefaultEmbeddingFunctionArgs & {
        progressCallback: ProgressCallback | undefined;
      }
    > = {},
  ) {
    const {
      modelName = "Xenova/all-MiniLM-L6-v2",
      revision = "main",
      dtype = undefined,
      progressCallback = undefined,
      quantized = false,
      wasm = false,
    } = args;

    this.modelName = modelName;
    this.revision = revision;
    this.dtype = dtype || (quantized ? "uint8" : "fp32");
    this.quantized = quantized;
    this.progressCallback = progressCallback;
    this.wasm = wasm;
    if (this.wasm) {
      TransformersEnv.backends.onnx.backend = "wasm";
    }
  }

  public static buildFromConfig(
    config: DefaultEmbeddingFunctionConfig,
  ): DefaultEmbeddingFunction {
    return new DefaultEmbeddingFunction({
      modelName: config.model_name,
      revision: config.revision,
      dtype: config.dtype,
      quantized: config.quantized,
      wasm: config.wasm,
    });
  }

  private getPipeline(): Promise<FeatureExtractionPipeline> {
    const key = JSON.stringify([
      this.modelName,
      this.revision,
      this.dtype,
      this.wasm,
    ]);
    const cached = pipelineCache.get(key);
    if (cached) {
      pipelineCache.delete(key);
      pipelineCache.set(key, cached);
      return cached;
    }

    const pipelinePromise: Promise<FeatureExtractionPipeline> = pipeline(
      "feature-extraction",
      this.modelName,
      {
        revision: this.revision,
        progress_callback: this.progressCallback,
        dtype: this.dtype,
      },
    ).catch((error) => {
      if (pipelineCache.get(key) === pipelinePromise) {
        pipelineCache.delete(key);
      }
      throw error;
    });
    pipelineCache.set(key, pipelinePromise);
    while (pipelineCache.size > MAX_CACHED_PIPELINES) {
      pipelineCache.delete(pipelineCache.keys().next().value as string);
    }
    return pipelinePromise;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const pipe = await this.getPipeline();

    const output = await pipe(texts, { pooling: "mean", normalize: true });
    return output.tolist();
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public getConfig(): DefaultEmbeddingFunctionConfig {
    return {
      model_name: this.modelName,
      revision: this.revision,
      dtype: this.dtype,
      quantized: this.quantized,
    };
  }

  public validateConfigUpdate(newConfig: DefaultEmbeddingFunctionConfig): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new Error(
        "The DefaultEmbeddingFunction's 'model' cannot be changed after initialization.",
      );
    }
  }

  public static validateConfig(config: DefaultEmbeddingFunctionConfig): void {
    validateConfigSchema(config, "transformers");
  }
}

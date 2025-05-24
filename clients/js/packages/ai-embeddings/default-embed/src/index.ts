import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import { pipeline, ProgressCallback } from "@huggingface/transformers";
import { env as TransformersEnv } from "@huggingface/transformers";

export type DType =
  | "auto"
  | "fp32"
  | "fp16"
  | "q8"
  | "int8"
  | "uint8"
  | "q4"
  | "bnb4"
  | "q4f16";

export type Quantization = DType | Record<string, DType>;

interface StoredConfig {
  model_name?: string;
  revision?: string;
  dtype?: Quantization;
  quantized?: boolean;
  wasm?: boolean;
}

export interface DefaultEmbeddingFunctionConfig {
  modelName?: string;
  revision?: string;
  dtype?: Quantization;
  /** @deprecated Use 'dtype' instead. If set to true, dtype value will be 'uint8' */
  quantized?: boolean;
  wasm?: boolean;
}

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
      DefaultEmbeddingFunctionConfig & {
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
    this.dtype = dtype || (quantized ? "uint8" : undefined);
    this.quantized = quantized;
    this.progressCallback = progressCallback;
    this.wasm = wasm;
    if (this.wasm) {
      TransformersEnv.backends.onnx.backend = "wasm";
    }
  }

  public static buildFromConfig(
    config: StoredConfig,
  ): DefaultEmbeddingFunction {
    return new DefaultEmbeddingFunction(config);
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const pipe = await pipeline("feature-extraction", this.modelName, {
      revision: this.revision,
      progress_callback: this.progressCallback,
      dtype: this.dtype,
    });

    const output = await pipe(texts, { pooling: "mean", normalize: true });
    return output.tolist();
  }

  public getConfig(): StoredConfig {
    return {
      model_name: this.modelName,
      revision: this.revision,
      dtype: this.dtype,
      quantized: this.quantized,
    };
  }

  public static validateConfigUpdate(
    oldConfig: StoredConfig,
    newConfig: StoredConfig,
  ): void {
    if (oldConfig.model_name !== newConfig.model_name) {
      throw new Error(
        "The DefaultEmbeddingFunction's 'model' cannot be changed after initialization.",
      );
    }
  }

  public static validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "transformers");
  }
}

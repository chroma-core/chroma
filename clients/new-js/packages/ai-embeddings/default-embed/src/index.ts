import { pipeline, ProgressCallback } from "@huggingface/transformers";

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
  model?: string;
  revision?: string;
  dtype?: Quantization;
}

export class DefaultEmbeddingFunction {
  public readonly name: string = "default";
  private readonly model: string;
  private readonly revision: string;
  private readonly dtype: Quantization | undefined;
  private readonly progressCallback: ProgressCallback | undefined = undefined;

  constructor(
    args: Partial<
      StoredConfig & { progressCallback: ProgressCallback | undefined }
    > = {},
  ) {
    const {
      model = "Xenova/all-MiniLM-L6-v2",
      revision = "main",
      dtype = undefined,
      progressCallback = undefined,
    } = args;

    this.model = model;
    this.revision = revision;
    this.dtype = dtype;
    this.progressCallback = progressCallback;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const pipe = await pipeline("feature-extraction", this.model, {
      revision: this.revision,
      progress_callback: this.progressCallback,
      dtype: this.dtype,
    });

    const output = await pipe(texts, { pooling: "mean", normalize: true });
    return output.tolist();
  }

  public getConfig(): Record<string, any> {
    return {
      model: this.model,
      revision: this.revision,
      dtype: this.dtype,
    };
  }

  public buildFromConfig(config: StoredConfig): DefaultEmbeddingFunction {
    return new DefaultEmbeddingFunction(config);
  }

  static buildFromConfig(config: StoredConfig): DefaultEmbeddingFunction {
    return new DefaultEmbeddingFunction(config);
  }
}

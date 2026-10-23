import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import {
  pipeline,
  FeatureExtractionPipeline,
  ProgressCallback,
} from "@huggingface/transformers";
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
// per model configuration across instances and calls. The cache keeps at
// most MAX_CACHED_PIPELINES loaded pipelines and evicts the least recently
// used one, so a long-lived process using many configurations does not
// retain every loaded model. Loads still in flight are never evicted, so
// concurrent callers for one configuration always share a single load.
export const MAX_CACHED_PIPELINES = 4;

interface CachedPipeline {
  promise: Promise<FeatureExtractionPipeline>;
  // Whether the load has succeeded. Only loaded pipelines count toward
  // MAX_CACHED_PIPELINES and can be evicted.
  loaded: boolean;
  // Progress callbacks of every caller waiting on the load, so instances
  // that join an in-flight load still receive its progress events. Unset
  // once the load settles, as no further events will be emitted.
  listeners: Set<ProgressCallback> | undefined;
}
const pipelineCache = new Map<string, CachedPipeline>();

function evictLeastRecentlyUsed(): void {
  let loaded = 0;
  for (const entry of pipelineCache.values()) {
    if (entry.loaded) loaded++;
  }
  // Map iteration follows insertion order, and cache hits re-insert their
  // entry, so the first loaded entries are the least recently used.
  for (const [key, entry] of pipelineCache) {
    if (loaded <= MAX_CACHED_PIPELINES) break;
    if (entry.loaded) {
      pipelineCache.delete(key);
      loaded--;
    }
  }
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
      if (this.progressCallback) {
        cached.listeners?.add(this.progressCallback);
      }
      return cached.promise;
    }

    const listeners = new Set<ProgressCallback>();
    if (this.progressCallback) {
      listeners.add(this.progressCallback);
    }
    const entry: CachedPipeline = {
      loaded: false,
      listeners,
      promise: pipeline("feature-extraction", this.modelName, {
        revision: this.revision,
        dtype: this.dtype,
        progress_callback: (info) => {
          for (const listener of listeners) {
            // The load is shared, so one caller's failing callback must not
            // fail it for the others or stop them receiving events.
            try {
              listener(info);
            } catch {}
          }
        },
      })
        .then((pipe) => {
          entry.loaded = true;
          evictLeastRecentlyUsed();
          return pipe;
        })
        .catch((error) => {
          if (pipelineCache.get(key) === entry) {
            pipelineCache.delete(key);
          }
          throw error;
        })
        .finally(() => {
          listeners.clear();
          entry.listeners = undefined;
        }),
    };
    pipelineCache.set(key, entry);
    return entry.promise;
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

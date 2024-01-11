import { IEmbeddingFunction } from "./IEmbeddingFunction";

// Dynamically import module
let TransformersApi: Promise<any>;

export class DefaultEmbeddingFunction implements IEmbeddingFunction {
  private pipelinePromise?: Promise<any> | null;
  private transformersApi: any;
  private model: string;
  private revision: string;
  private quantized: boolean;
  private progress_callback: Function | null;

  /**
   * DefaultEmbeddingFunction constructor.
   * @param options The configuration options.
   * @param options.model The model to use to calculate embeddings. Defaults to 'Xenova/all-MiniLM-L6-v2', which is an ONNX port of `sentence-transformers/all-MiniLM-L6-v2`.
   * @param options.revision The specific model version to use (can be a branch, tag name, or commit id). Defaults to 'main'.
   * @param options.quantized Whether to load the 8-bit quantized version of the model. Defaults to `false`.
   * @param options.progress_callback If specified, this function will be called during model construction, to provide the user with progress updates.
   */
  constructor({
    model = "Xenova/all-MiniLM-L6-v2",
    revision = "main",
    quantized = false,
    progress_callback = null,
  }: {
    model?: string;
    revision?: string;
    quantized?: boolean;
    progress_callback?: Function | null;
  } = {}) {
    this.model = model;
    this.revision = revision;
    this.quantized = quantized;
    this.progress_callback = progress_callback;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.loadClient();

     // Store a promise that resolves to the pipeline
    this.pipelinePromise = new Promise(async (resolve, reject) => {
      try {
        const pipeline = this.transformersApi

        const quantized = this.quantized
        const revision = this.revision
        const progress_callback = this.progress_callback

        resolve(
          await pipeline("feature-extraction", this.model, {
            quantized,
            revision,
            progress_callback,
          })
        );
      } catch (e) {
        reject(e);
      }
    });

    let pipe = await this.pipelinePromise;
    let output = await pipe(texts, { pooling: "mean", normalize: true });
    return output.tolist();
  }

  private async loadClient() {
      if(this.transformersApi) return;
      try {
          // eslint-disable-next-line global-require,import/no-extraneous-dependencies
          let { pipeline } = await DefaultEmbeddingFunction.import();
          TransformersApi = pipeline;
      } catch (_a) {
          // @ts-ignore
          if (_a.code === 'MODULE_NOT_FOUND') {
              throw new Error("Please install the chromadb-default-embed package to use the DefaultEmbeddingFunction, `npm install -S chromadb-default-embed`");
          }
          throw _a; // Re-throw other errors
      }
      this.transformersApi = TransformersApi;
  }

  /** @ignore */
  static async import(): Promise<{
      // @ts-ignore
      pipeline: typeof import("chromadb-default-embed");
  }> {
      try {
          // @ts-ignore
          const { pipeline } = await import("chromadb-default-embed");
          return { pipeline };
      } catch (e) {
          throw new Error(
              "Please install chromadb-default-embed as a dependency with, e.g. `yarn add chromadb-default-embed`"
          );
      }
  }
}

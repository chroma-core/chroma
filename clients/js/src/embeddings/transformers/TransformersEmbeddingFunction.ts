import { BaseEmbeddingFunction } from "../IEmbeddingFunction";

type TransformersEmbeddingFunctionOptions = {
  model?: string;
  revision?: string;
  quantized?: boolean;
  progress_callback?: Function | null;
};

/**
 * `TransformersEmbeddingFunction` class responsible for generating embeddings using transformer models.
 * 
 * @example
 * ```javascript
 * import { TransformersEmbeddingFunction } from "chromadb/transformers";
 * import { pipeline } from '@xenova/transformers';
 * 
 * // For a Node.js Environment, import module at build time
 * const transformers = new TransformersEmbeddingFunction({
 *   model: "Your-Model-Name",
 *   revision: "main",
 *   quantized: false,
 *   progress_callback: null,
 * }, { pipeline });
 * 
 * const embeddings = await transformers.generate(["text1", "text2"]);
 * ```
 * 
 * @example Let the embedding function load the transformers library on runtime using .init():
 * ```javascript
 * const transformers = new TransformersEmbeddingFunction({
 *   model: "Your-Model-Name",
 *   revision: "main",
 *   quantized: false,
 *   progress_callback: null,
 * });
 * await transformers.init();
 * 
 * const embeddings = await transformers.generate(["text1", "text2"]);
 * ```
 */
export class TransformersEmbeddingFunction extends BaseEmbeddingFunction<TransformersEmbeddingFunctionOptions, { TransformersApi: any, pipeline?: any }> {
  /**
   * @param options The configuration options.
   * @param options.model The model to use to calculate embeddings. Defaults to 'Xenova/all-MiniLM-L6-v2', which is an ONNX port of `sentence-transformers/all-MiniLM-L6-v2`.
   * @param options.revision The specific model version to use (can be a branch, tag name, or commit id). Defaults to 'main'.
   * @param options.quantized Whether to load the 8-bit quantized version of the model. Defaults to `false`.
   * @param options.progress_callback If specified, this function will be called during model construction, to provide the user with progress updates.
   * @param transformersApi Pass the transformers api here. If not provided it's required to run TransformersEmbeddingFunction#init before usage to import the module at runtime.
   *                        You may import { pipeline } from '@xenova/transformers' and pass the pipeline instead of the whole module if you want to make use of treeshaking.
   */
  constructor(options: TransformersEmbeddingFunctionOptions = {}, transformersApi: any = undefined) {
    super(options, { TransformersApi: transformersApi });

    if(transformersApi && !transformersApi.pipeline && this.modules){
      // We assume, that an initialized transformersApi.pipeline has been passed instead of the module.
      this.modules.pipeline = transformersApi.pipeline;
    }
  }

  public async init(): Promise<void> {
    // Define package name here because import('@xenova/transformers') throws typescript compilation errors because @xenova/transformers@2.5.3 exports broken type definitions.
    const packageName = '@xenova/transformers';
    try {
      this.modules = {
        TransformersApi: await import(packageName),
      }

      if (!this.options) {
        throw '[TransformersEmbeddingFunction] Initializing the TransformersApi pipline failed: this.options is undefined.';
      }
      
      this.modules.pipeline = await this.modules.TransformersApi.pipeline("feature-extraction", this.options.model, {
        quantized: this.options.quantized,
        revision: this.options.revision,
        progress_callback: this.options.progress_callback,
      })
    } catch (err) {
      console.warn('[TransformersEmbeddingFunction] Initializing the TransformersApi pipline failed. Please install the transformers package to use the TransformersEmbeddingFunction, `npm install -S @xenova/transformers`', err)
      throw err;
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    // Initialize if user fotgot to initialize
    if (!this.modules?.pipeline) {
      await this.init()
      console.warn('[TransformersEmbeddingFunction] You forgot to call TransformersEmbeddingFunction#init. Will call it now to be able to generate. It is recommended to pass the transformders module via constructor.')
    }

    if (!this.modules?.pipeline) {
      console.warn('[TransformersEmbeddingFunction] Something went wrong. The TransformersApi pipline is undefined.')
    }

    let output = await this.modules?.pipeline(texts, { pooling: "mean", normalize: true });
    return output.tolist();
  }
}

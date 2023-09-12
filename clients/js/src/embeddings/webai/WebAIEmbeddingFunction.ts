import { BaseEmbeddingFunction } from "../IEmbeddingFunction";

export type WebAIEmbeddingFunctionOptions = {
  modality: "text" | "image" | "multimodal";
  target: 'node' | 'browser';
  proxy?: boolean;
  wasmPath?: string;
  modelID?: string;
}

export type WebAIEmbeddingMethods = { webAI: any, webAIText?: any, webAIImage?: any, webAIMultimodal?: any }

/**
 * `WebAIEmbeddingFunction` uses the Web AI package to generate embeddings.
 * This embedding function can work in both NodeJS and browser environments.
 * 
 * @example
 * ```typescript
 * import { WebAIEmbeddingFunction } from "chromadb/webai";
 * 
 * const options = {
 *   modality: "text",
 *   target: "node",
 * };
 * 
 * const webAI = require('@visheratin/web-ai-node');
 * const webAIText = require('@visheratin/web-ai-node/text');
 * const webAIInstance = new WebAIEmbeddingFunction(options, { webAI, webAIText });
 * 
 * const embeddings = await webAIInstance.generate(["Hello World", "Another Text"]);
 * ```
 * @example Let the embedding function load the webai libraries on runtime using .init():
 * ```typescript
 * import { WebAIEmbeddingFunction } from "chromadb/webai";
 * 
 * const options = {
 *   modality: "text",
 *   target: "node",
 * };
 * 
 * const embeddingFunction = new WebAIEmbeddingFunction(options);
 * await embeddingFunction.init()
 * 
 * const embeddings = await embeddingFunction.generate(["Hello World", "Another Text"]);
 * ```
 * 
 * @remarks
 * Browser version of Web AI (@visheratin/web-ai) is an ESM module.
 * NodeJS version of Web AI (@visheratin/web-ai-node) is a CommonJS module.
 */
export class WebAIEmbeddingFunction extends BaseEmbeddingFunction<WebAIEmbeddingFunctionOptions, WebAIEmbeddingMethods> {
  private model: any | undefined = undefined;

  /**
   * WebAIEmbeddingFunction constructor.
   * @param modality - the modality of the embedding function, either "text", "image", or "multimodal".
   * @param node - whether the embedding function is being used in a NodeJS environment.
   * @param proxy - whether to use web worker to avoid blocking the main thread. Works only in browser.
   * @param wasmPath - the path/URL to the directory with ONNX runtime WebAssembly files.
   * @param modelID - the ID of the model to use, if not specified, the default model will be used.
   */
  constructor(options: WebAIEmbeddingFunctionOptions, { webAI, webAIText, webAIImage, webAIMultimodal }: WebAIEmbeddingMethods) {
    super(options, { webAI, webAIText, webAIImage, webAIMultimodal })

    if ((webAIText || webAIImage || webAIMultimodal) && !webAI) {
      throw new Error("[WebAIEmbeddingFunction] You have to pass the webAI too when you want to use webAIText, webAIImage, or webAIMultimodal!")
    }

    if (!this.options) {
      throw new Error("[WebAIEmbeddingFunction] You initialized the embedding function without passing options.")
    }

    switch (options.modality) {
      case 'text':
        if (!this.modules?.webAIText) {
          console.warn("[WebAIEmbeddingFunction] You initialized the embedding function with modality 'text' but did not pass webAIText.")
        }
        break;
      case 'image':
        if (!this.modules?.webAIImage) {
          console.warn("[WebAIEmbeddingFunction] You initialized the embedding function with modality 'image' but did not pass webAIImage.")
        }
        break;
      case 'multimodal':
        if (!this.modules?.webAIMultimodal) {
          console.warn("[WebAIEmbeddingFunction] You initialized the embedding function with modality 'multimodal' but did not pass webAIMultimodal.")
        }
        break;
      default:
        if (!this.modules?.webAI) {
          console.warn("[WebAIEmbeddingFunction] You initialized the embedding function without passing a valid value in options.modality.")
        }
    }

    if (!this.options?.target) {
      throw new Error("[WebAIEmbeddingFunction] You initialized the embedding function without passing options.target.")
    }

  }

  public async init(): Promise<void> {
    if (!this.options) {
      throw new Error("[WebAIEmbeddingFunction] You have to pass options to the WebAIEmbeddingFunction constructor!")
    }

    if(!this.modules?.webAI){
      this.modules = {
        webAI: await import(this.options?.target === 'node' ? '@visheratin/web-ai-node' : '@visheratin/web-ai')
      }
    }

    switch (this.options?.modality) {
      case "text": {
        this.modules.webAIText = await import(this.getPackageName());

        if (!this.modules?.webAIText) {
          throw new Error(`[WebAIEmbeddingFunction] Could not find webAIText. Please pass it using the constructor or install the package via npm i -S ${this.getPackageName()}.`)
        }

        let id = "mini-lm-v2-quant"; //default text model
        if (this.options.modelID) {
          id = this.options.modelID;
        }

        const models = this.modules.webAIText.ListTextModels();
        for (const modelMetadata of models) {
          if (modelMetadata.id === id) {
            this.model = new this.modules.webAIText.FeatureExtractionModel(modelMetadata);
            return;
          }
        }
        throw new Error(
          `[WebAIEmbeddingFunction] Could not find text model with id ${this.options.modelID} in the Web AI package`
        );
      }
      case "image": {
        this.modules.webAIImage = await import(this.getPackageName());

        if (!this.modules?.webAIImage) {
          throw new Error(`[WebAIEmbeddingFunction] Could not find webAIImage. Please pass it using the constructor or install the package via npm i -S ${this.getPackageName()}.`)
        }

        let id = "efficientformer-l1-feature-quant"; //default image model

        if (this.options.modelID) {
          id = this.options.modelID;
        }

        const imageModels = this.modules.webAIImage.ListImageModels();

        for (const modelMetadata of imageModels) {
          if (modelMetadata.id === id) {
            this.model = new this.modules.webAIImage.FeatureExtractionModel(modelMetadata);
            return;
          }
        }

        throw new Error(
          `[WebAIEmbeddingFunction] Could not find image model with id ${this.options.modelID} in the Web AI package`
        );
      }
      case "multimodal": {
        this.modules.webAIMultimodal = await import(this.getPackageName());

        if (!this.modules?.webAIMultimodal) {
          throw new Error(`[WebAIEmbeddingFunction] Could not find webAIMultimodal. Please pass it using the constructor or install the package via npm i -S ${this.getPackageName()}.`)
        }

        let id = "clip-base-quant"; //default multimodal model

        if (this.options.modelID) {
          id = this.options.modelID;
        }

        const multimodalModels = this.modules.webAIMultimodal.ListMultimodalModels();

        for (const modelMetadata of multimodalModels) {
          if (modelMetadata.id === id) {
            this.model = new this.modules.webAIMultimodal.ZeroShotClassificationModel(
              modelMetadata
            );
            return;
          }
        }

        throw new Error(
          `[WebAIEmbeddingFunction] Could not find multimodal model with id ${this.options.modelID} in the Web AI package`
        );
      }
    }
  }

  /**
   * Generates embeddings for the given values.
   * @param values - the values to generate embeddings for. For text models, this is an array of strings.
   *  For image models, this is an array of URLs to images. URLs can be data URLs.
   * @returns the embeddings.
   */
  public async generate(values: string[]): Promise<number[][]> {
    if (!this.model) {
      throw new Error("[WebAIEmbeddingFunction] You have to initialize the WebAIEmbeddingFunction before using init!")
    }

    if (!this.model.initialized) {
      await this.model.init(this.options?.proxy);
    }
    let embeddings = [];
    if (this.options?.modality === "text" || this.options?.modality === "image") {
      const output = await this.model.process(values);
      embeddings = output.result;
    } else {
      const urlValues: string[] = [];
      const textValues: string[] = [];
      for (const value of values) {
        try {
          new URL(value);
          urlValues.push(value);
        } catch {
          textValues.push(value);
        }
      }
      const urlOutput = await this.model.embedImages(urlValues);
      const textOutput = await this.model.embedTexts(textValues);
      embeddings = urlOutput.concat(textOutput);
    }
    if (embeddings.length > 0 && Array.isArray(embeddings[0])) {
      return embeddings;
    } else {
      return [embeddings];
    }
  }

  private getPackageName(): string {
    switch (this.options?.modality) {
      case 'text':
        return this.options?.target === 'node' ? '@visheratin/web-ai-node/text' : '@visheratin/web-ai/text';
      case 'image':
        return this.options?.target === 'node' ? '@visheratin/web-ai-node/image' : '@visheratin/web-ai/image';
      case 'multimodal':
        return this.options?.target === 'node' ? '@visheratin/web-ai-node/multimodal' : '@visheratin/web-ai/multimodal';
      default:
        throw new Error('[WebAIEmbeddingFunction] options.target or options.modality is undefined.')
    }
  }

}

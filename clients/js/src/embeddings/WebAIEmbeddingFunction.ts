import { IEmbeddingFunction } from "./IEmbeddingFunction";
let webAI: any;

export class WebAIEmbeddingFunction implements IEmbeddingFunction {
  private model;
  private proxy?: boolean;

  /**
   * WebAIEmbeddingFunction constructor.
   * @param modality - the modality of the embedding function, either "text" or "image".
   * @param node - whether the embedding function is being used in a NodeJS environment.
   * @param proxy - whether to use web worker to avoid blocking the main thread. Works only in browser.
   * @param wasmPath - the path/URL to the directory with ONNX runtime WebAssembly files. Has to be specified when running in NodeJS.
   * @param modelID - the ID of the model to use, if not specified, the default model will be used.
   */
  constructor(
    modality: "text" | "image",
    node: boolean,
    proxy?: boolean,
    wasmPath?: string,
    modelID?: string
  ) {
    if (node) {
      this.proxy = proxy ? proxy : false;
      try {
        webAI = require("@visheratin/web-ai-node");
      } catch (e) {
        console.log(e);
        throw new Error(
          "Please install the @visheratin/web-ai-node package to use the WebAIEmbeddingFunction, `npm install -S @visheratin/web-ai-node`"
        );
      }
    } else {
      this.proxy = proxy ? proxy : true;
      try {
        webAI = require("@visheratin/web-ai");
      } catch (e) {
        console.log(e);
        throw new Error(
          "Please install the @visheratin/web-ai package to use the WebAIEmbeddingFunction, `npm install -S @visheratin/web-ai`"
        );
      }
    }
    if (wasmPath) {
      webAI.SessionParams.wasmRoot = wasmPath;
    }
    switch (modality) {
      case "text": {
        let id = "mini-lm-v2-quant"; //default text model
        if (modelID) {
          id = modelID;
        }
        const models = webAI.ListTextModels();
        for (const modelMetadata of models) {
          if (modelMetadata.id === id) {
            this.model = new webAI.TextFeatureExtractionModel(modelMetadata);
            return;
          }
        }
        throw new Error(
          `Could not find text model with id ${modelID} in the WebAI package`
        );
      }
      case "image": {
        let id = "efficientformer-l1-feature-quant"; //default image model
        if (modelID) {
          id = modelID;
        }
        const imageModels = webAI.ListImageModels();
        for (const modelMetadata of imageModels) {
          if (modelMetadata.id === id) {
            this.model = new webAI.ImageFeatureExtractionModel(modelMetadata);
            return;
          }
        }
        throw new Error(
          `Could not find image model with id ${modelID} in the WebAI package`
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
    if (!this.model.initialized) {
      await this.model.init(this.proxy);
    }
    const output = await this.model.process(values);
    const embeddings = output.result;
    if (embeddings.length > 0 && Array.isArray(embeddings[0])) {
      return embeddings;
    } else {
      return [embeddings];
    }
  }
}

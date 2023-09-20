import { importOptionalModule } from "../utils";
import { IEmbeddingFunction } from "./IEmbeddingFunction";

/**
 * WebAIEmbeddingFunction is a function that uses the Web AI package to generate embeddings.
 * @remarks
 * This embedding function can be used in both NodeJS and browser environments.
 * Browser version of Web AI (@visheratin/web-ai) is an ESM module.
 * NodeJS version of Web AI (@visheratin/web-ai-node) is a CommonJS module.
 */
export class WebAIEmbeddingFunction implements IEmbeddingFunction {
  private model: any;
  private proxy?: boolean;
  private initPromise: Promise<any> | null;
  private modality: "text" | "image" | "multimodal";

  /**
   * WebAIEmbeddingFunction constructor.
   * @param modality - the modality of the embedding function, either "text", "image", or "multimodal".
   * @param node - whether the embedding function is being used in a NodeJS environment.
   * @param proxy - whether to use web worker to avoid blocking the main thread. Works only in browser.
   * @param wasmPath - the path/URL to the directory with ONNX runtime WebAssembly files.
   * @param modelID - the ID of the model to use, if not specified, the default model will be used.
   */
  constructor(
    modality: "text" | "image" | "multimodal",
    node: boolean,
    proxy?: boolean,
    wasmPath?: string,
    modelID?: string
  ) {
    this.initPromise = null;
    this.model = null;
    this.modality = modality;
    if (node) {
      this.initNode(modality, proxy, modelID);
    } else {
      this.initPromise = this.initBrowser(modality, proxy, wasmPath, modelID);
    }
  }

  /**
   * Generates embeddings for the given values.
   * @param values - the values to generate embeddings for. For text models, this is an array of strings.
   *  For image models, this is an array of URLs to images. URLs can be data URLs.
   * @returns the embeddings.
   */
  public async generate(values: string[]): Promise<number[][]> {
    if (this.initPromise) {
      await this.initPromise;
    }
    if (!this.model.initialized) {
      await this.model.init(this.proxy);
    }
    let embeddings = [];
    if (this.modality === "text" || this.modality === "image") {
      const output = await this.model.process(values);
      embeddings = output.result;
    } else {
      const urlValues = [];
      const textValues = [];
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

  private initNode(
    modality: "text" | "image" | "multimodal",
    proxy?: boolean,
    modelID?: string
  ): void {
    this.proxy = proxy ? proxy : false;
    try {
      const webAI = require("@visheratin/web-ai-node");
      webAI.SessionParams.executionProviders = ["cpu"];
      switch (modality) {
        case "text": {
          const webAIText = require("@visheratin/web-ai-node/text");
          let id = "mini-lm-v2-quant"; //default text model
          if (modelID) {
            id = modelID;
          }
          const models = webAIText.ListTextModels();
          for (const modelMetadata of models) {
            if (modelMetadata.id === id) {
              this.model = new webAIText.FeatureExtractionModel(modelMetadata);
              return;
            }
          }
          throw new Error(
            `Could not find text model with id ${modelID} in the Web AI package`
          );
        }
        case "image": {
          const webAIImage = require("@visheratin/web-ai-node/image");
          let id = "efficientformer-l1-feature-quant"; //default image model
          if (modelID) {
            id = modelID;
          }
          const imageModels = webAIImage.ListImageModels();
          for (const modelMetadata of imageModels) {
            if (modelMetadata.id === id) {
              this.model = new webAIImage.FeatureExtractionModel(modelMetadata);
              return;
            }
          }
          throw new Error(
            `Could not find image model with id ${modelID} in the Web AI package`
          );
        }
        case "multimodal": {
          const webAIMultimodal = require("@visheratin/web-ai-node/multimodal");
          let id = "clip-base-quant"; //default multimodal model
          if (modelID) {
            id = modelID;
          }
          const multimodalModels = webAIMultimodal.ListMultimodalModels();
          for (const modelMetadata of multimodalModels) {
            if (modelMetadata.id === id) {
              this.model = new webAIMultimodal.ZeroShotClassificationModel(
                modelMetadata
              );
              return;
            }
          }
          throw new Error(
            `Could not find multimodal model with id ${modelID} in the Web AI package`
          );
        }
      }
    } catch (e) {
      console.error(e);
      throw new Error(
        "Please install the @visheratin/web-ai-node package to use the WebAIEmbeddingFunction, `npm install -S @visheratin/web-ai-node`"
      );
    }
  }

  private async initBrowser(
    modality: "text" | "image" | "multimodal",
    proxy?: boolean,
    modelID?: string,
    wasmPath?: string
  ) {
    this.proxy = proxy ? proxy : true;
    try {
      const webAI = await importOptionalModule("@visheratin/web-ai");
      if (wasmPath) {
        webAI.SessionParams.wasmRoot = wasmPath;
      }
      switch (modality) {
        case "text": {
          const webAIText = await importOptionalModule(
            "@visheratin/web-ai/text"
          );
          let id = "mini-lm-v2-quant"; //default text model
          if (modelID) {
            id = modelID;
          }
          const models = webAIText.ListTextModels();
          for (const modelMetadata of models) {
            if (modelMetadata.id === id) {
              this.model = new webAIText.FeatureExtractionModel(modelMetadata);
              return;
            }
          }
          throw new Error(
            `Could not find text model with id ${modelID} in the Web AI package`
          );
        }
        case "image": {
          const webAIImage = await importOptionalModule(
            "@visheratin/web-ai/image"
          );
          let id = "efficientformer-l1-feature-quant"; //default image model
          if (modelID) {
            id = modelID;
          }
          const imageModels = webAIImage.ListImageModels();
          for (const modelMetadata of imageModels) {
            if (modelMetadata.id === id) {
              this.model = new webAIImage.FeatureExtractionModel(modelMetadata);
              return;
            }
          }
          throw new Error(
            `Could not find image model with id ${modelID} in the Web AI package`
          );
        }
        case "multimodal": {
          const webAIImage = await importOptionalModule(
            "@visheratin/web-ai/multimodal"
          );
          let id = "clip-base-quant"; //default multimodal model
          if (modelID) {
            id = modelID;
          }
          const imageModels = webAIImage.ListMultimodalModels();
          for (const modelMetadata of imageModels) {
            if (modelMetadata.id === id) {
              this.model = new webAIImage.ZeroShotClassificationModel(
                modelMetadata
              );
              return;
            }
          }
          throw new Error(
            `Could not find multimodal model with id ${modelID} in the Web AI package`
          );
        }
      }
    } catch (e) {
      throw new Error(
        "Please install the @visheratin/web-ai package to use the WebAIEmbeddingFunction, `npm install -S @visheratin/web-ai`"
      );
    }
  }
}

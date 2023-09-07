import { BaseEmbeddingFunction } from "../IEmbeddingFunction";
export type CohereEmbeddingFunctionOptions = { cohere_api_key: string, model?: string };

/**
 * `CohereEmbeddingFunction` class responsible for generating embeddings using the Cohere API.
 * 
 * @example
 * ```javascript
 * import { CohereEmbeddingFunction } from "chromadb/cohere";
 * 
 * const options = {
 *   cohere_api_key: "YOUR_COHERE_API_KEY",
 *   model: "Your-Model-Name"
 * };
 * 
 * // If you have cohere-ai module available, pass it during construction
 * const cohere = require('cohere-ai');
 * const transformers = new CohereEmbeddingFunction(options, cohere);
 * ```
* @example Let the embedding function load the cohere library on runtime using .init():
 * ```javascript
 * const transformersDynamic = new CohereEmbeddingFunction(options);
 * await transformersDynamic.init();
 * 
 * const embeddings = await transformersDynamic.generate(["text1", "text2"]);
 * ```
 */
export class CohereEmbeddingFunction extends BaseEmbeddingFunction<CohereEmbeddingFunctionOptions, { cohere: any }> {

    /**
    * @param options - Configuration options for generating embeddings using Cohere.
    * @param options.cohere_api_key - Your Cohere API key.
    * @param options.model - The Cohere model to use for generating embeddings. If not provided, a default will be used.
    * @param cohere - The `cohere-ai` module. If not provided, it's required to run `CohereEmbeddingFunction#init` before using, to import the module at runtime.
    */
    constructor(options: CohereEmbeddingFunctionOptions, cohere: any = undefined) {
        super(options, cohere)
    }

    public async init(): Promise<void> {
        try {
            this.modules = {
                // @ts-ignore
                cohere: await import('cohere-ai')
            }

            if (!this.options?.cohere_api_key) {
                throw '[CohereEmbeddingFunction] Cannot find cohere_api_key'
            }

            this.modules.cohere.init(this.options.cohere_api_key);
        } catch {
            throw new Error(
                "[CohereEmbeddingFunction] Failed to import the cohere-ai module. Please pass it via constructor."
            );
        }
    }

    public async generate(texts: string[]) {
        if (!this.modules?.cohere) {
            await this.init()
            console.warn('[CohereEmbeddingFunction] You forgot to call CohereEmbeddingFunction#init. Will call it now to be able to generate. It is recommended to pass the cohere module via constructor.')
        }

        if (!this.options?.model) {
            throw new Error(
                "[CohereEmbeddingFunction] options.model is undefined."
            );
        }

        const response = await this.modules?.cohere.embed({
            texts: texts,
            model: this.options.model,
        });
        return response.body.embeddings;
    }
}

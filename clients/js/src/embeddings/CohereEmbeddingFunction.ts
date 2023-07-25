import { IEmbeddingFunction } from "./IEmbeddingFunction";

let CohereAiApi: any;

export class CohereEmbeddingFunction implements IEmbeddingFunction {
    private api_key: string;
    private model: string;

    constructor({ cohere_api_key, model }: { cohere_api_key: string, model?: string }) {
        try {
            // eslint-disable-next-line global-require,import/no-extraneous-dependencies
            CohereAiApi = require("cohere-ai");
            CohereAiApi.init(cohere_api_key);
        } catch {
            throw new Error(
                "Please install the cohere-ai package to use the CohereEmbeddingFunction, `npm install -S cohere-ai`"
            );
        }
        this.api_key = cohere_api_key;
        this.model = model || "large";
    }

    public async generate(texts: string[]) {
        const response = await CohereAiApi.embed({
            texts: texts,
            model: this.model,
        });
        return response.body.embeddings;
    }
}

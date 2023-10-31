import { IEmbeddingFunction } from "./IEmbeddingFunction";

let CohereAiApi: any;

export class CohereEmbeddingFunction implements IEmbeddingFunction {
    private api_key: string;
    private model: string;
    private cohereAiApi?: any;

    constructor({ cohere_api_key, model }: { cohere_api_key: string, model?: string }) {
        // we used to construct the client here, but we need to async import the types
        // for the openai npm package, and the constructor can not be async
        this.api_key = cohere_api_key;
        this.model = model || "large";
    }

    private async loadClient() {
        if(this.cohereAiApi) return;
        try {
            // eslint-disable-next-line global-require,import/no-extraneous-dependencies
            const { cohere } = await CohereEmbeddingFunction.import();
            CohereAiApi = cohere;
            CohereAiApi.init(this.api_key);
        } catch (_a) {
            // @ts-ignore
            if (_a.code === 'MODULE_NOT_FOUND') {
                throw new Error("Please install the cohere-ai package to use the CohereEmbeddingFunction, `npm install -S cohere-ai`");
            }
            throw _a; // Re-throw other errors
        }
        this.cohereAiApi = CohereAiApi;
    }

    public async generate(texts: string[]) {

        await this.loadClient();

        const response = await this.cohereAiApi.embed({
            texts: texts,
            model: this.model,
        });
        return response.body.embeddings;
    }

    /** @ignore */
    static async import(): Promise<{
        // @ts-ignore
        cohere: typeof import("cohere-ai");
    }> {
        try {
            // @ts-ignore
            const { default: cohere } = await import("cohere-ai");
            return { cohere };
        } catch (e) {
            throw new Error(
                "Please install cohere-ai as a dependency with, e.g. `yarn add cohere-ai`"
            );
        }
    }

}

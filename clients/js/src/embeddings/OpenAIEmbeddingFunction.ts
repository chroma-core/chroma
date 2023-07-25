import { IEmbeddingFunction } from "./IEmbeddingFunction";
let OpenAIApi: any;

export class OpenAIEmbeddingFunction implements IEmbeddingFunction {
    private api_key: string;
    private org_id: string;
    private model: string;

    constructor({
        openai_api_key,
        openai_model,
        openai_organization_id,
    }: {
        openai_api_key: string,
        openai_model?: string,
        openai_organization_id?: string
    }) {
        try {
            // eslint-disable-next-line global-require,import/no-extraneous-dependencies
            OpenAIApi = require("openai");
        } catch {
            throw new Error(
                "Please install the openai package to use the OpenAIEmbeddingFunction, `npm install -S openai`"
            );
        }
        this.api_key = openai_api_key;
        this.org_id = openai_organization_id || "";
        this.model = openai_model || "text-embedding-ada-002";
    }

    public async generate(texts: string[]): Promise<number[][]> {
        const configuration = new OpenAIApi.Configuration({
            organization: this.org_id,
            apiKey: this.api_key,
        });
        const openai = new OpenAIApi.OpenAIApi(configuration);
        const embeddings = [];
        const response = await openai.createEmbedding({
            model: this.model,
            input: texts,
        });
        const data = response.data["data"];
        for (let i = 0; i < data.length; i += 1) {
            embeddings.push(data[i]["embedding"]);
        }
        return embeddings;
    }
}

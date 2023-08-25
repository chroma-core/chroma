import {IEmbeddingFunction} from "./IEmbeddingFunction";

let OpenAIApi: any;
let openAiVersion = null;
let openAiMajorVersion = null;

// interface EmbeddingResponse {
//     object: string;
//     data: { embedding: number[] }[];
//     model: string;
//     usage: { prompt_tokens: number; total_tokens: number };
// }

interface OpenAIAPI {
    createEmbedding: (params: {
        model: string;
        input: string[];
        user?: string;
    }) => Promise<object>;
}

class OpenAIAPIv3 implements OpenAIAPI {
    private readonly configuration: any;
    private openai: any;

    constructor(configuration: { organization: string, apiKey: string }) {
        this.configuration = new OpenAIApi.Configuration({
            organization: configuration.organization,
            apiKey: configuration.apiKey,
        });
        this.openai = new OpenAIApi.OpenAIApi(this.configuration);
    }

    public async createEmbedding(params: {
        model: string,
        input: string[],
        user?: string
    }): Promise<object> {
        const response = await this.openai.createEmbedding({
            model: params.model,
            input: params.input,
        });
        return response.data;
    }
}

class OpenAIAPIv4 implements OpenAIAPI {
    private readonly apiKey: any;
    private openai: any;

    constructor(apiKey: any) {
        this.apiKey = apiKey;
        this.openai = new OpenAIApi({
            apiKey: this.apiKey,
        });
    }

    public async createEmbedding(params: {
        model: string,
        input: string[],
        user?: string
    }): Promise<object> {
        return await this.openai.embeddings.create(params);
    }
}

export class OpenAIEmbeddingFunction implements IEmbeddingFunction {
    private api_key: string;
    private org_id: string;
    private model: string;
    private openaiApi: OpenAIAPI;

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
            const fs = require('fs');
            const packageJson = JSON.parse(fs.readFileSync('package.json', 'utf8'));
            const version = packageJson.dependencies.openai || packageJson.devDependencies.openai;
            openAiVersion = version.replace(/[^0-9.]/g, '');
            openAiMajorVersion = openAiVersion.split('.')[0];
        } catch (_a) {
            // @ts-ignore
            if (_a.code === 'MODULE_NOT_FOUND') {
                throw new Error("Please install the openai package to use the OpenAIEmbeddingFunction, `npm install -S openai@3`");
            }
            throw _a; // Re-throw other errors
        }
        this.api_key = openai_api_key;
        this.org_id = openai_organization_id || "";
        this.model = openai_model || "text-embedding-ada-002";
        if (openAiMajorVersion > 3) {
            this.openaiApi = new OpenAIAPIv4(this.api_key);
        } else {
            this.openaiApi = new OpenAIAPIv3({
                organization: this.org_id,
                apiKey: this.api_key,
            });
        }
    }

    public async generate(texts: string[]): Promise<number[][]> {
        const embeddings: number[][] = [];
        const response = await this.openaiApi.createEmbedding({
            model: this.model,
            input: texts,
        }).catch((error: any) => {
            console.log(error);
            throw error;
        });
        // @ts-ignore
        const data = response["data"];
        for (let i = 0; i < data.length; i += 1) {
            embeddings.push(data[i]["embedding"]);
        }
        return embeddings;
    }
}

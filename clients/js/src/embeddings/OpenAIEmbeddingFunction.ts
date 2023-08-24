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
        let openAiVersion = null;
        let openAiMajorVersion = null;
        try {
            // eslint-disable-next-line global-require,import/no-extraneous-dependencies
            OpenAIApi = require("openai");
            const fs = require('fs');
            const packageJson = JSON.parse(fs.readFileSync('package.json', 'utf8'));
            const version = packageJson.dependencies.openai || packageJson.devDependencies.openai;
            openAiVersion = version.replace(/[^0-9.]/g, '');
            openAiMajorVersion = openAiVersion.split('.')[0];
        }
        catch (_a) {
            if (_a.code === 'MODULE_NOT_FOUND') {
                throw new Error("Please install the openai package to use the OpenAIEmbeddingFunction, `npm install -S openai@3`");
            }
            throw _a; // Re-throw other errors
        }
        if (openAiMajorVersion > 3){
            throw new Error(`Your version of openai package [${openAiVersion}] is not supported. Run \`npm install -S openai@3\``);
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

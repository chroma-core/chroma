import { BaseEmbeddingFunction } from "../IEmbeddingFunction";

interface OpenAIAPI {
    createEmbedding: (params: {
        model: string;
        input: string[];
        user?: string;
    }) => Promise<number[][]>;
}

class OpenAIAPIv3 implements OpenAIAPI {
    private readonly configuration: any;
    private openai: any;

    constructor(configuration: { organization: string, apiKey: string }, OpenAIApi: any) {
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
    }): Promise<number[][]> {
        const embeddings: number[][] = [];
        const response = await this.openai.createEmbedding({
            model: params.model,
            input: params.input,
        }).catch((error: any) => {
            throw error;
        });
        // @ts-ignore
        const data = response.data["data"];
        for (let i = 0; i < data.length; i += 1) {
            embeddings.push(data[i]["embedding"]);
        }
        return embeddings
    }
}

class OpenAIAPIv4 implements OpenAIAPI {
    private readonly apiKey: any;
    private openai: any;

    /**
     * Creates an instance of the OpenAIEmbeddingFunction.
     * 
     * @class
     * @param {OpenAIEmbeddingFunctionOptions} [options] - Configuration options for the embedding function.
     * @param {string} options.openai_api_key - The API key for accessing OpenAI services.
     * @param {string} [options.openai_model='text-embedding-ada-002'] - The specific model to use for embeddings. Defaults to 'text-embedding-ada-002'.
     * @param {string} [options.openai_organization_id] - Optional OpenAI organization ID.
     * @param {'node' | 'browser'} [options.target] - Target environment where this will be used. Can be either 'node' for server-side or 'browser' for client-side.
     * @param {any} [OpenAIApi] - An optional instance of the OpenAIApi if you already have one initialized.
     */
    constructor(apiKey: any, organization: string | undefined, target: 'node' | 'browser' | undefined, OpenAIApi: any) {
        this.apiKey = apiKey;
        this.openai = new OpenAIApi({
            apiKey: this.apiKey,
            dangerouslyAllowBrowser: target === 'browser',
            organization
        });
    }

    public async createEmbedding(params: {
        model: string,
        input: string[],
        user?: string
    }): Promise<number[][]> {
        const embeddings: number[][] = [];
        const response = await this.openai.embeddings.create(params);
        const data = response["data"];
        for (let i = 0; i < data.length; i += 1) {
            embeddings.push(data[i]["embedding"]);
        }
        return embeddings
    }
}

export type OpenAIEmbeddingFunctionOptions = {
    openai_api_key: string,
    openai_model?: string,
    openai_organization_id?: string
    target?: 'node' | 'browser'
}

/**
 * The OpenAIEmbeddingFunction class provides an interface to obtain embeddings from OpenAI models.
 * This class transforms textual data into numerical representations using the OpenAI API.
 * 
 * @example Using a pre-initialized OpenAI instance:
 * ```typescript
 * import { OpenAIEmbeddingFunction } from 'chromadb/openai';
 * 
 * const openai = new OpenAIApi({
 *   apiKey: 'YOUR_OPENAI_API_KEY',
 *   dangerouslyAllowBrowser: true, // if using in browser
 *   organization: 'YOUR_OPENAI_ORGANIZATION_ID'
 *  // add any settings here
 * });
 * 
 * const embeddingFunction = new OpenAIEmbeddingFunction(undefined, openai);
 * const texts = ["Hello World!", "How are you?"];
 * embeddingFunction.generate(texts).then(embeddings => {
 *     console.log(embeddings);
 * });
 * ```
 *
 * @example Using the init method:
 * ```typescript
 * import { OpenAIEmbeddingFunction } from 'chromadb/openai';
 * 
 * const options = {
 *   openai_api_key: 'YOUR_OPENAI_API_KEY',
 *   openai_model: 'text-embedding-ada-002', // Optional
 *   target: 'node', // or 'browser'
 *   openai_organization_id: 'YOUR_OPENAI_ORGANIZATION_ID', // Optional
 * };
 * 
 * const embeddingFunction = new OpenAIEmbeddingFunction(options);
 * embeddingFunction.init('node').then(() => {
 *   const texts = ["Hello World!", "How are you?"];
 *   embeddingFunction.generate(texts).then(embeddings => {
 *     console.log(embeddings);
 *   });
 * });
 * ```
 */
export class OpenAIEmbeddingFunction extends BaseEmbeddingFunction<OpenAIEmbeddingFunctionOptions, { openai: any }>{
    private openAiMajorVersion: number | undefined;
    private openAiVersion: string | undefined;

    constructor(options?: OpenAIEmbeddingFunctionOptions, OpenAIApi: any = undefined) {
        // options are optional if an instance is passed, since openai would already be initialized.
        super(OpenAIApi ? options : undefined, { openai: OpenAIApi });
    }

    public async init(target?: 'node' | 'browser'): Promise<void> {
        if (!this.options) {
            throw new Error('[OpenAIEmbeddingFunction] You need to initialize the embeddings function with options before you can call OpenAIEmbeddingFunction#init.')
        }

        if (!target && !this.options.target) {
            throw new Error('[OpenAIEmbeddingFunction] You need to initialize the embeddings function with options.target or pass a target to OpenAIEmbeddingFunction#init.')
        }

        this.options.target = target;
        if (this.modules?.openai?.generate) {
            // An instance of openai has been passed, no need to initialize it
            return;
        }

        let OpenAIModule;

        try {
            if (!this.modules?.openai) {
                OpenAIModule = await import("openai")
            }

            let version: string | null = null;
            try {
                const { VERSION } = await import('openai/version');
                version = VERSION;
            } catch (e) {
                // openai/version is only defined in openai@4 so this is allowed to fail.
            }

            if (!version) {
                version = "3.x";
            }

            this.openAiVersion = version.replace(/[^0-9.]/g, '');
            this.openAiMajorVersion = parseInt(this.openAiVersion.split('.')[0]);
        } catch (_a) {
            // @ts-ignore
            if (_a.code === 'MODULE_NOT_FOUND') {
                throw new Error("[OpenAIEmbeddingFunction] Initializing the OpenAI Client failed. Please provide the initialized OpenAI instance through the constructor, or install the package with `npm install --save openai`");
            }
            throw _a; // Re-throw other errors
        }

        if (!this.options?.openai_api_key) {
            throw "[OpenAIEmbeddingFunction] options.openai_api_key is undefined."
        }

        if (this.openAiMajorVersion > 3) {
            this.modules = {
                openai: new OpenAIAPIv4(this.options.openai_api_key, this.options.openai_organization_id || undefined, target, OpenAIModule)
            }
        } else {
            this.modules = {
                openai: new OpenAIAPIv3({
                    organization: this.options.openai_organization_id || "",
                    apiKey: this.options.openai_api_key,
                }, OpenAIModule)
            }
        }
    }

    public async generate(texts: string[]): Promise<number[][]> {
        // Initialize if user fotgot to initialize
        if (!this.modules?.openai?.generate) {
            await this.init(this.options?.target || 'node')
            console.warn('[OpenAIEmbeddingFunction] You forgot to call OpenAIEmbeddingFunction#init. Will call it now to beable to generate. It is recommended to pass the initialized OpenAI instance through the constructor.')
        }

        if (!this.modules?.openai) {
            throw new Error(
                "[OpenAIEmbeddingFunction] Something went wrong. The OpenAI module is undefined."
            );
        }

        return await this.modules?.openai.createEmbedding({
            model: this.options?.openai_model || "text-embedding-ada-002",
            input: texts,
        }).catch((error: any) => {
            throw error;
        });
    }
}

import { EmbeddingFunction } from '@chroma-core/common';
import { OpenAI } from 'openai';

export interface MorphEmbeddingFunctionConfig {
    api_key?: string;
    model_name?: string;
    api_base?: string;
    encoding_format?: 'float' | 'base64';
    api_key_env_var?: string;
}

export class MorphEmbeddingFunction implements EmbeddingFunction {
    private client: OpenAI;
    private model_name: string;
    private encoding_format: 'float' | 'base64';

    constructor(config: MorphEmbeddingFunctionConfig = {}) {
        const {
            api_key,
            model_name = 'morph-embedding-v2',
            api_base = 'https://api.morphllm.com/v1',
            encoding_format = 'float',
            api_key_env_var = 'CHROMA_MORPH_API_KEY'
        } = config;

        // Get API key from config or environment
        const apiKey = api_key || process.env[api_key_env_var];
        if (!apiKey) {
            throw new Error(`API key not found. Please set ${api_key_env_var} environment variable or provide api_key in config.`);
        }

        this.client = new OpenAI({
            apiKey,
            baseURL: api_base,
        });

        this.model_name = model_name;
        this.encoding_format = encoding_format;
    }

    public async generate(texts: string[]): Promise<number[][]> {
        if (!texts || texts.length === 0) {
            return [];
        }

        try {
            const response = await this.client.embeddings.create({
                model: this.model_name,
                input: texts,
                encoding_format: this.encoding_format,
            });

            return response.data.map(item => item.embedding);
        } catch (error) {
            throw new Error(`Morph embedding generation failed: ${error}`);
        }
    }
}

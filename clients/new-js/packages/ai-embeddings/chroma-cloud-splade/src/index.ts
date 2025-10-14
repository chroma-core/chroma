import {
    ChromaValueError,
    type SparseEmbeddingFunction,
    type SparseVector,
    registerSparseEmbeddingFunction,
} from "chromadb";
import {
    snakeCase,
    validateConfigSchema,
} from "@chroma-core/ai-embeddings-common";

const NAME = "chroma-cloud-splade";

export interface ChromaCloudSpladeConfig {
    model: ChromaCloudSpladeEmbeddingModel;
    api_key_env_var: string;
}

export enum ChromaCloudSpladeEmbeddingModel {
    SPLADE_PP_EN_V1 = "prithivida/Splade_PP_en_v1",
}

export interface ChromaCloudSpladeArgs {
    model?: ChromaCloudSpladeEmbeddingModel;
    apiKeyEnvVar?: string;
}

interface ChromaCloudSparseEmbeddingRequest {
    texts: string[];
    task: string;
    target: string;
}

export interface ChromaCloudSparseEmbeddingsResponse {
    embeddings: SparseVector[];
}

/**
 * Sort sparse vectors by indices in ascending order.
 * This ensures consistency with the Python implementation.
 * @param embeddings - Array of sparse vectors to sort
 */
function sortSparseVectors(embeddings: SparseVector[]): void {
    for (const embedding of embeddings) {
        // Create an array of [index, value] pairs
        const pairs = embedding.indices.map((idx: number, i: number) => ({
            index: idx,
            value: embedding.values[i],
        }));

        // Sort by index
        pairs.sort(
            (a: { index: number; value: number }, b: { index: number; value: number }) =>
                a.index - b.index,
        );

        // Update the original arrays
        embedding.indices = pairs.map((p: { index: number; value: number }) => p.index);
        embedding.values = pairs.map((p: { index: number; value: number }) => p.value);
    }
}

export class ChromaCloudSpladeEmbeddingFunction
    implements SparseEmbeddingFunction {
    public readonly name = NAME;

    private readonly apiKeyEnvVar: string;
    private readonly model: ChromaCloudSpladeEmbeddingModel;
    private readonly url: string;
    private readonly headers: { [key: string]: string };

    constructor(args: ChromaCloudSpladeArgs = {}) {
        const {
            model = ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
            apiKeyEnvVar = "CHROMA_API_KEY",
        } = args;

        const apiKey = process.env[apiKeyEnvVar];

        if (!apiKey) {
            throw new Error(
                `Chroma Embedding API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
            );
        }

        this.model = model;
        this.apiKeyEnvVar = apiKeyEnvVar;

        this.url = "https://embed.trychroma.com/embed_sparse";
        this.headers = {
            "x-chroma-token": apiKey,
            "x-chroma-embedding-model": model,
            "Content-Type": "application/json",
        };
    }

    public async generate(texts: string[]): Promise<SparseVector[]> {
        if (texts.length === 0) {
            return [];
        }

        const body: ChromaCloudSparseEmbeddingRequest = {
            texts,
            task: "",
            target: "",
        };

        try {
            const response = await fetch(this.url, {
                method: "POST",
                headers: this.headers,
                body: JSON.stringify(snakeCase(body)),
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(
                    `HTTP ${response.status} ${response.statusText}: ${errorText}`,
                );
            }

            const data =
                (await response.json()) as ChromaCloudSparseEmbeddingsResponse;

            // Validate response structure
            if (!data || typeof data !== 'object') {
                throw new Error("Invalid response format: expected object");
            }

            if (!Array.isArray(data.embeddings)) {
                throw new Error("Invalid response format: missing or invalid embeddings array");
            }

            // Sort the sparse vectors to match Python behavior
            sortSparseVectors(data.embeddings);

            return data.embeddings;
        } catch (error) {
            if (error instanceof Error) {
                throw new Error(`Error calling Chroma Embedding API: ${error.message}`);
            } else {
                throw new Error(`Error calling Chroma Embedding API: ${error}`);
            }
        }
    }

    public async generateForQueries(texts: string[]): Promise<SparseVector[]> {
        return this.generate(texts);
    }

    public static buildFromConfig(
        config: ChromaCloudSpladeConfig,
    ): ChromaCloudSpladeEmbeddingFunction {
        return new ChromaCloudSpladeEmbeddingFunction({
            model: config.model,
            apiKeyEnvVar: config.api_key_env_var,
        });
    }

    public getConfig(): ChromaCloudSpladeConfig {
        return {
            model: this.model,
            api_key_env_var: this.apiKeyEnvVar,
        };
    }

    public validateConfigUpdate(newConfig: Record<string, any>): void {
        if ("model" in newConfig) {
            throw new ChromaValueError("Model cannot be updated");
        }
    }

    public static validateConfig(config: ChromaCloudSpladeConfig): void {
        validateConfigSchema(config, NAME);
    }
}

registerSparseEmbeddingFunction(NAME, ChromaCloudSpladeEmbeddingFunction);

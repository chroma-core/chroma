
export enum DistanceMetric {
    COSINE = "cosine",
    L2 = "l2",
    INNER_PRODUCT = "inner_product"
}

type DistanceMetrics = DistanceMetric[];
type Embeddings = Float32Array[];
type Embeddable = string | string[];

const supportedEmbeddingFunctions: Record<string, EmbeddingFunction<any>> = {};

interface EmbeddingFunction<D> {
    name(): string;
    call(input: D): Embeddings;
    defaultMetric(): DistanceMetric;
    supportedMetrics(): DistanceMetrics;
    buildFromConfig(config: Record<string, any>): EmbeddingFunction<D>;
    getConfig(): Record<string, any>;
    modifiableVariables(): string[];
    register(): void;
    validateConfig(config: Record<string, any>): void;
}

class CohereEmbeddingFunction implements EmbeddingFunction<Embeddable> {
    private _modelName: string | null;
    private _apiKeyEnvVar: string | null;

    constructor(modelName: string | null, apiKeyEnvVar: string | null) {
        this._modelName = modelName;
        this._apiKeyEnvVar = apiKeyEnvVar;
    }

    name(): string {
        return "cohere";
    }

    call(input: Embeddable): Embeddings {
        const inputs = typeof input === "string" ? [input] : input;
        return inputs.map(() =>
            new Float32Array(Array.from({ length: 1024 }, () => Math.random()))
        );
    }

    defaultMetric(): DistanceMetric {
        if (this._modelName === "large") {
            return DistanceMetric.COSINE;
        } else if (this._modelName === "small") {
            return DistanceMetric.L2;
        }
        throw new Error(`Unsupported model name: ${this._modelName}`);
    }

    supportedMetrics(): DistanceMetrics {
        if (this._modelName === "large") {
            return [DistanceMetric.COSINE, DistanceMetric.L2];
        } else if (this._modelName === "small") {
            return [DistanceMetric.COSINE, DistanceMetric.INNER_PRODUCT];
        }
        return [];
    }

    maxTokenLimit(): number {
        if (this._modelName === "large") {
            return 2345;
        } else if (this._modelName === "small") {
            return 1234;
        }
        throw new Error(`Unsupported model name: ${this._modelName}`);
    }

    buildFromConfig(config: Record<string, any>): CohereEmbeddingFunction {
        if ("model_name" in config) {
            this._modelName = config.model_name;
        }
        if ("api_key_env_var" in config) {
            this._apiKeyEnvVar = config.api_key_env_var;
        }
        return this;
    }

    getConfig(): Record<string, any> {
        return {
            model_name: this._modelName,
            api_key_env_var: this._apiKeyEnvVar,
        };
    }

    modifiableVariables(): string[] {
        return ["api_key_env_var"];
    }

    register(): void {
        supportedEmbeddingFunctions[this.name()] = this;
    }

    validateConfig(_config: Record<string, any>): void { }
}

interface HNSWConfig {
    ef_search?: number;
    num_threads?: number;
    batch_size?: number;
    sync_threshold?: number;
    resize_factor?: number;
}

interface HNSWCreateConfig extends HNSWConfig {
    ef_construction?: number;
    max_neighbors?: number;
}

interface CreateCollectionConfig {
    hnsw?: HNSWCreateConfig;
    embedding_function: EmbeddingFunction<Embeddable>;
}

interface UpdateCollectionConfig {
    hnsw?: HNSWConfig;
    embedding_function: EmbeddingFunction<Embeddable>;
}

// Helper for frontend
function configToEf(config: Record<string, any>): EmbeddingFunction<any> {
    return supportedEmbeddingFunctions[config.name].buildFromConfig(config);
}

// Example usage
const cef = new CohereEmbeddingFunction("large", "COHERE_API_KEY");

// Can generate embeddings
cef.call(["Hello, world!"]);

function createCollection(name: string, config: CreateCollectionConfig): void { }

function updateCollection(name: string, config: UpdateCollectionConfig): void { }

createCollection(
    "my_collection",
    {
        hnsw: {
            max_neighbors: 100,
            ef_search: 100,
            num_threads: 10,
            batch_size: 10,
            sync_threshold: 0,
            resize_factor: 1.0,
        },
        embedding_function: cef,
    }
);

updateCollection(
    "my_collection",
    {
        hnsw: {
            ef_search: 100,
            num_threads: 10,
            batch_size: 10,
            sync_threshold: 0,
            resize_factor: 1.0,
        },
        embedding_function: cef,
    }
);

import {
    ChromaValueError,
    EmbeddingFunction,
    EmbeddingFunctionSpace,
    registerEmbeddingFunction,
} from "chromadb";
import {
    validateConfigSchema,
} from "@chroma-core/ai-embeddings-common";
import { pipeline } from "@huggingface/transformers";

const NAME = "sentence_transformer";

export interface SentenceTransformersConfig {
    model_name: string;
    device: string;
    normalize_embeddings: boolean;
    kwargs?: Record<string, any>;
}

export interface SentenceTransformersArgs {
    modelName?: string;
    device?: string;
    normalizeEmbeddings?: boolean;
    kwargs?: Record<string, any>;
}

export class SentenceTransformersEmbeddingFunction
    implements EmbeddingFunction {
    public readonly name = NAME;
    private readonly modelName: string;
    private readonly device: string;
    private readonly normalizeEmbeddings: boolean;
    private readonly kwargs: Record<string, any>;
    private pipelinePromise: Promise<any> | null = null;
    private pipeline: any = null;

    constructor(args: SentenceTransformersArgs = {}) {
        const {
            modelName = "all-MiniLM-L6-v2",
            device = "cpu",
            normalizeEmbeddings = false,
            kwargs = {},
        } = args;

        // Validate kwargs are JSON-serializable (no functions or symbols)
        for (const [key, value] of Object.entries(kwargs)) {
            if (typeof value === "function" || typeof value === "symbol") {
                throw new ChromaValueError(
                    `Keyword argument '${key}' has a value of type '${typeof value}', which is not supported. Only JSON-serializable values are allowed.`
                );
            }
        }

        this.modelName = modelName;
        this.device = device;
        this.normalizeEmbeddings = normalizeEmbeddings;
        this.kwargs = kwargs;
    }

    private async getPipeline(): Promise<any> {
        if (this.pipeline) {
            return this.pipeline;
        }

        if (!this.pipelinePromise) {
            // Resolve model name: if it doesn't contain a '/', prefix with 'Xenova/'
            // to form a full model identifier for transformers.js
            // This allows short names like "all-MiniLM-L6-v2" to work while maintaining
            // compatibility with Python client which uses short names
            let resolvedModelName = this.modelName;
            if (!resolvedModelName.includes("/")) {
                resolvedModelName = `Xenova/${resolvedModelName}`;
            }

            this.pipelinePromise = pipeline(
                "feature-extraction",
                resolvedModelName,
                {
                    device: this.device as any,
                    ...this.kwargs,
                } as any
            ).catch((error) => {
                // Reset pipelinePromise on error to allow retry on next call
                this.pipelinePromise = null;
                throw error;
            });
        }

        this.pipeline = await this.pipelinePromise;
        return this.pipeline;
    }

    public async generate(texts: string[]): Promise<number[][]> {
        if (!texts || texts.length === 0) {
            return [];
        }

        const pipe = await this.getPipeline();

        // Process all texts in batch
        const output = await pipe(texts, {
            pooling: "mean",
            normalize: this.normalizeEmbeddings,
        });

        // Convert tensor output to JavaScript array
        return output.tolist();
    }

    public async dispose(): Promise<void> {
        // To avoid race conditions, we capture the promise and then nullify the
        // instance properties to prevent new operations from starting.
        const promiseToDispose = this.pipelinePromise;
        this.pipeline = null;
        this.pipelinePromise = null;

        if (!promiseToDispose) return;

        try {
            // If pipeline is already initialized, this will resolve immediately.
            // Otherwise, it will wait for initialization to complete.
            const pipeline = await promiseToDispose;
            if (pipeline && typeof pipeline.dispose === "function") {
                await pipeline.dispose();
            }
        } catch {
            // If the pipeline promise fails, there's nothing to dispose.
            // This error will be handled by callers of generate(), so we can ignore it here.
        }
    }

    public defaultSpace(): EmbeddingFunctionSpace {
        return "cosine";
    }

    public supportedSpaces(): EmbeddingFunctionSpace[] {
        return ["cosine", "l2", "ip"];
    }

    public static buildFromConfig(
        config: SentenceTransformersConfig,
    ): SentenceTransformersEmbeddingFunction {
        const { model_name, device, normalize_embeddings, kwargs } = config;

        if (model_name === undefined || device === undefined || normalize_embeddings === undefined) {
            throw new ChromaValueError("model_name, device, and normalize_embeddings are required");
        }

        return new SentenceTransformersEmbeddingFunction({
            modelName: model_name,
            device,
            normalizeEmbeddings: normalize_embeddings,
            kwargs: kwargs || {},
        });
    }

    public getConfig(): SentenceTransformersConfig {
        return {
            model_name: this.modelName,
            device: this.device,
            normalize_embeddings: this.normalizeEmbeddings,
            kwargs: this.kwargs,
        };
    }

    public validateConfigUpdate(newConfig: Record<string, any>): void {
        // Model name is also used as the identifier for model path if stored locally.
        // Users should be able to change the path if needed, so we should not validate that.
        // e.g. moving file path from /v1/my-model.bin to /v2/my-model.bin
        return;
    }

    public static validateConfig(config: SentenceTransformersConfig): void {
        validateConfigSchema(config, "sentence-transformer");
    }
}

// Register with both the Python name (sentence_transformer) and the mapped JS name (sentence-transformer)
registerEmbeddingFunction(NAME, SentenceTransformersEmbeddingFunction);
registerEmbeddingFunction("sentence-transformer", SentenceTransformersEmbeddingFunction);

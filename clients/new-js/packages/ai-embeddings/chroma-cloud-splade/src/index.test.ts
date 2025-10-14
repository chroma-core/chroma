import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import {
    ChromaCloudSpladeEmbeddingFunction,
    ChromaCloudSpladeEmbeddingModel,
} from "./index";

describe("ChromaCloudSpladeEmbeddingFunction", () => {
    beforeEach(() => {
        jest.resetAllMocks();
    });

    const defaultParametersTest = "should initialize with default parameters";
    if (!process.env.CHROMA_API_KEY) {
        it.skip(defaultParametersTest, () => { });
    } else {
        it(defaultParametersTest, () => {
            const embedder = new ChromaCloudSpladeEmbeddingFunction({
                model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
            });
            expect(embedder.name).toBe("chroma-cloud-splade");

            const config = embedder.getConfig();
            expect(config.model).toBe("prithivida/Splade_PP_en_v1");
            expect(config.api_key_env_var).toBe("CHROMA_API_KEY");
        });
    }

    it("should initialize with custom error for a API key", () => {
        const originalEnv = process.env.CHROMA_API_KEY;
        delete process.env.CHROMA_API_KEY;

        try {
            expect(() => {
                new ChromaCloudSpladeEmbeddingFunction({
                    model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
                });
            }).toThrow("Chroma Embedding API key is required");
        } finally {
            if (originalEnv) {
                process.env.CHROMA_API_KEY = originalEnv;
            }
        }
    });

    it("should use custom API key environment variable", () => {
        process.env.CUSTOM_CHROMA_API_KEY = "test-api-key";

        try {
            const embedder = new ChromaCloudSpladeEmbeddingFunction({
                model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
                apiKeyEnvVar: "CUSTOM_CHROMA_API_KEY",
            });

            expect(embedder.getConfig().api_key_env_var).toBe(
                "CUSTOM_CHROMA_API_KEY",
            );
        } finally {
            delete process.env.CUSTOM_CHROMA_API_KEY;
        }
    });

    const buildFromConfigTest = "should build from config";
    if (!process.env.CHROMA_API_KEY) {
        it.skip(buildFromConfigTest, () => { });
    } else {
        it(buildFromConfigTest, () => {
            const config = {
                api_key_env_var: "CHROMA_API_KEY",
                model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
            };

            const embedder =
                ChromaCloudSpladeEmbeddingFunction.buildFromConfig(config);

            expect(embedder.getConfig()).toEqual(config);
        });

        const generateEmbeddingsTest = "should generate sparse embeddings";
        if (!process.env.CHROMA_API_KEY) {
            it.skip(generateEmbeddingsTest, () => { });
        } else {
            it(generateEmbeddingsTest, async () => {
                const embedder = new ChromaCloudSpladeEmbeddingFunction({
                    model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
                });
                const texts = ["Hello world", "Test text"];
                const embeddings = await embedder.generate(texts);

                expect(embeddings.length).toBe(texts.length);

                embeddings.forEach((embedding) => {
                    expect(embedding.indices).toBeDefined();
                    expect(embedding.values).toBeDefined();
                    expect(embedding.indices.length).toBe(embedding.values.length);
                    expect(embedding.indices.length).toBeGreaterThan(0);

                    // Check that indices are sorted
                    for (let i = 1; i < embedding.indices.length; i++) {
                        expect(embedding.indices[i]).toBeGreaterThan(
                            embedding.indices[i - 1],
                        );
                    }
                });

                // Verify embeddings are different
                expect(embeddings[0].indices).not.toEqual(embeddings[1].indices);
            });
        }

        const generateQueryEmbeddingsTest = "should generate query embeddings";
        if (!process.env.CHROMA_API_KEY) {
            it.skip(generateQueryEmbeddingsTest, () => { });
        } else {
            it(generateQueryEmbeddingsTest, async () => {
                const embedder = new ChromaCloudSpladeEmbeddingFunction({
                    model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
                });
                const texts = ["search query"];
                const embeddings = await embedder.generateForQueries!(texts);

                expect(embeddings).toBeDefined();
                expect(embeddings.length).toBe(1);
                expect(embeddings[0].indices).toBeDefined();
                expect(embeddings[0].values).toBeDefined();
            });
        }
    }

    const validateConfigUpdateTest = "should throw error when updating model";
    if (!process.env.CHROMA_API_KEY) {
        it.skip(validateConfigUpdateTest, () => { });
    } else {
        it(validateConfigUpdateTest, () => {
            const embedder = new ChromaCloudSpladeEmbeddingFunction({
                model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
            });

            expect(() => {
                embedder.validateConfigUpdate({ model: "new-model" });
            }).toThrow("Model cannot be updated");
        });
    }
});

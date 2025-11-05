import {
	CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS,
	ChromaCloudQwenEmbeddingFunction,
	ChromaCloudQwenEmbeddingModel,
} from "./index";
import { beforeEach, describe, expect, it, jest } from "@jest/globals";

describe("ChromaCloudQwenEmbeddingFunction", () => {
	beforeEach(() => {
		jest.resetAllMocks();
	});

	const defaultParametersTest = "should initialize with default parameters";
	if (!process.env.CHROMA_API_KEY) {
		it.skip(defaultParametersTest, () => { });
	} else {
		it(defaultParametersTest, () => {
			const embedder = new ChromaCloudQwenEmbeddingFunction({
				model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
			});
			expect(embedder.name).toBe("chroma-cloud-qwen");

			const config = embedder.getConfig();
			expect(config.model).toBe("Qwen/Qwen3-Embedding-0.6B");
			expect(config.task).toBe("nl_to_code");
			expect(config.api_key_env_var).toBe("CHROMA_API_KEY");
		});
	}

	it("should initialize with custom error for a API key", () => {
		const originalEnv = process.env.CHROMA_API_KEY;
		delete process.env.CHROMA_API_KEY;

		try {
			expect(() => {
				new ChromaCloudQwenEmbeddingFunction({
					model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
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
			const embedder = new ChromaCloudQwenEmbeddingFunction({
				model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
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
				model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
				instructions: CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS,
				task: "nl_to_code",
			};

			const embedder = ChromaCloudQwenEmbeddingFunction.buildFromConfig(config);

			expect(embedder.getConfig()).toEqual(config);
		});

		const generateEmbeddingsTest = "should generate embeddings";
		if (!process.env.CHROMA_API_KEY) {
			it.skip(generateEmbeddingsTest, () => { });
		} else {
			it(generateEmbeddingsTest, async () => {
				const embedder = new ChromaCloudQwenEmbeddingFunction({
					model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
				});
				const texts = ["Hello world", "Test text"];
				const embeddings = await embedder.generate(texts);

				expect(embeddings.length).toBe(texts.length);

				embeddings.forEach((embedding) => {
					expect(embedding.length).toBeGreaterThan(0);
				});

				expect(embeddings[0]).not.toEqual(embeddings[1]);
			});
		}
	}
});

import { ChromaEmbeddingFunction } from "./index";
import { beforeEach, describe, expect, it, jest } from "@jest/globals";

describe("ChromaEmbeddingFunction", () => {
	beforeEach(() => {
		jest.resetAllMocks();
	});

	const defaultParametersTest = "should initialize with default parameters";
	if (!process.env.CHROMA_API_KEY) {
		it.skip(defaultParametersTest, () => {});
	} else {
		it(defaultParametersTest, () => {
			const embedder = new ChromaEmbeddingFunction();
			expect(embedder.name).toBe("chroma-embed");

			const config = embedder.getConfig();
			expect(config.model_id).toBe("Qwen/Qwen3-Embedding-0.6B");
			expect(config.api_key_env_var).toBe("CHROMA_API_KEY");
			expect(config.task).toBe("code");
		});
	}

	const customParametersTest = "should initialize with custom parameters";
	if (!process.env.CHROMA_API_KEY) {
		it.skip(customParametersTest, () => {});
	} else {
		it(customParametersTest, () => {
			const embedder = new ChromaEmbeddingFunction({
				modelId: "BAAI/bge-m3",
				task: "code",
			});

			const config = embedder.getConfig();
			expect(config.model_id).toBe("BAAI/bge-m3");
			expect(config.task).toBe("code");
		});
	}

	it("should initialize with custom error for a API key", () => {
		const originalEnv = process.env.CHROMA_API_KEY;
		delete process.env.CHROMA_API_KEY;

		try {
			expect(() => {
				new ChromaEmbeddingFunction();
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
			const embedder = new ChromaEmbeddingFunction({
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
		it.skip(buildFromConfigTest, () => {});
	} else {
		it(buildFromConfigTest, () => {
			const config = {
				api_key_env_var: "CHROMA_API_KEY",
				model_id: "Qwen/Qwen3-Embedding-0.6B",
				task: "code",
			};

			const embedder = ChromaEmbeddingFunction.buildFromConfig(config);

			expect(embedder.getConfig()).toEqual(config);
		});

		const generateEmbeddingsTest = "should generate embeddings";
		if (!process.env.CHROMA_API_KEY) {
			it.skip(generateEmbeddingsTest, () => {});
		} else {
			it(generateEmbeddingsTest, async () => {
				const embedder = new ChromaEmbeddingFunction();
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

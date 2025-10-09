import {
	ChromaValueError,
	type EmbeddingFunction,
	type EmbeddingFunctionSpace,
	registerEmbeddingFunction,
} from "chromadb";
import {
	snakeCase,
	validateConfigSchema,
} from "@chroma-core/ai-embeddings-common";

const NAME = "chroma-embed";

export interface ChromaConfig {
	api_key_env_var: string;
	model_id: string;
	task: string;
}

export interface ChromaArgs {
	modelId?: string;
	task?: string;
	apiKey?: string;
	apiKeyEnvVar?: string;
}

interface ChromaRequestBody extends ChromaArgs {
	texts: string[];
	task: string;
	target: string;
}

export interface ChromaEmbeddingsResponse {
	embeddings: number[][];
	num_tokens: number;
}

export class ChromaEmbeddingFunction implements EmbeddingFunction {
	public readonly name = NAME;

	private readonly apiKeyEnvVar: string;
	private readonly modelId: string;
	private readonly url: string;
	private readonly headers: { [key: string]: string };
	private readonly task: string;

	constructor(args: Partial<ChromaArgs> = {}) {
		const {
			apiKeyEnvVar = "CHROMA_API_KEY",
			modelId = "Qwen/Qwen3-Embedding-0.6B",
			task = "code",
		} = args;

		const apiKey = args.apiKey || process.env[apiKeyEnvVar];

		if (!apiKey) {
			throw new Error(
				`Chroma Embedding API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
			);
		}

		this.modelId = modelId;
		this.apiKeyEnvVar = apiKeyEnvVar;
		this.task = task;

		this.url = "https://embed.trychroma.com";
		this.headers = {
			"x-chroma-token": apiKey,
			"x-chroma-embedding-model": modelId,
		};
	}

	public async generate(texts: string[]): Promise<number[][]> {
		const body: ChromaRequestBody = {
			texts,
			task: this.task,
			target: "documents",
		};

		try {
			const response = await fetch(this.url, {
				method: "POST",
				headers: this.headers,
				body: JSON.stringify(snakeCase(body)),
			});

			const data = (await response.json()) as ChromaEmbeddingsResponse;
			if (!data || !data.embeddings) {
				throw new Error("Failed to generate embeddings.");
			}
			return data.embeddings;
		} catch (error) {
			if (error instanceof Error) {
				throw new Error(`Error calling Chroma Embedding API: ${error.message}`);
			} else {
				throw new Error(`Error calling Chroma Embedding API: ${error}`);
			}
		}
	}

	public defaultSpace(): EmbeddingFunctionSpace {
		return "cosine";
	}

	public supportedSpaces(): EmbeddingFunctionSpace[] {
		return ["cosine", "l2", "ip"];
	}

	public static buildFromConfig(config: ChromaConfig): ChromaEmbeddingFunction {
		return new ChromaEmbeddingFunction({
			modelId: config.model_id,
			task: config.task,
			apiKeyEnvVar: config.api_key_env_var,
		});
	}

	public getConfig(): ChromaConfig {
		return {
			api_key_env_var: this.apiKeyEnvVar,
			model_id: this.modelId,
			task: this.task,
		};
	}

	public validateConfigUpdate(newConfig: Record<string, string>): void {
		if (this.getConfig().model_id !== newConfig.model_id) {
			throw new ChromaValueError("Model cannot be updated");
		}

		if (this.getConfig().task !== newConfig.task) {
			throw new ChromaValueError("Task cannot be updated");
		}
	}

	public static validateConfig(config: ChromaConfig): void {
		validateConfigSchema(config, NAME);
	}
}

registerEmbeddingFunction(NAME, ChromaEmbeddingFunction);

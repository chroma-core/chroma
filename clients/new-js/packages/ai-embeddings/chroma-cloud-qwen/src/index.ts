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

const NAME = "chroma-cloud-qwen";


export interface ChromaCloudQwenConfig {
	model: ChromaCloudQwenEmbeddingModel;
	task: string;
	instructions: ChromaCloudQwenEmbeddingInstructions;
	api_key_env_var: string;
}

export enum ChromaCloudQwenEmbeddingModel {
	QWEN3_EMBEDDING_0p6B = "Qwen/Qwen3-Embedding-0.6B",
}

export enum ChromaCloudQwenEmbeddingTarget {
	DOCUMENTS = "documents",
	QUERY = "query",
}

export type ChromaCloudQwenEmbeddingInstructions = Record<
	string,
	Record<ChromaCloudQwenEmbeddingTarget, string>
>;

export const CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS: ChromaCloudQwenEmbeddingInstructions =
{
	"nl_to_code": {
		[ChromaCloudQwenEmbeddingTarget.DOCUMENTS]: "",
		[ChromaCloudQwenEmbeddingTarget.QUERY]:
			// Taken from https://github.com/QwenLM/Qwen3-Embedding/blob/main/evaluation/task_prompts.json
			"Given a question about coding, retrieval code or passage that can solve user's question",
	},
};

export interface ChromaCloudQwenArgs {
	model: ChromaCloudQwenEmbeddingModel;
	task?: string;
	instructions?: ChromaCloudQwenEmbeddingInstructions;
	apiKeyEnvVar?: string;
}

interface ChromaCloudEmbeddingRequest {
	texts: string[];
	instructions: string;
}

export interface ChromaCloudEmbeddingsResponse {
	embeddings: number[][];
	num_tokens: number;
}

export class ChromaCloudQwenEmbeddingFunction implements EmbeddingFunction {
	public readonly name = NAME;

	private readonly apiKeyEnvVar: string;
	private readonly model: ChromaCloudQwenEmbeddingModel;
	private readonly url: string;
	private readonly headers: { [key: string]: string };
	private readonly task: string;
	private readonly instructions: ChromaCloudQwenEmbeddingInstructions;

	constructor(args: ChromaCloudQwenArgs) {
		const {
			model,
			task = "nl_to_code",
			instructions = CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS,
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
		this.task = task;
		this.instructions = instructions;

		this.url = "https://embed.trychroma.com";
		this.headers = {
			"x-chroma-token": apiKey,
			"x-chroma-embedding-model": model,
			"Content-Type": "application/json",
		};
	}

	public async generate(texts: string[]): Promise<number[][]> {
		if (texts.length === 0) {
			return [];
		}

		const body: ChromaCloudEmbeddingRequest = {
			texts,
			instructions:
				this.instructions[this.task][ChromaCloudQwenEmbeddingTarget.DOCUMENTS],
		};

		try {
			const response = await fetch(this.url, {
				method: "POST",
				headers: this.headers,
				body: JSON.stringify(snakeCase(body)),
			});

			const data = (await response.json()) as ChromaCloudEmbeddingsResponse;
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

	public async generateForQueries(texts: string[]): Promise<number[][]> {
		if (texts.length === 0) {
			return [];
		}

		const body: ChromaCloudEmbeddingRequest = {
			texts,
			instructions:
				this.instructions[this.task][ChromaCloudQwenEmbeddingTarget.QUERY],
		};

		try {
			const response = await fetch(this.url, {
				method: "POST",
				headers: this.headers,
				body: JSON.stringify(snakeCase(body)),
			});

			const data = (await response.json()) as ChromaCloudEmbeddingsResponse;
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

	public static buildFromConfig(
		config: ChromaCloudQwenConfig,
	): ChromaCloudQwenEmbeddingFunction {
		// Deserialize instructions dict from string keys to enum keys (if needed)
		// The config.instructions will have string keys like "nl_to_code" and "documents"
		// We need to convert these to the enum values for proper runtime usage
		let deserializedInstructions: ChromaCloudQwenEmbeddingInstructions;

		if (config.instructions) {
			deserializedInstructions = {} as ChromaCloudQwenEmbeddingInstructions;
			for (const [taskKey, targets] of Object.entries(config.instructions)) {
				deserializedInstructions[taskKey] = {} as Record<
					ChromaCloudQwenEmbeddingTarget,
					string
				>;
				for (const [targetKey, instruction] of Object.entries(targets)) {
					// targetKey is the enum value string like "documents" or "query"
					const targetEnum = targetKey as ChromaCloudQwenEmbeddingTarget;
					deserializedInstructions[taskKey][targetEnum] = instruction;
				}
			}
		} else {
			deserializedInstructions = CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS;
		}

		return new ChromaCloudQwenEmbeddingFunction({
			model: config.model,
			task: config.task,
			instructions: deserializedInstructions,
			apiKeyEnvVar: config.api_key_env_var,
		});
	}

	public getConfig(): ChromaCloudQwenConfig {
		// Serialize instructions dict with enum keys to string keys for JSON compatibility
		const serializedInstructions: Record<string, Record<string, string>> = {};
		for (const [taskKey, targets] of Object.entries(this.instructions)) {
			serializedInstructions[taskKey] = {};
			for (const [targetKey, instruction] of Object.entries(targets)) {
				serializedInstructions[taskKey][targetKey] = instruction;
			}
		}

		return {
			model: this.model,
			task: this.task,
			instructions: serializedInstructions as any,
			api_key_env_var: this.apiKeyEnvVar,
		};
	}

	public validateConfigUpdate(newConfig: Record<string, any>): void {
		if ("model" in newConfig) {
			throw new ChromaValueError("Model cannot be updated");
		}

		if ("task" in newConfig) {
			throw new ChromaValueError("Task cannot be updated");
		}

		if ("instructions" in newConfig) {
			throw new ChromaValueError("Instructions cannot be updated");
		}
	}

	public static validateConfig(config: ChromaCloudQwenConfig): void {
		validateConfigSchema(config, NAME);
	}
}

registerEmbeddingFunction(NAME, ChromaCloudQwenEmbeddingFunction);

import {
  ChromaValueError,
  type SparseEmbeddingFunction,
  type SparseVector,
  registerSparseEmbeddingFunction,
  ChromaClient,
} from "chromadb";
import {
  snakeCase,
  validateConfigSchema,
} from "@chroma-core/ai-embeddings-common";

const NAME = "chroma-cloud-splade";

// SPLADE models are BERT-based with a 512 token max sequence length.
// 2 tokens are reserved for [CLS] and [SEP] special tokens, leaving 510
// content tokens. BERT tokenization averages ~4 characters per token for
// English text. We use a conservative estimate to avoid silent truncation.
const SPLADE_MAX_CHARS_PER_CHUNK = 2000;

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
  client?: ChromaClient;
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
 * Combine multiple sparse vectors using element-wise max pooling.
 * For each unique index across all input vectors, takes the maximum value.
 * This is the standard way to combine SPLADE embeddings across chunks.
 */
/** @internal Exported for testing only. */
export function maxPoolSparseVectors(vectors: SparseVector[]): SparseVector {
  if (vectors.length === 1) return vectors[0];

  const maxValues = new Map<number, number>();
  for (const vec of vectors) {
    for (let i = 0; i < vec.indices.length; i++) {
      const idx = vec.indices[i];
      const val = vec.values[i];
      const current = maxValues.get(idx);
      if (current === undefined || val > current) {
        maxValues.set(idx, val);
      }
    }
  }

  const entries = Array.from(maxValues.entries()).sort((a, b) => a[0] - b[0]);
  return {
    indices: entries.map((e) => e[0]),
    values: entries.map((e) => e[1]),
  };
}

/**
 * Split text into chunks that fit within the SPLADE token limit.
 * Uses a conservative character-based estimate (~4 chars/token for BERT).
 * Splits on word boundaries to avoid breaking words.
 */
/** @internal Exported for testing only. */
export function chunkText(text: string): string[] {
  if (text.length <= SPLADE_MAX_CHARS_PER_CHUNK) {
    return [text];
  }

  const chunks: string[] = [];
  let remaining = text;

  while (remaining.length > 0) {
    if (remaining.length <= SPLADE_MAX_CHARS_PER_CHUNK) {
      chunks.push(remaining);
      break;
    }

    // Find a word boundary to split at
    let splitAt = SPLADE_MAX_CHARS_PER_CHUNK;
    while (splitAt > 0 && remaining[splitAt] !== " ") {
      splitAt--;
    }
    // If no space found, force split at the limit
    if (splitAt === 0) {
      splitAt = SPLADE_MAX_CHARS_PER_CHUNK;
    }

    const chunk = remaining.slice(0, splitAt).trim();
    if (chunk.length > 0) {
      chunks.push(chunk);
    }
    remaining = remaining.slice(splitAt).trim();
  }

  return chunks.length > 0 ? chunks : [text];
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
      (
        a: { index: number; value: number },
        b: { index: number; value: number },
      ) => a.index - b.index,
    );

    // Update the original arrays
    embedding.indices = pairs.map(
      (p: { index: number; value: number }) => p.index,
    );
    embedding.values = pairs.map(
      (p: { index: number; value: number }) => p.value,
    );
  }
}

export class ChromaCloudSpladeEmbeddingFunction
  implements SparseEmbeddingFunction
{
  public readonly name = NAME;

  private readonly apiKeyEnvVar: string;
  private readonly model: ChromaCloudSpladeEmbeddingModel;
  private readonly url: string;
  private readonly headers: { [key: string]: string };

  constructor(args: ChromaCloudSpladeArgs = {}) {
    const {
      model = ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
      apiKeyEnvVar = "CHROMA_API_KEY",
      client,
    } = args;

    let apiKey = process.env[apiKeyEnvVar];

    if (!apiKey && client && client.headers) {
      apiKey = client.headers["x-chroma-token"];
    }

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

    // Chunk documents that exceed the token limit and track the mapping
    // back to the original document index.
    const allChunks: string[] = [];
    const docChunkRanges: [number, number][] = [];

    for (const text of texts) {
      const start = allChunks.length;
      const chunks = chunkText(text);
      allChunks.push(...chunks);
      docChunkRanges.push([start, allChunks.length]);
    }

    const chunkEmbeddings = await this.embedTexts(allChunks);

    // Aggregate chunk embeddings per original document via max pooling.
    const result: SparseVector[] = [];
    for (const [start, end] of docChunkRanges) {
      const docVectors = chunkEmbeddings.slice(start, end);
      if (docVectors.length === 1) {
        result.push(docVectors[0]);
      } else {
        result.push(maxPoolSparseVectors(docVectors));
      }
    }

    return result;
  }

  private async embedTexts(texts: string[]): Promise<SparseVector[]> {
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
      if (!data || typeof data !== "object") {
        throw new Error("Invalid response format: expected object");
      }

      if (!Array.isArray(data.embeddings)) {
        throw new Error(
          "Invalid response format: missing or invalid embeddings array",
        );
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
    client?: ChromaClient,
  ): ChromaCloudSpladeEmbeddingFunction {
    return new ChromaCloudSpladeEmbeddingFunction({
      model: config.model,
      apiKeyEnvVar: config.api_key_env_var,
      client,
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

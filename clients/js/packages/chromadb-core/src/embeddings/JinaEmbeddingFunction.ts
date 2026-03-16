import { IEmbeddingFunction } from "./IEmbeddingFunction";
import { validateConfigSchema } from "../schemas/schemaUtils";
type StoredConfig = {
  api_key_env_var: string;
  model_name: string;
  task?: string;
  late_chunking?: boolean;
  truncate?: boolean;
  dimensions?: number;
  embedding_type?: string;
  normalized?: boolean;
};

interface JinaRequestBody {
  input: string[];
  model: string;
  task?: string;
  late_chunking?: boolean;
  truncate?: boolean;
  dimensions?: number;
  embedding_type?: string;
  normalized?: boolean;
}

export class JinaEmbeddingFunction implements IEmbeddingFunction {
  name = "jina";

  private api_key_env_var: string;
  private model_name: string;
  private api_url: string;
  private headers: { [key: string]: string };
  private task: string | undefined;
  private late_chunking: boolean | undefined;
  private truncate: boolean | undefined;
  private dimensions: number | undefined;
  private embedding_type: string | undefined;
  private normalized: boolean | undefined;

  constructor({
    jinaai_api_key,
    model_name = "jina-embeddings-v2-base-en",
    api_key_env_var = "CHROMA_JINA_API_KEY",
    task,
    late_chunking,
    truncate,
    dimensions,
    embedding_type,
    normalized,
  }: {
    jinaai_api_key?: string;
    model_name?: string;
    api_key_env_var: string;
    task?: string;
    late_chunking?: boolean;
    truncate?: boolean;
    dimensions?: number;
    embedding_type?: string;
    normalized?: boolean;
  }) {
    const apiKey = jinaai_api_key ?? process.env[api_key_env_var];
    if (!apiKey) {
      throw new Error(
        `Jina AI API key is required. Please provide it in the constructor or set the environment variable ${api_key_env_var}.`,
      );
    }

    this.model_name = model_name;
    this.api_key_env_var = api_key_env_var;
    this.task = task;
    this.late_chunking = late_chunking;
    this.truncate = truncate;
    this.dimensions = dimensions;
    this.embedding_type = embedding_type;
    this.normalized = normalized;

    this.api_url = "https://api.jina.ai/v1/embeddings";
    this.headers = {
      Authorization: `Bearer ${jinaai_api_key}`,
      "Accept-Encoding": "identity",
      "Content-Type": "application/json",
    };
  }

  public async generate(texts: string[]) {
    let json_body: JinaRequestBody = {
      input: texts,
      model: this.model_name,
    };

    if (this.task) {
      json_body.task = this.task;
    }

    if (this.late_chunking) {
      json_body.late_chunking = this.late_chunking;
    }

    if (this.truncate) {
      json_body.truncate = this.truncate;
    }

    if (this.dimensions) {
      json_body.dimensions = this.dimensions;
    }

    if (this.embedding_type) {
      json_body.embedding_type = this.embedding_type;
    }

    if (this.normalized) {
      json_body.normalized = this.normalized;
    }

    try {
      const response = await fetch(this.api_url, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify(json_body),
      });

      const data = (await response.json()) as { data: any[]; detail: string };
      if (!data || !data.data) {
        throw new Error(data.detail);
      }

      const embeddings: any[] = data.data;
      const sortedEmbeddings = embeddings.sort((a, b) => a.index - b.index);

      return sortedEmbeddings.map((result) => result.embedding);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Error calling Jina AI API: ${error.message}`);
      } else {
        throw new Error(`Error calling Jina AI API: ${error}`);
      }
    }
  }

  buildFromConfig(config: StoredConfig): JinaEmbeddingFunction {
    return new JinaEmbeddingFunction({
      model_name: config.model_name,
      api_key_env_var: config.api_key_env_var,
      task: config.task,
      late_chunking: config.late_chunking,
      truncate: config.truncate,
      dimensions: config.dimensions,
      embedding_type: config.embedding_type,
      normalized: config.normalized,
    });
  }

  getConfig(): StoredConfig {
    return {
      api_key_env_var: this.api_key_env_var,
      model_name: this.model_name,
      task: this.task,
      late_chunking: this.late_chunking,
      truncate: this.truncate,
      dimensions: this.dimensions,
      embedding_type: this.embedding_type,
      normalized: this.normalized,
    };
  }

  validateConfig(config: StoredConfig): void {
    validateConfigSchema(config, "jina");
  }
}

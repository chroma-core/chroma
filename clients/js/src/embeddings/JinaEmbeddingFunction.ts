import { IEmbeddingFunction } from "./IEmbeddingFunction";
import axios, { AxiosInstance } from 'axios';

export class JinaEmbeddingFunction implements IEmbeddingFunction {
    private api_key: string;
    private model: string;

    constructor({ jinaai_api_key, model }: { jinaai_api_key: string, model?: string }) {
        this.model = model || "jina-embeddings-v2-base-en";
        this.api_url = "https://api.jina.ai/v1/embeddings";
        this.session = axios.create({
          headers: {
            "Authorization": `Bearer ${jinaai_api_key}`,
            "Accept-Encoding": "identity",
          },
        });
    }

    public async generate(texts: string[]) {
        try {
          const response = await this.session.post(this.api_url, {input: texts, model: this.model_name});

          const data = response.data;
          if (!data || !data.data) {
            throw new Error(data.detail);
          }

          const embeddings: EmbeddingResult[] = data.data;
          const sortedEmbeddings = embeddings.sort((a, b) => a.index - b.index);

          return sortedEmbeddings.map((result) => result.embedding);
        } catch (error) {
          throw new Error(`Error calling Jina API: ${error.message}`);
        }
      }
    }
}

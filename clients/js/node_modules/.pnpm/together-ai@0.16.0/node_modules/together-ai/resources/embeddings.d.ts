import { APIResource } from "../resource.js";
import * as Core from "../core.js";
export declare class Embeddings extends APIResource {
    /**
     * Query an embedding model for a given string of text.
     */
    create(body: EmbeddingCreateParams, options?: Core.RequestOptions): Core.APIPromise<Embedding>;
}
export interface Embedding {
    data: Array<Embedding.Data>;
    model: string;
    object: 'list';
}
export declare namespace Embedding {
    interface Data {
        embedding: Array<number>;
        index: number;
        object: 'embedding';
    }
}
export interface EmbeddingCreateParams {
    /**
     * A string providing the text for the model to embed.
     */
    input: string | Array<string>;
    /**
     * The name of the embedding model to use.
     *
     * [See all of Together AI's embedding models](https://docs.together.ai/docs/serverless-models#embedding-models)
     */
    model: 'WhereIsAI/UAE-Large-V1' | 'BAAI/bge-large-en-v1.5' | 'BAAI/bge-base-en-v1.5' | 'togethercomputer/m2-bert-80M-8k-retrieval' | (string & {});
}
export declare namespace Embeddings {
    export { type Embedding as Embedding, type EmbeddingCreateParams as EmbeddingCreateParams };
}
//# sourceMappingURL=embeddings.d.ts.map
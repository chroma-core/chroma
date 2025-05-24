// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import * as CompletionsAPI from './chat/completions';

export interface RerankResponse {
  /**
   * The model to be used for the rerank request.
   */
  model: string;

  /**
   * Object type
   */
  object: 'rerank';

  results: Array<RerankResponse.Result>;

  /**
   * Request ID
   */
  id?: string;

  usage?: CompletionsAPI.ChatCompletionUsage | null;
}

export namespace RerankResponse {
  export interface Result {
    document: Result.Document;

    index: number;

    relevance_score: number;
  }

  export namespace Result {
    export interface Document {
      text?: string | null;
    }
  }
}

export interface RerankParams {
  /**
   * List of documents, which can be either strings or objects.
   */
  documents: Array<Record<string, unknown>> | Array<string>;

  /**
   * The model to be used for the rerank request.
   *
   * [See all of Together AI's rerank models](https://docs.together.ai/docs/serverless-models#rerank-models)
   */
  model: 'Salesforce/Llama-Rank-v1' | (string & {});

  /**
   * The search query to be used for ranking.
   */
  query: string;

  /**
   * List of keys in the JSON Object document to rank by. Defaults to use all
   * supplied keys for ranking.
   */
  rank_fields?: Array<string>;

  /**
   * Whether to return supplied documents with the response.
   */
  return_documents?: boolean;

  /**
   * The number of top results to return.
   */
  top_n?: number;
}

export declare namespace TopLevel {
  export { type RerankResponse as RerankResponse, type RerankParams as RerankParams };
}

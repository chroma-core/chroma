// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import { APIResource } from '../resource';
import * as Core from '../core';

export class Models extends APIResource {
  /**
   * Lists all of Together's open-source models
   */
  list(options?: Core.RequestOptions): Core.APIPromise<ModelListResponse> {
    return this._client.get('/models', options);
  }

  /**
   * Upload a custom model from Hugging Face or S3
   */
  upload(body: ModelUploadParams, options?: Core.RequestOptions): Core.APIPromise<ModelUploadResponse> {
    return this._client.post('/models', { body, ...options });
  }
}

export type ModelListResponse = Array<ModelListResponse.ModelListResponseItem>;

export namespace ModelListResponse {
  export interface ModelListResponseItem {
    id: string;

    created: number;

    object: string;

    type: 'chat' | 'language' | 'code' | 'image' | 'embedding' | 'moderation' | 'rerank';

    context_length?: number;

    display_name?: string;

    license?: string;

    link?: string;

    organization?: string;

    pricing?: ModelListResponseItem.Pricing;
  }

  export namespace ModelListResponseItem {
    export interface Pricing {
      base: number;

      finetune: number;

      hourly: number;

      input: number;

      output: number;
    }
  }
}

export interface ModelUploadResponse {
  data: ModelUploadResponse.Data;

  message: string;
}

export namespace ModelUploadResponse {
  export interface Data {
    job_id: string;

    model_id: string;

    model_name: string;

    model_source: string;
  }
}

export interface ModelUploadParams {
  /**
   * The name to give to your uploaded model
   */
  model_name: string;

  /**
   * The source location of the model (Hugging Face repo or S3 path)
   */
  model_source: string;

  /**
   * A description of your model
   */
  description?: string;

  /**
   * Hugging Face token (if uploading from Hugging Face)
   */
  hf_token?: string;
}

export declare namespace Models {
  export {
    type ModelListResponse as ModelListResponse,
    type ModelUploadResponse as ModelUploadResponse,
    type ModelUploadParams as ModelUploadParams,
  };
}

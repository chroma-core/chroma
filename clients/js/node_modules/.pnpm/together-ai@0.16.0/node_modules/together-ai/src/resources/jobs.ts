// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import { APIResource } from '../resource';
import * as Core from '../core';

export class Jobs extends APIResource {
  /**
   * Get the status of a specific job
   */
  retrieve(jobId: string, options?: Core.RequestOptions): Core.APIPromise<JobRetrieveResponse> {
    return this._client.get(`/jobs/${jobId}`, options);
  }

  /**
   * List all jobs and their statuses
   */
  list(options?: Core.RequestOptions): Core.APIPromise<JobListResponse> {
    return this._client.get('/jobs', options);
  }
}

export interface JobRetrieveResponse {
  args: JobRetrieveResponse.Args;

  created_at: string;

  job_id: string;

  status: 'Queued' | 'Running' | 'Complete' | 'Failed';

  status_updates: Array<JobRetrieveResponse.StatusUpdate>;

  type: string;

  updated_at: string;
}

export namespace JobRetrieveResponse {
  export interface Args {
    description?: string;

    modelName?: string;

    modelSource?: string;
  }

  export interface StatusUpdate {
    message: string;

    status: string;

    timestamp: string;
  }
}

export interface JobListResponse {
  data: Array<JobListResponse.Data>;
}

export namespace JobListResponse {
  export interface Data {
    args: Data.Args;

    created_at: string;

    job_id: string;

    status: 'Queued' | 'Running' | 'Complete' | 'Failed';

    status_updates: Array<Data.StatusUpdate>;

    type: string;

    updated_at: string;
  }

  export namespace Data {
    export interface Args {
      description?: string;

      modelName?: string;

      modelSource?: string;
    }

    export interface StatusUpdate {
      message: string;

      status: string;

      timestamp: string;
    }
  }
}

export declare namespace Jobs {
  export { type JobRetrieveResponse as JobRetrieveResponse, type JobListResponse as JobListResponse };
}

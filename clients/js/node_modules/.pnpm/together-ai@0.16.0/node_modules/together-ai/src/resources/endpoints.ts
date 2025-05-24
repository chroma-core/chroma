// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import { APIResource } from '../resource';
import { isRequestOptions } from '../core';
import * as Core from '../core';

export class Endpoints extends APIResource {
  /**
   * Creates a new dedicated endpoint for serving models. The endpoint will
   * automatically start after creation. You can deploy any supported model on
   * hardware configurations that meet the model's requirements.
   */
  create(body: EndpointCreateParams, options?: Core.RequestOptions): Core.APIPromise<EndpointCreateResponse> {
    return this._client.post('/endpoints', { body, ...options });
  }

  /**
   * Retrieves details about a specific endpoint, including its current state,
   * configuration, and scaling settings.
   */
  retrieve(endpointId: string, options?: Core.RequestOptions): Core.APIPromise<EndpointRetrieveResponse> {
    return this._client.get(`/endpoints/${endpointId}`, options);
  }

  /**
   * Updates an existing endpoint's configuration. You can modify the display name,
   * autoscaling settings, or change the endpoint's state (start/stop).
   */
  update(
    endpointId: string,
    body: EndpointUpdateParams,
    options?: Core.RequestOptions,
  ): Core.APIPromise<EndpointUpdateResponse> {
    return this._client.patch(`/endpoints/${endpointId}`, { body, ...options });
  }

  /**
   * Returns a list of all endpoints associated with your account. You can filter the
   * results by type (dedicated or serverless).
   */
  list(query?: EndpointListParams, options?: Core.RequestOptions): Core.APIPromise<EndpointListResponse>;
  list(options?: Core.RequestOptions): Core.APIPromise<EndpointListResponse>;
  list(
    query: EndpointListParams | Core.RequestOptions = {},
    options?: Core.RequestOptions,
  ): Core.APIPromise<EndpointListResponse> {
    if (isRequestOptions(query)) {
      return this.list({}, query);
    }
    return this._client.get('/endpoints', { query, ...options });
  }

  /**
   * Permanently deletes an endpoint. This action cannot be undone.
   */
  delete(endpointId: string, options?: Core.RequestOptions): Core.APIPromise<void> {
    return this._client.delete(`/endpoints/${endpointId}`, {
      ...options,
      headers: { Accept: '*/*', ...options?.headers },
    });
  }
}

/**
 * Details about a dedicated endpoint deployment
 */
export interface EndpointCreateResponse {
  /**
   * Unique identifier for the endpoint
   */
  id: string;

  /**
   * Configuration for automatic scaling of the endpoint
   */
  autoscaling: EndpointCreateResponse.Autoscaling;

  /**
   * Timestamp when the endpoint was created
   */
  created_at: string;

  /**
   * Human-readable name for the endpoint
   */
  display_name: string;

  /**
   * The hardware configuration used for this endpoint
   */
  hardware: string;

  /**
   * The model deployed on this endpoint
   */
  model: string;

  /**
   * System name for the endpoint
   */
  name: string;

  /**
   * The type of object
   */
  object: 'endpoint';

  /**
   * The owner of this endpoint
   */
  owner: string;

  /**
   * Current state of the endpoint
   */
  state: 'PENDING' | 'STARTING' | 'STARTED' | 'STOPPING' | 'STOPPED' | 'ERROR';

  /**
   * The type of endpoint
   */
  type: 'dedicated';
}

export namespace EndpointCreateResponse {
  /**
   * Configuration for automatic scaling of the endpoint
   */
  export interface Autoscaling {
    /**
     * The maximum number of replicas to scale up to under load
     */
    max_replicas: number;

    /**
     * The minimum number of replicas to maintain, even when there is no load
     */
    min_replicas: number;
  }
}

/**
 * Details about a dedicated endpoint deployment
 */
export interface EndpointRetrieveResponse {
  /**
   * Unique identifier for the endpoint
   */
  id: string;

  /**
   * Configuration for automatic scaling of the endpoint
   */
  autoscaling: EndpointRetrieveResponse.Autoscaling;

  /**
   * Timestamp when the endpoint was created
   */
  created_at: string;

  /**
   * Human-readable name for the endpoint
   */
  display_name: string;

  /**
   * The hardware configuration used for this endpoint
   */
  hardware: string;

  /**
   * The model deployed on this endpoint
   */
  model: string;

  /**
   * System name for the endpoint
   */
  name: string;

  /**
   * The type of object
   */
  object: 'endpoint';

  /**
   * The owner of this endpoint
   */
  owner: string;

  /**
   * Current state of the endpoint
   */
  state: 'PENDING' | 'STARTING' | 'STARTED' | 'STOPPING' | 'STOPPED' | 'ERROR';

  /**
   * The type of endpoint
   */
  type: 'dedicated';
}

export namespace EndpointRetrieveResponse {
  /**
   * Configuration for automatic scaling of the endpoint
   */
  export interface Autoscaling {
    /**
     * The maximum number of replicas to scale up to under load
     */
    max_replicas: number;

    /**
     * The minimum number of replicas to maintain, even when there is no load
     */
    min_replicas: number;
  }
}

/**
 * Details about a dedicated endpoint deployment
 */
export interface EndpointUpdateResponse {
  /**
   * Unique identifier for the endpoint
   */
  id: string;

  /**
   * Configuration for automatic scaling of the endpoint
   */
  autoscaling: EndpointUpdateResponse.Autoscaling;

  /**
   * Timestamp when the endpoint was created
   */
  created_at: string;

  /**
   * Human-readable name for the endpoint
   */
  display_name: string;

  /**
   * The hardware configuration used for this endpoint
   */
  hardware: string;

  /**
   * The model deployed on this endpoint
   */
  model: string;

  /**
   * System name for the endpoint
   */
  name: string;

  /**
   * The type of object
   */
  object: 'endpoint';

  /**
   * The owner of this endpoint
   */
  owner: string;

  /**
   * Current state of the endpoint
   */
  state: 'PENDING' | 'STARTING' | 'STARTED' | 'STOPPING' | 'STOPPED' | 'ERROR';

  /**
   * The type of endpoint
   */
  type: 'dedicated';
}

export namespace EndpointUpdateResponse {
  /**
   * Configuration for automatic scaling of the endpoint
   */
  export interface Autoscaling {
    /**
     * The maximum number of replicas to scale up to under load
     */
    max_replicas: number;

    /**
     * The minimum number of replicas to maintain, even when there is no load
     */
    min_replicas: number;
  }
}

export interface EndpointListResponse {
  data: Array<EndpointListResponse.Data>;

  object: 'list';
}

export namespace EndpointListResponse {
  /**
   * Details about an endpoint when listed via the list endpoint
   */
  export interface Data {
    /**
     * Unique identifier for the endpoint
     */
    id: string;

    /**
     * Timestamp when the endpoint was created
     */
    created_at: string;

    /**
     * The model deployed on this endpoint
     */
    model: string;

    /**
     * System name for the endpoint
     */
    name: string;

    /**
     * The type of object
     */
    object: 'endpoint';

    /**
     * The owner of this endpoint
     */
    owner: string;

    /**
     * Current state of the endpoint
     */
    state: 'PENDING' | 'STARTING' | 'STARTED' | 'STOPPING' | 'STOPPED' | 'ERROR';

    /**
     * The type of endpoint
     */
    type: 'serverless' | 'dedicated';
  }
}

export interface EndpointCreateParams {
  /**
   * Configuration for automatic scaling of the endpoint
   */
  autoscaling: EndpointCreateParams.Autoscaling;

  /**
   * The hardware configuration to use for this endpoint
   */
  hardware: string;

  /**
   * The model to deploy on this endpoint
   */
  model: string;

  /**
   * Whether to disable the prompt cache for this endpoint
   */
  disable_prompt_cache?: boolean;

  /**
   * Whether to disable speculative decoding for this endpoint
   */
  disable_speculative_decoding?: boolean;

  /**
   * A human-readable name for the endpoint
   */
  display_name?: string;

  /**
   * The number of minutes of inactivity after which the endpoint will be
   * automatically stopped. Set to null, omit or set to 0 to disable automatic
   * timeout.
   */
  inactive_timeout?: number | null;

  /**
   * The desired state of the endpoint
   */
  state?: 'STARTED' | 'STOPPED';
}

export namespace EndpointCreateParams {
  /**
   * Configuration for automatic scaling of the endpoint
   */
  export interface Autoscaling {
    /**
     * The maximum number of replicas to scale up to under load
     */
    max_replicas: number;

    /**
     * The minimum number of replicas to maintain, even when there is no load
     */
    min_replicas: number;
  }
}

export interface EndpointUpdateParams {
  /**
   * New autoscaling configuration for the endpoint
   */
  autoscaling?: EndpointUpdateParams.Autoscaling;

  /**
   * A human-readable name for the endpoint
   */
  display_name?: string;

  /**
   * The number of minutes of inactivity after which the endpoint will be
   * automatically stopped. Set to 0 to disable automatic timeout.
   */
  inactive_timeout?: number | null;

  /**
   * The desired state of the endpoint
   */
  state?: 'STARTED' | 'STOPPED';
}

export namespace EndpointUpdateParams {
  /**
   * New autoscaling configuration for the endpoint
   */
  export interface Autoscaling {
    /**
     * The maximum number of replicas to scale up to under load
     */
    max_replicas: number;

    /**
     * The minimum number of replicas to maintain, even when there is no load
     */
    min_replicas: number;
  }
}

export interface EndpointListParams {
  /**
   * Filter endpoints by type
   */
  type?: 'dedicated' | 'serverless';
}

export declare namespace Endpoints {
  export {
    type EndpointCreateResponse as EndpointCreateResponse,
    type EndpointRetrieveResponse as EndpointRetrieveResponse,
    type EndpointUpdateResponse as EndpointUpdateResponse,
    type EndpointListResponse as EndpointListResponse,
    type EndpointCreateParams as EndpointCreateParams,
    type EndpointUpdateParams as EndpointUpdateParams,
    type EndpointListParams as EndpointListParams,
  };
}

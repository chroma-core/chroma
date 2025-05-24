// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import { APIResource } from '../../../resource';
import * as Core from '../../../core';

export class Content extends APIResource {
  /**
   * Retrieve Container File Content
   */
  retrieve(containerId: string, fileId: string, options?: Core.RequestOptions): Core.APIPromise<void> {
    return this._client.get(`/containers/${containerId}/files/${fileId}/content`, {
      ...options,
      headers: { Accept: '*/*', ...options?.headers },
    });
  }
}

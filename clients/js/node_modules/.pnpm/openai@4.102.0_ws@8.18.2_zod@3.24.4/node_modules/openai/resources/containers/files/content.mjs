// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
import { APIResource } from "../../../resource.mjs";
export class Content extends APIResource {
    /**
     * Retrieve Container File Content
     */
    retrieve(containerId, fileId, options) {
        return this._client.get(`/containers/${containerId}/files/${fileId}/content`, {
            ...options,
            headers: { Accept: '*/*', ...options?.headers },
        });
    }
}
//# sourceMappingURL=content.mjs.map
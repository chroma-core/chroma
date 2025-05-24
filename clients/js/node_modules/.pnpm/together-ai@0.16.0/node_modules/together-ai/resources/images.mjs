// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
import { APIResource } from "../resource.mjs";
export class Images extends APIResource {
    /**
     * Use an image model to generate an image for a given prompt.
     */
    create(body, options) {
        return this._client.post('/images/generations', { body, ...options });
    }
}
//# sourceMappingURL=images.mjs.map
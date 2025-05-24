// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
import { APIResource } from "../resource.mjs";
export class Embeddings extends APIResource {
    /**
     * Query an embedding model for a given string of text.
     */
    create(body, options) {
        return this._client.post('/embeddings', { body, ...options });
    }
}
//# sourceMappingURL=embeddings.mjs.map
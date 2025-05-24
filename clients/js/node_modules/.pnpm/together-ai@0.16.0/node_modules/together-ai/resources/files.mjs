// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
import { APIResource } from "../resource.mjs";
export class Files extends APIResource {
    /**
     * List the metadata for a single uploaded data file.
     */
    retrieve(id, options) {
        return this._client.get(`/files/${id}`, options);
    }
    /**
     * List the metadata for all uploaded data files.
     */
    list(options) {
        return this._client.get('/files', options);
    }
    /**
     * Delete a previously uploaded data file.
     */
    delete(id, options) {
        return this._client.delete(`/files/${id}`, options);
    }
    /**
     * Get the contents of a single uploaded data file.
     */
    content(id, options) {
        return this._client.get(`/files/${id}/content`, {
            ...options,
            headers: { Accept: 'application/binary', ...options?.headers },
            __binaryResponse: true,
        });
    }
    /**
     * Upload a file.
     */
    upload(_) {
        throw 'please use together-ai/lib/upload';
    }
}
//# sourceMappingURL=files.mjs.map
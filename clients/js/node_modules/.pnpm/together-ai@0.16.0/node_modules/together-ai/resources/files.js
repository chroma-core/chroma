"use strict";
// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
Object.defineProperty(exports, "__esModule", { value: true });
exports.Files = void 0;
const resource_1 = require("../resource.js");
class Files extends resource_1.APIResource {
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
exports.Files = Files;
//# sourceMappingURL=files.js.map
"use strict";
// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
Object.defineProperty(exports, "__esModule", { value: true });
exports.FineTuneResource = void 0;
const resource_1 = require("../resource.js");
class FineTuneResource extends resource_1.APIResource {
    /**
     * Use a model to create a fine-tuning job.
     */
    create(body, options) {
        return this._client.post('/fine-tunes', { body, ...options });
    }
    /**
     * List the metadata for a single fine-tuning job.
     */
    retrieve(id, options) {
        return this._client.get(`/fine-tunes/${id}`, options);
    }
    /**
     * List the metadata for all fine-tuning jobs.
     */
    list(options) {
        return this._client.get('/fine-tunes', options);
    }
    /**
     * Cancel a currently running fine-tuning job.
     */
    cancel(id, options) {
        return this._client.post(`/fine-tunes/${id}/cancel`, options);
    }
    /**
     * Download a compressed fine-tuned model or checkpoint to local disk.
     */
    download(query, options) {
        return this._client.get('/finetune/download', { query, ...options });
    }
    /**
     * List the events for a single fine-tuning job.
     */
    listEvents(id, options) {
        return this._client.get(`/fine-tunes/${id}/events`, options);
    }
}
exports.FineTuneResource = FineTuneResource;
//# sourceMappingURL=fine-tune.js.map
"use strict";
// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
Object.defineProperty(exports, "__esModule", { value: true });
exports.Images = void 0;
const resource_1 = require("../resource.js");
class Images extends resource_1.APIResource {
    /**
     * Use an image model to generate an image for a given prompt.
     */
    create(body, options) {
        return this._client.post('/images/generations', { body, ...options });
    }
}
exports.Images = Images;
//# sourceMappingURL=images.js.map
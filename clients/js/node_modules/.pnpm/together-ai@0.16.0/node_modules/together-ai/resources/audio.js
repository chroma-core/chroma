"use strict";
// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
Object.defineProperty(exports, "__esModule", { value: true });
exports.Audio = void 0;
const resource_1 = require("../resource.js");
class Audio extends resource_1.APIResource {
    /**
     * Generate audio from input text
     */
    create(body, options) {
        return this._client.post('/audio/speech', {
            body,
            ...options,
            headers: { Accept: 'application/octet-stream', ...options?.headers },
            __binaryResponse: true,
        });
    }
}
exports.Audio = Audio;
//# sourceMappingURL=audio.js.map
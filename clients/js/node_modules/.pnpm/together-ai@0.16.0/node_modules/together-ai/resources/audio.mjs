// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
import { APIResource } from "../resource.mjs";
export class Audio extends APIResource {
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
//# sourceMappingURL=audio.mjs.map
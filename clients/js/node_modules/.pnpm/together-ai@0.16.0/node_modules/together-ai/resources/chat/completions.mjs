// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
import { APIResource } from "../../resource.mjs";
import { ChatCompletionStream } from 'together-ai/lib/ChatCompletionStream';
export class Completions extends APIResource {
    create(body, options) {
        return this._client.post('/chat/completions', { body, ...options, stream: body.stream ?? false });
    }
    stream(body, options) {
        return ChatCompletionStream.createChatCompletion(this._client.chat.completions, body, options);
    }
}
//# sourceMappingURL=completions.mjs.map
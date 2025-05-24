"use strict";
// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
Object.defineProperty(exports, "__esModule", { value: true });
exports.Completions = void 0;
const resource_1 = require("../../resource.js");
const ChatCompletionStream_1 = require("together-ai/lib/ChatCompletionStream");
class Completions extends resource_1.APIResource {
    create(body, options) {
        return this._client.post('/chat/completions', { body, ...options, stream: body.stream ?? false });
    }
    stream(body, options) {
        return ChatCompletionStream_1.ChatCompletionStream.createChatCompletion(this._client.chat.completions, body, options);
    }
}
exports.Completions = Completions;
//# sourceMappingURL=completions.js.map
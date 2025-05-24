"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ChatCompletionStreamingRunner = void 0;
const ChatCompletionStream_1 = require("./ChatCompletionStream.js");
class ChatCompletionStreamingRunner extends ChatCompletionStream_1.ChatCompletionStream {
    static fromReadableStream(stream) {
        const runner = new ChatCompletionStreamingRunner();
        runner._run(() => runner._fromReadableStream(stream));
        return runner;
    }
    /** @deprecated - please use `runTools` instead. */
    static runFunctions(completions, params, options) {
        const runner = new ChatCompletionStreamingRunner();
        const opts = {
            ...options,
            headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runFunctions' },
        };
        runner._run(() => runner._runFunctions(completions, params, opts));
        return runner;
    }
    static runTools(completions, params, options) {
        const runner = new ChatCompletionStreamingRunner();
        const opts = {
            ...options,
            headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runTools' },
        };
        runner._run(() => runner._runTools(completions, params, opts));
        return runner;
    }
}
exports.ChatCompletionStreamingRunner = ChatCompletionStreamingRunner;
//# sourceMappingURL=ChatCompletionStreamingRunner.js.map
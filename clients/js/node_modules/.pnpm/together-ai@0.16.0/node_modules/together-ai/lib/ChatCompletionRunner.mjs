import { AbstractChatCompletionRunner, } from "./AbstractChatCompletionRunner.mjs";
import { isAssistantMessage } from "./chatCompletionUtils.mjs";
export class ChatCompletionRunner extends AbstractChatCompletionRunner {
    /** @deprecated - please use `runTools` instead. */
    static runFunctions(completions, params, options) {
        const runner = new ChatCompletionRunner();
        const opts = {
            ...options,
            headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runFunctions' },
        };
        runner._run(() => runner._runFunctions(completions, params, opts));
        return runner;
    }
    static runTools(completions, params, options) {
        const runner = new ChatCompletionRunner();
        const opts = {
            ...options,
            headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runTools' },
        };
        runner._run(() => runner._runTools(completions, params, opts));
        return runner;
    }
    _addMessage(message) {
        super._addMessage(message);
        if (isAssistantMessage(message) && message.content) {
            this._emit('content', message.content);
        }
    }
}
//# sourceMappingURL=ChatCompletionRunner.mjs.map
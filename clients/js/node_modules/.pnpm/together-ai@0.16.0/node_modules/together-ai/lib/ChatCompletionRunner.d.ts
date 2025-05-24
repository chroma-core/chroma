import { type Completions, type ChatCompletionMessageParam, type CompletionCreateParamsNonStreaming } from 'together-ai/resources/chat/completions';
import { type RunnableFunctions, type BaseFunctionsArgs, RunnableTools } from "./RunnableFunction.js";
import { AbstractChatCompletionRunner, AbstractChatCompletionRunnerEvents, RunnerOptions } from "./AbstractChatCompletionRunner.js";
export interface ChatCompletionRunnerEvents extends AbstractChatCompletionRunnerEvents {
    content: (content: string) => void;
}
export type ChatCompletionFunctionRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<CompletionCreateParamsNonStreaming, 'functions'> & {
    functions: RunnableFunctions<FunctionsArgs>;
};
export type ChatCompletionToolRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<CompletionCreateParamsNonStreaming, 'tools'> & {
    tools: RunnableTools<FunctionsArgs>;
};
export declare class ChatCompletionRunner extends AbstractChatCompletionRunner<ChatCompletionRunnerEvents> {
    /** @deprecated - please use `runTools` instead. */
    static runFunctions(completions: Completions, params: ChatCompletionFunctionRunnerParams<any[]>, options?: RunnerOptions): ChatCompletionRunner;
    static runTools(completions: Completions, params: ChatCompletionToolRunnerParams<any[]>, options?: RunnerOptions): ChatCompletionRunner;
    _addMessage(message: ChatCompletionMessageParam): void;
}
//# sourceMappingURL=ChatCompletionRunner.d.ts.map
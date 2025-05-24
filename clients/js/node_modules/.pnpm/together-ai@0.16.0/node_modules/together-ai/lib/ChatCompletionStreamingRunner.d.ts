import { Completions, type ChatCompletionChunk, type CompletionCreateParamsStreaming } from 'together-ai/resources/chat/completions';
import { RunnerOptions, type AbstractChatCompletionRunnerEvents } from "./AbstractChatCompletionRunner.js";
import { type ReadableStream } from 'together-ai/_shims/index';
import { RunnableTools, type BaseFunctionsArgs, type RunnableFunctions } from "./RunnableFunction.js";
import { ChatCompletionSnapshot, ChatCompletionStream } from "./ChatCompletionStream.js";
export interface ChatCompletionStreamEvents extends AbstractChatCompletionRunnerEvents {
    content: (contentDelta: string, contentSnapshot: string) => void;
    chunk: (chunk: ChatCompletionChunk, snapshot: ChatCompletionSnapshot) => void;
}
export type ChatCompletionStreamingFunctionRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<CompletionCreateParamsStreaming, 'functions'> & {
    functions: RunnableFunctions<FunctionsArgs>;
};
export type ChatCompletionStreamingToolRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<CompletionCreateParamsStreaming, 'tools'> & {
    tools: RunnableTools<FunctionsArgs>;
};
export declare class ChatCompletionStreamingRunner extends ChatCompletionStream implements AsyncIterable<ChatCompletionChunk> {
    static fromReadableStream(stream: ReadableStream): ChatCompletionStreamingRunner;
    /** @deprecated - please use `runTools` instead. */
    static runFunctions<T extends (string | object)[]>(completions: Completions, params: ChatCompletionStreamingFunctionRunnerParams<T>, options?: RunnerOptions): ChatCompletionStreamingRunner;
    static runTools<T extends (string | object)[]>(completions: Completions, params: ChatCompletionStreamingToolRunnerParams<T>, options?: RunnerOptions): ChatCompletionStreamingRunner;
}
//# sourceMappingURL=ChatCompletionStreamingRunner.d.ts.map
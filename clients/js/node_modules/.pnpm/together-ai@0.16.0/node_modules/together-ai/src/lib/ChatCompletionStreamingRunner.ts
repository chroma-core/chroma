import {
  Completions,
  type ChatCompletionChunk,
  type CompletionCreateParamsStreaming,
} from "../resources/chat/completions";
import { RunnerOptions, type AbstractChatCompletionRunnerEvents } from './AbstractChatCompletionRunner';
import { type ReadableStream } from "../_shims/index";
import { RunnableTools, type BaseFunctionsArgs, type RunnableFunctions } from './RunnableFunction';
import { ChatCompletionSnapshot, ChatCompletionStream } from './ChatCompletionStream';

export interface ChatCompletionStreamEvents extends AbstractChatCompletionRunnerEvents {
  content: (contentDelta: string, contentSnapshot: string) => void;
  chunk: (chunk: ChatCompletionChunk, snapshot: ChatCompletionSnapshot) => void;
}

export type ChatCompletionStreamingFunctionRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<
  CompletionCreateParamsStreaming,
  'functions'
> & {
  functions: RunnableFunctions<FunctionsArgs>;
};

export type ChatCompletionStreamingToolRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<
  CompletionCreateParamsStreaming,
  'tools'
> & {
  tools: RunnableTools<FunctionsArgs>;
};

export class ChatCompletionStreamingRunner
  extends ChatCompletionStream
  implements AsyncIterable<ChatCompletionChunk>
{
  static override fromReadableStream(stream: ReadableStream): ChatCompletionStreamingRunner {
    const runner = new ChatCompletionStreamingRunner();
    runner._run(() => runner._fromReadableStream(stream));
    return runner;
  }

  /** @deprecated - please use `runTools` instead. */
  static runFunctions<T extends (string | object)[]>(
    completions: Completions,
    params: ChatCompletionStreamingFunctionRunnerParams<T>,
    options?: RunnerOptions,
  ): ChatCompletionStreamingRunner {
    const runner = new ChatCompletionStreamingRunner();
    const opts = {
      ...options,
      headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runFunctions' },
    };
    runner._run(() => runner._runFunctions(completions, params, opts));
    return runner;
  }

  static runTools<T extends (string | object)[]>(
    completions: Completions,
    params: ChatCompletionStreamingToolRunnerParams<T>,
    options?: RunnerOptions,
  ): ChatCompletionStreamingRunner {
    const runner = new ChatCompletionStreamingRunner();
    const opts = {
      ...options,
      headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runTools' },
    };
    runner._run(() => runner._runTools(completions, params, opts));
    return runner;
  }
}

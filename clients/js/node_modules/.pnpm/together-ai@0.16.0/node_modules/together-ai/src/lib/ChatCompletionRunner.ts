import {
  type Completions,
  type ChatCompletionMessageParam,
  type CompletionCreateParamsNonStreaming,
} from "../resources/chat/completions";
import { type RunnableFunctions, type BaseFunctionsArgs, RunnableTools } from './RunnableFunction';
import {
  AbstractChatCompletionRunner,
  AbstractChatCompletionRunnerEvents,
  RunnerOptions,
} from './AbstractChatCompletionRunner';
import { isAssistantMessage } from './chatCompletionUtils';

export interface ChatCompletionRunnerEvents extends AbstractChatCompletionRunnerEvents {
  content: (content: string) => void;
}

export type ChatCompletionFunctionRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<
  CompletionCreateParamsNonStreaming,
  'functions'
> & {
  functions: RunnableFunctions<FunctionsArgs>;
};

export type ChatCompletionToolRunnerParams<FunctionsArgs extends BaseFunctionsArgs> = Omit<
  CompletionCreateParamsNonStreaming,
  'tools'
> & {
  tools: RunnableTools<FunctionsArgs>;
};

export class ChatCompletionRunner extends AbstractChatCompletionRunner<ChatCompletionRunnerEvents> {
  /** @deprecated - please use `runTools` instead. */
  static runFunctions(
    completions: Completions,
    params: ChatCompletionFunctionRunnerParams<any[]>,
    options?: RunnerOptions,
  ): ChatCompletionRunner {
    const runner = new ChatCompletionRunner();
    const opts = {
      ...options,
      headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runFunctions' },
    };
    runner._run(() => runner._runFunctions(completions, params, opts));
    return runner;
  }

  static runTools(
    completions: Completions,
    params: ChatCompletionToolRunnerParams<any[]>,
    options?: RunnerOptions,
  ): ChatCompletionRunner {
    const runner = new ChatCompletionRunner();
    const opts = {
      ...options,
      headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'runTools' },
    };
    runner._run(() => runner._runTools(completions, params, opts));
    return runner;
  }

  override _addMessage(message: ChatCompletionMessageParam) {
    super._addMessage(message);
    if (isAssistantMessage(message) && message.content) {
      this._emit('content', message.content as string);
    }
  }
}

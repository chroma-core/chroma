import * as Core from "../core";
import {
  type Completions,
  type ChatCompletion,
  type ChatCompletionMessage,
  type ChatCompletionMessageParam,
  type CompletionCreateParams,
  type ChatCompletionTool,
} from "../resources/chat/completions";
import { APIUserAbortError, TogetherError } from "../error";
import {
  type RunnableFunction,
  isRunnableFunctionWithParse,
  type BaseFunctionsArgs,
} from './RunnableFunction';
import { ChatCompletionFunctionRunnerParams, ChatCompletionToolRunnerParams } from './ChatCompletionRunner';
import {
  ChatCompletionStreamingFunctionRunnerParams,
  ChatCompletionStreamingToolRunnerParams,
} from './ChatCompletionStreamingRunner';
import { isAssistantMessage, isFunctionMessage, isToolMessage } from './chatCompletionUtils';
import { ChatCompletionUsage } from "../resources/chat/completions";

const DEFAULT_MAX_CHAT_COMPLETIONS = 10;
export interface RunnerOptions extends Core.RequestOptions {
  /** How many requests to make before canceling. Default 10. */
  maxChatCompletions?: number;
}

export abstract class AbstractChatCompletionRunner<
  Events extends CustomEvents<any> = AbstractChatCompletionRunnerEvents,
> {
  controller: AbortController = new AbortController();

  #connectedPromise: Promise<void>;
  #resolveConnectedPromise: () => void = () => {};
  #rejectConnectedPromise: (error: TogetherError) => void = () => {};

  #endPromise: Promise<void>;
  #resolveEndPromise: () => void = () => {};
  #rejectEndPromise: (error: TogetherError) => void = () => {};

  #listeners: { [Event in keyof Events]?: ListenersForEvent<Events, Event> } = {};

  protected _chatCompletions: ChatCompletion[] = [];
  messages: ChatCompletionMessageParam[] = [];

  #ended = false;
  #errored = false;
  #aborted = false;
  #catchingPromiseCreated = false;

  constructor() {
    this.#connectedPromise = new Promise<void>((resolve, reject) => {
      this.#resolveConnectedPromise = resolve;
      this.#rejectConnectedPromise = reject;
    });

    this.#endPromise = new Promise<void>((resolve, reject) => {
      this.#resolveEndPromise = resolve;
      this.#rejectEndPromise = reject;
    });

    // Don't let these promises cause unhandled rejection errors.
    // we will manually cause an unhandled rejection error later
    // if the user hasn't registered any error listener or called
    // any promise-returning method.
    this.#connectedPromise.catch(() => {});
    this.#endPromise.catch(() => {});
  }

  protected _run(executor: () => Promise<any>) {
    // Unfortunately if we call `executor()` immediately we get runtime errors about
    // references to `this` before the `super()` constructor call returns.
    setTimeout(() => {
      executor().then(() => {
        this._emitFinal();
        this._emit('end');
      }, this.#handleError);
    }, 0);
  }

  protected _addChatCompletion(chatCompletion: ChatCompletion): ChatCompletion {
    this._chatCompletions.push(chatCompletion);
    this._emit('chatCompletion', chatCompletion);
    const message = chatCompletion.choices[0]?.message;
    if (message) this._addMessage(message as ChatCompletionMessageParam);
    return chatCompletion;
  }

  protected _addMessage(message: ChatCompletionMessageParam, emit = true) {
    if (!('content' in message)) message.content = null;

    this.messages.push(message);

    if (emit) {
      this._emit('message', message);
      if ((isFunctionMessage(message) || isToolMessage(message)) && message.content) {
        // Note, this assumes that {role: 'tool', content: …} is always the result of a call of tool of type=function.
        this._emit('functionCallResult', message.content as string);
      } else if (isAssistantMessage(message) && message.function_call) {
        this._emit('functionCall', message.function_call);
      } else if (isAssistantMessage(message) && message.tool_calls) {
        for (const tool_call of message.tool_calls) {
          if (tool_call.type === 'function') {
            this._emit('functionCall', tool_call.function);
          }
        }
      }
    }
  }

  protected _connected() {
    if (this.ended) return;
    this.#resolveConnectedPromise();
    this._emit('connect');
  }

  get ended(): boolean {
    return this.#ended;
  }

  get errored(): boolean {
    return this.#errored;
  }

  get aborted(): boolean {
    return this.#aborted;
  }

  abort() {
    this.controller.abort();
  }

  /**
   * Adds the listener function to the end of the listeners array for the event.
   * No checks are made to see if the listener has already been added. Multiple calls passing
   * the same combination of event and listener will result in the listener being added, and
   * called, multiple times.
   * @returns this ChatCompletionStream, so that calls can be chained
   */
  on<Event extends keyof Events>(event: Event, listener: ListenerForEvent<Events, Event>): this {
    const listeners: ListenersForEvent<Events, Event> =
      this.#listeners[event] || (this.#listeners[event] = []);
    listeners.push({ listener });
    return this;
  }

  /**
   * Removes the specified listener from the listener array for the event.
   * off() will remove, at most, one instance of a listener from the listener array. If any single
   * listener has been added multiple times to the listener array for the specified event, then
   * off() must be called multiple times to remove each instance.
   * @returns this ChatCompletionStream, so that calls can be chained
   */
  off<Event extends keyof Events>(event: Event, listener: ListenerForEvent<Events, Event>): this {
    const listeners = this.#listeners[event];
    if (!listeners) return this;
    const index = listeners.findIndex((l) => l.listener === listener);
    if (index >= 0) listeners.splice(index, 1);
    return this;
  }

  /**
   * Adds a one-time listener function for the event. The next time the event is triggered,
   * this listener is removed and then invoked.
   * @returns this ChatCompletionStream, so that calls can be chained
   */
  once<Event extends keyof Events>(event: Event, listener: ListenerForEvent<Events, Event>): this {
    const listeners: ListenersForEvent<Events, Event> =
      this.#listeners[event] || (this.#listeners[event] = []);
    listeners.push({ listener, once: true });
    return this;
  }

  /**
   * This is similar to `.once()`, but returns a Promise that resolves the next time
   * the event is triggered, instead of calling a listener callback.
   * @returns a Promise that resolves the next time given event is triggered,
   * or rejects if an error is emitted.  (If you request the 'error' event,
   * returns a promise that resolves with the error).
   *
   * Example:
   *
   *   const message = await stream.emitted('message') // rejects if the stream errors
   */
  emitted<Event extends keyof Events>(
    event: Event,
  ): Promise<
    EventParameters<Events, Event> extends [infer Param] ? Param
    : EventParameters<Events, Event> extends [] ? void
    : EventParameters<Events, Event>
  > {
    return new Promise((resolve, reject) => {
      this.#catchingPromiseCreated = true;
      if (event !== 'error') this.once('error', reject);
      this.once(event, resolve as any);
    });
  }

  async done(): Promise<void> {
    this.#catchingPromiseCreated = true;
    await this.#endPromise;
  }

  /**
   * @returns a promise that resolves with the final ChatCompletion, or rejects
   * if an error occurred or the stream ended prematurely without producing a ChatCompletion.
   */
  async finalChatCompletion(): Promise<ChatCompletion> {
    await this.done();
    const completion = this._chatCompletions[this._chatCompletions.length - 1];
    if (!completion) throw new TogetherError('stream ended without producing a ChatCompletion');
    return completion;
  }

  #getFinalContent(): string | null {
    return this.#getFinalMessage().content ?? null;
  }

  /**
   * @returns a promise that resolves with the content of the final ChatCompletionMessage, or rejects
   * if an error occurred or the stream ended prematurely without producing a ChatCompletionMessage.
   */
  async finalContent(): Promise<string | null> {
    await this.done();
    return this.#getFinalContent();
  }

  #getFinalMessage(): ChatCompletionMessage {
    let i = this.messages.length;
    while (i-- > 0) {
      const message = this.messages[i];
      if (isAssistantMessage(message)) {
        return { ...message, content: message.content ?? null };
      }
    }
    throw new TogetherError('stream ended without producing a ChatCompletionMessage with role=assistant');
  }

  /**
   * @returns a promise that resolves with the the final assistant ChatCompletionMessage response,
   * or rejects if an error occurred or the stream ended prematurely without producing a ChatCompletionMessage.
   */
  async finalMessage(): Promise<ChatCompletionMessage> {
    await this.done();
    return this.#getFinalMessage();
  }

  #getFinalFunctionCall(): ChatCompletionMessage.FunctionCall | undefined {
    for (let i = this.messages.length - 1; i >= 0; i--) {
      const message = this.messages[i];
      if (isAssistantMessage(message) && message?.function_call) {
        return message.function_call;
      }
      if (isAssistantMessage(message) && message?.tool_calls?.length) {
        return message.tool_calls.at(-1)?.function;
      }
    }

    return;
  }

  /**
   * @returns a promise that resolves with the content of the final FunctionCall, or rejects
   * if an error occurred or the stream ended prematurely without producing a ChatCompletionMessage.
   */
  async finalFunctionCall(): Promise<ChatCompletionMessage.FunctionCall | undefined> {
    await this.done();
    return this.#getFinalFunctionCall();
  }

  #getFinalFunctionCallResult(): string | undefined {
    for (let i = this.messages.length - 1; i >= 0; i--) {
      const message = this.messages[i];
      if (isFunctionMessage(message) && message.content != null) {
        return message.content;
      }
      if (
        isToolMessage(message) &&
        message.content != null &&
        this.messages.some(
          (x) =>
            x.role === 'assistant' &&
            x.tool_calls?.some((y) => y.type === 'function' && y.id === message.tool_call_id),
        )
      ) {
        return message.content;
      }
    }

    return;
  }

  async finalFunctionCallResult(): Promise<string | undefined> {
    await this.done();
    return this.#getFinalFunctionCallResult();
  }

  #calculateTotalUsage(): ChatCompletionUsage {
    const total: ChatCompletionUsage = {
      completion_tokens: 0,
      prompt_tokens: 0,
      total_tokens: 0,
    };
    for (const { usage } of this._chatCompletions) {
      if (usage) {
        total.completion_tokens += usage.completion_tokens;
        total.prompt_tokens += usage.prompt_tokens;
        total.total_tokens += usage.total_tokens;
      }
    }
    return total;
  }

  async totalUsage(): Promise<ChatCompletionUsage> {
    await this.done();
    return this.#calculateTotalUsage();
  }

  allChatCompletions(): ChatCompletion[] {
    return [...this._chatCompletions];
  }

  #handleError = (error: unknown) => {
    this.#errored = true;
    if (error instanceof Error && error.name === 'AbortError') {
      error = new APIUserAbortError();
    }
    if (error instanceof APIUserAbortError) {
      this.#aborted = true;
      return this._emit('abort', error);
    }
    if (error instanceof TogetherError) {
      return this._emit('error', error);
    }
    if (error instanceof Error) {
      // @ts-ignore
      const TogetherError: TogetherError = new TogetherError(error.message);
      // @ts-ignore
      TogetherError.cause = error;
      return this._emit('error', TogetherError);
    }
    return this._emit('error', new TogetherError(String(error)));
  };

  protected _emit<Event extends keyof Events>(event: Event, ...args: EventParameters<Events, Event>) {
    // make sure we don't emit any events after end
    if (this.#ended) {
      return;
    }

    if (event === 'end') {
      this.#ended = true;
      this.#resolveEndPromise();
    }

    const listeners: ListenersForEvent<Events, Event> | undefined = this.#listeners[event];
    if (listeners) {
      this.#listeners[event] = listeners.filter((l) => !l.once) as any;
      listeners.forEach(({ listener }: any) => listener(...args));
    }

    if (event === 'abort') {
      const error = args[0] as APIUserAbortError;
      if (!this.#catchingPromiseCreated && !listeners?.length) {
        Promise.reject(error);
      }
      this.#rejectConnectedPromise(error);
      this.#rejectEndPromise(error);
      this._emit('end');
      return;
    }

    if (event === 'error') {
      // NOTE: _emit('error', error) should only be called from #handleError().

      const error = args[0] as TogetherError;
      if (!this.#catchingPromiseCreated && !listeners?.length) {
        // Trigger an unhandled rejection if the user hasn't registered any error handlers.
        // If you are seeing stack traces here, make sure to handle errors via either:
        // - runner.on('error', () => ...)
        // - await runner.done()
        // - await runner.finalChatCompletion()
        // - etc.
        Promise.reject(error);
      }
      this.#rejectConnectedPromise(error);
      this.#rejectEndPromise(error);
      this._emit('end');
    }
  }

  protected _emitFinal() {
    const completion = this._chatCompletions[this._chatCompletions.length - 1];
    if (completion) this._emit('finalChatCompletion', completion);
    const finalMessage = this.#getFinalMessage();
    if (finalMessage) this._emit('finalMessage', finalMessage);
    const finalContent = this.#getFinalContent();
    if (finalContent) this._emit('finalContent', finalContent);

    const finalFunctionCall = this.#getFinalFunctionCall();
    if (finalFunctionCall) this._emit('finalFunctionCall', finalFunctionCall);

    const finalFunctionCallResult = this.#getFinalFunctionCallResult();
    if (finalFunctionCallResult != null) this._emit('finalFunctionCallResult', finalFunctionCallResult);

    if (this._chatCompletions.some((c) => c.usage)) {
      this._emit('totalUsage', this.#calculateTotalUsage());
    }
  }

  #validateParams(params: CompletionCreateParams): void {
    if (params.n != null && params.n > 1) {
      throw new TogetherError(
        'ChatCompletion convenience helpers only support n=1 at this time. To use n>1, please use chat.completions.create() directly.',
      );
    }
  }

  protected async _createChatCompletion(
    completions: Completions,
    params: CompletionCreateParams,
    options?: Core.RequestOptions,
  ): Promise<ChatCompletion> {
    const signal = options?.signal;
    if (signal) {
      if (signal.aborted) this.controller.abort();
      signal.addEventListener('abort', () => this.controller.abort());
    }
    this.#validateParams(params);

    const chatCompletion = await completions.create(
      { ...params, stream: false },
      { ...options, signal: this.controller.signal },
    );
    this._connected();
    return this._addChatCompletion(chatCompletion);
  }

  protected async _runChatCompletion(
    completions: Completions,
    params: CompletionCreateParams,
    options?: Core.RequestOptions,
  ): Promise<ChatCompletion> {
    for (const message of params.messages) {
      this._addMessage(
        // @ts-ignore
        message,
        false,
      );
    }
    return await this._createChatCompletion(completions, params, options);
  }

  protected async _runFunctions<FunctionsArgs extends BaseFunctionsArgs>(
    completions: Completions,
    params:
      | ChatCompletionFunctionRunnerParams<FunctionsArgs>
      | ChatCompletionStreamingFunctionRunnerParams<FunctionsArgs>,
    options?: RunnerOptions,
  ) {
    const role = 'function' as const;
    const { function_call = 'auto', stream, ...restParams } = params;
    const singleFunctionToCall = typeof function_call !== 'string' && function_call?.name;
    const { maxChatCompletions = DEFAULT_MAX_CHAT_COMPLETIONS } = options || {};

    const functionsByName: Record<string, RunnableFunction<any>> = {};
    for (const f of params.functions) {
      functionsByName[f.name || f.function.name] = f;
    }

    // @ts-ignore
    const functions: CompletionCreateParams.Function[] = params.functions.map(
      // @ts-ignore
      (f): CompletionCreateParams.Function => ({
        name: f.name || f.function.name,
        parameters: f.parameters as Record<string, unknown>,
        description: f.description,
      }),
    );

    for (const message of params.messages) {
      this._addMessage(
        // @ts-ignore
        message,
        false,
      );
    }

    for (let i = 0; i < maxChatCompletions; ++i) {
      const chatCompletion: ChatCompletion = await this._createChatCompletion(
        completions,
        {
          ...restParams,
          function_call,
          functions,
          // @ts-ignore
          messages: [...this.messages],
        },
        options,
      );
      const message = chatCompletion.choices[0]?.message;
      if (!message) {
        throw new TogetherError(`missing message in ChatCompletion response`);
      }
      if (!message.function_call) return;
      const { name, arguments: args } = message.function_call;
      const fn = functionsByName[name];
      if (!fn) {
        const content = `Invalid function_call: ${JSON.stringify(name)}. Available options are: ${functions
          .map((f) => JSON.stringify(f.name))
          .join(', ')}. Please try again`;

        this._addMessage({ role, name, content });
        continue;
      } else if (singleFunctionToCall && singleFunctionToCall !== name) {
        const content = `Invalid function_call: ${JSON.stringify(name)}. ${JSON.stringify(
          singleFunctionToCall,
        )} requested. Please try again`;

        this._addMessage({ role, name, content });
        continue;
      }

      let parsed;
      try {
        parsed = isRunnableFunctionWithParse(fn) ? await fn.parse(args) : args;
      } catch (error) {
        this._addMessage({
          role,
          name,
          content: error instanceof Error ? error.message : String(error),
        });
        continue;
      }

      // @ts-expect-error it can't rule out `never` type.
      const rawContent = await fn.function(parsed, this);
      const content = this.#stringifyFunctionCallResult(rawContent);

      this._addMessage({ role, name, content });

      if (singleFunctionToCall) return;
    }
  }

  protected async _runTools<FunctionsArgs extends BaseFunctionsArgs>(
    completions: Completions,
    params:
      | ChatCompletionToolRunnerParams<FunctionsArgs>
      | ChatCompletionStreamingToolRunnerParams<FunctionsArgs>,
    options?: RunnerOptions,
  ) {
    const role = 'tool' as const;
    const { tool_choice = 'auto', stream, ...restParams } = params;
    const singleFunctionToCall = typeof tool_choice !== 'string' && tool_choice?.function?.name;
    const { maxChatCompletions = DEFAULT_MAX_CHAT_COMPLETIONS } = options || {};

    const functionsByName: Record<string, RunnableFunction<any>> = {};
    for (const f of params.tools) {
      if (f.type === 'function') {
        functionsByName[f.function.name || f.function.function.name] = f.function;
      }
    }

    const tools: ChatCompletionTool[] =
      'tools' in params ?
        params.tools.map((t) =>
          t.type === 'function' ?
            {
              type: 'function',
              function: {
                name: t.function.name || t.function.function.name,
                parameters: t.function.parameters as Record<string, unknown>,
                description: t.function.description,
              },
            }
          : (t as unknown as ChatCompletionTool),
        )
      : (undefined as any);

    for (const message of params.messages) {
      this._addMessage(
        // @ts-ignore
        message,
        false,
      );
    }

    for (let i = 0; i < maxChatCompletions; ++i) {
      const chatCompletion: ChatCompletion = await this._createChatCompletion(
        completions,
        {
          ...restParams,
          tool_choice,
          tools,
          // @ts-ignore
          messages: [...this.messages],
        },
        options,
      );
      const message = chatCompletion.choices[0]?.message;
      if (!message) {
        throw new TogetherError(`missing message in ChatCompletion response`);
      }
      if (!message.tool_calls) {
        return;
      }

      for (const tool_call of message.tool_calls) {
        if (tool_call.type !== 'function') continue;
        const tool_call_id = tool_call.id;
        const { name, arguments: args } = tool_call.function;
        const fn = functionsByName[name];

        if (!fn) {
          const content = `Invalid tool_call: ${JSON.stringify(name)}. Available options are: ${tools
            .map((f) => JSON.stringify(f.function.name))
            .join(', ')}. Please try again`;

          this._addMessage({ role, tool_call_id, content });
          continue;
        } else if (singleFunctionToCall && singleFunctionToCall !== name) {
          const content = `Invalid tool_call: ${JSON.stringify(name)}. ${JSON.stringify(
            singleFunctionToCall,
          )} requested. Please try again`;

          this._addMessage({ role, tool_call_id, content });
          continue;
        }

        let parsed;
        try {
          parsed = isRunnableFunctionWithParse(fn) ? await fn.parse(args) : args;
        } catch (error) {
          const content = error instanceof Error ? error.message : String(error);
          this._addMessage({ role, tool_call_id, content });
          continue;
        }

        // @ts-expect-error it can't rule out `never` type.
        const rawContent = await fn.function(parsed, this);
        const content = this.#stringifyFunctionCallResult(rawContent);
        this._addMessage({ role, tool_call_id, content });

        if (singleFunctionToCall) {
          return;
        }
      }
    }

    return;
  }

  #stringifyFunctionCallResult(rawContent: unknown): string {
    return (
      typeof rawContent === 'string' ? rawContent
      : rawContent === undefined ? 'undefined'
      : JSON.stringify(rawContent)
    );
  }
}

type CustomEvents<Event extends string> = {
  [k in Event]: k extends keyof AbstractChatCompletionRunnerEvents ? AbstractChatCompletionRunnerEvents[k]
  : (...args: any[]) => void;
};

type ListenerForEvent<Events extends CustomEvents<any>, Event extends keyof Events> = Event extends (
  keyof AbstractChatCompletionRunnerEvents
) ?
  AbstractChatCompletionRunnerEvents[Event]
: Events[Event];

type ListenersForEvent<Events extends CustomEvents<any>, Event extends keyof Events> = Array<{
  listener: ListenerForEvent<Events, Event>;
  once?: boolean;
}>;
type EventParameters<Events extends CustomEvents<any>, Event extends keyof Events> = Parameters<
  ListenerForEvent<Events, Event>
>;

export interface AbstractChatCompletionRunnerEvents {
  connect: () => void;
  functionCall: (functionCall: ChatCompletionMessage.FunctionCall) => void;
  message: (message: ChatCompletionMessageParam) => void;
  chatCompletion: (completion: ChatCompletion) => void;
  finalContent: (contentSnapshot: string) => void;
  finalMessage: (message: ChatCompletionMessageParam) => void;
  finalChatCompletion: (completion: ChatCompletion) => void;
  finalFunctionCall: (functionCall: ChatCompletionMessage.FunctionCall) => void;
  functionCallResult: (content: string) => void;
  finalFunctionCallResult: (content: string) => void;
  error: (error: TogetherError) => void;
  abort: (error: APIUserAbortError) => void;
  end: () => void;
  totalUsage: (usage: ChatCompletionUsage) => void;
}

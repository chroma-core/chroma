import * as Core from "../core";
import { TogetherError, APIUserAbortError } from "../error";
import {
  Completions,
  type ChatCompletion,
  type ChatCompletionChunk,
  type CompletionCreateParams,
  type CompletionCreateParamsBase,
} from "../resources/chat/completions";
import {
  AbstractChatCompletionRunner,
  type AbstractChatCompletionRunnerEvents,
} from './AbstractChatCompletionRunner';
import { type ReadableStream } from "../_shims/index";
import { Stream } from "../streaming";
import { LogProbs } from "../resources";

export interface ChatCompletionStreamEvents extends AbstractChatCompletionRunnerEvents {
  content: (contentDelta: string, contentSnapshot: string) => void;
  chunk: (chunk: ChatCompletionChunk, snapshot: ChatCompletionSnapshot) => void;
}

export type ChatCompletionStreamParams = Omit<CompletionCreateParamsBase, 'stream'> & {
  stream?: true;
};

export class ChatCompletionStream
  extends AbstractChatCompletionRunner<ChatCompletionStreamEvents>
  implements AsyncIterable<ChatCompletionChunk>
{
  #currentChatCompletionSnapshot: ChatCompletionSnapshot | undefined;

  get currentChatCompletionSnapshot(): ChatCompletionSnapshot | undefined {
    return this.#currentChatCompletionSnapshot;
  }

  /**
   * Intended for use on the frontend, consuming a stream produced with
   * `.toReadableStream()` on the backend.
   *
   * Note that messages sent to the model do not appear in `.on('message')`
   * in this context.
   */
  static fromReadableStream(stream: ReadableStream): ChatCompletionStream {
    const runner = new ChatCompletionStream();
    runner._run(() => runner._fromReadableStream(stream));
    return runner;
  }

  static createChatCompletion(
    completions: Completions,
    params: ChatCompletionStreamParams,
    options?: Core.RequestOptions,
  ): ChatCompletionStream {
    const runner = new ChatCompletionStream();
    runner._run(() =>
      runner._runChatCompletion(
        completions,
        { ...params, stream: true },
        { ...options, headers: { ...options?.headers, 'X-Stainless-Helper-Method': 'stream' } },
      ),
    );
    return runner;
  }

  #beginRequest() {
    if (this.ended) return;
    this.#currentChatCompletionSnapshot = undefined;
  }
  #addChunk(chunk: ChatCompletionChunk) {
    if (this.ended) return;
    const completion = this.#accumulateChatCompletion(chunk);
    this._emit('chunk', chunk, completion);
    const delta = chunk.choices[0]?.delta?.content;
    const snapshot = completion.choices[0]?.message;
    if (delta != null && snapshot?.role === 'assistant' && snapshot?.content) {
      this._emit('content', delta, snapshot.content);
    }
  }
  #endRequest(): ChatCompletion {
    if (this.ended) {
      throw new TogetherError(`stream has ended, this shouldn't happen`);
    }
    const snapshot = this.#currentChatCompletionSnapshot;
    if (!snapshot) {
      throw new TogetherError(`request ended without sending any chunks`);
    }
    this.#currentChatCompletionSnapshot = undefined;
    return finalizeChatCompletion(snapshot);
  }

  protected override async _createChatCompletion(
    completions: Completions,
    params: CompletionCreateParams,
    options?: Core.RequestOptions,
  ): Promise<ChatCompletion> {
    const signal = options?.signal;
    if (signal) {
      if (signal.aborted) this.controller.abort();
      signal.addEventListener('abort', () => this.controller.abort());
    }
    this.#beginRequest();
    const stream = await completions.create(
      { ...params, stream: true },
      { ...options, signal: this.controller.signal },
    );
    this._connected();
    for await (const chunk of stream) {
      this.#addChunk(chunk);
    }
    if (stream.controller.signal?.aborted) {
      throw new APIUserAbortError();
    }
    return this._addChatCompletion(this.#endRequest());
  }

  protected async _fromReadableStream(
    readableStream: ReadableStream,
    options?: Core.RequestOptions,
  ): Promise<ChatCompletion> {
    const signal = options?.signal;
    if (signal) {
      if (signal.aborted) this.controller.abort();
      signal.addEventListener('abort', () => this.controller.abort());
    }
    this.#beginRequest();
    this._connected();
    const stream = Stream.fromReadableStream<ChatCompletionChunk>(readableStream, this.controller);
    let chatId;
    for await (const chunk of stream) {
      if (chatId && chatId !== chunk.id) {
        // A new request has been made.
        this._addChatCompletion(this.#endRequest());
      }

      this.#addChunk(chunk);
      chatId = chunk.id;
    }
    if (stream.controller.signal?.aborted) {
      throw new APIUserAbortError();
    }
    return this._addChatCompletion(this.#endRequest());
  }

  #accumulateChatCompletion(chunk: ChatCompletionChunk): ChatCompletionSnapshot {
    let snapshot = this.#currentChatCompletionSnapshot;
    const { choices, ...rest } = chunk;
    if (!snapshot) {
      snapshot = this.#currentChatCompletionSnapshot = {
        ...rest,
        choices: [],
      };
    } else {
      Object.assign(snapshot, rest);
    }

    for (const { delta, finish_reason, index, logprobs = null, ...other } of chunk.choices) {
      let choice = snapshot.choices[index];
      if (!choice) {
        choice = snapshot.choices[index] = {
          finish_reason,
          index,
          message: {},
          logprobs: { token_ids: [], token_logprobs: [], tokens: [] },
          ...other,
        };
      }

      if (logprobs) {
        console.log({ logprobs });
        if (!choice.logprobs) {
          choice.logprobs = { token_ids: [], token_logprobs: [], tokens: [] };
        }
        choice.logprobs.token_ids ??= [];
        choice.logprobs.token_ids.push(delta.token_id ?? null);
        choice.logprobs.token_logprobs ??= [];
        choice.logprobs.token_logprobs.push(logprobs ?? null);
        choice.logprobs.tokens ??= [];
        choice.logprobs.tokens.push(delta.content ?? null);
      }

      if (finish_reason) choice.finish_reason = finish_reason;
      Object.assign(choice, other);

      if (!delta) continue; // Shouldn't happen; just in case.
      const { content, function_call, role, tool_calls, ...rest } = delta;
      Object.assign(choice.message, rest);

      if (content) choice.message.content = (choice.message.content || '') + content;
      if (role) choice.message.role = role;
      if (function_call) {
        if (!choice.message.function_call) {
          choice.message.function_call = function_call;
        } else {
          if (function_call.name) choice.message.function_call.name = function_call.name;
          if (function_call.arguments) {
            choice.message.function_call.arguments ??= '';
            choice.message.function_call.arguments += function_call.arguments;
          }
        }
      }
      if (tool_calls) {
        if (!choice.message.tool_calls) choice.message.tool_calls = [];
        for (const { index, id, type, function: fn, ...rest } of tool_calls) {
          const tool_call = (choice.message.tool_calls[index] ??= {});
          Object.assign(tool_call, rest);
          if (id) tool_call.id = id;
          if (type) tool_call.type = type;
          if (fn) tool_call.function ??= { arguments: '' };
          if (fn?.name) tool_call.function!.name = fn.name;
          if (fn?.arguments) tool_call.function!.arguments += fn.arguments;
        }
      }
    }
    return snapshot;
  }

  [Symbol.asyncIterator](): AsyncIterator<ChatCompletionChunk> {
    const pushQueue: ChatCompletionChunk[] = [];
    const readQueue: {
      resolve: (chunk: ChatCompletionChunk | undefined) => void;
      reject: (err: unknown) => void;
    }[] = [];
    let done = false;

    this.on('chunk', (chunk) => {
      const reader = readQueue.shift();
      if (reader) {
        reader.resolve(chunk);
      } else {
        pushQueue.push(chunk);
      }
    });

    this.on('end', () => {
      done = true;
      for (const reader of readQueue) {
        reader.resolve(undefined);
      }
      readQueue.length = 0;
    });

    this.on('abort', (err) => {
      done = true;
      for (const reader of readQueue) {
        reader.reject(err);
      }
      readQueue.length = 0;
    });

    this.on('error', (err) => {
      done = true;
      for (const reader of readQueue) {
        reader.reject(err);
      }
      readQueue.length = 0;
    });

    return {
      next: async (): Promise<IteratorResult<ChatCompletionChunk>> => {
        if (!pushQueue.length) {
          if (done) {
            return { value: undefined, done: true };
          }
          return new Promise<ChatCompletionChunk | undefined>((resolve, reject) =>
            readQueue.push({ resolve, reject }),
          ).then((chunk) => (chunk ? { value: chunk, done: false } : { value: undefined, done: true }));
        }
        const chunk = pushQueue.shift()!;
        return { value: chunk, done: false };
      },
      return: async () => {
        this.abort();
        return { value: undefined, done: true };
      },
    };
  }

  toReadableStream(): ReadableStream {
    const stream = new Stream(this[Symbol.asyncIterator].bind(this), this.controller);
    return stream.toReadableStream();
  }
}

function finalizeChatCompletion(snapshot: ChatCompletionSnapshot): ChatCompletion {
  const { id, choices, created, model, system_fingerprint, ...rest } = snapshot;
  return {
    ...rest,
    id,
    choices: choices.map(
      ({ message, finish_reason, index, logprobs, ...choiceRest }): ChatCompletion.Choice => {
        if (!finish_reason) throw new TogetherError(`missing finish_reason for choice ${index}`);
        const { content = null, function_call, tool_calls, ...messageRest } = message;
        const role = message.role as 'assistant'; // this is what we expect; in theory it could be different which would make our types a slight lie but would be fine.
        if (!role) throw new TogetherError(`missing role for choice ${index}`);
        if (function_call) {
          const { arguments: args, name } = function_call;
          if (args == null) throw new TogetherError(`missing function_call.arguments for choice ${index}`);
          if (!name) throw new TogetherError(`missing function_call.name for choice ${index}`);
          return {
            ...choiceRest,
            message: { content, function_call: { arguments: args, name }, role },
            finish_reason,
            index,
            logprobs,
          };
        }
        if (tool_calls) {
          return {
            ...choiceRest,
            index,
            finish_reason,
            logprobs,
            message: {
              ...messageRest,
              role,
              content,
              // @ts-ignore
              tool_calls: tool_calls.map((tool_call, i) => {
                const { function: fn, type, id, ...toolRest } = tool_call;
                const { arguments: args, name, ...fnRest } = fn || {};
                if (id == null)
                  throw new TogetherError(`missing choices[${index}].tool_calls[${i}].id\n${str(snapshot)}`);
                if (type == null)
                  throw new TogetherError(
                    `missing choices[${index}].tool_calls[${i}].type\n${str(snapshot)}`,
                  );
                if (name == null)
                  throw new TogetherError(
                    `missing choices[${index}].tool_calls[${i}].function.name\n${str(snapshot)}`,
                  );
                if (args == null)
                  throw new TogetherError(
                    `missing choices[${index}].tool_calls[${i}].function.arguments\n${str(snapshot)}`,
                  );

                return { ...toolRest, id, type, function: { ...fnRest, name, arguments: args } };
              }),
            },
          };
        }
        return {
          ...choiceRest,
          message: { ...messageRest, content, role },
          finish_reason,
          index,
          logprobs,
        };
      },
    ),
    created,
    model,
    object: 'chat.completion',
    ...(system_fingerprint ? { system_fingerprint } : {}),
  };
}

function str(x: unknown) {
  return JSON.stringify(x);
}

/**
 * Represents a streamed chunk of a chat completion response returned by model,
 * based on the provided input.
 */
export interface ChatCompletionSnapshot {
  /**
   * A unique identifier for the chat completion.
   */
  id: string;

  /**
   * A list of chat completion choices. Can be more than one if `n` is greater
   * than 1.
   */
  choices: Array<ChatCompletionSnapshot.Choice>;

  /**
   * The Unix timestamp (in seconds) of when the chat completion was created.
   */
  created: number;

  /**
   * The model to generate the completion.
   */
  model: string;

  // Note we do not include an "object" type on the snapshot,
  // because the object is not a valid "chat.completion" until finalized.
  // object: 'chat.completion';

  /**
   * This fingerprint represents the backend configuration that the model runs with.
   *
   * Can be used in conjunction with the `seed` request parameter to understand when
   * backend changes have been made that might impact determinism.
   */
  system_fingerprint?: string;
}

export namespace ChatCompletionSnapshot {
  export interface Choice {
    /**
     * A chat completion delta generated by streamed model responses.
     */
    message: Choice.Message;

    /**
     * The reason the model stopped generating tokens. This will be `stop` if the model
     * hit a natural stop point or a provided stop sequence, `length` if the maximum
     * number of tokens specified in the request was reached, `content_filter` if
     * content was omitted due to a flag from our content filters, or `function_call`
     * if the model called a function.
     */
    finish_reason: ChatCompletion.Choice['finish_reason'] | null;

    /**
     * Log probability information for the choice.
     */
    logprobs: LogProbs | null;

    /**
     * The index of the choice in the list of choices.
     */
    index: number;
  }

  export namespace Choice {
    /**
     * A chat completion delta generated by streamed model responses.
     */
    export interface Message {
      /**
       * The contents of the chunk message.
       */
      content?: string | null;

      /**
       * The name and arguments of a function that should be called, as generated by the
       * model.
       */
      function_call?: Message.FunctionCall;

      tool_calls?: Array<Message.ToolCall>;

      /**
       * The role of the author of this message.
       */
      role?: 'system' | 'user' | 'assistant' | 'function' | 'tool';
    }

    export namespace Message {
      export interface ToolCall {
        /**
         * The ID of the tool call.
         */
        id?: string;

        function?: ToolCall.Function;

        /**
         * The type of the tool.
         */
        type?: 'function';
      }

      export namespace ToolCall {
        export interface Function {
          /**
           * The arguments to call the function with, as generated by the model in JSON
           * format. Note that the model does not always generate valid JSON, and may
           * hallucinate parameters not defined by your function schema. Validate the
           * arguments in your code before calling your function.
           */
          arguments?: string;

          /**
           * The name of the function to call.
           */
          name?: string;
        }
      }

      /**
       * The name and arguments of a function that should be called, as generated by the
       * model.
       */
      export interface FunctionCall {
        /**
         * The arguments to call the function with, as generated by the model in JSON
         * format. Note that the model does not always generate valid JSON, and may
         * hallucinate parameters not defined by your function schema. Validate the
         * arguments in your code before calling your function.
         */
        arguments?: string;

        /**
         * The name of the function to call.
         */
        name?: string;
      }
    }
  }
}

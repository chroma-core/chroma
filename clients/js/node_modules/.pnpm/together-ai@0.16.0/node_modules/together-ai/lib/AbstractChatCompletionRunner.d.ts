                              
import * as Core from 'together-ai/core';
import { type Completions, type ChatCompletion, type ChatCompletionMessage, type ChatCompletionMessageParam, type CompletionCreateParams } from 'together-ai/resources/chat/completions';
import { APIUserAbortError, TogetherError } from 'together-ai/error';
import { type BaseFunctionsArgs } from "./RunnableFunction.js";
import { ChatCompletionFunctionRunnerParams, ChatCompletionToolRunnerParams } from "./ChatCompletionRunner.js";
import { ChatCompletionStreamingFunctionRunnerParams, ChatCompletionStreamingToolRunnerParams } from "./ChatCompletionStreamingRunner.js";
import { ChatCompletionUsage } from 'together-ai/resources/chat/completions';
export interface RunnerOptions extends Core.RequestOptions {
    /** How many requests to make before canceling. Default 10. */
    maxChatCompletions?: number;
}
export declare abstract class AbstractChatCompletionRunner<Events extends CustomEvents<any> = AbstractChatCompletionRunnerEvents> {
    #private;
    controller: AbortController;
    protected _chatCompletions: ChatCompletion[];
    messages: ChatCompletionMessageParam[];
    constructor();
    protected _run(executor: () => Promise<any>): void;
    protected _addChatCompletion(chatCompletion: ChatCompletion): ChatCompletion;
    protected _addMessage(message: ChatCompletionMessageParam, emit?: boolean): void;
    protected _connected(): void;
    get ended(): boolean;
    get errored(): boolean;
    get aborted(): boolean;
    abort(): void;
    /**
     * Adds the listener function to the end of the listeners array for the event.
     * No checks are made to see if the listener has already been added. Multiple calls passing
     * the same combination of event and listener will result in the listener being added, and
     * called, multiple times.
     * @returns this ChatCompletionStream, so that calls can be chained
     */
    on<Event extends keyof Events>(event: Event, listener: ListenerForEvent<Events, Event>): this;
    /**
     * Removes the specified listener from the listener array for the event.
     * off() will remove, at most, one instance of a listener from the listener array. If any single
     * listener has been added multiple times to the listener array for the specified event, then
     * off() must be called multiple times to remove each instance.
     * @returns this ChatCompletionStream, so that calls can be chained
     */
    off<Event extends keyof Events>(event: Event, listener: ListenerForEvent<Events, Event>): this;
    /**
     * Adds a one-time listener function for the event. The next time the event is triggered,
     * this listener is removed and then invoked.
     * @returns this ChatCompletionStream, so that calls can be chained
     */
    once<Event extends keyof Events>(event: Event, listener: ListenerForEvent<Events, Event>): this;
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
    emitted<Event extends keyof Events>(event: Event): Promise<EventParameters<Events, Event> extends [infer Param] ? Param : EventParameters<Events, Event> extends [] ? void : EventParameters<Events, Event>>;
    done(): Promise<void>;
    /**
     * @returns a promise that resolves with the final ChatCompletion, or rejects
     * if an error occurred or the stream ended prematurely without producing a ChatCompletion.
     */
    finalChatCompletion(): Promise<ChatCompletion>;
    /**
     * @returns a promise that resolves with the content of the final ChatCompletionMessage, or rejects
     * if an error occurred or the stream ended prematurely without producing a ChatCompletionMessage.
     */
    finalContent(): Promise<string | null>;
    /**
     * @returns a promise that resolves with the the final assistant ChatCompletionMessage response,
     * or rejects if an error occurred or the stream ended prematurely without producing a ChatCompletionMessage.
     */
    finalMessage(): Promise<ChatCompletionMessage>;
    /**
     * @returns a promise that resolves with the content of the final FunctionCall, or rejects
     * if an error occurred or the stream ended prematurely without producing a ChatCompletionMessage.
     */
    finalFunctionCall(): Promise<ChatCompletionMessage.FunctionCall | undefined>;
    finalFunctionCallResult(): Promise<string | undefined>;
    totalUsage(): Promise<ChatCompletionUsage>;
    allChatCompletions(): ChatCompletion[];
    protected _emit<Event extends keyof Events>(event: Event, ...args: EventParameters<Events, Event>): void;
    protected _emitFinal(): void;
    protected _createChatCompletion(completions: Completions, params: CompletionCreateParams, options?: Core.RequestOptions): Promise<ChatCompletion>;
    protected _runChatCompletion(completions: Completions, params: CompletionCreateParams, options?: Core.RequestOptions): Promise<ChatCompletion>;
    protected _runFunctions<FunctionsArgs extends BaseFunctionsArgs>(completions: Completions, params: ChatCompletionFunctionRunnerParams<FunctionsArgs> | ChatCompletionStreamingFunctionRunnerParams<FunctionsArgs>, options?: RunnerOptions): Promise<void>;
    protected _runTools<FunctionsArgs extends BaseFunctionsArgs>(completions: Completions, params: ChatCompletionToolRunnerParams<FunctionsArgs> | ChatCompletionStreamingToolRunnerParams<FunctionsArgs>, options?: RunnerOptions): Promise<void>;
}
type CustomEvents<Event extends string> = {
    [k in Event]: k extends keyof AbstractChatCompletionRunnerEvents ? AbstractChatCompletionRunnerEvents[k] : (...args: any[]) => void;
};
type ListenerForEvent<Events extends CustomEvents<any>, Event extends keyof Events> = Event extends (keyof AbstractChatCompletionRunnerEvents) ? AbstractChatCompletionRunnerEvents[Event] : Events[Event];
type EventParameters<Events extends CustomEvents<any>, Event extends keyof Events> = Parameters<ListenerForEvent<Events, Event>>;
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
export {};
//# sourceMappingURL=AbstractChatCompletionRunner.d.ts.map
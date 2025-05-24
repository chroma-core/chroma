import { type ChatCompletionAssistantMessageParam, type ChatCompletionFunctionMessageParam, type ChatCompletionMessageParam, type ChatCompletionToolMessageParam } from 'together-ai/resources/chat';
export declare const isAssistantMessage: (message: ChatCompletionMessageParam | null | undefined) => message is ChatCompletionAssistantMessageParam;
export declare const isFunctionMessage: (message: ChatCompletionMessageParam | null | undefined) => message is ChatCompletionFunctionMessageParam;
export declare const isToolMessage: (message: ChatCompletionMessageParam | null | undefined) => message is ChatCompletionToolMessageParam;
export declare function isPresent<T>(obj: T | null | undefined): obj is T;
//# sourceMappingURL=chatCompletionUtils.d.ts.map
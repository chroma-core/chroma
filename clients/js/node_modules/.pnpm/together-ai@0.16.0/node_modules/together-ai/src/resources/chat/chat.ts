// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import { APIResource } from '../../resource';
import * as CompletionsAPI from './completions';
import {
  ChatCompletion,
  ChatCompletionAssistantMessageParam,
  ChatCompletionChunk,
  ChatCompletionFunctionMessageParam,
  ChatCompletionMessage,
  ChatCompletionMessageParam,
  ChatCompletionSystemMessageParam,
  ChatCompletionTool,
  ChatCompletionToolMessageParam,
  ChatCompletionStructuredMessageImageURL,
  ChatCompletionStructuredMessageText,
  ChatCompletionUsage,
  ChatCompletionUserMessageParam,
  CompletionCreateParams,
  CompletionCreateParamsNonStreaming,
  CompletionCreateParamsStreaming,
  Completions,
} from './completions';

export class Chat extends APIResource {
  completions: CompletionsAPI.Completions = new CompletionsAPI.Completions(this._client);
}

Chat.Completions = Completions;

export declare namespace Chat {
  export {
    Completions as Completions,
    type ChatCompletion as ChatCompletion,
    type ChatCompletionAssistantMessageParam as ChatCompletionAssistantMessageParam,
    type ChatCompletionChunk as ChatCompletionChunk,
    type ChatCompletionFunctionMessageParam as ChatCompletionFunctionMessageParam,
    type ChatCompletionMessage as ChatCompletionMessage,
    type ChatCompletionMessageParam as ChatCompletionMessageParam,
    type ChatCompletionSystemMessageParam as ChatCompletionSystemMessageParam,
    type ChatCompletionTool as ChatCompletionTool,
    type ChatCompletionToolMessageParam as ChatCompletionToolMessageParam,
    type ChatCompletionStructuredMessageImageURL as ChatCompletionStructuredMessageImageURL,
    type ChatCompletionStructuredMessageText as ChatCompletionStructuredMessageText,
    type ChatCompletionUsage as ChatCompletionUsage,
    type ChatCompletionUserMessageParam as ChatCompletionUserMessageParam,
    type CompletionCreateParams as CompletionCreateParams,
    type CompletionCreateParamsNonStreaming as CompletionCreateParamsNonStreaming,
    type CompletionCreateParamsStreaming as CompletionCreateParamsStreaming,
  };
}

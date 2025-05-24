import { type ParsedResponse, type ResponseCreateParamsBase, type ResponseStreamEvent } from "../../resources/responses/responses.js";
import * as Core from "../../core.js";
import OpenAI from "../../index.js";
import { type BaseEvents, EventStream } from "../EventStream.js";
import { type ResponseFunctionCallArgumentsDeltaEvent, type ResponseTextDeltaEvent } from "./EventTypes.js";
export type ResponseStreamParams = Omit<ResponseCreateParamsBase, 'stream'> & {
    stream?: true;
};
type ResponseEvents = BaseEvents & Omit<{
    [K in ResponseStreamEvent['type']]: (event: Extract<ResponseStreamEvent, {
        type: K;
    }>) => void;
}, 'response.output_text.delta' | 'response.function_call_arguments.delta'> & {
    event: (event: ResponseStreamEvent) => void;
    'response.output_text.delta': (event: ResponseTextDeltaEvent) => void;
    'response.function_call_arguments.delta': (event: ResponseFunctionCallArgumentsDeltaEvent) => void;
};
export type ResponseStreamingParams = Omit<ResponseCreateParamsBase, 'stream'> & {
    stream?: true;
};
export declare class ResponseStream<ParsedT = null> extends EventStream<ResponseEvents> implements AsyncIterable<ResponseStreamEvent> {
    #private;
    constructor(params: ResponseStreamingParams | null);
    static createResponse<ParsedT>(client: OpenAI, params: ResponseStreamParams, options?: Core.RequestOptions): ResponseStream<ParsedT>;
    protected _createResponse(client: OpenAI, params: ResponseStreamingParams, options?: Core.RequestOptions): Promise<ParsedResponse<ParsedT>>;
    [Symbol.asyncIterator](this: ResponseStream<ParsedT>): AsyncIterator<ResponseStreamEvent>;
    /**
     * @returns a promise that resolves with the final Response, or rejects
     * if an error occurred or the stream ended prematurely without producing a REsponse.
     */
    finalResponse(): Promise<ParsedResponse<ParsedT>>;
}
export {};
//# sourceMappingURL=ResponseStream.d.ts.map
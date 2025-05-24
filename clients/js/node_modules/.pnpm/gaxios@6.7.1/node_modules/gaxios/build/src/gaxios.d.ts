import { Agent } from 'http';
import { URL } from 'url';
import { GaxiosOptions, GaxiosPromise, GaxiosResponse } from './common';
import { GaxiosInterceptorManager } from './interceptor';
export declare class Gaxios {
    #private;
    protected agentCache: Map<string | URL, Agent | ((parsedUrl: URL) => Agent)>;
    /**
     * Default HTTP options that will be used for every HTTP request.
     */
    defaults: GaxiosOptions;
    /**
     * Interceptors
     */
    interceptors: {
        request: GaxiosInterceptorManager<GaxiosOptions>;
        response: GaxiosInterceptorManager<GaxiosResponse>;
    };
    /**
     * The Gaxios class is responsible for making HTTP requests.
     * @param defaults The default set of options to be used for this instance.
     */
    constructor(defaults?: GaxiosOptions);
    /**
     * Perform an HTTP request with the given options.
     * @param opts Set of HTTP options that will be used for this HTTP request.
     */
    request<T = any>(opts?: GaxiosOptions): GaxiosPromise<T>;
    private _defaultAdapter;
    /**
     * Internal, retryable version of the `request` method.
     * @param opts Set of HTTP options that will be used for this HTTP request.
     */
    protected _request<T = any>(opts?: GaxiosOptions): GaxiosPromise<T>;
    private getResponseData;
    /**
     * By default, throw for any non-2xx status code
     * @param status status code from the HTTP response
     */
    private validateStatus;
    /**
     * Encode a set of key/value pars into a querystring format (?foo=bar&baz=boo)
     * @param params key value pars to encode
     */
    private paramsSerializer;
    private translateResponse;
    /**
     * Attempts to parse a response by looking at the Content-Type header.
     * @param {FetchResponse} response the HTTP response.
     * @returns {Promise<any>} a promise that resolves to the response data.
     */
    private getResponseDataFromContentType;
    /**
     * Creates an async generator that yields the pieces of a multipart/related request body.
     * This implementation follows the spec: https://www.ietf.org/rfc/rfc2387.txt. However, recursive
     * multipart/related requests are not currently supported.
     *
     * @param {GaxioMultipartOptions[]} multipartOptions the pieces to turn into a multipart/related body.
     * @param {string} boundary the boundary string to be placed between each part.
     */
    private getMultipartRequest;
}

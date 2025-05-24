import { GaxiosOptions } from './common';
import { Gaxios } from './gaxios';
export { GaxiosError, GaxiosPromise, GaxiosResponse, Headers, RetryConfig, } from './common';
export { Gaxios, GaxiosOptions };
export * from './interceptor';
/**
 * The default instance used when the `request` method is directly
 * invoked.
 */
export declare const instance: Gaxios;
/**
 * Make an HTTP request using the given options.
 * @param opts Options for the request
 */
export declare function request<T>(opts: GaxiosOptions): Promise<import("./common").GaxiosResponse<T>>;

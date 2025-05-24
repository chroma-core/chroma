import { GaxiosError, GaxiosOptions, GaxiosResponse } from './common';
/**
 * Interceptors that can be run for requests or responses. These interceptors run asynchronously.
 */
export interface GaxiosInterceptor<T extends GaxiosOptions | GaxiosResponse> {
    /**
     * Function to be run when applying an interceptor.
     *
     * @param {T} configOrResponse The current configuration or response.
     * @returns {Promise<T>} Promise that resolves to the modified set of options or response.
     */
    resolved?: (configOrResponse: T) => Promise<T>;
    /**
     * Function to be run if the previous call to resolved throws / rejects or the request results in an invalid status
     * as determined by the call to validateStatus.
     *
     * @param {GaxiosError} err The error thrown from the previously called resolved function.
     */
    rejected?: (err: GaxiosError) => void;
}
/**
 * Class to manage collections of GaxiosInterceptors for both requests and responses.
 */
export declare class GaxiosInterceptorManager<T extends GaxiosOptions | GaxiosResponse> extends Set<GaxiosInterceptor<T> | null> {
}

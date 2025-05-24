import { GaxiosError } from './common';
export declare function getRetryConfig(err: GaxiosError): Promise<{
    shouldRetry: boolean;
    config?: undefined;
} | {
    shouldRetry: boolean;
    config: import("./common").GaxiosOptions;
}>;

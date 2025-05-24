/**
 * A read-only object containing information about the APIs available in the current environment.
 */
export const apis: Readonly<{
    /** Whether we are running in a browser environment (and not a web worker) */
    IS_BROWSER_ENV: boolean;
    /** Whether we are running in a web worker environment */
    IS_WEBWORKER_ENV: boolean;
    /** Whether the Cache API is available */
    IS_WEB_CACHE_AVAILABLE: boolean;
    /** Whether the WebGPU API is available */
    IS_WEBGPU_AVAILABLE: boolean;
    /** Whether the WebNN API is available */
    IS_WEBNN_AVAILABLE: boolean;
    /** Whether the Node.js process API is available */
    IS_PROCESS_AVAILABLE: boolean;
    /** Whether we are running in a Node.js environment */
    IS_NODE_ENV: boolean;
    /** Whether the filesystem API is available */
    IS_FS_AVAILABLE: boolean;
    /** Whether the path API is available */
    IS_PATH_AVAILABLE: boolean;
}>;
/**
 * Global variable given visible to users to control execution. This provides users a simple way to configure Transformers.js.
 * @typedef {Object} TransformersEnvironment
 * @property {string} version This version of Transformers.js.
 * @property {{onnx: Partial<import('onnxruntime-common').Env>}} backends Expose environment variables of different backends,
 * allowing users to set these variables if they want to.
 * @property {boolean} allowRemoteModels Whether to allow loading of remote files, defaults to `true`.
 * If set to `false`, it will have the same effect as setting `local_files_only=true` when loading pipelines, models, tokenizers, processors, etc.
 * @property {string} remoteHost Host URL to load models from. Defaults to the Hugging Face Hub.
 * @property {string} remotePathTemplate Path template to fill in and append to `remoteHost` when loading models.
 * @property {boolean} allowLocalModels Whether to allow loading of local files, defaults to `false` if running in-browser, and `true` otherwise.
 * If set to `false`, it will skip the local file check and try to load the model from the remote host.
 * @property {string} localModelPath Path to load local models from. Defaults to `/models/`.
 * @property {boolean} useFS Whether to use the file system to load files. By default, it is `true` if available.
 * @property {boolean} useBrowserCache Whether to use Cache API to cache models. By default, it is `true` if available.
 * @property {boolean} useFSCache Whether to use the file system to cache files. By default, it is `true` if available.
 * @property {string} cacheDir The directory to use for caching files with the file system. By default, it is `./.cache`.
 * @property {boolean} useCustomCache Whether to use a custom cache system (defined by `customCache`), defaults to `false`.
 * @property {Object} customCache The custom cache to use. Defaults to `null`. Note: this must be an object which
 * implements the `match` and `put` functions of the Web Cache API. For more information, see https://developer.mozilla.org/en-US/docs/Web/API/Cache.
 * If you wish, you may also return a `Promise<string>` from the `match` function if you'd like to use a file path instead of `Promise<Response>`.
 */
/** @type {TransformersEnvironment} */
export const env: TransformersEnvironment;
/**
 * Global variable given visible to users to control execution. This provides users a simple way to configure Transformers.js.
 */
export type TransformersEnvironment = {
    /**
     * This version of Transformers.js.
     */
    version: string;
    /**
     * Expose environment variables of different backends,
     * allowing users to set these variables if they want to.
     */
    backends: {
        onnx: Partial<import("onnxruntime-common").Env>;
    };
    /**
     * Whether to allow loading of remote files, defaults to `true`.
     * If set to `false`, it will have the same effect as setting `local_files_only=true` when loading pipelines, models, tokenizers, processors, etc.
     */
    allowRemoteModels: boolean;
    /**
     * Host URL to load models from. Defaults to the Hugging Face Hub.
     */
    remoteHost: string;
    /**
     * Path template to fill in and append to `remoteHost` when loading models.
     */
    remotePathTemplate: string;
    /**
     * Whether to allow loading of local files, defaults to `false` if running in-browser, and `true` otherwise.
     * If set to `false`, it will skip the local file check and try to load the model from the remote host.
     */
    allowLocalModels: boolean;
    /**
     * Path to load local models from. Defaults to `/models/`.
     */
    localModelPath: string;
    /**
     * Whether to use the file system to load files. By default, it is `true` if available.
     */
    useFS: boolean;
    /**
     * Whether to use Cache API to cache models. By default, it is `true` if available.
     */
    useBrowserCache: boolean;
    /**
     * Whether to use the file system to cache files. By default, it is `true` if available.
     */
    useFSCache: boolean;
    /**
     * The directory to use for caching files with the file system. By default, it is `./.cache`.
     */
    cacheDir: string;
    /**
     * Whether to use a custom cache system (defined by `customCache`), defaults to `false`.
     */
    useCustomCache: boolean;
    /**
     * The custom cache to use. Defaults to `null`. Note: this must be an object which
     * implements the `match` and `put` functions of the Web Cache API. For more information, see https://developer.mozilla.org/en-US/docs/Web/API/Cache.
     * If you wish, you may also return a `Promise<string>` from the `match` function if you'd like to use a file path instead of `Promise<Response>`.
     */
    customCache: any;
};
//# sourceMappingURL=env.d.ts.map
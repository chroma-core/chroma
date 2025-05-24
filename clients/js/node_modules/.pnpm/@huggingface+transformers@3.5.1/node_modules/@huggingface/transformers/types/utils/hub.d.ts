/**
 * Helper function to get a file, using either the Fetch API or FileSystem API.
 *
 * @param {URL|string} urlOrPath The URL/path of the file to get.
 * @returns {Promise<FileResponse|Response>} A promise that resolves to a FileResponse object (if the file is retrieved using the FileSystem API), or a Response object (if the file is retrieved using the Fetch API).
 */
export function getFile(urlOrPath: URL | string): Promise<FileResponse | Response>;
/**
 * Retrieves a file from either a remote URL using the Fetch API or from the local file system using the FileSystem API.
 * If the filesystem is available and `env.useCache = true`, the file will be downloaded and cached.
 *
 * @param {string} path_or_repo_id This can be either:
 * - a string, the *model id* of a model repo on huggingface.co.
 * - a path to a *directory* potentially containing the file.
 * @param {string} filename The name of the file to locate in `path_or_repo`.
 * @param {boolean} [fatal=true] Whether to throw an error if the file is not found.
 * @param {PretrainedOptions} [options] An object containing optional parameters.
 * @param {boolean} [return_path=false] Whether to return the path of the file instead of the file content.
 *
 * @throws Will throw an error if the file is not found and `fatal` is true.
 * @returns {Promise<string|Uint8Array>} A Promise that resolves with the file content as a Uint8Array if `return_path` is false, or the file path as a string if `return_path` is true.
 */
export function getModelFile(path_or_repo_id: string, filename: string, fatal?: boolean, options?: PretrainedOptions, return_path?: boolean): Promise<string | Uint8Array>;
/**
 * Fetches a JSON file from a given path and file name.
 *
 * @param {string} modelPath The path to the directory containing the file.
 * @param {string} fileName The name of the file to fetch.
 * @param {boolean} [fatal=true] Whether to throw an error if the file is not found.
 * @param {PretrainedOptions} [options] An object containing optional parameters.
 * @returns {Promise<Object>} The JSON data parsed into a JavaScript object.
 * @throws Will throw an error if the file is not found and `fatal` is true.
 */
export function getModelJSON(modelPath: string, fileName: string, fatal?: boolean, options?: PretrainedOptions): Promise<any>;
/**
 * @typedef {boolean|number} ExternalData Whether to load the model using the external data format (used for models >= 2GB in size).
 * If `true`, the model will be loaded using the external data format.
 * If a number, this many chunks will be loaded using the external data format (of the form: "model.onnx_data[_{chunk_number}]").
 */
export const MAX_EXTERNAL_DATA_CHUNKS: 100;
/**
 * Whether to load the model using the external data format (used for models >= 2GB in size).
 * If `true`, the model will be loaded using the external data format.
 * If a number, this many chunks will be loaded using the external data format (of the form: "model.onnx_data[_{chunk_number}]").
 */
export type ExternalData = boolean | number;
/**
 * Options for loading a pretrained model.
 */
export type PretrainedOptions = {
    /**
     * If specified, this function will be called during model construction, to provide the user with progress updates.
     */
    progress_callback?: import("./core.js").ProgressCallback;
    /**
     * Configuration for the model to use instead of an automatically loaded configuration. Configuration can be automatically loaded when:
     * - The model is a model provided by the library (loaded with the *model id* string of a pretrained model).
     * - The model is loaded by supplying a local directory as `pretrained_model_name_or_path` and a configuration JSON file named *config.json* is found in the directory.
     */
    config?: import("../configs.js").PretrainedConfig;
    /**
     * Path to a directory in which a downloaded pretrained model configuration should be cached if the standard cache should not be used.
     */
    cache_dir?: string;
    /**
     * Whether or not to only look at local files (e.g., not try downloading the model).
     */
    local_files_only?: boolean;
    /**
     * The specific model version to use. It can be a branch name, a tag name, or a commit id,
     * since we use a git-based system for storing models and other artifacts on huggingface.co, so `revision` can be any identifier allowed by git.
     * NOTE: This setting is ignored for local requests.
     */
    revision?: string;
};
/**
 * Options for loading a pretrained model.
 */
export type ModelSpecificPretrainedOptions = {
    /**
     * In case the relevant files are located inside a subfolder of the model repo on huggingface.co,
     * you can specify the folder name here.
     */
    subfolder?: string;
    /**
     * If specified, load the model with this name (excluding the .onnx suffix). Currently only valid for encoder- or decoder-only models.
     */
    model_file_name?: string;
    /**
     * The device to run the model on. If not specified, the device will be chosen from the environment settings.
     */
    device?: import("./devices.js").DeviceType | Record<string, import("./devices.js").DeviceType>;
    /**
     * The data type to use for the model. If not specified, the data type will be chosen from the environment settings.
     */
    dtype?: import("./dtypes.js").DataType | Record<string, import("./dtypes.js").DataType>;
    /**
     * Whether to load the model using the external data format (used for models >= 2GB in size).
     */
    use_external_data_format?: ExternalData | Record<string, ExternalData>;
    /**
     * (Optional) User-specified session options passed to the runtime. If not provided, suitable defaults will be chosen.
     */
    session_options?: import("onnxruntime-common").InferenceSession.SessionOptions;
};
/**
 * Options for loading a pretrained model.
 */
export type PretrainedModelOptions = PretrainedOptions & ModelSpecificPretrainedOptions;
declare class FileResponse {
    /**
     * Creates a new `FileResponse` object.
     * @param {string} filePath
     */
    constructor(filePath: string);
    filePath: string;
    headers: Headers;
    exists: boolean;
    status: number;
    statusText: string;
    body: ReadableStream<any>;
    /**
     * Updates the 'content-type' header property of the response based on the extension of
     * the file specified by the filePath property of the current object.
     * @returns {void}
     */
    updateContentType(): void;
    /**
     * Clone the current FileResponse object.
     * @returns {FileResponse} A new FileResponse object with the same properties as the current object.
     */
    clone(): FileResponse;
    /**
     * Reads the contents of the file specified by the filePath property and returns a Promise that
     * resolves with an ArrayBuffer containing the file's contents.
     * @returns {Promise<ArrayBuffer>} A Promise that resolves with an ArrayBuffer containing the file's contents.
     * @throws {Error} If the file cannot be read.
     */
    arrayBuffer(): Promise<ArrayBuffer>;
    /**
     * Reads the contents of the file specified by the filePath property and returns a Promise that
     * resolves with a Blob containing the file's contents.
     * @returns {Promise<Blob>} A Promise that resolves with a Blob containing the file's contents.
     * @throws {Error} If the file cannot be read.
     */
    blob(): Promise<Blob>;
    /**
     * Reads the contents of the file specified by the filePath property and returns a Promise that
     * resolves with a string containing the file's contents.
     * @returns {Promise<string>} A Promise that resolves with a string containing the file's contents.
     * @throws {Error} If the file cannot be read.
     */
    text(): Promise<string>;
    /**
     * Reads the contents of the file specified by the filePath property and returns a Promise that
     * resolves with a parsed JavaScript object containing the file's contents.
     *
     * @returns {Promise<Object>} A Promise that resolves with a parsed JavaScript object containing the file's contents.
     * @throws {Error} If the file cannot be read.
     */
    json(): Promise<any>;
}
export {};
//# sourceMappingURL=hub.d.ts.map
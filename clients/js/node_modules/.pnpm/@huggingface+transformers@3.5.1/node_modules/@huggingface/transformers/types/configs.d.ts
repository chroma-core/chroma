/**
 *
 * @param {PretrainedConfig} config
 * @returns {Record<string, number[]>}
 */
export function getKeyValueShapes(config: PretrainedConfig, { prefix, batch_size, }?: {
    prefix?: string;
    batch_size?: number;
}): Record<string, number[]>;
/**
 * Base class for all configuration classes. For more information, see the corresponding
 * [Python documentation](https://huggingface.co/docs/transformers/main/en/main_classes/configuration#transformers.PretrainedConfig).
 */
export class PretrainedConfig {
    /**
     * Loads a pre-trained config from the given `pretrained_model_name_or_path`.
     *
     * @param {string} pretrained_model_name_or_path The path to the pre-trained config.
     * @param {PretrainedOptions} options Additional options for loading the config.
     * @throws {Error} Throws an error if the config.json is not found in the `pretrained_model_name_or_path`.
     *
     * @returns {Promise<PretrainedConfig>} A new instance of the `PretrainedConfig` class.
     */
    static from_pretrained(pretrained_model_name_or_path: string, { progress_callback, config, cache_dir, local_files_only, revision, }?: PretrainedOptions): Promise<PretrainedConfig>;
    /**
     * Create a new PreTrainedTokenizer instance.
     * @param {Object} configJSON The JSON of the config.
     */
    constructor(configJSON: any);
    /** @type {string|null} */
    model_type: string | null;
    /** @type {boolean} */
    is_encoder_decoder: boolean;
    /** @type {number} */
    max_position_embeddings: number;
    /** @type {TransformersJSConfig} */
    'transformers.js_config': TransformersJSConfig;
    normalized_config: any;
}
/**
 * Helper class which is used to instantiate pretrained configs with the `from_pretrained` function.
 *
 * @example
 * const config = await AutoConfig.from_pretrained('Xenova/bert-base-uncased');
 */
export class AutoConfig {
    /**
     * Loads a pre-trained config from the given `pretrained_model_name_or_path`.
     *
     * @param {string} pretrained_model_name_or_path The path to the pre-trained config.
     * @param {PretrainedOptions} options Additional options for loading the config.
     * @throws {Error} Throws an error if the config.json is not found in the `pretrained_model_name_or_path`.
     *
     * @returns {Promise<PretrainedConfig>} A new instance of the `PretrainedConfig` class.
     */
    static from_pretrained(pretrained_model_name_or_path: string, { progress_callback, config, cache_dir, local_files_only, revision, }?: PretrainedOptions): Promise<PretrainedConfig>;
}
export type PretrainedOptions = import("./utils/hub.js").PretrainedOptions;
export type ProgressCallback = import("./utils/core.js").ProgressCallback;
export type ProgressInfo = import("./utils/core.js").ProgressInfo;
/**
 * Transformers.js-specific configuration, possibly present in config.json under the key `transformers.js_config`.
 */
export type TransformersJSConfig = {
    /**
     * Device-specific configurations.
     */
    device_config?: Record<import("./utils/devices.js").DeviceType, DeviceConfig>;
    /**
     * The data type of the key-value cache.
     */
    kv_cache_dtype?: import("./utils/tensor.js").DataType | Record<import("./utils/dtypes.js").DataType, import("./utils/tensor.js").DataType>;
    /**
     * Override the free dimensions of the model.
     * See https://onnxruntime.ai/docs/tutorials/web/env-flags-and-session-options.html#freedimensionoverrides
     * for more information.
     */
    free_dimension_overrides?: Record<string, number>;
    /**
     * The default device to use for the model.
     */
    device?: import("./utils/devices.js").DeviceType;
    /**
     * The default data type to use for the model.
     */
    dtype?: import("./utils/dtypes.js").DataType | Record<string, import("./utils/dtypes.js").DataType>;
    /**
     * Whether to load the model using the external data format (used for models >= 2GB in size).
     */
    use_external_data_format?: import("./utils/hub.js").ExternalData | Record<string, import("./utils/hub.js").ExternalData>;
};
/**
 * Device-specific configuration options.
 */
export type DeviceConfig = Omit<TransformersJSConfig, "device" | "device_config">;
//# sourceMappingURL=configs.d.ts.map
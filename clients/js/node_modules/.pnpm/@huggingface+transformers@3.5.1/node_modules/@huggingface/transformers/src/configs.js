
/**
 * @file Helper module for using model configs. For more information, see the corresponding
 * [Python documentation](https://huggingface.co/docs/transformers/main/en/model_doc/auto#transformers.AutoConfig).
 * 
 * **Example:** Load an `AutoConfig`.
 * 
 * ```javascript
 * import { AutoConfig } from '@huggingface/transformers';
 * const config = await AutoConfig.from_pretrained('bert-base-uncased');
 * console.log(config);
 * // PretrainedConfig {
 * //   "model_type": "bert",
 * //   "is_encoder_decoder": false,
 * //   "architectures": [
 * //       "BertForMaskedLM"
 * //   ],
 * //   "vocab_size": 30522
 * //   "num_attention_heads": 12,
 * //   "num_hidden_layers": 12,
 * //   "hidden_size": 768,
 * //   "max_position_embeddings": 512,
 * //   ...
 * // }
 * ```
 * 
 * @module configs
 */

import { pick } from './utils/core.js';
import {
    getModelJSON,
} from './utils/hub.js';

/**
 * @typedef {import('./utils/hub.js').PretrainedOptions} PretrainedOptions
 */

/**
 * @typedef {import('./utils/core.js').ProgressCallback} ProgressCallback
 */

/**
 * @typedef {import('./utils/core.js').ProgressInfo} ProgressInfo
 */

/**
 * Loads a config from the specified path.
 * @param {string} pretrained_model_name_or_path The path to the config directory.
 * @param {PretrainedOptions} options Additional options for loading the config.
 * @returns {Promise<Object>} A promise that resolves with information about the loaded config.
 */
async function loadConfig(pretrained_model_name_or_path, options) {
    return await getModelJSON(pretrained_model_name_or_path, 'config.json', true, options);
}

/**
 * 
 * @param {PretrainedConfig} config 
 * @returns {Object} The normalized configuration.
 */
function getNormalizedConfig(config) {
    const mapping = {};

    let init_normalized_config = {};
    switch (config.model_type) {
        // Sub-configs
        case 'llava':
        case 'paligemma':
        case 'gemma3':
        case 'florence2':
        case 'llava_onevision':
        case 'idefics3':
        case 'ultravox':
        case 'smolvlm':
            // @ts-expect-error TS2339
            init_normalized_config = getNormalizedConfig(config.text_config);
            break;
        case 'moondream1':
            // @ts-expect-error TS2339
            init_normalized_config = getNormalizedConfig(config.phi_config);
            break;
        case 'musicgen':
            // @ts-expect-error TS2339
            init_normalized_config = getNormalizedConfig(config.decoder);
            break;
        case 'multi_modality':
            // @ts-expect-error TS2339
            init_normalized_config = getNormalizedConfig(config.language_config);
            break;

        // Decoder-only models
        case 'gpt2':
        case 'gptj':
        case 'jais':
        case 'codegen':
        case 'gpt_bigcode':
            mapping['num_heads'] = 'n_head';
            mapping['num_layers'] = 'n_layer';
            mapping['hidden_size'] = 'n_embd';
            break;
        case 'gpt_neox':
        case 'stablelm':
        case 'opt':
        case 'falcon':
            mapping['num_heads'] = 'num_attention_heads';
            mapping['num_layers'] = 'num_hidden_layers';
            mapping['hidden_size'] = 'hidden_size';
            break;
        case 'llama':
        case 'olmo':
        case 'olmo2':
        case 'mobilellm':
        case 'granite':
        case 'cohere':
        case 'mistral':
        case 'starcoder2':
        case 'qwen2':
        case 'qwen2_vl':
        case 'phi':
        case 'phi3':
        case 'phi3_v':
            mapping['num_heads'] = 'num_key_value_heads';
            mapping['num_layers'] = 'num_hidden_layers';
            mapping['hidden_size'] = 'hidden_size';
            mapping['num_attention_heads'] = 'num_attention_heads';
            break;
        case 'qwen3':
        case 'gemma':
        case 'gemma2':
        case 'gemma3_text':
        case 'glm':
        case 'helium':
            mapping['num_heads'] = 'num_key_value_heads';
            mapping['num_layers'] = 'num_hidden_layers';
            mapping['dim_kv'] = 'head_dim';
            break;
        case 'openelm':
            mapping['num_heads'] = 'num_kv_heads';
            mapping['num_layers'] = 'num_transformer_layers';
            mapping['dim_kv'] = 'head_dim';
            break;
        case 'gpt_neo':
        case 'donut-swin':
            mapping['num_heads'] = 'num_heads';
            mapping['num_layers'] = 'num_layers';
            mapping['hidden_size'] = 'hidden_size';
            break;
        case 'bloom':
            mapping['num_heads'] = 'n_head';
            mapping['num_layers'] = 'n_layer';
            mapping['hidden_size'] = 'hidden_size';
            break;
        case 'mpt':
            mapping['num_heads'] = 'n_heads';
            mapping['num_layers'] = 'n_layers';
            mapping['hidden_size'] = 'd_model';
            break;
        case 'exaone':
            mapping['num_heads'] = 'num_key_value_heads';
            mapping['num_layers'] = 'num_layers';
            mapping['dim_kv'] = 'head_dim';
            mapping['num_attention_heads'] = 'num_attention_heads';
            break;

        // Encoder-decoder models
        case 't5':
        case 'mt5':
        case 'longt5':
            mapping['num_decoder_layers'] = 'num_decoder_layers';
            mapping['num_decoder_heads'] = 'num_heads';
            mapping['decoder_dim_kv'] = 'd_kv';
            mapping['num_encoder_layers'] = 'num_layers';
            mapping['num_encoder_heads'] = 'num_heads';
            mapping['encoder_dim_kv'] = 'd_kv';
            break;
        case 'bart':
        case 'mbart':
        case 'marian':
        case 'whisper':
        case 'lite-whisper':
        case 'm2m_100':
        case 'blenderbot':
        case 'blenderbot-small':
        case 'florence2_language':
            mapping['num_decoder_layers'] = 'decoder_layers';
            mapping['num_decoder_heads'] = 'decoder_attention_heads';
            mapping['decoder_hidden_size'] = 'd_model';
            mapping['num_encoder_layers'] = 'encoder_layers';
            mapping['num_encoder_heads'] = 'encoder_attention_heads';
            mapping['encoder_hidden_size'] = 'd_model';
            break;
        case 'speecht5':
            mapping['num_decoder_layers'] = 'decoder_layers';
            mapping['num_decoder_heads'] = 'decoder_attention_heads';
            mapping['decoder_hidden_size'] = 'hidden_size';
            mapping['num_encoder_layers'] = 'encoder_layers';
            mapping['num_encoder_heads'] = 'encoder_attention_heads';
            mapping['encoder_hidden_size'] = 'hidden_size';
            break;
        case 'trocr':
            mapping['num_encoder_layers'] = mapping['num_decoder_layers'] = 'decoder_layers';
            mapping['num_encoder_heads'] = mapping['num_decoder_heads'] = 'decoder_attention_heads';
            mapping['encoder_hidden_size'] = mapping['decoder_hidden_size'] = 'd_model';
            break;
        case 'musicgen_decoder':
            mapping['num_encoder_layers'] = mapping['num_decoder_layers'] = 'num_hidden_layers';
            mapping['num_encoder_heads'] = mapping['num_decoder_heads'] = 'num_attention_heads';
            mapping['encoder_hidden_size'] = mapping['decoder_hidden_size'] = 'hidden_size';
            break;
        case 'moonshine':
            mapping['num_decoder_layers'] = 'decoder_num_hidden_layers';
            mapping['num_decoder_heads'] = 'decoder_num_key_value_heads';
            mapping['num_encoder_layers'] = 'encoder_num_hidden_layers';
            mapping['num_encoder_heads'] = 'encoder_num_key_value_heads';
            mapping['encoder_hidden_size'] = mapping['decoder_hidden_size'] = 'hidden_size';
            break;
        case 'vision-encoder-decoder':
            // @ts-expect-error TS2339
            const decoderConfig = getNormalizedConfig(config.decoder);

            const add_encoder_pkv = 'num_decoder_layers' in decoderConfig;
            const result = pick(config, ['model_type', 'is_encoder_decoder']);
            if (add_encoder_pkv) {
                // Decoder is part of an encoder-decoder model
                result.num_decoder_layers = decoderConfig.num_decoder_layers;
                result.num_decoder_heads = decoderConfig.num_decoder_heads;
                result.decoder_hidden_size = decoderConfig.decoder_hidden_size;

                result.num_encoder_layers = decoderConfig.num_encoder_layers;
                result.num_encoder_heads = decoderConfig.num_encoder_heads;
                result.encoder_hidden_size = decoderConfig.encoder_hidden_size;
            } else {
                // Decoder is a decoder-only model
                result.num_layers = decoderConfig.num_layers;
                result.num_heads = decoderConfig.num_heads;
                result.hidden_size = decoderConfig.hidden_size;
            }
            return result;

    }

    // NOTE: If `num_attention_heads` is not set, it is assumed to be equal to `num_heads`
    const normalized_config = {
        ...init_normalized_config,
        ...pick(config, ['model_type', 'multi_query', 'is_encoder_decoder']),
    };
    for (const key in mapping) {
        normalized_config[key] = config[mapping[key]];
    }
    return normalized_config;
}

/**
 * 
 * @param {PretrainedConfig} config 
 * @returns {Record<string, number[]>}
 */
export function getKeyValueShapes(config, {
    prefix = 'past_key_values',
    batch_size=1,
} = {}) {
    /** @type {Record<string, number[]>} */
    const decoderFeeds = {};
    const normalized_config = config.normalized_config;

    if (normalized_config.is_encoder_decoder && (
        'num_encoder_heads' in normalized_config && 'num_decoder_heads' in normalized_config
    )) {
        const encoder_dim_kv = normalized_config.encoder_dim_kv ?? (
            normalized_config.encoder_hidden_size / normalized_config.num_encoder_heads
        );
        const decoder_dim_kv = normalized_config.decoder_dim_kv ?? (
            normalized_config.decoder_hidden_size / normalized_config.num_decoder_heads
        );

        const encoder_dims = [batch_size, normalized_config.num_encoder_heads, 0, encoder_dim_kv];
        const decoder_dims = [batch_size, normalized_config.num_decoder_heads, 0, decoder_dim_kv];
        for (let i = 0; i < normalized_config.num_decoder_layers; ++i) {
            decoderFeeds[`${prefix}.${i}.encoder.key`] = encoder_dims;
            decoderFeeds[`${prefix}.${i}.encoder.value`] = encoder_dims;
            decoderFeeds[`${prefix}.${i}.decoder.key`] = decoder_dims;
            decoderFeeds[`${prefix}.${i}.decoder.value`] = decoder_dims;
        }
    } else { // Decoders
        const num_heads = normalized_config.num_heads;
        const num_layers = normalized_config.num_layers;
        const dim_kv = normalized_config.dim_kv ?? (
            normalized_config.hidden_size /
            (normalized_config.num_attention_heads ?? num_heads)
        );

        if (normalized_config.model_type === 'falcon') {
            // NOTE: Custom implementation for Falcon
            const dims = [batch_size * num_heads, 0, dim_kv]
            for (let i = 0; i < num_layers; ++i) {
                decoderFeeds[`${prefix}.${i}.key`] = dims;
                decoderFeeds[`${prefix}.${i}.value`] = dims;
            }
        } else if (normalized_config.multi_query) { // e.g., for `gpt_bigcode`
            const dims = [batch_size * num_heads, 0, 2 * dim_kv]

            for (let i = 0; i < num_layers; ++i) {
                decoderFeeds[`${prefix}.${i}.key_value`] = dims;
            }
        } else if (normalized_config.model_type === 'bloom') {
            // NOTE: Custom implementation for Bloom

            const keyDims = [batch_size * num_heads, dim_kv, 0] // [batch_size x num_heads,64,past_sequence_length]
            const valueDims = [batch_size * num_heads, 0, dim_kv] // [batch_size x num_heads,past_sequence_length,64]
            for (let i = 0; i < num_layers; ++i) {
                decoderFeeds[`${prefix}.${i}.key`] = keyDims;
                decoderFeeds[`${prefix}.${i}.value`] = valueDims;
            }
        } else if (normalized_config.model_type === 'openelm') {
            for (let i = 0; i < num_layers; ++i) {
                const dims = [batch_size, num_heads[i], 0, dim_kv]

                decoderFeeds[`${prefix}.${i}.key`] = dims;
                decoderFeeds[`${prefix}.${i}.value`] = dims;
            }
        } else { // Decoder-only
            const dims = [batch_size, num_heads, 0, dim_kv]
            for (let i = 0; i < num_layers; ++i) {
                decoderFeeds[`${prefix}.${i}.key`] = dims;
                decoderFeeds[`${prefix}.${i}.value`] = dims;
            }
        }
    }

    return decoderFeeds;
}
/**
 * Base class for all configuration classes. For more information, see the corresponding
 * [Python documentation](https://huggingface.co/docs/transformers/main/en/main_classes/configuration#transformers.PretrainedConfig).
 */
export class PretrainedConfig {
    // NOTE: Typo in original

    /** @type {string|null} */
    model_type = null;

    /** @type {boolean} */
    is_encoder_decoder = false;

    /** @type {number} */
    max_position_embeddings;

    /** @type {TransformersJSConfig} */
    'transformers.js_config';

    /**
     * Create a new PreTrainedTokenizer instance.
     * @param {Object} configJSON The JSON of the config.
     */
    constructor(configJSON) {
        Object.assign(this, configJSON);
        this.normalized_config = getNormalizedConfig(this);
    }

    /**
     * Loads a pre-trained config from the given `pretrained_model_name_or_path`. 
     * 
     * @param {string} pretrained_model_name_or_path The path to the pre-trained config.
     * @param {PretrainedOptions} options Additional options for loading the config.
     * @throws {Error} Throws an error if the config.json is not found in the `pretrained_model_name_or_path`.
     * 
     * @returns {Promise<PretrainedConfig>} A new instance of the `PretrainedConfig` class.
     */
    static async from_pretrained(pretrained_model_name_or_path, {
        progress_callback = null,
        config = null,
        cache_dir = null,
        local_files_only = false,
        revision = 'main',
    } = {}) {
        if (config && !(config instanceof PretrainedConfig)) {
            config = new PretrainedConfig(config);
        }

        const data = config ?? await loadConfig(pretrained_model_name_or_path, {
            progress_callback,
            config,
            cache_dir,
            local_files_only,
            revision,
        })
        return new this(data);
    }
}

/**
 * Helper class which is used to instantiate pretrained configs with the `from_pretrained` function.
 * 
 * @example
 * const config = await AutoConfig.from_pretrained('Xenova/bert-base-uncased'); 
 */
export class AutoConfig {
    /** @type {typeof PretrainedConfig.from_pretrained} */
    static async from_pretrained(...args) {
        return PretrainedConfig.from_pretrained(...args);
    }
}

/**
 * Transformers.js-specific configuration, possibly present in config.json under the key `transformers.js_config`.
 * @typedef {Object} TransformersJSConfig
 * @property {Record<import('./utils/devices.js').DeviceType, DeviceConfig>} [device_config] Device-specific configurations.
 * @property {import('./utils/tensor.js').DataType|Record<import('./utils/dtypes.js').DataType, import('./utils/tensor.js').DataType>} [kv_cache_dtype] The data type of the key-value cache.
 * @property {Record<string, number>} [free_dimension_overrides] Override the free dimensions of the model.
 * See https://onnxruntime.ai/docs/tutorials/web/env-flags-and-session-options.html#freedimensionoverrides
 * for more information.
 * @property {import('./utils/devices.js').DeviceType} [device] The default device to use for the model.
 * @property {import('./utils/dtypes.js').DataType|Record<string, import('./utils/dtypes.js').DataType>} [dtype] The default data type to use for the model.
 * @property {import('./utils/hub.js').ExternalData|Record<string, import('./utils/hub.js').ExternalData>} [use_external_data_format=false] Whether to load the model using the external data format (used for models >= 2GB in size).
 */

/**
 * Device-specific configuration options.
 * @typedef {Omit<TransformersJSConfig, "device" | "device_config">} DeviceConfig
 */

declare const Processor_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 * @typedef {Object} ProcessorProperties Additional processor-specific properties.
 * @typedef {import('../utils/hub.js').PretrainedOptions & ProcessorProperties} PretrainedProcessorOptions
 * @typedef {import('../tokenizers.js').PreTrainedTokenizer} PreTrainedTokenizer
 */
/**
 * Represents a Processor that extracts features from an input.
 */
export class Processor extends Processor_base {
    static classes: string[];
    static uses_processor_config: boolean;
    /**
     * Instantiate one of the processor classes of the library from a pretrained model.
     *
     * The processor class to instantiate is selected based on the `image_processor_type` (or `feature_extractor_type`; legacy)
     * property of the config object (either passed as an argument or loaded from `pretrained_model_name_or_path` if possible)
     *
     * @param {string} pretrained_model_name_or_path The name or path of the pretrained model. Can be either:
     * - A string, the *model id* of a pretrained processor hosted inside a model repo on huggingface.co.
     *   Valid model ids can be located at the root-level, like `bert-base-uncased`, or namespaced under a
     *   user or organization name, like `dbmdz/bert-base-german-cased`.
     * - A path to a *directory* containing processor files, e.g., `./my_model_directory/`.
     * @param {PretrainedProcessorOptions} options Additional options for loading the processor.
     *
     * @returns {Promise<Processor>} A new instance of the Processor class.
     */
    static from_pretrained(pretrained_model_name_or_path: string, options: PretrainedProcessorOptions): Promise<Processor>;
    /**
     * Creates a new Processor with the given components
     * @param {Object} config
     * @param {Record<string, Object>} components
     */
    constructor(config: any, components: Record<string, any>);
    config: any;
    components: Record<string, any>;
    /**
     * @returns {import('./image_processors_utils.js').ImageProcessor|undefined} The image processor of the processor, if it exists.
     */
    get image_processor(): import("./image_processors_utils.js").ImageProcessor | undefined;
    /**
     * @returns {PreTrainedTokenizer|undefined} The tokenizer of the processor, if it exists.
     */
    get tokenizer(): PreTrainedTokenizer | undefined;
    /**
     * @returns {import('./feature_extraction_utils.js').FeatureExtractor|undefined} The feature extractor of the processor, if it exists.
     */
    get feature_extractor(): import("./feature_extraction_utils.js").FeatureExtractor | undefined;
    /**
     * @param {Parameters<PreTrainedTokenizer['apply_chat_template']>[0]} messages
     * @param {Parameters<PreTrainedTokenizer['apply_chat_template']>[1]} options
     * @returns {ReturnType<PreTrainedTokenizer['apply_chat_template']>}
     */
    apply_chat_template(messages: Parameters<PreTrainedTokenizer["apply_chat_template"]>[0], options?: Parameters<PreTrainedTokenizer["apply_chat_template"]>[1]): ReturnType<PreTrainedTokenizer["apply_chat_template"]>;
    /**
     * @param {Parameters<PreTrainedTokenizer['batch_decode']>} args
     * @returns {ReturnType<PreTrainedTokenizer['batch_decode']>}
     */
    batch_decode(batch: number[][] | import("../transformers.js").Tensor, decode_args?: any): ReturnType<PreTrainedTokenizer["batch_decode"]>;
    /**
     * @param {Parameters<PreTrainedTokenizer['decode']>} args
     * @returns {ReturnType<PreTrainedTokenizer['decode']>}
     */
    decode(token_ids: number[] | bigint[] | import("../transformers.js").Tensor, decode_args?: {
        skip_special_tokens?: boolean;
        clean_up_tokenization_spaces?: boolean;
    }): ReturnType<PreTrainedTokenizer["decode"]>;
    /**
     * Calls the feature_extractor function with the given input.
     * @param {any} input The input to extract features from.
     * @param {...any} args Additional arguments.
     * @returns {Promise<any>} A Promise that resolves with the extracted features.
     */
    _call(input: any, ...args: any[]): Promise<any>;
}
/**
 * Additional processor-specific properties.
 */
export type ProcessorProperties = any;
export type PretrainedProcessorOptions = import("../utils/hub.js").PretrainedOptions & ProcessorProperties;
export type PreTrainedTokenizer = import("../tokenizers.js").PreTrainedTokenizer;
export {};
//# sourceMappingURL=processing_utils.d.ts.map
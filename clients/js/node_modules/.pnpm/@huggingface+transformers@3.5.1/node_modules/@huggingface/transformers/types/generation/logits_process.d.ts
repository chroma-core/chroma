declare const LogitsProcessor_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 * Abstract base class for all logit processors that can be applied during generation.
 */
export class LogitsProcessor extends LogitsProcessor_base {
    /**
     * Apply the processor to the input logits.
     *
     * @abstract
     * @param {bigint[][]} input_ids The input ids.
     * @param {Tensor} logits The logits to process.
     * @throws {Error} Throws an error if `_call` is not implemented in the subclass.
     */
    _call(input_ids: bigint[][], logits: Tensor): void;
}
declare const LogitsWarper_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 * Abstract base class for all logit warpers that can be applied during generation with multinomial sampling.
 */
export class LogitsWarper extends LogitsWarper_base {
    /**
     * Apply the processor to the input logits.
     *
     * @abstract
     * @param {bigint[][]} input_ids The input ids.
     * @param {Tensor} logits The logits to process.
     * @throws {Error} Throws an error if `_call` is not implemented in the subclass.
     */
    _call(input_ids: bigint[][], logits: Tensor): void;
}
declare const LogitsProcessorList_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 * A class representing a list of logits processors. A logits processor is a function that modifies the logits
 * output of a language model. This class provides methods for adding new processors and applying all processors to a
 * batch of logits.
 */
export class LogitsProcessorList extends LogitsProcessorList_base {
    processors: any[];
    /**
     * Adds a new logits processor to the list.
     *
     * @param {LogitsProcessor} item The logits processor function to add.
     */
    push(item: LogitsProcessor): void;
    /**
     * Adds multiple logits processors to the list.
     *
     * @param {LogitsProcessor[]} items The logits processor functions to add.
     */
    extend(items: LogitsProcessor[]): void;
    /**
     * Applies all logits processors in the list to a batch of logits, modifying them in-place.
     *
     * @param {bigint[][]} input_ids The input IDs for the language model.
     * @param {Tensor} logits
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
    [Symbol.iterator](): ArrayIterator<any>;
}
/**
 * A LogitsProcessor that forces a BOS token at the beginning of the generated sequence.
 */
export class ForcedBOSTokenLogitsProcessor extends LogitsProcessor {
    /**
     * Create a ForcedBOSTokenLogitsProcessor.
     * @param {number} bos_token_id The ID of the beginning-of-sequence token to be forced.
     */
    constructor(bos_token_id: number);
    bos_token_id: number;
    /**
     * Apply the BOS token forcing to the logits.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The logits with BOS token forcing.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * A logits processor that enforces the specified token as the last generated token when `max_length` is reached.
 */
export class ForcedEOSTokenLogitsProcessor extends LogitsProcessor {
    /**
     * Create a ForcedEOSTokenLogitsProcessor.
     * @param {number} max_length The maximum length of the sequence to be generated.
     * @param {number|number[]} eos_token_id The id(s) of the *end-of-sequence* token.
     */
    constructor(max_length: number, eos_token_id: number | number[]);
    max_length: number;
    eos_token_id: number[];
    /**
     * Apply the processor to input_ids and logits.
     *
     * @param {bigint[][]} input_ids The input ids.
     * @param {Tensor} logits The logits tensor.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * A LogitsProcessor that suppresses a list of tokens as soon as the `generate` function starts
 * generating using `begin_index` tokens. This should ensure that the tokens defined by
 * `begin_suppress_tokens` at not sampled at the begining of the generation.
 */
export class SuppressTokensAtBeginLogitsProcessor extends LogitsProcessor {
    /**
     * Create a SuppressTokensAtBeginLogitsProcessor.
     * @param {number[]} begin_suppress_tokens The IDs of the tokens to suppress.
     * @param {number} begin_index The number of tokens to generate before suppressing tokens.
     */
    constructor(begin_suppress_tokens: number[], begin_index: number);
    begin_suppress_tokens: number[];
    begin_index: number;
    /**
     * Apply the BOS token forcing to the logits.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The logits with BOS token forcing.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * A LogitsProcessor that handles adding timestamps to generated text.
 */
export class WhisperTimeStampLogitsProcessor extends LogitsProcessor {
    /**
     * Constructs a new WhisperTimeStampLogitsProcessor.
     * @param {import('../models/whisper/generation_whisper.js').WhisperGenerationConfig} generate_config The config object passed to the `generate()` method of a transformer model.
     * @param {number[]} init_tokens The initial tokens of the input sequence.
     */
    constructor(generate_config: import("../models/whisper/generation_whisper.js").WhisperGenerationConfig, init_tokens: number[]);
    eos_token_id: number;
    no_timestamps_token_id: number;
    timestamp_begin: number;
    begin_index: number;
    max_initial_timestamp_index: number;
    /**
     * Modify the logits to handle timestamp tokens.
     * @param {bigint[][]} input_ids The input sequence of tokens.
     * @param {Tensor} logits The logits output by the model.
     * @returns {Tensor} The modified logits.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * A logits processor that disallows ngrams of a certain size to be repeated.
 */
export class NoRepeatNGramLogitsProcessor extends LogitsProcessor {
    /**
     * Create a NoRepeatNGramLogitsProcessor.
     * @param {number} no_repeat_ngram_size The no-repeat-ngram size. All ngrams of this size can only occur once.
     */
    constructor(no_repeat_ngram_size: number);
    no_repeat_ngram_size: number;
    /**
     * Generate n-grams from a sequence of token ids.
     * @param {bigint[]} prevInputIds List of previous input ids
     * @returns {Map<string, number[]>} Map of generated n-grams
     */
    getNgrams(prevInputIds: bigint[]): Map<string, number[]>;
    /**
     * Generate n-grams from a sequence of token ids.
     * @param {Map<string, number[]>} bannedNgrams Map of banned n-grams
     * @param {bigint[]} prevInputIds List of previous input ids
     * @returns {number[]} Map of generated n-grams
     */
    getGeneratedNgrams(bannedNgrams: Map<string, number[]>, prevInputIds: bigint[]): number[];
    /**
     * Calculate banned n-gram tokens
     * @param {bigint[]} prevInputIds List of previous input ids
     * @returns {number[]} Map of generated n-grams
     */
    calcBannedNgramTokens(prevInputIds: bigint[]): number[];
    /**
     * Apply the no-repeat-ngram processor to the logits.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The logits with no-repeat-ngram processing.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * A logits processor that prevents the repetition of previous tokens through a penalty.
 * This penalty is applied at most once per token. Note that, for decoder-only models like most LLMs,
 * the considered tokens include the prompt.
 *
 * In the original [paper](https://arxiv.org/pdf/1909.05858.pdf), the authors suggest the use of a
 * penalty of around 1.2 to achieve a good balance between truthful generation and lack of repetition.
 * To penalize and reduce repetition, use `penalty` values above 1.0, where a higher value penalizes
 * more strongly. To reward and encourage repetition, use `penalty` values between 0.0 and 1.0, where
 * a lower value rewards more strongly.
 */
export class RepetitionPenaltyLogitsProcessor extends LogitsProcessor {
    /**
     * Create a RepetitionPenaltyLogitsProcessor.
     * @param {number} penalty The parameter for repetition penalty.
     * - 1.0 means no penalty. Above 1.0 penalizes previously generated tokens.
     * - Between 0.0 and 1.0 rewards previously generated tokens.
     */
    constructor(penalty: number);
    penalty: number;
    /**
     * Apply the repetition penalty to the logits.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The logits with repetition penalty processing.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * A logits processor that enforces a minimum number of tokens.
 */
export class MinLengthLogitsProcessor extends LogitsProcessor {
    /**
     * Create a MinLengthLogitsProcessor.
     * @param {number} min_length The minimum length below which the score of `eos_token_id` is set to negative infinity.
     * @param {number|number[]} eos_token_id The ID/IDs of the end-of-sequence token.
     */
    constructor(min_length: number, eos_token_id: number | number[]);
    min_length: number;
    eos_token_id: number[];
    /**
     * Apply logit processor.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The processed logits.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * A logits processor that enforces a minimum number of new tokens.
 */
export class MinNewTokensLengthLogitsProcessor extends LogitsProcessor {
    /**
     * Create a MinNewTokensLengthLogitsProcessor.
     * @param {number} prompt_length_to_skip The input tokens length.
     * @param {number} min_new_tokens The minimum *new* tokens length below which the score of `eos_token_id` is set to negative infinity.
     * @param {number|number[]} eos_token_id The ID/IDs of the end-of-sequence token.
     */
    constructor(prompt_length_to_skip: number, min_new_tokens: number, eos_token_id: number | number[]);
    prompt_length_to_skip: number;
    min_new_tokens: number;
    eos_token_id: number[];
    /**
     * Apply logit processor.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The processed logits.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
export class NoBadWordsLogitsProcessor extends LogitsProcessor {
    /**
     * Create a `NoBadWordsLogitsProcessor`.
     * @param {number[][]} bad_words_ids List of list of token ids that are not allowed to be generated.
     * @param {number|number[]} eos_token_id The id of the *end-of-sequence* token. Optionally, use a list to set multiple *end-of-sequence* tokens.
     */
    constructor(bad_words_ids: number[][], eos_token_id: number | number[]);
    bad_words_ids: number[][];
    eos_token_id: number[];
    /**
     * Apply logit processor.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The processed logits.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * [`LogitsProcessor`] for classifier free guidance (CFG). The scores are split over the batch dimension,
 * where the first half correspond to the conditional logits (predicted from the input prompt) and the second half
 * correspond to the unconditional logits (predicted from an empty or 'null' prompt). The processor computes a
 * weighted average across the conditional and unconditional logits, parameterised by the `guidance_scale`.
 *
 * See [the paper](https://arxiv.org/abs/2306.05284) for more information.
 */
export class ClassifierFreeGuidanceLogitsProcessor extends LogitsProcessor {
    /**
     * Create a `ClassifierFreeGuidanceLogitsProcessor`.
     * @param {number} guidance_scale The guidance scale for classifier free guidance (CFG). CFG is enabled by setting `guidance_scale > 1`.
     * Higher guidance scale encourages the model to generate samples that are more closely linked to the input
     * prompt, usually at the expense of poorer quality.
     */
    constructor(guidance_scale: number);
    guidance_scale: number;
    /**
     * Apply logit processor.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The processed logits.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * [`LogitsWarper`] for temperature (exponential scaling output probability distribution), which effectively means
 * that it can control the randomness of the predicted tokens. Often used together with [`TopPLogitsWarper`] and [`TopKLogitsWarper`].
 */
export class TemperatureLogitsWarper extends LogitsWarper {
    /**
     * Create a `TemperatureLogitsWarper`.
     * @param {number} temperature Strictly positive float value used to modulate the logits distribution.
     * A value smaller than `1` decreases randomness (and vice versa), with `0` being equivalent to shifting
     * all probability mass to the most likely token.
     */
    constructor(temperature: number);
    temperature: number;
    /**
     * Apply logit warper.
     * @param {bigint[][]} input_ids The input IDs.
     * @param {Tensor} logits The logits.
     * @returns {Tensor} The processed logits.
     */
    _call(input_ids: bigint[][], logits: Tensor): Tensor;
}
/**
 * [`LogitsWarper`] that performs top-p, i.e. restricting to top tokens summing to prob_cut_off <= prob_cut_off.
 * Often used together with [`TemperatureLogitsWarper`] and [`TopKLogitsWarper`].
 */
export class TopPLogitsWarper extends LogitsWarper {
    /**
     * Create a `TopPLogitsWarper`.
     * @param {number} top_p If set to < 1, only the smallest set of most probable tokens with
     * probabilities that add up to `top_p` or higher are kept for generation.
     * @param {Object} options Additional options for the top-p sampling.
     * @param {number} [options.filter_value=-Infinity] All filtered values will be set to this float value.
     * @param {number} [options.min_tokens_to_keep=1] Minimum number of tokens that cannot be filtered.
     */
    constructor(top_p: number, { filter_value, min_tokens_to_keep, }?: {
        filter_value?: number;
        min_tokens_to_keep?: number;
    });
    top_p: number;
    filter_value: number;
    min_tokens_to_keep: number;
}
/**
 * [`LogitsWarper`] that performs top-k, i.e. restricting to the k highest probability elements.
 * Often used together with [`TemperatureLogitsWarper`] and [`TopPLogitsWarper`].
 */
export class TopKLogitsWarper extends LogitsWarper {
    /**
     * Create a `TopKLogitsWarper`.
     * @param {number} top_k If set to > 0, only the top `top_k` tokens are kept for generation.
     * @param {Object} options Additional options for the top-k sampling.
     * @param {number} [options.filter_value=-Infinity] All filtered values will be set to this float value.
     * @param {number} [options.min_tokens_to_keep=1] Minimum number of tokens that cannot be filtered.
     */
    constructor(top_k: number, { filter_value, min_tokens_to_keep, }?: {
        filter_value?: number;
        min_tokens_to_keep?: number;
    });
    top_k: number;
    filter_value: number;
}
import { Tensor } from "../utils/tensor.js";
export {};
//# sourceMappingURL=logits_process.d.ts.map
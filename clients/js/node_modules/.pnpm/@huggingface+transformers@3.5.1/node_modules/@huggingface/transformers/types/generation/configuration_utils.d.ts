/**
 * Class that holds a configuration for a generation task.
 */
export class GenerationConfig {
    /**
     *
     * @param {GenerationConfig|import('../configs.js').PretrainedConfig} config
     */
    constructor(config: GenerationConfig | import("../configs.js").PretrainedConfig);
    /**
     * The maximum length the generated tokens can have.
     * Corresponds to the length of the input prompt + `max_new_tokens`.
     * Its effect is overridden by `max_new_tokens`, if also set.
     * @type {number}
     * @default 20
     */
    max_length: number;
    /**
     * The maximum numbers of tokens to generate, ignoring the number of tokens in the prompt.
     * @type {number}
     * @default null
     */
    max_new_tokens: number;
    /**
     * The minimum length of the sequence to be generated.
     * Corresponds to the length of the input prompt + `min_new_tokens`.
     * Its effect is overridden by `min_new_tokens`, if also set.
     * @type {number}
     * @default 0
     */
    min_length: number;
    /**
     * The minimum numbers of tokens to generate, ignoring the number of tokens in the prompt.
     * @type {number}
     * @default null
     */
    min_new_tokens: number;
    /**
     * Controls the stopping condition for beam-based methods, like beam-search. It accepts the following values:
     * - `true`, where the generation stops as soon as there are `num_beams` complete candidates;
     * - `false`, where an heuristic is applied and the generation stops when is it very unlikely to find better candidates;
     * - `"never"`, where the beam search procedure only stops when there cannot be better candidates (canonical beam search algorithm).
     * @type {boolean|"never"}
     * @default false
     */
    early_stopping: boolean | "never";
    /**
     * The maximum amount of time you allow the computation to run for in seconds.
     * Generation will still finish the current pass after allocated time has been passed.
     * @type {number}
     * @default null
     */
    max_time: number;
    /**
     * Whether or not to use sampling; use greedy decoding otherwise.
     * @type {boolean}
     * @default false
     */
    do_sample: boolean;
    /**
     * Number of beams for beam search. 1 means no beam search.
     * @type {number}
     * @default 1
     */
    num_beams: number;
    /**
     * Number of groups to divide `num_beams` into in order to ensure diversity among different groups of beams.
     * See [this paper](https://arxiv.org/pdf/1610.02424.pdf) for more details.
     * @type {number}
     * @default 1
     */
    num_beam_groups: number;
    /**
     * The values balance the model confidence and the degeneration penalty in contrastive search decoding.
     * @type {number}
     * @default null
     */
    penalty_alpha: number;
    /**
     * Whether or not the model should use the past last key/values attentions (if applicable to the model) to speed up decoding.
     * @type {boolean}
     * @default true
     */
    use_cache: boolean;
    /**
     * The value used to modulate the next token probabilities.
     * @type {number}
     * @default 1.0
     */
    temperature: number;
    /**
     * The number of highest probability vocabulary tokens to keep for top-k-filtering.
     * @type {number}
     * @default 50
     */
    top_k: number;
    /**
     * If set to float < 1, only the smallest set of most probable tokens with probabilities that add up to `top_p` or higher are kept for generation.
     * @type {number}
     * @default 1.0
     */
    top_p: number;
    /**
     * Local typicality measures how similar the conditional probability of predicting a target token next is to the expected conditional probability of predicting a random token next, given the partial text already generated.
     * If set to float < 1, the smallest set of the most locally typical tokens with probabilities that add up to `typical_p` or higher are kept for generation.
     * See [this paper](https://arxiv.org/pdf/2202.00666.pdf) for more details.
     * @type {number}
     * @default 1.0
     */
    typical_p: number;
    /**
     * If set to float strictly between 0 and 1, only tokens with a conditional probability greater than `epsilon_cutoff` will be sampled.
     * In the paper, suggested values range from 3e-4 to 9e-4, depending on the size of the model.
     * See [Truncation Sampling as Language Model Desmoothing](https://arxiv.org/abs/2210.15191) for more details.
     * @type {number}
     * @default 0.0
     */
    epsilon_cutoff: number;
    /**
     * Eta sampling is a hybrid of locally typical sampling and epsilon sampling.
     * If set to float strictly between 0 and 1, a token is only considered if it is greater than either `eta_cutoff` or `sqrt(eta_cutoff) * exp(-entropy(softmax(next_token_logits)))`.
     * The latter term is intuitively the expected next token probability, scaled by `sqrt(eta_cutoff)`. In the paper, suggested values range from 3e-4 to 2e-3, depending on the size of the model.
     * See [Truncation Sampling as Language Model Desmoothing](https://arxiv.org/abs/2210.15191) for more details.
     * @type {number}
     * @default 0.0
     */
    eta_cutoff: number;
    /**
     * This value is subtracted from a beam's score if it generates a token same as any beam from other group at a particular time.
     * Note that `diversity_penalty` is only effective if `group beam search` is enabled.
     * @type {number}
     * @default 0.0
     */
    diversity_penalty: number;
    /**
     * The parameter for repetition penalty. 1.0 means no penalty.
     * See [this paper](https://arxiv.org/pdf/1909.05858.pdf) for more details.
     * @type {number}
     * @default 1.0
     */
    repetition_penalty: number;
    /**
     * The paramater for encoder_repetition_penalty.
     * An exponential penalty on sequences that are not in the original input.
     * 1.0 means no penalty.
     * @type {number}
     * @default 1.0
     */
    encoder_repetition_penalty: number;
    /**
     * Exponential penalty to the length that is used with beam-based generation.
     * It is applied as an exponent to the sequence length, which in turn is used to divide the score of the sequence.
     * Since the score is the log likelihood of the sequence (i.e. negative), `length_penalty` > 0.0 promotes longer sequences, while `length_penalty` < 0.0 encourages shorter sequences.
     * @type {number}
     * @default 1.0
     */
    length_penalty: number;
    /**
     * If set to int > 0, all ngrams of that size can only occur once.
     * @type {number}
     * @default 0
     */
    no_repeat_ngram_size: number;
    /**
     * List of token ids that are not allowed to be generated.
     * In order to get the token ids of the words that should not appear in the generated text, use
     * `tokenizer(bad_words, { add_prefix_space: true, add_special_tokens: false }).input_ids`.
     * @type {number[][]}
     * @default null
     */
    bad_words_ids: number[][];
    /**
     * List of token ids that must be generated.
     * If given a `number[][]`, this is treated as a simple list of words that must be included, the opposite to `bad_words_ids`.
     * If given `number[][][]`, this triggers a [disjunctive constraint](https://github.com/huggingface/transformers/issues/14081), where one can allow different forms of each word.
     * @type {number[][]|number[][][]}
     * @default null
     */
    force_words_ids: number[][] | number[][][];
    /**
     * Whether to renormalize the logits after applying all the logits processors or warpers (including the custom ones).
     * It's highly recommended to set this flag to `true` as the search algorithms suppose the score logits are normalized but some logit processors or warpers break the normalization.
     * @type {boolean}
     * @default false
     */
    renormalize_logits: boolean;
    /**
     * Custom constraints that can be added to the generation to ensure that the output will contain the use of certain tokens as defined by `Constraint` objects, in the most sensible way possible.
     * @type {Object[]}
     * @default null
     */
    constraints: any[];
    /**
     * The id of the token to force as the first generated token after the `decoder_start_token_id`.
     * Useful for multilingual models like mBART where the first generated token needs to be the target language token.
     * @type {number}
     * @default null
     */
    forced_bos_token_id: number;
    /**
     * The id of the token to force as the last generated token when `max_length` is reached.
     * Optionally, use a list to set multiple *end-of-sequence* tokens.
     * @type {number|number[]}
     * @default null
     */
    forced_eos_token_id: number | number[];
    /**
     * Whether to remove possible *nan* and *inf* outputs of the model to prevent the generation method to crash. Note that using `remove_invalid_values` can slow down generation.
     * @type {boolean}
     */
    remove_invalid_values: boolean;
    /**
     * This Tuple adds an exponentially increasing length penalty, after a certain amount of tokens have been generated.
     * The tuple shall consist of: `(start_index, decay_factor)` where `start_index` indicates where penalty starts and `decay_factor` represents the factor of exponential decay.
     * @type {[number, number]}
     * @default null
     */
    exponential_decay_length_penalty: [number, number];
    /**
     * A list of tokens that will be suppressed at generation.
     * The `SuppressTokens` logit processor will set their log probs to `-inf` so that they are not sampled.
     * @type {number[]}
     * @default null
     */
    suppress_tokens: number[];
    /**
     * A streamer that will be used to stream the generation.
     * @type {import('./streamers.js').TextStreamer}
     * @default null
     */
    streamer: import("./streamers.js").TextStreamer;
    /**
     * A list of tokens that will be suppressed at the beginning of the generation.
     * The `SuppressBeginTokens` logit processor will set their log probs to `-inf` so that they are not sampled.
     * @type {number[]}
     * @default null
     */
    begin_suppress_tokens: number[];
    /**
     * A list of pairs of integers which indicates a mapping from generation indices to token indices that will be forced before sampling.
     * For example, `[[1, 123]]` means the second generated token will always be a token of index 123.
     * @type {[number, number][]}
     * @default null
     */
    forced_decoder_ids: [number, number][];
    /**
     * The guidance scale for classifier free guidance (CFG). CFG is enabled by setting `guidance_scale > 1`.
     * Higher guidance scale encourages the model to generate samples that are more closely linked to the input
     * prompt, usually at the expense of poorer quality.
     * @type {number}
     * @default null
     */
    guidance_scale: number;
    /**
     * The number of independently computed returned sequences for each element in the batch.
     * @type {number}
     * @default 1
     */
    num_return_sequences: number;
    /**
     * Whether or not to return the attentions tensors of all attention layers.
     * See `attentions` under returned tensors for more details.
     * @type {boolean}
     * @default false
     */
    output_attentions: boolean;
    /**
     * Whether or not to return the hidden states of all layers.
     * See `hidden_states` under returned tensors for more details.
     * @type {boolean}
     * @default false
     */
    output_hidden_states: boolean;
    /**
     * Whether or not to return the prediction scores.
     * See `scores` under returned tensors for more details.
     * @type {boolean}
     * @default false
     */
    output_scores: boolean;
    /**
     * Whether or not to return a `ModelOutput` instead of a plain tuple.
     * @type {boolean}
     * @default false
     */
    return_dict_in_generate: boolean;
    /**
     * The id of the *padding* token.
     * @type {number}
     * @default null
     */
    pad_token_id: number;
    /**
     * The id of the *beginning-of-sequence* token.
     * @type {number}
     * @default null
     */
    bos_token_id: number;
    /**
     * The id of the *end-of-sequence* token.
     * Optionally, use a list to set multiple *end-of-sequence* tokens.
     * @type {number|number[]}
     * @default null
     */
    eos_token_id: number | number[];
    /**
     * If set to int > 0, all ngrams of that size that occur in the `encoder_input_ids` cannot occur in the `decoder_input_ids`.
     * @type {number}
     * @default 0
     */
    encoder_no_repeat_ngram_size: number;
    /**
     * If an encoder-decoder model starts decoding with a different token than *bos*, the id of that token.
     * @type {number}
     * @default null
     */
    decoder_start_token_id: number;
    /**
     * Additional generation kwargs will be forwarded to the `generate` function of the model.
     * Kwargs that are not present in `generate`'s signature will be used in the model forward pass.
     * @type {Object}
     * @default {}
     */
    generation_kwargs: any;
}
//# sourceMappingURL=configuration_utils.d.ts.map
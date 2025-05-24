declare const StoppingCriteria_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 * Abstract base class for all stopping criteria that can be applied during generation.
 */
export class StoppingCriteria extends StoppingCriteria_base {
    /**
     *
     * @param {number[][]} input_ids (`number[][]` of shape `(batch_size, sequence_length)`):
     * Indices of input sequence tokens in the vocabulary.
     * @param {number[][]} scores scores (`number[][]` of shape `(batch_size, config.vocab_size)`):
     * Prediction scores of a language modeling head. These can be scores for each vocabulary token before SoftMax
     * or scores for each vocabulary token after SoftMax.
     * @returns {boolean[]} A list of booleans indicating whether each sequence should be stopped.
     */
    _call(input_ids: number[][], scores: number[][]): boolean[];
}
declare const StoppingCriteriaList_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 */
export class StoppingCriteriaList extends StoppingCriteriaList_base {
    criteria: any[];
    /**
     * Adds a new stopping criterion to the list.
     *
     * @param {StoppingCriteria} item The stopping criterion to add.
     */
    push(item: StoppingCriteria): void;
    /**
     * Adds multiple stopping criteria to the list.
     *
     * @param {StoppingCriteria|StoppingCriteriaList|StoppingCriteria[]} items The stopping criteria to add.
     */
    extend(items: StoppingCriteria | StoppingCriteriaList | StoppingCriteria[]): void;
    _call(input_ids: any, scores: any): any[];
    [Symbol.iterator](): ArrayIterator<any>;
}
/**
 * This class can be used to stop generation whenever the full generated number of tokens exceeds `max_length`.
 * Keep in mind for decoder-only type of transformers, this will include the initial prompted tokens.
 */
export class MaxLengthCriteria extends StoppingCriteria {
    /**
     *
     * @param {number} max_length The maximum length that the output sequence can have in number of tokens.
     * @param {number} [max_position_embeddings=null] The maximum model length, as defined by the model's `config.max_position_embeddings` attribute.
     */
    constructor(max_length: number, max_position_embeddings?: number);
    max_length: number;
    max_position_embeddings: number;
    _call(input_ids: any): any;
}
/**
 * This class can be used to stop generation whenever the "end-of-sequence" token is generated.
 * By default, it uses the `model.generation_config.eos_token_id`.
 */
export class EosTokenCriteria extends StoppingCriteria {
    /**
     *
     * @param {number|number[]} eos_token_id The id of the *end-of-sequence* token.
     * Optionally, use a list to set multiple *end-of-sequence* tokens.
     */
    constructor(eos_token_id: number | number[]);
    eos_token_id: number[];
}
/**
 * This class can be used to stop generation whenever the user interrupts the process.
 */
export class InterruptableStoppingCriteria extends StoppingCriteria {
    interrupted: boolean;
    interrupt(): void;
    reset(): void;
    _call(input_ids: any, scores: any): any[];
}
export {};
//# sourceMappingURL=stopping_criteria.d.ts.map
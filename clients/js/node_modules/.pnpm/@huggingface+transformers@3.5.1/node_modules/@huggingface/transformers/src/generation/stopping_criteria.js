
/**
 * @module generation/stopping_criteria
 */

import { Callable } from "../utils/generic.js";

// NOTE:
// Stopping Criteria returns a list of `batch_size` booleans, indicating whether each sequence in the batch should be stopped.

/**
 * Abstract base class for all stopping criteria that can be applied during generation.
 */
export class StoppingCriteria extends Callable {
    /**
     * 
     * @param {number[][]} input_ids (`number[][]` of shape `(batch_size, sequence_length)`):
     * Indices of input sequence tokens in the vocabulary.
     * @param {number[][]} scores scores (`number[][]` of shape `(batch_size, config.vocab_size)`):
     * Prediction scores of a language modeling head. These can be scores for each vocabulary token before SoftMax
     * or scores for each vocabulary token after SoftMax.
     * @returns {boolean[]} A list of booleans indicating whether each sequence should be stopped.
     */
    _call(input_ids, scores) {
        throw Error("StoppingCriteria needs to be subclassed");
    }
}
/**
 */
export class StoppingCriteriaList extends Callable {
    /**
     * Constructs a new instance of `StoppingCriteriaList`.
     */
    constructor() {
        super();
        this.criteria = [];
    }

    /**
     * Adds a new stopping criterion to the list.
     *
     * @param {StoppingCriteria} item The stopping criterion to add.
     */
    push(item) {
        this.criteria.push(item);
    }

    /**
     * Adds multiple stopping criteria to the list.
     *
     * @param {StoppingCriteria|StoppingCriteriaList|StoppingCriteria[]} items The stopping criteria to add.
     */
    extend(items) {
        if (items instanceof StoppingCriteriaList) {
            items = items.criteria;
        } else if (items instanceof StoppingCriteria) {
            items = [items];
        }
        this.criteria.push(...items);
    }

    _call(input_ids, scores) {
        const is_done = new Array(input_ids.length).fill(false);
        for (const criterion of this.criteria) {
            const criterion_done = criterion(input_ids, scores);
            for (let i = 0; i < is_done.length; ++i) {
                is_done[i] ||= criterion_done[i];
            }
        }
        return is_done;
    }

    [Symbol.iterator]() {
        return this.criteria.values();
    }
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
    constructor(max_length, max_position_embeddings = null) {
        super();
        this.max_length = max_length;
        this.max_position_embeddings = max_position_embeddings;
    }

    _call(input_ids) {
        return input_ids.map(ids => ids.length >= this.max_length);
    }
}

// TODO: add MaxTimeCriteria

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
    constructor(eos_token_id) {
        super();
        if (!Array.isArray(eos_token_id)) {
            eos_token_id = [eos_token_id];
        }
        this.eos_token_id = eos_token_id;
    }

    /**
     * 
     * @param {number[][]} input_ids 
     * @param {number[][]} scores 
     * @returns {boolean[]}
     */
    _call(input_ids, scores) {
        return input_ids.map(ids => {
            const last = ids.at(-1);
            // NOTE: We use == instead of === to allow for number/bigint comparison
            return this.eos_token_id.some(eos_id => last == eos_id);
        });
    }
}

/**
 * This class can be used to stop generation whenever the user interrupts the process.
 */
export class InterruptableStoppingCriteria extends StoppingCriteria {
    constructor() {
        super();
        this.interrupted = false;
    }

    interrupt() {
        this.interrupted = true;
    }

    reset() {
        this.interrupted = false;
    }

    _call(input_ids, scores) {
        return new Array(input_ids.length).fill(this.interrupted);
    }
}

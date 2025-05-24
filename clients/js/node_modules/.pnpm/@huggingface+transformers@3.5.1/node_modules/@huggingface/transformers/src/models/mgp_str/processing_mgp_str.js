import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { max, softmax } from "../../utils/maths.js";

const DECODE_TYPE_MAPPING = {
    'char': ['char_decode', 1],
    'bpe': ['bpe_decode', 2],
    'wp': ['wp_decode', 102],
}
export class MgpstrProcessor extends Processor {
    static tokenizer_class = AutoTokenizer
    static image_processor_class = AutoImageProcessor

    /**
     * @returns {import('../../tokenizers.js').MgpstrTokenizer} The character tokenizer.
     */
    get char_tokenizer() {
        return this.components.char_tokenizer;
    }

    /**
     * @returns {import('../../tokenizers.js').GPT2Tokenizer} The BPE tokenizer.
     */
    get bpe_tokenizer() {
        return this.components.bpe_tokenizer;
    }

    /**
     * @returns {import('../../tokenizers.js').BertTokenizer} The WordPiece tokenizer.
     */
    get wp_tokenizer() {
        return this.components.wp_tokenizer;
    }

    /**
     * Helper function to decode the model prediction logits.
     * @param {import('../../utils/tensor.js').Tensor} pred_logits Model prediction logits.
     * @param {string} format Type of model prediction. Must be one of ['char', 'bpe', 'wp'].
     * @returns {[string[], number[]]} The decoded sentences and their confidence scores.
     */
    _decode_helper(pred_logits, format) {
        if (!DECODE_TYPE_MAPPING.hasOwnProperty(format)) {
            throw new Error(`Format ${format} is not supported.`);
        }

        const [decoder_name, eos_token] = DECODE_TYPE_MAPPING[format];
        const decoder = this[decoder_name].bind(this);

        const [batch_size, batch_max_length] = pred_logits.dims;
        const conf_scores = [];
        const all_ids = [];

        /** @type {number[][][]} */
        const pred_logits_list = pred_logits.tolist();
        for (let i = 0; i < batch_size; ++i) {
            const logits = pred_logits_list[i];
            const ids = [];
            const scores = [];

            // Start and index=1 to skip the first token
            for (let j = 1; j < batch_max_length; ++j) {
                // NOTE: == to match bigint and number
                const [max_prob, max_prob_index] = max(softmax(logits[j]));
                scores.push(max_prob);
                if (max_prob_index == eos_token) {
                    break;
                }
                ids.push(max_prob_index);
            }

            const confidence_score = scores.length > 0
                ? scores.reduce((a, b) => a * b, 1)
                : 0;

            all_ids.push(ids);
            conf_scores.push(confidence_score);
        }

        const decoded = decoder(all_ids);
        return [decoded, conf_scores];
    }

    /**
     * Convert a list of lists of char token ids into a list of strings by calling char tokenizer.
     * @param {number[][]} sequences List of tokenized input ids.
     * @returns {string[]} The list of char decoded sentences.
     */
    char_decode(sequences) {
        return this.char_tokenizer.batch_decode(sequences).map(str => str.replaceAll(' ', ''));
    }

    /**
     * Convert a list of lists of BPE token ids into a list of strings by calling BPE tokenizer.
     * @param {number[][]} sequences List of tokenized input ids.
     * @returns {string[]} The list of BPE decoded sentences.
     */
    bpe_decode(sequences) {
        return this.bpe_tokenizer.batch_decode(sequences)
    }

    /**
     * Convert a list of lists of word piece token ids into a list of strings by calling word piece tokenizer.
     * @param {number[][]} sequences List of tokenized input ids.
     * @returns {string[]} The list of wp decoded sentences.
     */
    wp_decode(sequences) {
        return this.wp_tokenizer.batch_decode(sequences).map(str => str.replaceAll(' ', ''));
    }

    /**
     * Convert a list of lists of token ids into a list of strings by calling decode.
     * @param {import('../../utils/tensor.js').Tensor[]} sequences List of tokenized input ids.
     * @returns {{generated_text: string[], scores: number[], char_preds: string[], bpe_preds: string[], wp_preds: string[]}}
     * Dictionary of all the outputs of the decoded results.
     * - generated_text: The final results after fusion of char, bpe, and wp.
     * - scores: The final scores after fusion of char, bpe, and wp.
     * - char_preds: The list of character decoded sentences.
     * - bpe_preds: The list of BPE decoded sentences.
     * - wp_preds: The list of wp decoded sentences.
     */
    // @ts-expect-error The type of this method is not compatible with the one
    // in the base class. It might be a good idea to fix this.
    batch_decode([char_logits, bpe_logits, wp_logits]) {
        const [char_preds, char_scores] = this._decode_helper(char_logits, 'char');
        const [bpe_preds, bpe_scores] = this._decode_helper(bpe_logits, 'bpe');
        const [wp_preds, wp_scores] = this._decode_helper(wp_logits, 'wp');

        const generated_text = [];
        const scores = [];
        for (let i = 0; i < char_preds.length; ++i) {
            const [max_score, max_score_index] = max([char_scores[i], bpe_scores[i], wp_scores[i]]);
            generated_text.push([char_preds[i], bpe_preds[i], wp_preds[i]][max_score_index]);
            scores.push(max_score);
        }

        return {
            generated_text,
            scores,
            char_preds,
            bpe_preds,
            wp_preds,
        }
    }
    /** @type {typeof Processor.from_pretrained} */
    static async from_pretrained(...args) {
        const base = await super.from_pretrained(...args);

        // Load Transformers.js-compatible versions of the BPE and WordPiece tokenizers
        const bpe_tokenizer = await AutoTokenizer.from_pretrained("Xenova/gpt2") // openai-community/gpt2
        const wp_tokenizer = await AutoTokenizer.from_pretrained("Xenova/bert-base-uncased") // google-bert/bert-base-uncased

        // Update components
        base.components = {
            image_processor: base.image_processor,
            char_tokenizer: base.tokenizer,
            bpe_tokenizer: bpe_tokenizer,
            wp_tokenizer: wp_tokenizer,
        }
        return base;
    }

    async _call(images, text = null) {
        const result = await this.image_processor(images);

        if (text) {
            result.labels = this.tokenizer(text).input_ids
        }

        return result;
    }
}

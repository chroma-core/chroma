export class MgpstrProcessor extends Processor {
    static tokenizer_class: typeof AutoTokenizer;
    static image_processor_class: typeof AutoImageProcessor;
    /**
     * @returns {import('../../tokenizers.js').MgpstrTokenizer} The character tokenizer.
     */
    get char_tokenizer(): import("../../tokenizers.js").MgpstrTokenizer;
    /**
     * @returns {import('../../tokenizers.js').GPT2Tokenizer} The BPE tokenizer.
     */
    get bpe_tokenizer(): import("../../tokenizers.js").GPT2Tokenizer;
    /**
     * @returns {import('../../tokenizers.js').BertTokenizer} The WordPiece tokenizer.
     */
    get wp_tokenizer(): import("../../tokenizers.js").BertTokenizer;
    /**
     * Helper function to decode the model prediction logits.
     * @param {import('../../utils/tensor.js').Tensor} pred_logits Model prediction logits.
     * @param {string} format Type of model prediction. Must be one of ['char', 'bpe', 'wp'].
     * @returns {[string[], number[]]} The decoded sentences and their confidence scores.
     */
    _decode_helper(pred_logits: import("../../utils/tensor.js").Tensor, format: string): [string[], number[]];
    /**
     * Convert a list of lists of char token ids into a list of strings by calling char tokenizer.
     * @param {number[][]} sequences List of tokenized input ids.
     * @returns {string[]} The list of char decoded sentences.
     */
    char_decode(sequences: number[][]): string[];
    /**
     * Convert a list of lists of BPE token ids into a list of strings by calling BPE tokenizer.
     * @param {number[][]} sequences List of tokenized input ids.
     * @returns {string[]} The list of BPE decoded sentences.
     */
    bpe_decode(sequences: number[][]): string[];
    /**
     * Convert a list of lists of word piece token ids into a list of strings by calling word piece tokenizer.
     * @param {number[][]} sequences List of tokenized input ids.
     * @returns {string[]} The list of wp decoded sentences.
     */
    wp_decode(sequences: number[][]): string[];
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
    batch_decode([char_logits, bpe_logits, wp_logits]: import("../../utils/tensor.js").Tensor[]): {
        generated_text: string[];
        scores: number[];
        char_preds: string[];
        bpe_preds: string[];
        wp_preds: string[];
    };
    _call(images: any, text?: any): Promise<any>;
}
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
//# sourceMappingURL=processing_mgp_str.d.ts.map
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoFeatureExtractor } from "../auto/feature_extraction_auto.js";

export class SpeechT5Processor extends Processor {
    static tokenizer_class = AutoTokenizer
    static feature_extractor_class = AutoFeatureExtractor

    /**
     * Calls the feature_extractor function with the given input.
     * @param {any} input The input to extract features from.
     * @returns {Promise<any>} A Promise that resolves with the extracted features.
     */
    async _call(input) {
        return await this.feature_extractor(input)
    }
}

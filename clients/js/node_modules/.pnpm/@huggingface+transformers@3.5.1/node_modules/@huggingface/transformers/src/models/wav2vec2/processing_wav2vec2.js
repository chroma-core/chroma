import { AutoTokenizer } from "../../tokenizers.js";
import { AutoFeatureExtractor } from "../auto/feature_extraction_auto.js";
import { Processor } from "../../base/processing_utils.js";

export class Wav2Vec2Processor extends Processor {
    static tokenizer_class = AutoTokenizer
    static feature_extractor_class = AutoFeatureExtractor

    /**
     * Calls the feature_extractor function with the given audio input.
     * @param {any} audio The audio input to extract features from.
     * @returns {Promise<any>} A Promise that resolves with the extracted features.
     */
    async _call(audio) {
        return await this.feature_extractor(audio)
    }
}

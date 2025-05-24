/**
 * Represents a WhisperProcessor that extracts features from an audio input.
 */
export class WhisperProcessor extends Processor {
    static tokenizer_class: typeof AutoTokenizer;
    static feature_extractor_class: typeof AutoFeatureExtractor;
    /**
     * Calls the feature_extractor function with the given audio input.
     * @param {any} audio The audio input to extract features from.
     * @returns {Promise<any>} A Promise that resolves with the extracted features.
     */
    _call(audio: any): Promise<any>;
}
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoFeatureExtractor } from "../auto/feature_extraction_auto.js";
//# sourceMappingURL=processing_whisper.d.ts.map
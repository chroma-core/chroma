/**
 * Represents a UltravoxProcessor that extracts features from an audio input.
 */
export class UltravoxProcessor extends Processor {
    static tokenizer_class: typeof AutoTokenizer;
    static feature_extractor_class: typeof AutoFeatureExtractor;
    /**
     * @param {string} text The text input to process.
     * @param {Float32Array} audio The audio input to process.
     */
    _call(text: string, audio?: Float32Array, kwargs?: {}): Promise<any>;
}
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoFeatureExtractor } from "../auto/feature_extraction_auto.js";
//# sourceMappingURL=processing_ultravox.d.ts.map